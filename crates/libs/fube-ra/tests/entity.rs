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
fn generation_header(number: i64) -> GenerationHeader {
    GenerationHeader {
        number: GenerationNumber {
            generation: number,
            owner: admission().fm_source.id,
        },
        for_fm_replica: false,
    }
}

#[tokio::test]
async fn generation_proposals_bypass_readiness_and_preserve_receive_and_send() {
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    for node_open in [true, false] {
        let mut initial = admission();
        initial.owner_ready = false;
        initial.node_open = node_open;
        let executor =
            Executor::new(Arc::new(store.clone()), initial, Settings::default()).unwrap();
        for number in [9, 8, 9] {
            let reply = executor
                .process_generation_proposal(&admission().fm_source, &generation_header(number))
                .unwrap();
            assert_eq!(reply.proposed, generation_header(number).number);
            assert_eq!(reply.current, generation_header(9).number);
            assert_eq!(reply.error, if node_open { 0 } else { RA_NOT_READY });
        }
        let state = executor.generation_state();
        assert_eq!(state.proposed, generation_header(9).number);
        assert_eq!(state.receive, admission().generation);
        assert_eq!(state.send, admission().generation);
        assert!(!state.upload_pending);
        let mut foreign = admission().fm_source;
        foreign.instance += 1;
        assert!(executor
            .process_generation_proposal(&foreign, &generation_header(10))
            .is_err());
        let mut fmm = generation_header(10);
        fmm.for_fm_replica = true;
        assert!(matches!(
            executor.process_generation_proposal(&admission().fm_source, &fmm),
            Err(Failure::Unsupported(_))
        ));
        assert_eq!(executor.generation_state(), state);
        assert_eq!(
            executor
                .process_generation_update(
                    &admission().fm_source,
                    &GenerationUpdate {
                        fm_service_epoch: Epoch::default()
                    },
                    &generation_header(9),
                )
                .unwrap(),
            if node_open {
                GenerationUpdateOutcome::UploadRequired
            } else {
                GenerationUpdateOutcome::IgnoredNotReady
            }
        );
        assert_eq!(executor.generation_state().send, admission().generation);
        assert!(executor
            .seed_fresh_node_activation(NodeActivationInfo {
                activated: true,
                sequence: 99
            })
            .is_err());
        executor.close(WAIT).await.unwrap();
    }
    store.close().await.unwrap();
}

#[tokio::test]
async fn generation_update_rejects_mismatches_and_never_acks_upload_or_advances_send() {
    let (store, executor, _) = setup(WAIT).await;
    let source = admission().fm_source;
    let update = GenerationUpdate {
        fm_service_epoch: Epoch::default(),
    };
    assert_eq!(
        executor
            .process_generation_update(&source, &update, &generation_header(8))
            .unwrap(),
        GenerationUpdateOutcome::Rejected(admission().generation)
    );
    executor
        .process_generation_proposal(&source, &generation_header(8))
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            executor
                .process_generation_update(&source, &update, &generation_header(8))
                .unwrap(),
            GenerationUpdateOutcome::UploadRequired
        );
    }
    executor
        .process_generation_proposal(&source, &generation_header(10))
        .unwrap();
    assert_eq!(
        executor
            .process_generation_update(&source, &update, &generation_header(9))
            .unwrap(),
        GenerationUpdateOutcome::Rejected(generation_header(10).number)
    );
    assert_eq!(
        executor
            .process_generation_update(&source, &update, &generation_header(7))
            .unwrap(),
        GenerationUpdateOutcome::IgnoredStale
    );
    let state = executor.generation_state();
    assert_eq!(state.proposed, generation_header(10).number);
    assert_eq!(state.receive, generation_header(8).number);
    assert_eq!(state.send, admission().generation);
    assert!(state.upload_pending);
    assert!(executor.take_effects().is_empty());
    let ticket = executor.submit(add(body(1))).unwrap();
    assert_eq!(ticket.wait(WAIT).await.unwrap(), Disposition::IgnoredStale);
    assert!(executor.snapshot([1; 16]).committed.is_none());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn generation_update_does_not_publish_private_commits_and_queued_work_rechecks_receive() {
    for fail in [true, false] {
        let (store, executor, hooks) = setup(Duration::from_millis(40)).await;
        let (entered, release) = hooks.pause(fail);
        let first = executor.submit(add(body(1))).unwrap();
        entered.await.unwrap();
        let queued = executor.submit(add(body(1))).unwrap();
        executor
            .process_generation_proposal(&admission().fm_source, &generation_header(8))
            .unwrap();
        assert_eq!(
            executor
                .process_generation_update(
                    &admission().fm_source,
                    &GenerationUpdate {
                        fm_service_epoch: Epoch::default()
                    },
                    &generation_header(8),
                )
                .unwrap(),
            GenerationUpdateOutcome::UploadRequired
        );
        assert!(executor.snapshot([1; 16]).committed.is_none());
        assert!(executor.take_effects().is_empty());
        if !fail {
            assert_eq!(first.wait(WAIT).await, Err(Failure::Unknown));
            assert!(executor.snapshot([1; 16]).fenced);
            assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
        }
        release.send(()).unwrap();
        if fail {
            assert!(matches!(first.wait(WAIT).await, Err(Failure::Store(_))));
        } else {
            for _ in 0..300 {
                if matches!(first.status(), Status::Complete(_)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(first.wait(WAIT).await.unwrap(), Disposition::Applied);
        }
        assert_eq!(queued.wait(WAIT).await.unwrap(), Disposition::IgnoredStale);
        assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
        let effects = executor.take_effects();
        if fail {
            assert!(effects.is_empty());
            assert!(executor.snapshot([1; 16]).committed.is_none());
        } else {
            assert_eq!(effects.len(), 1);
            assert!(matches!(effects[0].effect, Effect::Open { .. }));
        }
        assert_eq!(executor.generation_state().send, admission().generation);
        assert!(executor.generation_state().upload_pending);
        finish(&store, &executor).await;
    }
}
#[tokio::test]
async fn generation_update_cannot_skip_the_local_fm_service_epoch_branch() {
    let (store, executor, _) = setup(WAIT).await;
    let mut request = body(1);
    request.unit.id.guid.0 = [0; 16];
    request.unit.id.guid.0[15] = 1;
    request.unit.consistency.id = request.unit.id.clone();
    executor
        .submit(add(request))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    executor.take_effects();
    executor
        .process_generation_proposal(&admission().fm_source, &generation_header(8))
        .unwrap();
    let before = executor.generation_state();
    assert!(matches!(
        executor.process_generation_update(
            &admission().fm_source,
            &GenerationUpdate {
                fm_service_epoch: Epoch {
                    configuration: 999,
                    data_loss: 9
                }
            },
            &generation_header(8),
        ),
        Err(Failure::Unsupported(_))
    ));
    assert_eq!(executor.generation_state(), before);
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
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

fn node_control(sequence: i64, activated: bool) -> Command {
    let body = NodeActivationRequest {
        sequence,
        from_fmm: false,
    };
    let source = admission().fm_source;
    let generation = GenerationHeader {
        number: admission().generation,
        for_fm_replica: false,
    };
    if activated {
        Command::NodeActivate {
            body,
            source,
            generation,
        }
    } else {
        Command::NodeDeactivate {
            body,
            source,
            generation,
        }
    }
}
fn node_service_update(service: Service) -> Command {
    Command::NodeUpdateService {
        body: Box::new(NodeUpdateServiceRequest { service }),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn fresh_node_activation_seed_cannot_replace_accepted_or_bootstrapped_intent() {
    for initially_seeded in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        if initially_seeded {
            executor
                .seed_fresh_node_activation(NodeActivationInfo {
                    activated: true,
                    sequence: 1,
                })
                .unwrap();
            assert!(matches!(
                executor.seed_fresh_node_activation(NodeActivationInfo {
                    activated: false,
                    sequence: 0
                }),
                Err(Failure::Unsupported(_))
            ));
        }
        run(&executor, node_control(2, false)).await;
        effects(&executor);
        assert!(matches!(
            executor.seed_fresh_node_activation(NodeActivationInfo {
                activated: true,
                sequence: 100
            }),
            Err(Failure::Unsupported(_))
        ));
        assert_eq!(
            executor.node_activation(),
            NodeActivationInfo {
                activated: false,
                sequence: 2
            }
        );
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn node_service_groups_bind_jobs_and_suppress_reply_after_definite_or_unknown_commit_failure()
{
    for unknown in [false, true] {
        let (store, executor, hooks) = setup(if unknown {
            Duration::from_millis(30)
        } else {
            WAIT
        })
        .await;
        let one = primary_body();
        let mut two = one.clone();
        two.unit.id.guid.0 = [2; 16];
        two.unit.consistency.id = two.unit.id.clone();
        if unknown {
            two.service.name = "fabric:/unrelated".into();
        }
        make_ready(&executor, &one).await;
        make_ready(&executor, &two).await;
        let mut service = one.service.clone();
        service.update_version = 1;
        let parent = executor
            .submit(node_service_update(service.clone()))
            .unwrap();
        parent.wait(WAIT).await.unwrap();
        effects(&executor);
        let mut forged = service.clone();
        forged.update_version = 99;
        let invalid = executor
            .submit(Command::UpdateServiceForNode {
                unit: one.unit.id.clone(),
                work: parent.id,
                body: Box::new(NodeUpdateServiceRequest { service: forged }),
            })
            .unwrap();
        assert!(matches!(
            invalid.wait(WAIT).await,
            Err(Failure::Admission(_))
        ));
        assert_eq!(state(&executor).body.service.update_version, 0);
        let (entered, release) = hooks.pause(true);
        let job = executor
            .submit(Command::UpdateServiceForNode {
                unit: one.unit.id.clone(),
                work: parent.id,
                body: Box::new(NodeUpdateServiceRequest {
                    service: service.clone(),
                }),
            })
            .unwrap();
        entered.await.unwrap();
        if unknown {
            assert_eq!(job.wait(WAIT).await, Err(Failure::Unknown));
            assert!(executor.snapshot([1; 16]).fenced);
        }
        assert!(effects(&executor).is_empty());
        release.send(()).unwrap();
        assert!(matches!(job.resolve(WAIT).await, Err(Failure::Store(_))));
        run(
            &executor,
            Command::UpdateServiceForNode {
                unit: two.unit.id.clone(),
                work: parent.id,
                body: Box::new(NodeUpdateServiceRequest {
                    service: service.clone(),
                }),
            },
        )
        .await;
        assert!(!effects(&executor)
            .iter()
            .any(|effect| matches!(effect, Effect::NodeUpdateServiceReply(_))));
        assert_eq!(
            executor
                .snapshot([2; 16])
                .committed
                .unwrap()
                .body
                .service
                .update_version,
            if unknown { 0 } else { 1 }
        );
        if !unknown {
            let retry = executor
                .submit(node_service_update(service.clone()))
                .unwrap();
            retry.wait(WAIT).await.unwrap();
            effects(&executor);
            for unit in [one.unit.id.clone(), two.unit.id.clone()] {
                run(
                    &executor,
                    Command::UpdateServiceForNode {
                        unit,
                        work: retry.id,
                        body: Box::new(NodeUpdateServiceRequest {
                            service: service.clone(),
                        }),
                    },
                )
                .await;
            }
            assert_eq!(
                effects(&executor)
                    .iter()
                    .filter(|effect| matches!(effect, Effect::NodeUpdateServiceReply(_)))
                    .count(),
                1
            );
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn node_deactivation_includes_private_creation_and_new_activation_cancels_queued_close() {
    let (store, executor, hooks) = setup(Duration::from_millis(30)).await;
    let (entered, release) = hooks.pause(false);
    let creating = executor.submit(primary_add(primary_body())).unwrap();
    entered.await.unwrap();
    assert_eq!(creating.wait(WAIT).await, Err(Failure::Unknown));
    run(&executor, node_control(2, false)).await;
    let scheduled = effects(&executor);
    assert!(scheduled.iter().any(|effect| matches!(effect, Effect::NodeDeactivateUnits { units, .. } if units == &vec![[1; 16]])));
    run(&executor, Command::CheckNodeDeactivation { sequence: 2 }).await;
    assert!(!effects(&executor).iter().any(|effect| matches!(
        effect,
        Effect::NodeDeactivateReply(_) | Effect::TerminateHost { .. }
    )));
    let closing = executor
        .submit(Command::CloseForNodeDeactivation {
            unit: primary_body().unit.id,
            sequence: 2,
        })
        .unwrap();
    run(&executor, node_control(3, true)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::NodeActivateReply(_)]
    ));
    release.send(()).unwrap();
    creating.resolve(WAIT).await.unwrap();
    assert_eq!(closing.wait(WAIT).await.unwrap(), Disposition::IgnoredStale);
    assert_eq!(state(&executor).phase, Phase::Opening);
    assert_eq!(executor.pending_node_deactivation(), None);
    assert!(!effects(&executor)
        .iter()
        .any(|effect| matches!(effect, Effect::NodeDeactivateReply(_))));
    run(&executor, Command::CheckNodeDeactivation { sequence: 2 }).await;
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn deactivation_monitor_uses_dynamic_deadline_deduplicates_hosts_and_waits_for_real_hostdown()
{
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
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
    let one = primary_body();
    let mut two = one.clone();
    two.unit.id.guid.0 = [2; 16];
    two.unit.consistency.id = two.unit.id.clone();
    make_ready(&executor, &one).await;
    make_ready(&executor, &two).await;
    run(&executor, node_control(5, false)).await;
    effects(&executor);
    for request in [&one, &two] {
        run(
            &executor,
            Command::CloseForNodeDeactivation {
                unit: request.unit.id.clone(),
                sequence: 5,
            },
        )
        .await;
    }
    effects(&executor);
    clock.set(CLOCK_ORIGIN + 9_000_000_000 - 1);
    run(&executor, Command::CheckNodeDeactivation { sequence: 5 }).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ArmNodeDeactivationCheck {
            after_ticks: 150_000_000,
            ..
        }]
    ));
    executor
        .config()
        .set_node(NodeProperty::DeactivationCloseTimeoutTicks, 10);
    executor
        .config()
        .set_node(NodeProperty::DeactivationCheckIntervalTicks, 25);
    run(&executor, Command::CheckNodeDeactivation { sequence: 5 }).await;
    let timed = effects(&executor);
    assert_eq!(timed.iter().filter(|effect| matches!(effect, Effect::TerminateHost { registration: r } if r.host == registration().host)).count(), 1);
    assert!(timed.iter().any(|effect| matches!(
        effect,
        Effect::ArmNodeDeactivationCheck {
            after_ticks: 25,
            ..
        }
    )));
    run(&executor, Command::CheckNodeDeactivation { sequence: 5 }).await;
    assert!(!effects(&executor).iter().any(|effect| matches!(
        effect,
        Effect::TerminateHost { .. } | Effect::NodeDeactivateReply(_)
    )));
    for request in [&one, &two] {
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id.clone(),
                host: registration().host,
            })),
        )
        .await;
    }
    assert_eq!(
        effects(&executor)
            .iter()
            .filter(|effect| matches!(effect, Effect::NodeDeactivateReply(_)))
            .count(),
        1
    );
    run(&executor, node_control(5, false)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::NodeDeactivateReply(body)] if body.sequence == 5)
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn deactivation_unknown_close_does_not_escape_reply_or_terminate_fenced_entity() {
    let (store, executor, hooks) = setup(Duration::from_millis(30)).await;
    make_ready(&executor, &primary_body()).await;
    executor
        .config()
        .set_node(NodeProperty::DeactivationCloseTimeoutTicks, 0);
    run(&executor, node_control(4, false)).await;
    effects(&executor);
    run(
        &executor,
        Command::CloseForNodeDeactivation {
            unit: primary_body().unit.id,
            sequence: 4,
        },
    )
    .await;
    effects(&executor);
    let (entered, release) = hooks.pause(false);
    let closed = executor
        .submit(Command::CompleteClose(Box::new(close_completion(
            &executor, 0,
        ))))
        .unwrap();
    entered.await.unwrap();
    assert_eq!(closed.wait(WAIT).await, Err(Failure::Unknown));
    run(&executor, Command::CheckNodeDeactivation { sequence: 4 }).await;
    assert!(!effects(&executor).iter().any(|effect| matches!(
        effect,
        Effect::TerminateHost { .. } | Effect::NodeDeactivateReply(_)
    )));
    release.send(()).unwrap();
    closed.resolve(WAIT).await.unwrap();
    assert_eq!(
        effects(&executor)
            .iter()
            .filter(|effect| matches!(effect, Effect::NodeDeactivateReply(_)))
            .count(),
        1
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_control_empty_inventory_and_service_completion_respect_effect_capacity() {
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_effects: 9,
            ..Settings::default()
        },
    )
    .unwrap();
    run(&executor, node_control(1, false)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::NodeDeactivateReply(_)]
    ));
    run(&executor, node_control(2, true)).await;
    effects(&executor);
    run(&executor, node_service_update(primary_body().service)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::NodeUpdateServiceReply(_)]
    ));
    make_ready(&executor, &primary_body()).await;
    let mut service = primary_body().service;
    service.update_version = 1;
    let parent = executor
        .submit(node_service_update(service.clone()))
        .unwrap();
    parent.wait(WAIT).await.unwrap();
    // The schedule occupies one slot and the group retains its reply slot.
    assert!(matches!(
        executor.submit(Command::UpdateServiceForNode {
            unit: primary_body().unit.id.clone(),
            work: parent.id,
            body: Box::new(NodeUpdateServiceRequest {
                service: service.clone()
            }),
        }),
        Err(Failure::Capacity)
    ));
    effects(&executor);
    run(
        &executor,
        Command::UpdateServiceForNode {
            unit: primary_body().unit.id,
            work: parent.id,
            body: Box::new(NodeUpdateServiceRequest { service }),
        },
    )
    .await;
    assert!(executor.statistics().effects <= 9);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_deactivation_waits_for_all_original_entities_and_stale_activation_cannot_reopen() {
    let (store, executor, _) = setup(WAIT).await;
    let one = primary_body();
    let mut two = primary_body();
    two.unit.id.guid.0 = [2; 16];
    two.unit.consistency.id = two.unit.id.clone();
    make_ready(&executor, &one).await;
    make_ready(&executor, &two).await;
    let control = |sequence, activate| {
        let body = NodeActivationRequest {
            sequence,
            from_fmm: false,
        };
        if activate {
            Command::NodeActivate {
                body,
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false,
                },
            }
        } else {
            Command::NodeDeactivate {
                body,
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false,
                },
            }
        }
    };
    run(&executor, control(8, false)).await;
    let actions = effects(&executor);
    let [Effect::NodeDeactivateUnits { sequence, units }, Effect::ArmNodeDeactivationCheck { .. }] =
        actions.as_slice()
    else {
        panic!("owned multi-entity close");
    };
    assert_eq!(units.len(), 2);
    for unit in units {
        executor
            .submit(Command::CloseForNodeDeactivation {
                unit: PartitionId {
                    guid: fube_transport::serialization::serde::Guid(*unit),
                },
                sequence: *sequence,
            })
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
    }
    let close_effects = effects(&executor);
    assert_eq!(
        close_effects
            .iter()
            .filter(|effect| matches!(effect, Effect::Close { flags: 0, .. }))
            .count(),
        2
    );
    for (index, request) in [&one, &two].into_iter().enumerate() {
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
        let replies = effects(&executor);
        assert_eq!(
            replies
                .iter()
                .filter(|effect| matches!(effect, Effect::NodeDeactivateReply(_)))
                .count(),
            index
        );
    }
    run(&executor, control(8, true)).await;
    assert!(!executor.node_activation().activated);
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::NodeActivateReply(body)] if body.sequence == 8)
    );
    run(&executor, control(9, true)).await;
    assert!(executor.node_activation().activated);
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::NodeActivateReply(_)]
    ));
    assert!(
        executor
            .snapshot(one.unit.id.guid.0)
            .committed
            .unwrap()
            .phase
            == Phase::Closed
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn node_service_update_reply_requires_all_issued_commits_and_not_rap_completion() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = primary_body();
    make_ready(&executor, &request).await;
    let mut service = request.service.clone();
    service.update_version += 1;
    let parent = executor
        .submit(Command::NodeUpdateService {
            body: Box::new(NodeUpdateServiceRequest {
                service: service.clone(),
            }),
            source: admission().fm_source,
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        })
        .unwrap();
    parent.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::NodeUpdateServiceUnits { .. }]
    ));
    let (entered, release) = hooks.pause(false);
    let update = executor
        .submit(Command::UpdateServiceForNode {
            unit: request.unit.id.clone(),
            work: parent.id,
            body: Box::new(NodeUpdateServiceRequest {
                service: service.clone(),
            }),
        })
        .unwrap();
    entered.await.unwrap();
    assert!(effects(&executor).is_empty());
    release.send(()).unwrap();
    update.wait(WAIT).await.unwrap();
    let applied = effects(&executor);
    assert!(applied.iter().any(|effect| matches!(effect, Effect::NodeUpdateServiceReply(body) if body.version == service.update_version)));
    assert!(state(&executor).service_description_pending);
    finish(&store, &executor).await;
}

#[test]
fn endpoint_publish_helper_waits_for_native_timeout_and_requires_reconfiguration() {
    let mut state = EndpointPublishState::default();
    let mut available = false;
    assert!(!state.endpoint_updated(9, 10, &mut available));
    assert!(state.validate(false, false, available).is_err());
    assert!(state.validate(true, true, available).is_err());
    assert_eq!(state.timer(9, 10, &mut available), (false, true));
    assert_eq!(state.timer(10, 10, &mut available), (true, false));
    assert_eq!(state.timer(11, 10, &mut available), (false, false));
    state.start_reconfiguration();
    assert!(
        available,
        "native StartReconfiguration does not reset external FM state"
    );
    assert!(state.validate(true, false, available).is_err());
    available = false; // The enclosing reconfiguration resets its own FM stage.
    assert!(state.endpoint_updated(10, 10, &mut available));
    state.acknowledge(&mut available);
    assert!(!available);
    assert!(!state.endpoint_updated(100, 10, &mut available));
    assert_eq!(state.timer(100, 10, &mut available), (false, false));
    state.clear(&mut available);
    state.start_reconfiguration();
    assert!(!state.endpoint_updated(1, 10, &mut available));
    state.clear(&mut available);
    assert_eq!(state.timer(100, 10, &mut available), (false, false));
}

#[tokio::test]
async fn local_stateful_idle_open_is_commit_gated_and_cannot_enable_primary_or_network_add() {
    let (store, executor, hooks) = setup(WAIT).await;
    let mut request = body(1);
    request.service.stateful = true;
    request.service.partitions = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.replica.current_role = 2;
    assert!(matches!(
        executor
            .submit(add(request.clone()))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    let mut primary = request.clone();
    primary.replica.current_role = 4;
    assert!(matches!(
        executor
            .submit(Command::OpenLocalStateful(Box::new(primary)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 0);

    let (entered, release) = hooks.pause(false);
    let open = executor
        .submit(Command::OpenLocalStateful(Box::new(request.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    open.wait(WAIT).await.unwrap();
    executor.take_effects();
    registered(&executor, &request, open.id).await;
    let normalized = (*executor.snapshot([1; 16]).committed.unwrap().body).clone();
    let mut result = complete(&normalized, open.id);
    result.replica.service_endpoint.clear();
    result.replica.replication_endpoint = "127.0.0.1:12345".into();
    let mut forged = result.clone();
    forged.replica.current_role = 4;
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(forged)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    let mut stale = result.clone();
    stale.replica.id -= 1;
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(stale)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    let (entered, release) = hooks.pause(false);
    let completion = executor
        .submit(Command::CompleteOpen(Box::new(result.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Opening
    );
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    completion.wait(WAIT).await.unwrap();
    let state = executor.snapshot([1; 16]).committed.unwrap();
    assert_eq!(
        (state.body.replica.current_role, state.body.replica.state),
        (2, 1)
    );
    assert_eq!(state.phase, Phase::Ready);
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::LocalStatefulOpenReply(_)]
    ));
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(result)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    executor
        .submit(Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        })
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    let closing = executor.snapshot([1; 16]).committed.unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Close { flags: 0, .. }]
    ));
    let closed = CloseCompletion {
        unit: closing.body.unit.clone(),
        replica: closing.body.replica.clone(),
        error: 0,
    };
    let mut foreign = closed.clone();
    foreign.replica.node.instance += 1;
    assert_eq!(
        executor
            .submit(Command::CompleteClose(Box::new(foreign)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    executor
        .submit(Command::CompleteClose(Box::new(closed.clone())))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    let final_effects = effects(&executor);
    assert!(final_effects.iter().any(|effect| matches!(
        effect,
        Effect::StatefulClosePending {
            mode: CloseMode::Close,
            ..
        }
    )));
    assert_eq!(
        final_effects
            .iter()
            .filter(|effect| matches!(effect, Effect::ReleaseUsage { .. }))
            .count(),
        1
    );
    assert_eq!(
        executor
            .submit(Command::CompleteClose(Box::new(closed)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    assert!(matches!(
        executor
            .submit(Command::OpenLocalStateful(Box::new(request)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn local_initial_primary_requires_exact_committed_bootstrap_and_cannot_promote_or_reopen() {
    let (store, executor, hooks) = setup(WAIT).await;
    let mut request = body(1);
    request.service.stateful = true;
    request.service.partitions = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.replica.current_role = 4;
    request.unit.current.configuration = (1 << 32) + 5;
    let mut previous = request.clone();
    previous.unit.previous = request.unit.current;
    assert!(matches!(
        executor
            .submit(Command::OpenLocalPrimary(Box::new(previous)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    assert!(matches!(
        executor
            .submit(add(request.clone()))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 0);
    let (entered, release) = hooks.pause(true);
    let vetoed = executor
        .submit(Command::OpenLocalPrimary(Box::new(request.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    assert!(matches!(vetoed.wait(WAIT).await, Err(Failure::Store(_))));
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());

    let (entered, release) = hooks.pause(false);
    let open = executor
        .submit(Command::OpenLocalPrimary(Box::new(request.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    open.wait(WAIT).await.unwrap();
    let issued = effects(&executor);
    assert!(matches!(issued.as_slice(), [Effect::Open { .. }]));
    registered(&executor, &request, open.id).await;
    let normalized = (*executor.snapshot([1; 16]).committed.unwrap().body).clone();
    let mut result = complete(&normalized, open.id);
    result.replica.replication_endpoint = "127.0.0.1:12345".into();
    for foreign in 0..6 {
        let mut event = result.clone();
        match foreign {
            0 => event.replica.node.instance += 1,
            1 => event.replica.id -= 1,
            2 => event.replica.instance -= 1,
            3 => event.unit.current.configuration += 1,
            4 => event.replica.current_role = 2,
            _ => event.replica.up = false,
        }
        assert_eq!(
            executor
                .submit(Command::CompleteOpen(Box::new(event)))
                .unwrap()
                .wait(WAIT)
                .await,
            Ok(Disposition::IgnoredStale)
        );
        assert!(executor.take_effects().is_empty());
    }
    for invalid in 0..4 {
        let mut event = result.clone();
        match invalid {
            0 => event.replica.service_endpoint.clear(),
            1 => event.replica.replication_endpoint.clear(),
            2 => event.replica.state = 2,
            _ => event.error = PROXY_STATE_CHANGED_ON_DATA_LOSS,
        }
        assert!(matches!(
            executor
                .submit(Command::CompleteOpen(Box::new(event)))
                .unwrap()
                .wait(WAIT)
                .await,
            Err(Failure::Admission(_))
        ));
        assert_eq!(
            executor.snapshot([1; 16]).committed.unwrap().phase,
            Phase::Opening
        );
        assert!(executor.take_effects().is_empty());
    }
    let (entered, release) = hooks.pause(false);
    let completion = executor
        .submit(Command::CompleteOpen(Box::new(result.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Opening
    );
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    completion.wait(WAIT).await.unwrap();
    let opened = executor.snapshot([1; 16]).committed.unwrap();
    assert_eq!(
        (opened.body.replica.current_role, opened.body.replica.state),
        (4, 2)
    );
    assert_eq!(
        opened.body.replica.service_endpoint,
        result.replica.service_endpoint
    );
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::LocalStatefulOpenReply(_)]
    ));
    let mut idle = request.clone();
    idle.replica.current_role = 2;
    assert!(matches!(
        executor
            .submit(Command::OpenLocalStateful(Box::new(idle)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    let mut updated = request.clone();
    updated.unit.current.configuration += 1;
    assert!(matches!(
        executor
            .submit(Command::OpenLocalPrimary(Box::new(updated)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    executor
        .submit(Command::OpenLocalPrimary(Box::new(request.clone())))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::LocalStatefulOpenReply(_)]
    ));
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        opened.usage
    );
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(result)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    executor
        .submit(Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        })
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Close { flags: 0, .. }]
    ));
    let closing = executor.snapshot([1; 16]).committed.unwrap();
    let closed = CloseCompletion {
        unit: closing.body.unit.clone(),
        replica: closing.body.replica.clone(),
        error: 0,
    };
    let mut wrong_role = closed.clone();
    wrong_role.replica.current_role = 2;
    assert_eq!(
        executor
            .submit(Command::CompleteClose(Box::new(wrong_role)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    let (entered, release) = hooks.pause(false);
    let close = executor
        .submit(Command::CompleteClose(Box::new(closed.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        opened.usage
    );
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    close.wait(WAIT).await.unwrap();
    let released = effects(&executor);
    assert_eq!(
        released
            .iter()
            .filter(|effect| matches!(effect, Effect::ReleaseUsage { .. }))
            .count(),
        1
    );
    assert!(released
        .iter()
        .any(|effect| matches!(effect, Effect::StatefulClosePending { .. })));
    assert_eq!(
        executor
            .snapshot([1; 16])
            .committed
            .unwrap()
            .body
            .replica
            .service_endpoint,
        ""
    );
    assert_eq!(
        executor
            .submit(Command::CompleteClose(Box::new(closed)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(executor.take_effects().is_empty());
    assert!(matches!(
        executor
            .submit(Command::OpenLocalPrimary(Box::new(request)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    finish(&store, &executor).await;
}

fn primary_add(request: AddPrimary) -> Command {
    Command::AddPrimary {
        body: Box::new(request),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn fm_add_primary_preserves_native_commit_retry_reply_and_closed_stale_semantics() {
    let (store, executor, hooks) = setup(WAIT).await;
    let mut request = body(1);
    request.service.stateful = true;
    request.service.partitions = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.replica.current_role = 4;
    request.unit.current.configuration = (1 << 32) + 5;
    let mut generation = admission().generation;
    generation.generation -= 1;
    assert_eq!(
        executor
            .submit(Command::AddPrimary {
                body: Box::new(request.clone()),
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: generation,
                    for_fm_replica: false
                },
            })
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    assert!(matches!(
        executor
            .submit(Command::AddPrimary {
                body: Box::new(request.clone()),
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: true
                },
            })
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    let mut foreign_source = admission().fm_source;
    foreign_source.instance += 1;
    assert!(matches!(
        executor
            .submit(Command::AddPrimary {
                body: Box::new(request.clone()),
                source: foreign_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false
                },
            })
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Admission(_))
    ));
    assert!(executor.snapshot([1; 16]).committed.is_none());
    let (entered, release) = hooks.pause(true);
    let vetoed = executor.submit(primary_add(request.clone())).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    assert!(matches!(vetoed.wait(WAIT).await, Err(Failure::Store(_))));
    let (entered, release) = hooks.pause(false);
    let accepted = executor.submit(primary_add(request.clone())).unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    accepted.wait(WAIT).await.unwrap();
    let opened = executor.snapshot([1; 16]).committed.unwrap();
    assert!(opened.fm_primary);
    assert_eq!(opened.primary_deactivation, Some((request.unit.current, 0)));
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Open { .. }]
    ));
    assert_eq!(
        executor
            .submit(primary_add(request.clone()))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredPending)
    );
    assert!(executor.take_effects().is_empty());
    registered(&executor, &request, accepted.id).await;
    let normalized = (*executor.snapshot([1; 16]).committed.unwrap().body).clone();
    let mut reply = complete(&normalized, accepted.id);
    reply.replica.replication_endpoint = "127.0.0.1:12345".into();
    let mut forged = reply.clone();
    forged.replica.current_role = 2;
    assert_eq!(
        executor
            .submit(Command::CompleteOpen(Box::new(forged)))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredStale)
    );
    let (entered, release) = hooks.pause(false);
    let completion = executor
        .submit(Command::CompleteOpen(Box::new(reply.clone())))
        .unwrap();
    tokio::time::timeout(WAIT, entered).await.unwrap().unwrap();
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().phase,
        Phase::Opening
    );
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    completion.wait(WAIT).await.unwrap();
    let effects_after = effects(&executor);
    assert!(matches!(
        effects_after.as_slice(),
        [Effect::PrimaryReply(_)]
    ));
    let ready = executor.snapshot([1; 16]).committed.unwrap();
    assert_eq!(
        (ready.body.replica.current_role, ready.body.replica.state),
        (4, 2)
    );
    assert_eq!(ready.primary_deactivation, Some((request.unit.current, 0)));
    // Native Ready duplicate replies from the existing FT, not the new request epoch.
    let mut duplicate = request.clone();
    duplicate.unit.current.configuration += 1;
    executor
        .submit(primary_add(duplicate))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::PrimaryReply(body)] if body.unit == ready.body.unit)
    );
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        ready.usage
    );
    assert!(matches!(
        executor
            .submit(Command::OpenLocalPrimary(Box::new(request.clone())))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Unsupported(_))
    ));
    executor
        .submit(Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        })
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    effects(&executor);
    let closing = executor.snapshot([1; 16]).committed.unwrap();
    executor
        .submit(Command::CompleteClose(Box::new(CloseCompletion {
            unit: closing.body.unit.clone(),
            replica: closing.body.replica.clone(),
            error: 0,
        })))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(
        effects(&executor)
            .iter()
            .filter(|effect| matches!(effect, Effect::ReleaseUsage { .. }))
            .count(),
        1
    );
    executor
        .submit(primary_add(request.clone()))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ReplicaUp(_)]
    ));
    let mut stale = request.clone();
    stale.replica.instance -= 1;
    executor
        .submit(primary_add(stale))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::ReplicaUp(_), Effect::PrimaryReply(body)] if body.error == STALE_REQUEST));
    request.replica.instance += 1;
    run(&executor, primary_add(request.clone())).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Open { .. }]
    ));
    assert_eq!(
        state(&executor).primary_deactivation,
        Some((request.unit.current, 0))
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_add_primary_unknown_completion_holds_original_reply_and_duplicate_until_resolution() {
    let (store, executor, hooks) = setup(Duration::from_millis(50)).await;
    let mut request = body(1);
    request.service.stateful = true;
    request.service.partitions = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.replica.current_role = 4;
    let accepted = executor.submit(primary_add(request.clone())).unwrap();
    accepted.wait(WAIT).await.unwrap();
    effects(&executor);
    registered(&executor, &request, accepted.id).await;
    let old = executor.snapshot([1; 16]).committed.unwrap();
    let normalized = (*old.body).clone();
    let mut reply = complete(&normalized, accepted.id);
    reply.replica.replication_endpoint = "127.0.0.1:12345".into();
    let (entered, release) = hooks.pause(false);
    let completed = executor
        .submit(Command::CompleteOpen(Box::new(reply)))
        .unwrap();
    entered.await.unwrap();
    assert_eq!(completed.wait(WAIT).await, Err(Failure::Unknown));
    let duplicate = executor.submit(primary_add(request)).unwrap();
    assert_eq!(duplicate.status(), Status::Pending);
    let held = executor.snapshot([1; 16]);
    assert!(held.fenced && Arc::ptr_eq(&old, &held.committed.unwrap()));
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    completed.resolve(WAIT).await.unwrap();
    duplicate.resolve(WAIT).await.unwrap();
    let replies = executor.take_effects();
    assert_eq!(replies.len(), 2);
    assert_eq!(
        (replies[0].operation, replies[1].operation),
        (completed.id, duplicate.id)
    );
    assert!(replies
        .iter()
        .all(|effect| matches!(effect.effect, Effect::PrimaryReply(ref body) if body.error == 0)));
    assert_eq!(
        executor.snapshot([1; 16]).committed.unwrap().usage,
        old.usage
    );
    finish(&store, &executor).await;
}

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
async fn pending_add_precedes_staleness_while_outer_profile_session_checks_remain() {
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    let first = executor.submit(add(request.clone())).unwrap();
    first.wait(WAIT).await.unwrap();
    executor.take_effects();
    let mut stale = request.clone();
    stale.unit.current.configuration -= 1;
    assert_eq!(
        executor.submit(add(stale)).unwrap().wait(WAIT).await,
        Ok(Disposition::IgnoredPending)
    );
    let mut stale_instance = request.clone();
    stale_instance.replica.instance -= 1;
    assert_eq!(
        executor
            .submit(add(stale_instance))
            .unwrap()
            .wait(WAIT)
            .await,
        Ok(Disposition::IgnoredPending)
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
    assert_eq!(
        executor.submit(add(replacement)).unwrap().wait(WAIT).await,
        Ok(Disposition::IgnoredPending)
    );
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
fn primary_delete(value: AddInstance, force: bool) -> Command {
    Command::DeleteReplica {
        body: Box::new(DeleteReplica::new(value, force)),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}
fn peer_source() -> NodeInstance {
    NodeInstance {
        id: [6; 16],
        instance: 40,
    }
}
fn peer_request() -> CreateReplica {
    let mut request = primary_body();
    request.replica.current_role = 2;
    let epoch = request.unit.current;
    CreateReplica::new(
        request,
        ReplicaDeactivationInfo {
            epoch,
            catchup_lsn: 0,
        },
    )
}
fn peer_create(body: CreateReplica) -> Command {
    Command::CreateReplica {
        body: Box::new(body),
        source: peer_source(),
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn peer_idle_open_reply_waits_for_both_commits_and_preserves_original_sender() {
    let (store, executor, hooks) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    assert!(executor.bind_peer(peer_source()).is_err());
    let request = peer_request();
    let (entered, release) = hooks.pause(false);
    let created = executor.submit(peer_create(request.clone())).unwrap();
    entered.await.unwrap();
    let duplicate = executor.submit(peer_create(request.clone())).unwrap();
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    created.wait(WAIT).await.unwrap();
    assert_eq!(
        duplicate.wait(WAIT).await.unwrap(),
        Disposition::IgnoredPending
    );
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Open { .. }]
    ));
    registered(&executor, &request.replica.0, created.id).await;
    let opened = state(&executor);
    let mut completion = complete(&opened.body, created.id);
    completion.replica.service_endpoint.clear();
    let mut missing_endpoint = completion.clone();
    missing_endpoint.replica.replication_endpoint.clear();
    assert!(matches!(
        executor
            .submit(Command::CompleteOpen(Box::new(missing_endpoint)))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Admission(_))
    ));
    completion.replica.replication_endpoint = "127.0.0.1:12345".into();
    let (entered, release) = hooks.pause(false);
    let completed = executor
        .submit(Command::CompleteOpen(Box::new(completion)))
        .unwrap();
    entered.await.unwrap();
    assert_eq!(state(&executor).phase, Phase::Opening);
    assert!(executor.take_effects().is_empty());
    release.send(()).unwrap();
    completed.wait(WAIT).await.unwrap();
    let reply = match effects(&executor).as_slice() {
        [Effect::PeerCreateReplicaReply { target, body }] => {
            assert_eq!(target, &peer_source());
            (**body).clone()
        }
        other => panic!("not the original peer reply: {other:?}"),
    };
    assert_eq!(
        (reply.replica.current_role, reply.replica.state, reply.error),
        (2, 1, 0)
    );
    assert!(reply.replica.service_endpoint.is_empty());
    assert!(!reply.replica.replication_endpoint.is_empty());
    let committed = state(&executor);
    let token = committed.open_token;
    let usage = committed.usage;
    let writes = hooks.calls.load(Ordering::SeqCst);
    for _ in 0..2 {
        run(&executor, peer_create(request.clone())).await;
        assert!(matches!(effects(&executor).as_slice(),
            [Effect::PeerCreateReplicaReply { body, .. }] if **body == reply));
    }
    let mut stale = request.clone();
    stale.replica.0.replica.instance -= 1;
    assert_eq!(
        run(&executor, peer_create(stale)).await,
        Disposition::IgnoredStale
    );
    assert!(executor.take_effects().is_empty());
    let mut stale_epoch = request;
    stale_epoch.replica.0.unit.current.configuration -= 1;
    stale_epoch.deactivation.epoch = stale_epoch.replica.0.unit.current;
    assert_eq!(
        run(&executor, peer_create(stale_epoch)).await,
        Disposition::IgnoredStale
    );
    assert!(executor.take_effects().is_empty());
    assert_eq!(state(&executor).open_token, token);
    assert_eq!(state(&executor).usage, usage);
    assert_eq!(hooks.calls.load(Ordering::SeqCst), writes);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn peer_idle_failed_or_unknown_open_commit_cannot_publish_reply_or_reopen() {
    for fail in [true, false] {
        let (store, executor, hooks) = setup(if fail {
            WAIT
        } else {
            Duration::from_millis(40)
        })
        .await;
        executor.bind_peer(peer_source()).unwrap();
        let request = peer_request();
        let created = executor.submit(peer_create(request.clone())).unwrap();
        created.wait(WAIT).await.unwrap();
        executor.take_effects();
        registered(&executor, &request.replica.0, created.id).await;
        let mut completion = complete(&state(&executor).body, created.id);
        completion.replica.service_endpoint.clear();
        completion.replica.replication_endpoint = "127.0.0.1:12345".into();
        let (entered, release) = hooks.pause(fail);
        let ticket = executor
            .submit(Command::CompleteOpen(Box::new(completion.clone())))
            .unwrap();
        entered.await.unwrap();
        let repeated = executor.submit(peer_create(request)).unwrap();
        if !fail {
            assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
            assert!(executor.snapshot([1; 16]).fenced);
        }
        assert_eq!(state(&executor).phase, Phase::Opening);
        assert!(executor.take_effects().is_empty());
        release.send(()).unwrap();
        if fail {
            assert!(matches!(ticket.wait(WAIT).await, Err(Failure::Store(_))));
            assert_eq!(
                repeated.wait(WAIT).await.unwrap(),
                Disposition::IgnoredPending
            );
            assert!(executor.take_effects().is_empty());
            run(&executor, Command::CompleteOpen(Box::new(completion))).await;
        } else {
            repeated.wait(WAIT).await.unwrap();
        }
        let effects = effects(&executor);
        assert!(effects
            .iter()
            .all(|effect| matches!(effect, Effect::PeerCreateReplicaReply { .. })));
        assert_eq!(effects.len(), if fail { 1 } else { 2 });
        assert_eq!(state(&executor).body.replica.current_role, 2);
        assert_eq!(state(&executor).body.replica.state, 1);
        assert_eq!(hooks.calls.load(Ordering::SeqCst), if fail { 3 } else { 2 });
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn peer_creation_rejects_unbound_foreign_primary_and_persisted_requests() {
    let (store, executor, hooks) = setup(WAIT).await;
    let unbound = Executor::new(Arc::new(store.clone()), admission(), Settings::default()).unwrap();
    assert!(matches!(
        unbound
            .submit(peer_create(peer_request()))
            .unwrap()
            .wait(WAIT)
            .await,
        Err(Failure::Admission(_))
    ));
    assert!(unbound.take_effects().is_empty());
    unbound.close(WAIT).await.unwrap();
    executor.bind_peer(peer_source()).unwrap();
    let request = peer_request();
    for invalid in [
        Command::CreateReplica {
            body: Box::new(request.clone()),
            source: admission().fm_source,
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        },
        Command::CreateReplica {
            body: Box::new(request.clone()),
            source: NodeInstance {
                instance: peer_source().instance - 1,
                ..peer_source()
            },
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        },
        Command::CreateReplica {
            body: Box::new(request.clone()),
            source: peer_source(),
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: true,
            },
        },
        peer_create({
            let mut value = request.clone();
            value.replica.0.replica.current_role = 4;
            value
        }),
        peer_create({
            let mut value = request;
            value.replica.0.service.persisted = true;
            value
        }),
    ] {
        assert!(executor.submit(invalid).unwrap().wait(WAIT).await.is_err());
    }
    let mut stale = admission().generation;
    stale.generation -= 1;
    assert_eq!(
        run(
            &executor,
            Command::CreateReplica {
                body: Box::new(peer_request()),
                source: peer_source(),
                generation: GenerationHeader {
                    number: stale,
                    for_fm_replica: false
                },
            }
        )
        .await,
        Disposition::IgnoredStale
    );
    assert!(executor.take_effects().is_empty());
    assert!(executor.snapshot([1; 16]).committed.is_none());
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 0);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn peer_terminal_registration_failure_keeps_error_reply_and_gated_fm_obligation() {
    let (store, executor, _) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    let request = peer_request();
    let created = executor.submit(peer_create(request.clone())).unwrap();
    created.wait(WAIT).await.unwrap();
    executor.take_effects();
    let lookup = *state(&executor).registration_lookups.keys().next().unwrap();
    run(
        &executor,
        Command::Registered(Box::new(RegistrationCompletion {
            unit: request.replica.0.unit.id.clone(),
            token: created.id,
            lookup,
            error: APPLICATION_INSTANCE_DELETED,
            ..RegistrationCompletion::default()
        })),
    )
    .await;
    let actions = effects(&executor);
    assert!(actions.iter().any(|effect| matches!(effect,
        Effect::PeerCreateReplicaReply { target, body }
            if target == &peer_source() && body.error == APPLICATION_INSTANCE_DELETED
    )));
    assert!(actions.iter().any(|effect| matches!(
        effect,
        Effect::StatefulClosePending {
            mode: CloseMode::Drop,
            ..
        }
    )));
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(state(&executor).usage.is_none());
    assert!(executor
        .submit(peer_create(request))
        .unwrap()
        .wait(WAIT)
        .await
        .is_err());
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn peer_selected_package_instance_does_not_change_original_request_or_repeat_open() {
    let (store, executor, hooks) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    let request = peer_request();
    let created = executor.submit(peer_create(request.clone())).unwrap();
    created.wait(WAIT).await.unwrap();
    executor.take_effects();
    let lookup = *state(&executor).registration_lookups.keys().next().unwrap();
    let mut selected = request.replica.0.service.version.clone();
    selected.instance += 1;
    let mut runtime = registration();
    runtime.acquisition = lookup;
    run(
        &executor,
        Command::Registered(Box::new(RegistrationCompletion {
            unit: request.replica.0.unit.id.clone(),
            token: created.id,
            lookup,
            registration: runtime,
            error: HOSTING_REGISTRATION_VERSION_MISMATCH,
            version: selected.clone(),
            usage_acquired: true,
            ..RegistrationCompletion::default()
        })),
    )
    .await;
    executor.take_effects();
    assert_eq!(
        state(&executor).requested_version,
        request.replica.0.service.version
    );
    assert_eq!(state(&executor).body.service.version, selected);
    let mut completion = complete(&state(&executor).body, created.id);
    completion.replica.service_endpoint.clear();
    completion.replica.replication_endpoint = "127.0.0.1:12345".into();
    run(&executor, Command::CompleteOpen(Box::new(completion))).await;
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::PeerCreateReplicaReply { body, .. }] if body.replica.version == selected
    ));
    let writes = hooks.calls.load(Ordering::SeqCst);
    run(&executor, peer_create(request)).await;
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::PeerCreateReplicaReply { body, .. }] if body.replica.version == selected
    ));
    assert_eq!(hooks.calls.load(Ordering::SeqCst), writes);
    assert_eq!(state(&executor).open_token, created.id);
    finish(&store, &executor).await;
}

#[tokio::test]
async fn primary_peer_open_observation_never_builds_or_changes_quorum_and_close_fences_late_reply()
{
    let (store, executor, _) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    make_ready(&executor, &primary_body()).await;
    let primary = state(&executor);
    let target = Replica {
        node: peer_source(),
        id: 41,
        instance: 51,
        current_role: 2,
        state: 1,
        version: primary.body.service.version.clone(),
        ..Replica::default()
    };
    let create = || Command::CreateRemoteIdle {
        unit: primary.body.unit.id.clone(),
        replica: target.clone(),
    };
    run(&executor, create()).await;
    let body = match effects(&executor).as_slice() {
        [Effect::PeerCreateReplica { target, body }] => {
            assert_eq!(target, &peer_source());
            (**body).clone()
        }
        other => panic!("missing owned peer request: {other:?}"),
    };
    assert_eq!(body.deactivation.epoch, primary.body.unit.current);
    run(&executor, create()).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::PeerCreateReplica { .. }]
    ));
    let mut reply = CreateReplicaReply {
        unit: body.replica.0.unit.clone(),
        replica: target,
        error: 0,
    };
    reply.replica.replication_endpoint = "127.0.0.1:12346".into();
    reply.replica.version.instance += 1;
    let ack = |body: CreateReplicaReply| Command::CreateReplicaReply {
        body: Box::new(body),
        source: peer_source(),
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    };
    let mut stale = reply.clone();
    stale.replica.instance -= 1;
    assert_eq!(run(&executor, ack(stale)).await, Disposition::IgnoredStale);
    assert!(state(&executor)
        .remote_idle
        .as_ref()
        .unwrap()
        .reply
        .is_none());
    run(&executor, ack(reply.clone())).await;
    assert_eq!(
        state(&executor)
            .remote_idle
            .as_ref()
            .unwrap()
            .reply
            .as_ref(),
        Some(&reply)
    );
    assert_eq!(state(&executor).body, primary.body);
    assert_eq!(
        state(&executor).primary_deactivation,
        primary.primary_deactivation
    );
    assert!(executor.take_effects().is_empty());
    assert_eq!(
        run(&executor, ack(reply.clone())).await,
        Disposition::IgnoredStale
    );
    run(
        &executor,
        Command::CloseLocal {
            unit: primary.body.unit.id.clone(),
            mode: CloseMode::Close,
        },
    )
    .await;
    executor.take_effects();
    assert!(state(&executor).remote_idle.is_none());
    assert_eq!(run(&executor, ack(reply)).await, Disposition::IgnoredStale);
    assert!(executor.take_effects().is_empty());
    finish(&store, &executor).await;
}

async fn opened_peer_primary() -> (Store, Executor, CreateReplicaReply) {
    let (store, executor, _) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    make_ready(&executor, &primary_body()).await;
    let primary = state(&executor);
    let mut replica = primary.body.replica.clone();
    replica.node = peer_source();
    replica.id += 1;
    replica.instance += 1;
    replica.current_role = 2;
    replica.state = 1;
    replica.service_endpoint.clear();
    replica.replication_endpoint.clear();
    run(
        &executor,
        Command::CreateRemoteIdle {
            unit: primary.body.unit.id.clone(),
            replica: replica.clone(),
        },
    )
    .await;
    effects(&executor);
    let pending = executor
        .submit(Command::BuildRemoteIdle(primary.body.unit.id.clone()))
        .unwrap();
    assert!(matches!(
        pending.wait(WAIT).await,
        Err(Failure::Unsupported(_))
    ));
    let premature = executor
        .submit(Command::RemoveRemoteIdle(primary.body.unit.id.clone()))
        .unwrap();
    assert!(matches!(
        premature.wait(WAIT).await,
        Err(Failure::Unsupported(_))
    ));
    replica.replication_endpoint = "127.0.0.1:12346".into();
    let reply = CreateReplicaReply {
        unit: primary.body.unit.clone(),
        replica,
        error: 0,
    };
    run(
        &executor,
        Command::CreateReplicaReply {
            body: Box::new(reply.clone()),
            source: peer_source(),
            generation: GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        },
    )
    .await;
    assert!(effects(&executor).is_empty());
    (store, executor, reply)
}

#[tokio::test]
async fn peer_build_is_explicit_correlated_and_never_grants_membership() {
    let (store, executor, remote) = opened_peer_primary().await;
    let primary = state(&executor);
    let build = || Command::BuildRemoteIdle(primary.body.unit.id.clone());
    run(&executor, build()).await;
    let first = effects(&executor);
    assert!(
        matches!(&first[..], [Effect::BuildIdle { body, replica, registration: runtime }]
        if body == &primary.body && **replica == remote.replica && Some(runtime) == primary.runtime.as_ref())
    );
    assert_eq!(
        state(&executor)
            .remote_idle
            .as_ref()
            .unwrap()
            .build
            .as_ref()
            .unwrap()
            .result,
        None
    );
    run(&executor, build()).await;
    assert_eq!(effects(&executor), first);
    let event = BuildCompletion {
        unit: primary.body.unit.clone(),
        local: primary.body.replica.clone(),
        remote: remote.replica.clone(),
        error: 0,
        detail: ProxyErrorDetail::default(),
    };
    for event in [
        {
            let mut e = event.clone();
            e.remote.instance += 1;
            e
        },
        {
            let mut e = event.clone();
            e.local.id += 1;
            e
        },
        {
            let mut e = event.clone();
            e.unit.current.configuration += 1;
            e
        },
        {
            let mut e = event.clone();
            e.remote.replication_endpoint = "foreign".into();
            e
        },
    ] {
        assert_eq!(
            run(&executor, Command::CompleteBuild(Box::new(event))).await,
            Disposition::IgnoredStale
        );
    }
    assert_eq!(
        state(&executor)
            .remote_idle
            .as_ref()
            .unwrap()
            .build
            .as_ref()
            .unwrap()
            .result,
        None
    );
    let mut progress = event.clone();
    progress.local.last_lsn = 12;
    run(&executor, Command::CompleteBuild(Box::new(progress))).await;
    let after = state(&executor);
    assert_eq!(
        after
            .remote_idle
            .as_ref()
            .unwrap()
            .build
            .as_ref()
            .unwrap()
            .result,
        Some(0)
    );
    assert_eq!(after.body, primary.body);
    assert_eq!(
        after
            .remote_idle
            .as_ref()
            .unwrap()
            .reply
            .as_ref()
            .unwrap()
            .replica
            .state,
        1
    );
    assert_eq!(run(&executor, build()).await, Disposition::IgnoredStale);
    assert_eq!(
        run(&executor, Command::CompleteBuild(Box::new(event))).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn peer_build_errors_retry_the_original_rap_obligation_and_close_fences_late_success() {
    for error in [0, -2_147_017_398] {
        let (store, executor, remote) = opened_peer_primary().await;
        let primary = state(&executor);
        run(
            &executor,
            Command::BuildRemoteIdle(primary.body.unit.id.clone()),
        )
        .await;
        let first = effects(&executor);
        let event = BuildCompletion {
            unit: primary.body.unit.clone(),
            local: primary.body.replica.clone(),
            remote: remote.replica,
            error,
            detail: ProxyErrorDetail {
                message: "original build error".into(),
                ..Default::default()
            },
        };
        if error != 0 {
            run(&executor, Command::CompleteBuild(Box::new(event.clone()))).await;
            let after = state(&executor);
            let build = after.remote_idle.as_ref().unwrap().build.as_ref().unwrap();
            assert_eq!(build.result, Some(error));
            assert_eq!(build.detail, event.detail);
            run(
                &executor,
                Command::BuildRemoteIdle(primary.body.unit.id.clone()),
            )
            .await;
            assert_eq!(effects(&executor), first);
            assert_eq!(
                state(&executor)
                    .remote_idle
                    .as_ref()
                    .unwrap()
                    .build
                    .as_ref()
                    .unwrap()
                    .result,
                None,
            );
            // A repeated failure does not consume a later native success.
            run(&executor, Command::CompleteBuild(Box::new(event.clone()))).await;
            let mut success = event.clone();
            success.error = 0;
            run(&executor, Command::CompleteBuild(Box::new(success))).await;
            assert_eq!(
                state(&executor)
                    .remote_idle
                    .as_ref()
                    .unwrap()
                    .build
                    .as_ref()
                    .unwrap()
                    .result,
                Some(0),
            );
        }
        run(
            &executor,
            Command::CloseLocal {
                unit: primary.body.unit.id.clone(),
                mode: CloseMode::Close,
            },
        )
        .await;
        effects(&executor);
        let mut late = event;
        late.error = 0;
        assert_eq!(
            run(&executor, Command::CompleteBuild(Box::new(late))).await,
            Disposition::IgnoredStale
        );
        assert!(state(&executor).remote_idle.is_none());
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn peer_idle_removal_is_terminal_for_unbuilt_pending_successful_and_failed_builds() {
    for (started, result) in [
        (false, None),
        (true, None),
        (true, Some(0)),
        (true, Some(-993)),
    ] {
        let (store, executor, remote) = opened_peer_primary().await;
        let primary = state(&executor);
        let build = || Command::BuildRemoteIdle(primary.body.unit.id.clone());
        let built = BuildCompletion {
            unit: primary.body.unit.clone(),
            local: primary.body.replica.clone(),
            remote: remote.replica.clone(),
            error: 0,
            detail: ProxyErrorDetail::default(),
        };
        assert_eq!(
            run(&executor, Command::CompleteRemoval(Box::new(built.clone()))).await,
            Disposition::IgnoredStale,
        );
        if started {
            run(&executor, build()).await;
            effects(&executor);
            if let Some(error) = result {
                let mut event = built.clone();
                event.error = error;
                run(&executor, Command::CompleteBuild(Box::new(event))).await;
            }
        }
        let remove = || Command::RemoveRemoteIdle(primary.body.unit.id.clone());
        run(&executor, remove()).await;
        let first = effects(&executor);
        let mut dropped = remote.replica.clone();
        dropped.state = 3;
        assert!(
            matches!(&first[..], [Effect::RemoveIdle { body, replica, registration }]
            if body == &primary.body && **replica == dropped
                && Some(registration) == primary.runtime.as_ref())
        );
        assert_eq!(state(&executor).body, primary.body);
        assert_eq!(
            state(&executor)
                .remote_idle
                .as_ref()
                .unwrap()
                .removal
                .as_ref()
                .unwrap()
                .result,
            None,
        );
        run(&executor, remove()).await;
        assert_eq!(effects(&executor), first);
        assert!(matches!(
            executor.submit(build()).unwrap().wait(WAIT).await,
            Err(Failure::Unsupported(_))
        ));
        assert_eq!(
            run(&executor, Command::CompleteBuild(Box::new(built.clone()))).await,
            Disposition::IgnoredStale,
        );
        let removed = RemovalCompletion {
            remote: dropped,
            ..built
        };
        for event in [
            {
                let mut e = removed.clone();
                e.remote.instance += 1;
                e
            },
            {
                let mut e = removed.clone();
                e.local.id += 1;
                e
            },
            {
                let mut e = removed.clone();
                e.unit.current.configuration += 1;
                e
            },
            {
                let mut e = removed.clone();
                e.remote.replication_endpoint = "foreign".into();
                e
            },
        ] {
            assert_eq!(
                run(&executor, Command::CompleteRemoval(Box::new(event))).await,
                Disposition::IgnoredStale,
            );
        }
        let mut failed = removed.clone();
        failed.error = -992;
        failed.detail.message = "partial removal requires retry".into();
        run(
            &executor,
            Command::CompleteRemoval(Box::new(failed.clone())),
        )
        .await;
        let after = state(&executor);
        let removal = after
            .remote_idle
            .as_ref()
            .unwrap()
            .removal
            .as_ref()
            .unwrap();
        assert_eq!(removal.result, Some(-992));
        assert_eq!(removal.detail, failed.detail);
        run(&executor, remove()).await;
        assert_eq!(effects(&executor), first);
        run(&executor, Command::CompleteRemoval(Box::new(failed))).await;
        let mut progress = removed.clone();
        progress.local.first_lsn = 12;
        progress.local.last_lsn = 14;
        run(&executor, Command::CompleteRemoval(Box::new(progress))).await;
        let after = state(&executor);
        assert_eq!(
            after
                .remote_idle
                .as_ref()
                .unwrap()
                .removal
                .as_ref()
                .unwrap()
                .result,
            Some(0)
        );
        assert_eq!(
            after.remote_idle.as_ref().unwrap().reply,
            Some(remote.clone())
        );
        assert_eq!(after.body, primary.body);
        assert_eq!(after.runtime, primary.runtime);
        assert_eq!(after.usage, primary.usage);
        assert_eq!(run(&executor, remove()).await, Disposition::IgnoredStale);
        assert_eq!(
            run(
                &executor,
                Command::CompleteRemoval(Box::new(removed.clone()))
            )
            .await,
            Disposition::IgnoredStale,
        );
        let mut original = remote.replica;
        original.replication_endpoint.clear();
        assert_eq!(
            run(
                &executor,
                Command::CreateRemoteIdle {
                    unit: primary.body.unit.id.clone(),
                    replica: original,
                }
            )
            .await,
            Disposition::IgnoredStale,
        );
        assert!(effects(&executor).is_empty());
        run(
            &executor,
            Command::CloseLocal {
                unit: primary.body.unit.id.clone(),
                mode: CloseMode::Close,
            },
        )
        .await;
        effects(&executor);
        assert_eq!(
            run(&executor, Command::CompleteRemoval(Box::new(removed))).await,
            Disposition::IgnoredStale,
        );
        assert!(state(&executor).remote_idle.is_none());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn primary_close_fences_pending_idle_removal_without_claiming_remote_disposal() {
    let (store, executor, remote) = opened_peer_primary().await;
    let primary = state(&executor);
    run(
        &executor,
        Command::RemoveRemoteIdle(primary.body.unit.id.clone()),
    )
    .await;
    effects(&executor);
    let mut replica = remote.replica;
    replica.state = 3;
    let completion = RemovalCompletion {
        unit: primary.body.unit.clone(),
        local: primary.body.replica.clone(),
        remote: replica,
        error: 0,
        detail: ProxyErrorDetail::default(),
    };
    run(
        &executor,
        Command::CloseLocal {
            unit: primary.body.unit.id.clone(),
            mode: CloseMode::Close,
        },
    )
    .await;
    let closing = state(&executor);
    assert_eq!(closing.phase, Phase::Closing);
    assert!(closing.remote_idle.is_none());
    effects(&executor);
    assert_eq!(
        run(&executor, Command::CompleteRemoval(Box::new(completion))).await,
        Disposition::IgnoredStale,
    );
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

fn fm_idle_request(primary: &EntityState) -> AddReplica {
    let mut body = (*primary.body).clone();
    body.replica.node = peer_source();
    body.replica.id += 1;
    body.replica.instance += 1;
    body.replica.current_role = 2;
    body.replica.state = 1;
    body.replica.service_endpoint.clear();
    body.replica.replication_endpoint.clear();
    body
}
fn fm_add_idle(body: AddReplica) -> Command {
    Command::AddReplica {
        body: Box::new(body),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}
fn fm_remove_idle(body: RemoveReplica) -> Command {
    Command::RemoveReplica {
        body: Box::new(body),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn fm_remove_idle_retries_failed_and_pending_builds_and_replays_only_semantic_success() {
    for outcome in [None, Some(-993), Some(0)] {
        let (store, executor, _) = setup(WAIT).await;
        executor.bind_peer(peer_source()).unwrap();
        assert!(executor.submit(fm_remove_idle(primary_body())).is_err());
        assert!(executor.entity_ids().is_empty());
        make_ready(&executor, &primary_body()).await;
        let primary = state(&executor);
        let mut request = fm_idle_request(&primary);
        assert!(executor
            .submit(fm_remove_idle(request.clone()))
            .unwrap()
            .wait(WAIT)
            .await
            .is_err());
        assert!(state(&executor).remote_idle.is_none());
        run(&executor, fm_add_idle(request.clone())).await;
        effects(&executor);
        let mut opened = CreateReplicaReply {
            unit: request.unit.clone(),
            replica: request.replica.clone(),
            error: 0,
        };
        opened.replica.replication_endpoint = "127.0.0.1:12346".into();
        run(&executor, peer_opened(opened.clone())).await;
        effects(&executor);
        let built = BuildCompletion {
            unit: primary.body.unit.clone(),
            local: primary.body.replica.clone(),
            remote: opened.replica.clone(),
            error: 0,
            detail: ProxyErrorDetail::default(),
        };
        if let Some(error) = outcome {
            let mut completion = built.clone();
            completion.error = error;
            run(&executor, Command::CompleteBuild(Box::new(completion))).await;
            effects(&executor);
            if error == 0 {
                request.replica = opened.replica.clone();
                request.replica.state = 2;
            }
        }
        for command in [
            Command::RemoveReplica {
                body: Box::new(request.clone()),
                source: peer_source(),
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false,
                },
            },
            Command::RemoveReplica {
                body: Box::new(request.clone()),
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: true,
                },
            },
            {
                let mut foreign = request.clone();
                foreign.replica.instance += 1;
                fm_remove_idle(foreign)
            },
            {
                let mut foreign = request.clone();
                foreign.unit.current.configuration += 1;
                fm_remove_idle(foreign)
            },
            {
                let mut foreign = request.clone();
                foreign.replica.replication_endpoint = "foreign".into();
                fm_remove_idle(foreign)
            },
        ] {
            assert!(executor.submit(command).unwrap().wait(WAIT).await.is_err());
        }
        assert!(effects(&executor).is_empty());
        run(&executor, fm_remove_idle(request.clone())).await;
        let first = effects(&executor);
        let mut dropped = opened.replica.clone();
        dropped.state = 3;
        assert!(
            matches!(&first[..], [Effect::RemoveIdle { body, replica, registration }]
                if body == &primary.body && **replica == dropped
                    && Some(registration) == primary.runtime.as_ref())
        );
        assert!(!state(&executor).remote_idle.as_ref().unwrap().ready);
        assert!(state(&executor)
            .remote_idle
            .as_ref()
            .unwrap()
            .add_reply()
            .is_none());
        run(&executor, fm_remove_idle(request.clone())).await;
        assert_eq!(effects(&executor), first);
        let mut changed = request.clone();
        changed.replica.state = if request.replica.state == 1 { 2 } else { 1 };
        assert!(executor
            .submit(fm_remove_idle(changed))
            .unwrap()
            .wait(WAIT)
            .await
            .is_err());
        assert_eq!(
            state(&executor)
                .remote_idle
                .as_ref()
                .unwrap()
                .removal
                .as_ref()
                .unwrap()
                .fm_request
                .as_deref(),
            Some(&request)
        );
        assert_eq!(
            run(&executor, Command::CompleteBuild(Box::new(built.clone()))).await,
            Disposition::IgnoredStale
        );
        assert_eq!(
            run(&executor, fm_add_idle(fm_idle_request(&primary))).await,
            Disposition::IgnoredStale
        );
        let mut completion = RemovalCompletion {
            remote: dropped.clone(),
            ..built
        };
        for field in 0..3 {
            let mut foreign = completion.clone();
            match field {
                0 => foreign.remote.instance += 1,
                1 => foreign.local.id += 1,
                _ => foreign.unit.current.configuration += 1,
            }
            assert_eq!(
                run(&executor, Command::CompleteRemoval(Box::new(foreign))).await,
                Disposition::IgnoredStale
            );
        }
        completion.error = -992;
        completion.detail.message = "retry native cancellation/removal".into();
        run(
            &executor,
            Command::CompleteRemoval(Box::new(completion.clone())),
        )
        .await;
        assert!(effects(&executor).is_empty());
        assert_eq!(
            state(&executor)
                .remote_idle
                .as_ref()
                .unwrap()
                .removal
                .as_ref()
                .unwrap()
                .result,
            Some(-992)
        );
        run(&executor, fm_remove_idle(request.clone())).await;
        assert_eq!(effects(&executor), first);
        completion.error = 0;
        completion.detail = ProxyErrorDetail::default();
        run(
            &executor,
            Command::CompleteRemoval(Box::new(completion.clone())),
        )
        .await;
        let reply = vec![Effect::RemoveReplicaReply(RemoveReplicaReply {
            unit: primary.body.unit.clone(),
            replica: dropped,
            error: 0,
        })];
        assert_eq!(effects(&executor), reply);
        run(&executor, fm_remove_idle(request.clone())).await;
        assert_eq!(effects(&executor), reply);
        assert_eq!(
            run(
                &executor,
                Command::CompleteRemoval(Box::new(completion.clone()))
            )
            .await,
            Disposition::IgnoredStale
        );
        assert_eq!(state(&executor).body, primary.body);
        assert_eq!(state(&executor).runtime, primary.runtime);
        run(
            &executor,
            Command::CloseLocal {
                unit: request.unit.id.clone(),
                mode: CloseMode::Close,
            },
        )
        .await;
        effects(&executor);
        assert_eq!(
            run(&executor, fm_remove_idle(request)).await,
            Disposition::IgnoredStale
        );
        assert_eq!(
            run(&executor, Command::CompleteRemoval(Box::new(completion))).await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}
fn peer_opened(body: CreateReplicaReply) -> Command {
    Command::CreateReplicaReply {
        body: Box::new(body),
        source: peer_source(),
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn fm_remove_in_create_echoes_request_without_rap_and_fences_late_open_and_build() {
    let (store, executor, _) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    make_ready(&executor, &primary_body()).await;
    let primary = state(&executor);
    let request = fm_idle_request(&primary);
    run(&executor, fm_add_idle(request.clone())).await;
    assert!(matches!(
        &effects(&executor)[..],
        [Effect::PeerCreateReplica { .. }]
    ));
    for field in 0..4 {
        let mut foreign = request.clone();
        match field {
            0 => foreign.replica.instance += 1,
            1 => foreign.replica.state = 2,
            2 => foreign.replica.replication_endpoint = "unobserved".into(),
            _ => foreign.unit.current.configuration += 1,
        }
        assert!(executor
            .submit(fm_remove_idle(foreign))
            .unwrap()
            .wait(WAIT)
            .await
            .is_err());
    }
    let mut stale = request.clone();
    stale.replica.instance -= 1;
    assert_eq!(
        run(&executor, fm_remove_idle(stale)).await,
        Disposition::IgnoredStale
    );
    assert!(executor
        .submit(Command::RemoveRemoteIdle(request.unit.id.clone()))
        .unwrap()
        .wait(WAIT)
        .await
        .is_err());
    assert!(effects(&executor).is_empty());
    run(&executor, fm_remove_idle(request.clone())).await;
    let reply = vec![Effect::RemoveReplicaReply(RemoveReplicaReply {
        unit: request.unit.clone(),
        replica: request.replica.clone(),
        error: 0,
    })];
    assert_eq!(effects(&executor), reply);
    let cancelled = state(&executor);
    let remote = cancelled.remote_idle.as_ref().unwrap();
    assert!(remote.reply.is_none() && remote.build.is_none() && !remote.ready);
    assert!(remote.add_reply().is_none());
    let removal = remote.removal.as_ref().unwrap();
    assert_eq!(removal.fm_request.as_deref(), Some(&request));
    assert_eq!(removal.result, Some(0));
    assert_eq!(removal.replica, request.replica);
    assert_eq!(cancelled.body, primary.body);
    assert_eq!(cancelled.runtime, primary.runtime);
    assert_eq!(cancelled.usage, primary.usage);
    run(&executor, fm_remove_idle(request.clone())).await;
    assert_eq!(effects(&executor), reply);
    for error in [0, APPLICATION_INSTANCE_DELETED] {
        let mut opened = CreateReplicaReply {
            unit: request.unit.clone(),
            replica: request.replica.clone(),
            error,
        };
        opened.replica.replication_endpoint = "127.0.0.1:12346".into();
        assert_eq!(
            run(&executor, peer_opened(opened)).await,
            Disposition::IgnoredStale
        );
    }
    let completion = BuildCompletion {
        unit: primary.body.unit.clone(),
        local: primary.body.replica.clone(),
        remote: request.replica.clone(),
        error: 0,
        detail: ProxyErrorDetail::default(),
    };
    for command in [
        fm_add_idle(request.clone()),
        Command::CompleteBuild(Box::new(completion.clone())),
        Command::CompleteRemoval(Box::new(completion)),
    ] {
        assert_eq!(run(&executor, command).await, Disposition::IgnoredStale);
    }
    assert!(executor
        .submit(Command::BuildRemoteIdle(request.unit.id.clone()))
        .unwrap()
        .wait(WAIT)
        .await
        .is_err());
    assert_eq!(state(&executor).remote_idle, cancelled.remote_idle);
    assert!(effects(&executor).is_empty());
    run(
        &executor,
        Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        },
    )
    .await;
    effects(&executor);
    assert_eq!(
        run(&executor, fm_remove_idle(request)).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_remove_unbuilt_idle_keeps_original_lifetime_and_close_fences_pending_reply() {
    let (store, executor, opened) = opened_peer_primary().await;
    let primary = state(&executor);
    let request = primary.remote_idle.as_ref().unwrap().body.replica.0.clone();
    run(&executor, fm_remove_idle(request.clone())).await;
    assert!(matches!(
        &effects(&executor)[..],
        [Effect::RemoveIdle { .. }]
    ));
    let mut dropped = opened.replica;
    dropped.state = 3;
    let completion = RemovalCompletion {
        unit: primary.body.unit.clone(),
        local: primary.body.replica.clone(),
        remote: dropped,
        error: 0,
        detail: ProxyErrorDetail::default(),
    };
    run(
        &executor,
        Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        },
    )
    .await;
    effects(&executor);
    assert_eq!(
        run(&executor, Command::CompleteRemoval(Box::new(completion))).await,
        Disposition::IgnoredStale
    );
    assert_eq!(
        run(&executor, fm_remove_idle(request)).await,
        Disposition::IgnoredStale
    );
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_add_idle_retries_creation_build_and_ready_reply_without_granting_membership() {
    let (store, executor, _) = setup(WAIT).await;
    executor.bind_peer(peer_source()).unwrap();
    make_ready(&executor, &primary_body()).await;
    let primary = state(&executor);
    let request = fm_idle_request(&primary);
    run(&executor, fm_add_idle(request.clone())).await;
    let create = effects(&executor);
    assert!(
        matches!(&create[..], [Effect::PeerCreateReplica { target, body }]
        if target == &peer_source() && body.replica.0 == request)
    );
    let creating = state(&executor);
    let remote = creating.remote_idle.as_ref().unwrap();
    assert_eq!(remote.fm_request.as_deref(), Some(&request));
    assert!(remote.reply.is_none() && remote.build.is_none() && !remote.ready);
    run(&executor, fm_add_idle(request.clone())).await;
    assert_eq!(effects(&executor), create);
    let mut opened = CreateReplicaReply {
        unit: request.unit.clone(),
        replica: request.replica.clone(),
        error: 0,
    };
    opened.replica.replication_endpoint = "127.0.0.1:12346".into();
    let mut foreign = opened.clone();
    foreign.replica.instance += 1;
    assert_eq!(
        run(&executor, peer_opened(foreign)).await,
        Disposition::IgnoredStale
    );
    run(&executor, peer_opened(opened.clone())).await;
    let build = effects(&executor);
    assert!(
        matches!(&build[..], [Effect::BuildIdle { body, replica, registration }]
        if body == &primary.body && **replica == opened.replica
            && Some(registration) == primary.runtime.as_ref())
    );
    run(&executor, fm_add_idle(request.clone())).await;
    assert_eq!(effects(&executor), build);
    let mut completion = BuildCompletion {
        unit: primary.body.unit.clone(),
        local: primary.body.replica.clone(),
        remote: opened.replica.clone(),
        error: -993,
        detail: ProxyErrorDetail::default(),
    };
    run(
        &executor,
        Command::CompleteBuild(Box::new(completion.clone())),
    )
    .await;
    assert!(effects(&executor).is_empty()); // Native build errors do not finish FM AddReplica.
    assert!(!state(&executor).remote_idle.as_ref().unwrap().ready);
    run(&executor, fm_add_idle(request.clone())).await;
    assert_eq!(effects(&executor), build);
    completion.error = 0;
    completion.local.last_lsn = 12;
    run(
        &executor,
        Command::CompleteBuild(Box::new(completion.clone())),
    )
    .await;
    let mut ready = opened.clone();
    ready.replica.state = 2;
    let reply = vec![Effect::AddReplicaReply(ready)];
    assert_eq!(effects(&executor), reply);
    let built = state(&executor);
    assert!(built.remote_idle.as_ref().unwrap().ready);
    assert_eq!(
        built.remote_idle.as_ref().unwrap().reply.as_ref(),
        Some(&opened)
    );
    assert_eq!(built.body, primary.body);
    assert_eq!(built.runtime, primary.runtime);
    run(&executor, fm_add_idle(request.clone())).await;
    assert_eq!(effects(&executor), reply);
    run(&executor, peer_opened(opened)).await;
    assert_eq!(effects(&executor), reply);
    run(
        &executor,
        Command::RemoveRemoteIdle(request.unit.id.clone()),
    )
    .await;
    assert!(matches!(
        &effects(&executor)[..],
        [Effect::RemoveIdle { .. }]
    ));
    assert_eq!(
        run(&executor, fm_add_idle(request)).await,
        Disposition::IgnoredStale
    );
    assert_eq!(
        run(&executor, Command::CompleteBuild(Box::new(completion))).await,
        Disposition::IgnoredStale
    );
    assert!(state(&executor)
        .remote_idle
        .as_ref()
        .unwrap()
        .add_reply()
        .is_none());
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_add_idle_admission_open_failure_and_close_never_fabricate_ready() {
    for close in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        executor.bind_peer(peer_source()).unwrap();
        let absent = primary_body();
        assert!(executor.submit(fm_add_idle(absent)).is_err());
        assert!(executor.entity_ids().is_empty());
        make_ready(&executor, &primary_body()).await;
        let primary = state(&executor);
        let request = fm_idle_request(&primary);
        for command in [
            Command::AddReplica {
                body: Box::new(request.clone()),
                source: peer_source(),
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: false,
                },
            },
            Command::AddReplica {
                body: Box::new(request.clone()),
                source: admission().fm_source,
                generation: GenerationHeader {
                    number: admission().generation,
                    for_fm_replica: true,
                },
            },
            {
                let mut foreign = request.clone();
                foreign.unit.current.configuration += 1;
                fm_add_idle(foreign)
            },
        ] {
            assert!(executor.submit(command).unwrap().wait(WAIT).await.is_err());
        }
        assert!(state(&executor).remote_idle.is_none());
        assert!(effects(&executor).is_empty());
        run(&executor, fm_add_idle(request.clone())).await;
        effects(&executor);
        let mut reply = CreateReplicaReply {
            unit: request.unit.clone(),
            replica: request.replica.clone(),
            error: APPLICATION_INSTANCE_DELETED,
        };
        if close {
            reply.error = 0;
            reply.replica.replication_endpoint = "127.0.0.1:12346".into();
            run(&executor, peer_opened(reply.clone())).await;
            effects(&executor);
            run(
                &executor,
                Command::CloseLocal {
                    unit: request.unit.id.clone(),
                    mode: CloseMode::Close,
                },
            )
            .await;
            effects(&executor);
            let event = BuildCompletion {
                unit: primary.body.unit.clone(),
                local: primary.body.replica.clone(),
                remote: reply.replica.clone(),
                error: 0,
                detail: ProxyErrorDetail::default(),
            };
            assert_eq!(
                run(&executor, Command::CompleteBuild(Box::new(event))).await,
                Disposition::IgnoredStale
            );
            assert_eq!(
                run(&executor, fm_add_idle(request)).await,
                Disposition::IgnoredStale
            );
            assert!(state(&executor).remote_idle.is_none());
        } else {
            run(&executor, peer_opened(reply.clone())).await;
            assert_eq!(effects(&executor), [Effect::AddReplicaReply(reply.clone())]);
            run(&executor, fm_add_idle(request.clone())).await;
            assert_eq!(effects(&executor), [Effect::AddReplicaReply(reply)]);
            assert!(!state(&executor).remote_idle.as_ref().unwrap().ready);
            assert!(executor
                .submit(fm_remove_idle(request.clone()))
                .unwrap()
                .wait(WAIT)
                .await
                .is_err());
            assert!(state(&executor)
                .remote_idle
                .as_ref()
                .unwrap()
                .removal
                .is_none());
            let mut replacement = request;
            replacement.replica.instance += 1;
            assert!(executor
                .submit(fm_add_idle(replacement))
                .unwrap()
                .wait(WAIT)
                .await
                .is_err());
        }
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}

fn primary_body() -> AddPrimary {
    let mut request = body(1);
    request.service.stateful = true;
    request.service.partitions = 1;
    request.service.target = 1;
    request.service.minimum = 1;
    request.replica.current_role = 4;
    request.unit.current.configuration = (1 << 32) + 5;
    request
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
    let first = executor
        .submit(if request.service.stateful {
            primary_add(request.clone())
        } else {
            add(request.clone())
        })
        .unwrap();
    first.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(executor).as_slice(),
        [Effect::Open { .. }]
    ));
    registered(executor, request, first.id).await;
    let mut completion = complete(request, first.id);
    if request.service.stateful {
        completion.unit = executor
            .snapshot(request.unit.id.guid.0)
            .committed
            .unwrap()
            .body
            .unit
            .clone();
        completion.replica.replication_endpoint = "127.0.0.1:12345".into();
    }
    run(executor, Command::CompleteOpen(Box::new(completion))).await;
    assert!(matches!(effects(executor).as_slice(),
        [Effect::Reply(r)] | [Effect::PrimaryReply(r)] if r.error==0));
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
async fn fm_stateful_terminal_reports_use_native_stages_and_ack_lists() {
    for mode in [
        CloseMode::AppHostDown,
        CloseMode::Close,
        CloseMode::Drop,
        CloseMode::Abort,
        CloseMode::ForceAbort,
    ] {
        let expected_stage = if matches!(
            mode,
            CloseMode::Drop | CloseMode::Abort | CloseMode::ForceAbort
        ) {
            FmStage::ReplicaDropped
        } else {
            FmStage::ReplicaDown
        };
        for in_dropped in [false, true] {
            let (store, executor, hooks) = setup(WAIT).await;
            let request = primary_body();
            make_ready(&executor, &request).await;
            let host = state(&executor).runtime.as_ref().unwrap().host.clone();
            if mode != CloseMode::AppHostDown {
                run(
                    &executor,
                    Command::CloseLocal {
                        unit: request.unit.id.clone(),
                        mode,
                    },
                )
                .await;
                assert_eq!(state(&executor).phase, Phase::Closing);
                assert!(state(&executor).usage.is_some());
                assert!(matches!(effects(&executor).as_slice(),
                [Effect::Close { flags, body, .. }]
                if *flags == match mode {
                    CloseMode::Drop => 8,
                    CloseMode::Abort | CloseMode::ForceAbort => 4,
                    _ => 0,
                }
                    && body.replica.current_role == 4));
            }
            let completion = if mode != CloseMode::AppHostDown {
                Command::CompleteClose(Box::new(close_completion(&executor, 0)))
            } else {
                Command::HostDown(Box::new(HostDown {
                    unit: request.unit.id.clone(),
                    host,
                }))
            };
            run(&executor, completion).await;
            let closed = state(&executor);
            assert_eq!(
                (closed.phase, closed.fm_stage),
                (Phase::Closed, expected_stage)
            );
            assert!(!closed.local_deleted && !closed.cleanup_pending);
            assert!(closed.runtime.is_none() && closed.usage.is_none());
            assert!(matches!(
                effects(&executor).as_slice(),
                [
                    Effect::ReleaseRegistration(_),
                    Effect::ReleaseUsage { .. },
                    Effect::RequestReportRetry,
                    Effect::Health(HealthEvent::Close)
                ]
            ));
            run(&executor, Command::RetryReport(request.unit.id.clone())).await;
            let Effect::ReplicaUp(report) = effects(&executor).remove(0) else {
                panic!("native report");
            };
            assert!(report.replicas.0.is_empty() && !report.last && !report.from_fmm);
            let info = &report.dropped.0[0];
            assert_eq!(
                info.replicas.0[0].description,
                Replica {
                    node: admission().local_node,
                    id: request.replica.id,
                    instance: request.replica.instance,
                    state: 3,
                    up: false,
                    ..Replica::default()
                }
            );
            assert!(!info.local_deleted && !info.report_from_primary);
            assert_eq!(info.intermediate, Epoch::default());
            let pending = state(&executor);
            for stale in 0..if expected_stage == FmStage::ReplicaDropped {
                6
            } else {
                5
            } {
                let mut invalid = (*report).clone();
                let info = &mut invalid.dropped.0[0];
                match stale {
                    0 => info.replicas.0[0].description.up = true,
                    1 => info.replicas.0[0].description.instance -= 1,
                    2 => info.replicas.0[0].description.node.instance += 1,
                    3 => info.unit.current.configuration -= 1,
                    4 => info.replicas.0[0].description.id += 1,
                    _ => info.replicas.0[0].description.state = 2,
                }
                assert_eq!(
                    run(&executor, ack(invalid, in_dropped)).await,
                    Disposition::IgnoredStale
                );
                assert!(Arc::ptr_eq(&pending, &state(&executor)));
                assert!(effects(&executor).is_empty());
            }
            run(&executor, ack((*report).clone(), in_dropped)).await;
            assert_eq!(
                (state(&executor).phase, state(&executor).fm_stage),
                (Phase::Closed, FmStage::None)
            );
            assert!(state(&executor).local_deleted && state(&executor).cleanup_pending);
            let terminal = effects(&executor);
            if in_dropped {
                assert!(terminal.is_empty()); // Closed ForceDelete has no semantic reply.
            } else {
                assert!(
                    matches!(terminal.as_slice(), [Effect::DeleteReplicaReply(body)]
                if body.error == 0 && body.replica == info.replicas.0[0].description)
                );
            }
            let writes = operations(&hooks).len();
            run(&executor, ack((*report).clone(), in_dropped)).await;
            assert_eq!(operations(&hooks).len(), writes);
            assert!(effects(&executor)
                .iter()
                .all(|effect| matches!(effect, Effect::DeleteReplicaReply(_))));
            assert_eq!(
                run(&executor, Command::RetryReport(request.unit.id.clone())).await,
                Disposition::IgnoredPending
            );
            assert!(effects(&executor).is_empty());
            run(&executor, primary_add(request)).await;
            assert!(effects(&executor).is_empty());
            finish(&store, &executor).await;
        }
    }
}

#[tokio::test]
async fn fm_stateful_terminal_and_ack_keep_private_proposals_until_actual_commit_resolution() {
    for mode in [
        CloseMode::AppHostDown,
        CloseMode::Close,
        CloseMode::Drop,
        CloseMode::Abort,
        CloseMode::ForceAbort,
    ] {
        let expected_stage = if matches!(
            mode,
            CloseMode::Drop | CloseMode::Abort | CloseMode::ForceAbort
        ) {
            FmStage::ReplicaDropped
        } else {
            FmStage::ReplicaDown
        };
        for ack_stage in [false, true] {
            for (unknown, fail) in [(false, true), (true, false), (true, true)] {
                let (store, executor, hooks) = setup(if unknown {
                    Duration::from_millis(50)
                } else {
                    WAIT
                })
                .await;
                let request = primary_body();
                make_ready(&executor, &request).await;
                let host = state(&executor).runtime.as_ref().unwrap().host.clone();
                if mode != CloseMode::AppHostDown {
                    run(
                        &executor,
                        Command::CloseLocal {
                            unit: request.unit.id.clone(),
                            mode,
                        },
                    )
                    .await;
                    assert!(matches!(
                        effects(&executor).as_slice(),
                        [Effect::Close { flags, .. }]
                        if *flags == match mode {
                            CloseMode::Drop => 8,
                            CloseMode::Abort | CloseMode::ForceAbort => 4,
                            _ => 0,
                        }
                    ));
                }
                let completion = close_completion(&executor, 0);
                let terminal = || {
                    if mode != CloseMode::AppHostDown {
                        Command::CompleteClose(Box::new(completion.clone()))
                    } else {
                        Command::HostDown(Box::new(HostDown {
                            unit: request.unit.id.clone(),
                            host: host.clone(),
                        }))
                    }
                };
                let report = if ack_stage {
                    run(&executor, terminal()).await;
                    effects(&executor);
                    run(&executor, Command::RetryReport(request.unit.id.clone())).await;
                    let Effect::ReplicaUp(report) = effects(&executor).remove(0) else {
                        panic!("report");
                    };
                    Some(report)
                } else {
                    None
                };
                let command = || match &report {
                    Some(report) => ack((**report).clone(), false),
                    None => terminal(),
                };
                let old = state(&executor);
                let writes = operations(&hooks).len();
                let (entered, release) = hooks.pause(fail);
                let original = executor.submit(command()).unwrap();
                entered.await.unwrap();
                assert!(Arc::ptr_eq(&old, &state(&executor)));
                assert!(effects(&executor).is_empty());
                let queued = if unknown && !fail {
                    Some(executor.submit(command()).unwrap())
                } else {
                    None
                };
                if unknown {
                    assert_eq!(original.wait(WAIT).await, Err(Failure::Unknown));
                    assert!(executor.snapshot([1; 16]).fenced);
                    if let Some(queued) = &queued {
                        assert_eq!(queued.status(), Status::Pending);
                    }
                    assert!(effects(&executor).is_empty());
                }
                assert_eq!(operations(&hooks).len(), writes + 1);
                release.send(()).unwrap();
                let result = original.resolve(WAIT).await;
                if fail {
                    assert!(matches!(result, Err(Failure::Store(_))));
                    assert!(Arc::ptr_eq(&old, &state(&executor)));
                    assert!(effects(&executor).is_empty());
                } else {
                    assert_eq!(result, Ok(Disposition::Applied));
                    queued.unwrap().resolve(WAIT).await.unwrap();
                    let actions = executor.take_effects();
                    assert_eq!(actions[0].operation, original.id);
                    if ack_stage {
                        assert!(state(&executor).local_deleted);
                        assert_eq!(state(&executor).fm_stage, FmStage::None);
                        assert!(actions
                            .iter()
                            .all(|effect| matches!(effect.effect, Effect::DeleteReplicaReply(_))));
                    } else {
                        assert_eq!(state(&executor).fm_stage, expected_stage);
                        assert_eq!(
                            actions
                                .iter()
                                .filter(|effect| matches!(
                                    effect.effect,
                                    Effect::ReleaseUsage { .. }
                                ))
                                .count(),
                            1
                        );
                        assert_eq!(
                            actions
                                .iter()
                                .filter(|effect| matches!(
                                    effect.effect,
                                    Effect::RequestReportRetry
                                ))
                                .count(),
                            1
                        );
                    }
                }
                finish(&store, &executor).await;
            }
        }
    }
}

#[tokio::test]
async fn fm_stateful_closed_add_primary_reports_without_relabeling_or_allocating() {
    let mode = CloseMode::ForceAbort;
    let (store, executor, _) = setup(WAIT).await;
    let request = primary_body();
    make_ready(&executor, &request).await;
    run(
        &executor,
        Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode,
        },
    )
    .await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Close { .. }]
    ));
    close_done(&executor, 0).await;
    assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
    assert!(!state(&executor).local_deleted);
    let retained = effects(&executor);
    assert!(retained
        .iter()
        .any(|effect| matches!(effect, Effect::RequestReportRetry | Effect::ReplicaUp(_))));
    let original = state(&executor);
    for stale in [false, true] {
        let mut incoming = request.clone();
        if stale {
            incoming.replica.instance -= 1;
        }
        run(&executor, primary_add(incoming.clone())).await;
        assert_eq!(state(&executor), original);
        let sent = effects(&executor);
        assert!(matches!(&sent[0], Effect::ReplicaUp(report)
            if report.dropped.0[0].replicas.0[0].description.instance == request.replica.instance));
        if stale {
            assert!(matches!(&sent[1], Effect::PrimaryReply(reply)
                if reply.error == STALE_REQUEST
                    && reply.replica.instance == incoming.replica.instance
                    && reply.unit == original.body.unit));
        } else {
            assert_eq!(sent.len(), 1);
        }
    }
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_stateful_drop_does_not_relabel_closing_or_closed_down_lifetime() {
    let (store, executor, _) = setup(WAIT).await;
    let request = primary_body();
    make_ready(&executor, &request).await;
    run(
        &executor,
        Command::CloseLocal {
            unit: request.unit.id.clone(),
            mode: CloseMode::Close,
        },
    )
    .await;
    effects(&executor);
    let closing = state(&executor);
    for mode in [CloseMode::Drop, CloseMode::Abort, CloseMode::ForceAbort] {
        assert_eq!(
            run(
                &executor,
                Command::CloseLocal {
                    unit: request.unit.id.clone(),
                    mode,
                }
            )
            .await,
            Disposition::IgnoredPending
        );
        assert_eq!(state(&executor), closing);
        assert!(effects(&executor).is_empty());
    }
    close_done(&executor, 0).await;
    effects(&executor);
    for reported in [false, true] {
        let closed = state(&executor);
        assert_eq!(closed.fm_stage, FmStage::ReplicaDown);
        assert_eq!(closed.report_sequence, u64::from(reported));
        for mode in [CloseMode::Drop, CloseMode::Abort, CloseMode::ForceAbort] {
            run(
                &executor,
                Command::CloseLocal {
                    unit: request.unit.id.clone(),
                    mode,
                },
            )
            .await;
            assert_eq!(state(&executor), closed);
            assert!(effects(&executor).is_empty());
        }
        if !reported {
            run(&executor, Command::RetryReport(request.unit.id.clone())).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::ReplicaUp(_)]
            ));
        }
    }
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_stateful_drop_ack_requires_dropped_state_before_first_retry() {
    for mode in [CloseMode::Drop, CloseMode::Abort, CloseMode::ForceAbort] {
        for in_dropped in [false, true] {
            let (store, executor, _) = setup(WAIT).await;
            let request = primary_body();
            make_ready(&executor, &request).await;
            run(
                &executor,
                Command::CloseLocal {
                    unit: request.unit.id.clone(),
                    mode,
                },
            )
            .await;
            effects(&executor);
            close_done(&executor, 0).await;
            effects(&executor);
            let current = state(&executor);
            assert_eq!(current.fm_stage, FmStage::ReplicaDropped);
            assert_eq!(current.report_sequence, 0);
            let report = ReplicaUp {
                dropped: fube_transport::serialization::serde::ObjectArray(vec![
                    FailoverUnitInfo {
                        service: current.body.service.clone(),
                        unit: current.body.unit.clone(),
                        replicas: fube_transport::serialization::serde::ObjectArray(vec![
                            ReplicaInfo {
                                description: Replica {
                                    node: request.replica.node.clone(),
                                    id: request.replica.id,
                                    instance: request.replica.instance,
                                    state: 3,
                                    up: false,
                                    ..Replica::default()
                                },
                                intermediate_role: 1,
                            },
                        ]),
                        ..FailoverUnitInfo::default()
                    },
                ]),
                ..ReplicaUp::default()
            };
            let mut stale = report.clone();
            stale.dropped.0[0].replicas.0[0].description.state = 2;
            assert_eq!(
                run(&executor, ack(stale, in_dropped)).await,
                Disposition::IgnoredStale
            );
            assert_eq!(state(&executor), current);
            assert!(effects(&executor).is_empty());
            run(&executor, ack(report, in_dropped)).await;
            assert!(state(&executor).local_deleted);
            assert_eq!(state(&executor).fm_stage, FmStage::None);
            let terminal = effects(&executor);
            if in_dropped {
                assert!(terminal.is_empty());
            } else {
                assert!(matches!(
                    terminal.as_slice(),
                    [Effect::DeleteReplicaReply(_)]
                ));
            }
            assert_eq!(
                run(&executor, Command::RetryReport(request.unit.id.clone())).await,
                Disposition::IgnoredPending
            );
            assert!(effects(&executor).is_empty());
            finish(&store, &executor).await;
        }
    }
}

#[tokio::test]
async fn native_stateful_delete_uses_original_epoch_id_and_canonical_reply_without_report_ack() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = primary_body();
    make_ready(&executor, &request).await;
    let old = state(&executor);
    for command in [remove(request.clone(), false)] {
        assert!(matches!(
            executor.submit(command).unwrap().wait(WAIT).await,
            Err(Failure::Unsupported(_))
        ));
    }
    let mut wrong_source = admission().fm_source;
    wrong_source.instance += 1;
    for (source, generation, expected) in [
        (
            wrong_source,
            GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
            false,
        ),
        (
            admission().fm_source,
            GenerationHeader {
                number: admission().generation,
                for_fm_replica: true,
            },
            false,
        ),
        (
            admission().fm_source,
            GenerationHeader {
                number: GenerationNumber {
                    generation: 6,
                    ..admission().generation
                },
                for_fm_replica: false,
            },
            true,
        ),
    ] {
        let result = executor
            .submit(Command::DeleteReplica {
                body: Box::new(DeleteReplica::new(request.clone(), false)),
                source,
                generation,
            })
            .unwrap()
            .wait(WAIT)
            .await;
        if expected {
            assert_eq!(result, Ok(Disposition::IgnoredStale));
        } else {
            assert!(matches!(
                result,
                Err(Failure::Admission(_) | Failure::Unsupported(_))
            ));
        }
    }
    for stale in 0..2 {
        let mut incoming = request.clone();
        if stale == 0 {
            incoming.unit.current.configuration -= 1;
        } else {
            incoming.replica.id += 1;
        }
        assert_eq!(
            run(&executor, primary_delete(incoming, false)).await,
            Disposition::IgnoredStale
        );
        assert!(Arc::ptr_eq(&old, &state(&executor)));
    }
    assert!(effects(&executor).is_empty());
    // Native DeleteLocalReplica matches ReplicaId, not the FM's instance: the
    // local replica may have restarted since FM observed it.
    let mut incoming = request.clone();
    incoming.replica.instance -= 1;
    run(&executor, primary_delete(incoming, false)).await;
    let closing = state(&executor);
    assert!(closing.local_deleted && !closing.cleanup_pending);
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::Close { flags: 8, body, .. }] if body.replica.current_role == 4));
    assert_eq!(
        run(&executor, primary_delete(request.clone(), false)).await,
        Disposition::IgnoredPending
    );
    assert!(effects(&executor).is_empty());
    let mut wrong_role = close_completion(&executor, 0);
    wrong_role.replica.current_role = 2;
    assert_eq!(
        run(&executor, Command::CompleteClose(Box::new(wrong_role))).await,
        Disposition::IgnoredStale
    );
    close_done(&executor, 0).await;
    let closed = state(&executor);
    assert_eq!(closed.phase, Phase::Closed);
    assert!(closed.local_deleted && closed.cleanup_pending);
    assert_eq!(closed.primary_deactivation, Some((Epoch::default(), -2)));
    assert!(closed.usage.is_none() && closed.runtime.is_none());
    let actions = effects(&executor);
    assert!(matches!(actions.as_slice(), [
        Effect::ReleaseRegistration(_), Effect::ReleaseUsage { .. },
        Effect::Health(HealthEvent::Close), Effect::DeleteReplicaReply(reply)
    ] if reply.replica == Replica {
        node: admission().local_node, id: request.replica.id, instance: request.replica.instance,
        up: false, state: 3, ..Replica::default()
    } && reply.unit == old.body.unit && reply.error == 0));
    let writes = operations(&hooks).len();
    run(&executor, primary_delete(request.clone(), false)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::DeleteReplicaReply(_)]
    ));
    assert_eq!(operations(&hooks).len(), writes);
    assert_eq!(close_done(&executor, 0).await, Disposition::IgnoredStale);
    run(&executor, primary_add(request)).await;
    assert!(effects(&executor).is_empty());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_stateful_delete_tombstones_update_identity_and_epoch_before_own_guards() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = primary_body();
    run(&executor, primary_delete(request.clone(), true)).await;
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::DeleteReplicaReply(reply)] if reply.replica.id == request.replica.id
            && reply.replica.instance == request.replica.instance));
    assert!(state(&executor).local_deleted && state(&executor).cleanup_pending);
    assert_eq!(
        state(&executor).primary_deactivation,
        Some((Epoch::default(), -2))
    );
    let mut next = request.clone();
    next.replica.id += 1;
    next.replica.instance += 1;
    next.unit.current.configuration += 1;
    run(&executor, primary_delete(next.clone(), false)).await;
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::DeleteReplicaReply(reply)] if reply.replica.id == next.replica.id
            && reply.replica.instance == next.replica.instance));
    let mut old_id = request.clone();
    old_id.unit.current.configuration = next.unit.current.configuration + 1;
    run(&executor, primary_delete(old_id, false)).await;
    assert!(effects(&executor).is_empty());
    assert_eq!(
        state(&executor).body.unit.current.configuration,
        next.unit.current.configuration + 1
    );
    assert_eq!(
        run(&executor, primary_delete(next.clone(), false)).await,
        Disposition::IgnoredStale
    );
    run(&executor, primary_delete(next, true)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::DeleteReplicaReply(_)]
    ));
    assert_eq!(operations(&hooks), ["Insert", "Update", "Update"]);
    assert!(state(&executor).runtime.is_none() && state(&executor).usage.is_none());
    finish(&store, &executor).await;
}

#[tokio::test]
async fn fm_stateful_force_delete_commits_before_termination_and_waits_for_original_host_down() {
    for (unknown, fail) in [(false, false), (false, true), (true, false), (true, true)] {
        let (store, executor, hooks) = setup(if unknown {
            Duration::from_millis(50)
        } else {
            WAIT
        })
        .await;
        let request = primary_body();
        make_ready(&executor, &request).await;
        let original = state(&executor);
        let mut forced = request.clone();
        forced.unit.current.configuration -= 1;
        let (entered, release) = hooks.pause(fail);
        let accepted = executor.submit(primary_delete(forced, true)).unwrap();
        entered.await.unwrap();
        assert!(Arc::ptr_eq(&original, &state(&executor)));
        assert!(effects(&executor).is_empty());
        if unknown {
            assert_eq!(accepted.wait(WAIT).await, Err(Failure::Unknown));
            assert!(executor.snapshot([1; 16]).fenced);
        }
        release.send(()).unwrap();
        let result = accepted.resolve(WAIT).await;
        if fail {
            assert!(matches!(result, Err(Failure::Store(_))));
            assert!(Arc::ptr_eq(&original, &state(&executor)));
            assert!(effects(&executor).is_empty());
        } else {
            assert_eq!(result, Ok(Disposition::Applied));
            assert!(matches!(effects(&executor).as_slice(),
                [Effect::TerminateHost { registration }] if registration == original.runtime.as_ref().unwrap()));
            assert_eq!(state(&executor).close_mode, Some(CloseMode::Obliterate));
            assert!(!state(&executor).local_deleted);
            if unknown {
                // P1 retires admission after resolving a timed-out operation.
                finish(&store, &executor).await;
                continue;
            }
            assert_eq!(close_done(&executor, 0).await, Disposition::IgnoredStale);
            run(&executor, Command::RetryClose(request.unit.id.clone())).await;
            assert!(effects(&executor).is_empty());
            run(
                &executor,
                Command::HostDown(Box::new(HostDown {
                    unit: request.unit.id.clone(),
                    host: original.runtime.as_ref().unwrap().host.clone(),
                })),
            )
            .await;
            assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
            assert!(state(&executor).usage.is_none());
            assert!(matches!(
                effects(&executor).as_slice(),
                [
                    Effect::ReleaseRegistration(_),
                    Effect::ReleaseUsage { .. },
                    Effect::RequestReportRetry,
                    Effect::Health(HealthEvent::Close)
                ]
            ));
            run(&executor, Command::RetryReport(request.unit.id.clone())).await;
            let Effect::ReplicaUp(report) = effects(&executor).remove(0) else {
                panic!("report");
            };
            run(&executor, ack(*report, false)).await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::DeleteReplicaReply(_)]
            ));
            assert!(state(&executor).local_deleted);
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn fm_stateful_fresh_recreation_resets_deactivation_and_fences_predecessor_completions() {
    let (store, executor, _) = setup(WAIT).await;
    let request = primary_body();
    make_ready(&executor, &request).await;
    let ready = state(&executor);
    run(&executor, primary_delete(request.clone(), false)).await;
    effects(&executor);
    let old_close = close_completion(&executor, 0);
    close_done(&executor, 0).await;
    effects(&executor);
    let mut next = request.clone();
    next.replica.id += 1;
    next.replica.instance += 1;
    next.unit.previous = request.unit.current;
    let created = executor.submit(primary_add(next.clone())).unwrap();
    assert_eq!(created.wait(WAIT).await, Ok(Disposition::Applied));
    assert!(matches!(effects(&executor).as_slice(),
        [Effect::Open { token, usage: None, .. }] if *token == created.id));
    let new = state(&executor);
    assert_eq!(new.primary_deactivation, Some((next.unit.current, 0)));
    assert_eq!(new.body.unit.previous, Epoch::default());
    assert_eq!(new.report_sequence, 0);
    assert!(!new.local_deleted && !new.cleanup_pending);
    assert_eq!(
        run(&executor, Command::CompleteClose(Box::new(old_close))).await,
        Disposition::IgnoredStale
    );
    assert_eq!(
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&ready.body, ready.open_token)))
        )
        .await,
        Disposition::IgnoredStale
    );
    assert_eq!(state(&executor), new);
    assert!(effects(&executor).is_empty());
    registered(&executor, &next, created.id).await;
    let opening = state(&executor);
    let mut completed = complete(&opening.body, created.id);
    completed.replica.replication_endpoint = "127.0.0.1:12345".into();
    run(&executor, Command::CompleteOpen(Box::new(completed))).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::PrimaryReply(reply)]
        if reply.replica.id == next.replica.id && reply.replica.instance == next.replica.instance)
    );
    assert_eq!(state(&executor).phase, Phase::Ready);
    finish(&store, &executor).await;
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
    for stateful in [false, true] {
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
        let request = if stateful { primary_body() } else { body(1) };
        let deletion = |body| {
            if stateful {
                primary_delete(body, false)
            } else {
                remove(body, false)
            }
        };
        make_ready(&executor, &request).await;
        run(&executor, deletion(request.clone())).await;
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
        let aborted = effects(&executor);
        if stateful {
            assert!(matches!(
                aborted.as_slice(),
                [Effect::Close { flags: 4, .. }]
            ));
        } else {
            assert_close(&aborted, 4, &state(&executor));
        }
        close_done(&executor, 0).await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [
                Effect::ReleaseRegistration(_),
                Effect::ReleaseUsage { .. },
                Effect::Health(HealthEvent::Close)
            ]
        ));
        if stateful {
            assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
            assert_eq!(
                run(&executor, Command::RetryReport(request.unit.id.clone())).await,
                Disposition::IgnoredPending
            );
            assert!(effects(&executor).is_empty());
        }
        run(&executor, deletion(request)).await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::RemoveReply(_)] | [Effect::DeleteReplicaReply(_)]
        ));
        finish(&store, &executor).await;
    }
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

// Replacement combinations below are processor/consumer-derived stateless
// scenarios, not executions/translations of a named native replacement test.
// Direct native constituents: Test.StateMachine.cpp:3552–3604 (SL staleness and
// transitioning); ReplicaLifecycle.cpp:1747–1761,1914–1929 (shared Drop helper);
// UpdateServiceDescription.cpp:393–485,529–590 (shared description helper).
#[tokio::test]
async fn native_replacement_drops_old_identity_and_recreates_only_from_fm_resend() {
    let (store, executor, hooks) = setup(WAIT).await;
    let request = body(1);
    let token = make_ready(&executor, &request).await;
    let ready = state(&executor);
    let old_registration = ready.runtime.clone().unwrap();
    let old_usage = ready.usage.unwrap();
    let mut replace = request.clone();
    replace.replica.id += 1;
    replace.unit.current.configuration += 10;
    // Equal instance is enough to start Drop, but not enough to recreate.
    run(&executor, add(replace.clone())).await;
    assert_eq!(state(&executor).close_mode, Some(CloseMode::Drop));
    assert_eq!(state(&executor).body.unit, ready.body.unit);
    assert_eq!(state(&executor).body.replica.id, request.replica.id);
    assert_eq!(state(&executor).runtime, ready.runtime);
    assert_eq!(state(&executor).usage, ready.usage);
    assert!(!state(&executor).local_deleted);
    assert_close(&effects(&executor), 8, &state(&executor));
    let writes = operations(&hooks).len();
    replace.replica.instance += 1;
    replace.service.update_version = 100;
    assert_eq!(
        run(&executor, add(replace.clone())).await,
        Disposition::IgnoredPending
    );
    assert_eq!(
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&request, token)))
        )
        .await,
        Disposition::IgnoredStale
    );
    assert_eq!(operations(&hooks).len(), writes);
    assert!(effects(&executor).is_empty());
    let late_close = close_completion(&executor, 0);
    close_done(&executor, 0).await;
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ReleaseRegistration(r), Effect::ReleaseUsage { acquisition },
         Effect::RequestReportRetry, Effect::Health(HealthEvent::Close)]
         if *r == old_registration && *acquisition == old_usage
    ));
    run(&executor, Command::RetryOpen(request.unit.id.clone())).await;
    assert!(effects(&executor).is_empty()); // no retained request or automatic Open
    let mut equal_instance = replace.clone();
    equal_instance.replica.instance = request.replica.instance;
    run(&executor, add(equal_instance)).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ReplicaUp(_)]
    ));
    run(&executor, Command::RetryReport(request.unit.id.clone())).await;
    let Effect::ReplicaUp(old_report) = effects(&executor).remove(0) else {
        panic!("dropped report");
    };
    // No dropped acknowledgment prerequisite; creation uses THIS lower-ID,
    // greater-instance request and even a lower description update version.
    replace.replica.id = request.replica.id - 1;
    replace.service.update_version = 1;
    replace.service.target = 9;
    replace.unit.previous.configuration = 99;
    let created = executor.submit(add(replace.clone())).unwrap();
    created.wait(WAIT).await.unwrap();
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Open { .. }]
    ));
    assert_eq!(state(&executor).body.replica.id, replace.replica.id);
    assert_eq!(state(&executor).body.unit.current, replace.unit.current);
    assert_eq!(state(&executor).body.unit.previous, Epoch::default());
    assert_eq!(state(&executor).body.service, replace.service);
    assert_eq!(state(&executor).body.unit.target, 9);
    assert_eq!(state(&executor).fm_stage, FmStage::None);
    assert!(state(&executor).runtime.is_none());
    assert!(state(&executor).usage.is_none());
    assert_eq!(
        run(&executor, Command::CompleteClose(Box::new(late_close))).await,
        Disposition::IgnoredStale
    );
    assert_eq!(
        run(
            &executor,
            Command::CompleteOpen(Box::new(complete(&request, token)))
        )
        .await,
        Disposition::IgnoredStale
    );
    run(&executor, ack(*old_report, false)).await; // old replica ID cannot delete new one
    assert_eq!(state(&executor).phase, Phase::Opening);
    assert!(effects(&executor).is_empty());
    registered(&executor, &replace, created.id).await;
    run(
        &executor,
        Command::CompleteOpen(Box::new(complete(&replace, created.id))),
    )
    .await;
    assert!(matches!(effects(&executor).as_slice(), [Effect::Reply(r)]
        if r.error==0 && r.replica.id==replace.replica.id
        && r.replica.instance==replace.replica.instance));
    assert_ne!(
        state(&executor).runtime.as_ref().unwrap().acquisition,
        old_registration.acquisition
    );
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_ready_add_identity_epoch_matrix_has_no_independent_lower_id_guard() {
    // RA.cpp:1660–1687; entity traits:253–275.
    for case in 0..6 {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        let old = state(&executor);
        let writes = operations(&hooks).len();
        let mut incoming = request.clone();
        incoming.unit.current.configuration += 1;
        match case {
            0 => incoming.unit.current.configuration = 0,
            1 => {
                incoming.replica.id += 1;
                incoming.replica.instance -= 1;
            }
            2 => incoming.replica.id -= 1,
            3 => {
                incoming.replica.id -= 1;
                incoming.replica.instance += 1;
            }
            4 => incoming.replica.instance += 1,
            5 => incoming.replica.id += 1,
            _ => unreachable!(),
        }
        let result = executor.submit(add(incoming)).unwrap().wait(WAIT).await;
        match case {
            0 | 1 => assert_eq!(result, Ok(Disposition::IgnoredStale)),
            4 => assert!(matches!(result, Err(Failure::Admission(_)))),
            _ => assert_eq!(result, Ok(Disposition::Applied)),
        }
        let actions = effects(&executor);
        match case {
            2 => assert!(matches!(actions.as_slice(), [Effect::Reply(r)]
                if r.unit==old.body.unit && r.replica==old.body.replica)),
            5 => {
                assert_close(&actions, 8, &state(&executor));
                assert_eq!(state(&executor).body.unit.current, old.body.unit.current);
            }
            _ => assert!(actions.is_empty()),
        }
        if case != 5 {
            assert!(Arc::ptr_eq(&old, &state(&executor)));
            assert_eq!(operations(&hooks).len(), writes);
        }
        finish(&store, &executor).await;
    }
}

fn description_completion(executor: &Executor) -> ServiceDescriptionCompletion {
    let current = state(executor);
    ServiceDescriptionCompletion {
        error: 0,
        unit: current.body.unit.clone(),
        replica: current.body.replica.clone(),
        service: current.body.service.clone(),
    }
}

#[tokio::test]
async fn native_add_description_update_precedes_drop_or_duplicate_and_preserves_owned_keys() {
    // FT.cpp:5862–5973; RA.cpp:1660–1684. Key changes are deliberately NOT
    // extra helper rejection predicates; opaque acquired refs remain exact.
    for dropping in [false, true] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        let old = state(&executor);
        let mut incoming = request.clone();
        incoming.service.update_version = 2;
        incoming.service.version.instance += 7;
        incoming.replica.version = incoming.service.version.clone();
        incoming.service.target = 5;
        incoming.service.minimum = 2;
        incoming.service.service_type.name = "ChangedType".into();
        incoming.service.service_type.package.name = "ChangedPackage".into();
        incoming.service.activation_mode = 1;
        incoming.service.constraints = "NodeType == Blue".into();
        incoming.unit.current.configuration += 1;
        if dropping {
            incoming.replica.id += 1;
        }
        let (entered, release) = hooks.pause(false);
        let ticket = executor.submit(add(incoming.clone())).unwrap();
        entered.await.unwrap();
        assert!(Arc::ptr_eq(&old, &state(&executor)));
        assert!(effects(&executor).is_empty());
        release.send(()).unwrap();
        ticket.wait(WAIT).await.unwrap();
        let current = state(&executor);
        assert_eq!(current.body.service.update_version, 2);
        assert_eq!(current.body.service.version, old.body.service.version);
        assert_eq!(current.body.replica.version, old.body.replica.version);
        assert_eq!(
            current.body.service.service_type,
            incoming.service.service_type
        );
        assert_eq!(current.body.service.activation_mode, 1);
        assert_eq!(
            (current.body.unit.target, current.body.unit.minimum),
            (5, 2)
        );
        assert_eq!(current.body.unit.current, old.body.unit.current);
        assert_eq!(current.runtime, old.runtime);
        assert_eq!(current.usage, old.usage);
        assert_eq!(
            executor.service_types()[0].info.versioned.id,
            old.runtime.as_ref().unwrap().service_type
        );
        let actions = effects(&executor);
        let [Effect::UpdateServiceDescription {
            body: captured,
            registration,
        }, second] = actions.as_slice()
        else {
            panic!("update before lifecycle effect: {actions:?}");
        };
        assert_eq!(captured.service, current.body.service);
        assert_eq!(captured.replica.state, 2); // captured BEFORE Drop's InDrop
        assert_eq!(Some(registration), old.runtime.as_ref());
        if dropping {
            assert!(matches!(second, Effect::Close { .. }));
            assert!(!current.service_description_pending);
            run(
                &executor,
                Command::RetryServiceDescription(request.unit.id.clone()),
            )
            .await;
            assert!(effects(&executor).is_empty());
            close_done(&executor, 0).await;
            let cleanup = effects(&executor);
            assert!(matches!(cleanup.as_slice(),
                [Effect::ReleaseRegistration(r), Effect::ReleaseUsage { acquisition },
                 Effect::RequestReportRetry, Effect::Health(HealthEvent::Close)]
                 if Some(r)==old.runtime.as_ref() && Some(*acquisition)==old.usage));
            assert!(executor.service_types().is_empty());
        } else {
            assert!(matches!(second, Effect::Reply(r)
                if r.replica==old.body.replica && r.unit==current.body.unit && r.error==0));
            assert!(current.service_description_pending); // success does NOT wait for RAP ack
            run(
                &executor,
                Command::RetryServiceDescription(request.unit.id.clone()),
            )
            .await;
            assert!(
                matches!(effects(&executor).as_slice(), [Effect::UpdateServiceDescription { body, .. }]
                if body==captured)
            );
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_ineligible_description_is_helper_noop_not_add_rejection() {
    // FT.cpp:5862–5883; Add ignores helper's false return, RA.cpp:1660–1684.
    for case in 0..4 {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        let old = state(&executor);
        let mut incoming = request;
        incoming.service.update_version = 2;
        incoming.service.target = 8;
        match case {
            0 => incoming.service.name.push_str("/other"),
            1 => incoming.service.instance += 1,
            2 => incoming.service.update_version = 0,
            3 => incoming.service.name = incoming.service.name.to_uppercase(), // exact name
            _ => unreachable!(),
        }
        incoming.replica.id += 1;
        run(&executor, add(incoming)).await;
        assert_eq!(state(&executor).body.service, old.body.service);
        assert!(!state(&executor).service_description_pending);
        assert_close(&effects(&executor), 8, &state(&executor));
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_raw_description_reply_guards_retry_and_higher_version_diagnostic() {
    // FT.cpp:5660–5699; generic IPC entity traits:281–410.
    for higher in [false, true] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        make_ready(&executor, &request).await;
        let mut update = request.clone();
        update.service.update_version = 5;
        run(&executor, add(update)).await;
        effects(&executor);
        let writes = operations(&hooks).len();
        let raw = description_completion(&executor);
        for case in 0..5 {
            let mut bad = raw.clone();
            match case {
                0 => bad.error = -1,
                1 => bad.replica.id += 1,
                2 => bad.replica.instance -= 1,
                3 => bad.unit.current.configuration -= 1,
                4 => bad.service.update_version -= 1,
                _ => unreachable!(),
            }
            assert_eq!(
                run(
                    &executor,
                    Command::CompleteServiceDescription(Box::new(bad))
                )
                .await,
                Disposition::IgnoredStale
            );
            assert!(state(&executor).service_description_pending);
            assert!(effects(&executor).is_empty());
        }
        run(
            &executor,
            Command::RetryServiceDescription(request.unit.id.clone()),
        )
        .await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::UpdateServiceDescription { .. }]
        ));
        assert!(state(&executor).service_description_pending);
        let mut accepted = raw;
        if higher {
            accepted.service.update_version += 1;
        }
        accepted.replica.node = NodeInstance::default();
        accepted.replica.version = VersionInstance::invalid();
        accepted.service.name = "not-compared-in-reply".into();
        accepted.service.service_type.name = "not-compared-in-reply".into();
        accepted.unit.current.configuration += 1;
        let old_body = state(&executor).body.clone();
        run(
            &executor,
            Command::CompleteServiceDescription(Box::new(accepted.clone())),
        )
        .await;
        assert!(!state(&executor).service_description_pending);
        assert_eq!(state(&executor).body, old_body); // no copying reply description
        assert_eq!(operations(&hooks).len(), writes); // flag clear is memory-only
        assert_eq!(
            run(
                &executor,
                Command::CompleteServiceDescription(Box::new(accepted))
            )
            .await,
            Disposition::IgnoredStale
        );
        run(
            &executor,
            Command::RetryServiceDescription(request.unit.id.clone()),
        )
        .await;
        assert!(effects(&executor).is_empty());
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_replacement_host_crash_and_late_replies_do_not_replay_add() {
    // Shared helper-derived SL scenario: RA.cpp:1138–1181,671–706.
    for during_close in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        let token = make_ready(&executor, &request).await;
        let mut incoming = request.clone();
        incoming.replica.id += 1;
        incoming.replica.instance += 1;
        if during_close {
            run(&executor, add(incoming.clone())).await;
            effects(&executor);
        }
        let late = CloseCompletion {
            unit: request.unit.clone(),
            replica: request.replica.clone(),
            error: 0,
        };
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id.clone(),
                host: registration().host,
            })),
        )
        .await;
        assert_eq!(state(&executor).phase, Phase::Closed);
        assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDown);
        let actions = effects(&executor);
        assert_eq!(
            actions
                .iter()
                .filter(|e| matches!(e, Effect::ReleaseRegistration(_)))
                .count(),
            1
        );
        assert_eq!(
            actions
                .iter()
                .filter(|e| matches!(e, Effect::ReleaseUsage { .. }))
                .count(),
            1
        );
        assert!(actions.iter().all(|e| !matches!(
            e,
            Effect::Reply(_) | Effect::RemoveReply(_) | Effect::Open { .. }
        )));
        assert_eq!(
            run(&executor, Command::CompleteClose(Box::new(late))).await,
            Disposition::IgnoredStale
        );
        assert_eq!(
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&request, token)))
            )
            .await,
            Disposition::IgnoredStale
        );
        assert!(effects(&executor).is_empty());
        run(&executor, add(incoming)).await;
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::Open { .. }]
        ));
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn native_replacement_close_failures_use_drop_retry_and_forceabort_not_delete_reply() {
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    executor
        .config()
        .set(RetryState::Closing, Threshold::Warning, 1);
    executor
        .config()
        .set(RetryState::Closing, Threshold::Drop, 2);
    let mut incoming = request.clone();
    incoming.replica.id += 1;
    run(&executor, add(incoming)).await;
    effects(&executor);
    close_done(&executor, -123).await;
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::Health(HealthEvent::CloseWarning)]
    ));
    run(&executor, Command::RetryClose(request.unit.id.clone())).await;
    assert_close(&effects(&executor), 8, &state(&executor));
    close_done(&executor, -123).await;
    assert_eq!(state(&executor).close_mode, Some(CloseMode::ForceAbort));
    assert!(effects(&executor).is_empty());
    run(&executor, Command::RetryClose(request.unit.id.clone())).await;
    assert_close(&effects(&executor), 4, &state(&executor));
    close_done(&executor, 0).await;
    assert_eq!(state(&executor).phase, Phase::Closed);
    assert!(!state(&executor).local_deleted);
    assert_eq!(state(&executor).fm_stage, FmStage::ReplicaDropped);
    assert!(effects(&executor).iter().all(|e| !matches!(
        e,
        Effect::Reply(_) | Effect::RemoveReply(_) | Effect::Open { .. }
    )));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn replacement_private_proposal_definite_and_unknown_outcomes_keep_original_work() {
    // Rust fault injection, not a native scenario or new store algorithm.
    for dropping in [false, true] {
        for (unknown, fail) in [(false, true), (true, false), (true, true)] {
            let (store, executor, hooks) = setup(if unknown {
                Duration::from_millis(50)
            } else {
                WAIT
            })
            .await;
            let request = body(1);
            make_ready(&executor, &request).await;
            let old = state(&executor);
            let writes = operations(&hooks).len();
            let mut incoming = request.clone();
            incoming.service.update_version = 3;
            incoming.service.target = 8;
            if dropping {
                incoming.replica.id += 1;
            }
            let (entered, release) = hooks.pause(fail);
            let ticket = executor.submit(add(incoming)).unwrap();
            entered.await.unwrap();
            assert!(Arc::ptr_eq(&old, &state(&executor)));
            assert!(effects(&executor).is_empty());
            assert_eq!(executor.service_types()[0].references, 1);
            if unknown {
                assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
                tokio::time::timeout(WAIT, async {
                    while store.stats().lifecycle != fube_ra_store::Lifecycle::Fenced {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                executor
                    .config()
                    .set(RetryState::Closing, Threshold::Drop, -1);
                assert!(executor.snapshot([1; 16]).fenced);
                assert!(Arc::ptr_eq(&old, &state(&executor)));
            }
            assert_eq!(operations(&hooks).len(), writes + 1);
            release.send(()).unwrap();
            let result = ticket.resolve(WAIT).await;
            if fail {
                assert!(matches!(result, Err(Failure::Store(_))));
                assert!(Arc::ptr_eq(&old, &state(&executor)));
                assert!(effects(&executor).is_empty());
            } else {
                assert_eq!(result, Ok(Disposition::Applied));
                let current = state(&executor);
                assert_eq!(current.body.service.update_version, 3);
                assert_eq!(current.body.unit.target, 8);
                let actions = executor.take_effects();
                assert_eq!(actions.len(), 2);
                assert!(actions.iter().all(|e| e.operation == ticket.id));
                assert!(matches!(
                    actions[0].effect,
                    Effect::UpdateServiceDescription { .. }
                ));
                if dropping {
                    assert_eq!(current.close_mode, Some(CloseMode::Drop));
                    assert!(matches!(actions[1].effect, Effect::Close { flags: 8, .. }));
                } else {
                    assert!(current.service_description_pending);
                    assert!(matches!(actions[1].effect, Effect::Reply(_)));
                }
            }
            assert_eq!(operations(&hooks).len(), writes + 1); // never resubmitted
            finish(&store, &executor).await;
        }
    }
}

#[tokio::test]
async fn queued_replacement_add_uses_state_after_predecessor_commit_not_arrival_snapshot() {
    for finish_open in [false, true] {
        let (store, executor, hooks) = setup(WAIT).await;
        let request = body(1);
        let initial = executor.submit(add(request.clone())).unwrap();
        initial.wait(WAIT).await.unwrap();
        effects(&executor);
        registered(&executor, &request, initial.id).await;
        if !finish_open {
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&request, initial.id))),
            )
            .await;
            effects(&executor);
            let mut replace = request.clone();
            replace.replica.id += 1;
            run(&executor, add(replace)).await;
            effects(&executor);
        }
        let old = state(&executor);
        let (entered, release) = hooks.pause(false);
        let predecessor = executor
            .submit(if finish_open {
                Command::CompleteOpen(Box::new(complete(&request, initial.id)))
            } else {
                Command::CompleteClose(Box::new(close_completion(&executor, 0)))
            })
            .unwrap();
        entered.await.unwrap();
        let mut next = request.clone();
        next.replica.id += 2;
        next.replica.instance += 1;
        let queued = executor.submit(add(next)).unwrap();
        assert_eq!(queued.status(), Status::Pending);
        assert!(Arc::ptr_eq(&old, &state(&executor)));
        release.send(()).unwrap();
        predecessor.wait(WAIT).await.unwrap();
        queued.wait(WAIT).await.unwrap();
        let actions = executor.take_effects();
        let queued_actions: Vec<_> = actions
            .iter()
            .filter(|d| d.operation == queued.id)
            .collect();
        assert_eq!(queued_actions.len(), 1);
        if finish_open {
            assert_eq!(state(&executor).phase, Phase::Closing);
            assert!(matches!(queued_actions[0].effect, Effect::Close { .. }));
        } else {
            assert_eq!(state(&executor).phase, Phase::Opening);
            assert!(matches!(queued_actions[0].effect, Effect::Open { .. }));
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn description_update_and_replacement_use_two_reserved_effects_without_hidden_growth() {
    // Rust capacity bookkeeping: maximum Add effects remains two. Ready owns a
    // runtime, so Drop cannot synchronously dispose its resources in this slice.
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let executor = Executor::new(
        Arc::new(store.clone()),
        admission(),
        Settings {
            max_effects: 8, // Registered uses the existing lifecycle reservation.
            ..Settings::default()
        },
    )
    .unwrap();
    let request = body(1);
    make_ready(&executor, &request).await;
    // Retain six independently owned Open effects, leaving exactly two slots.
    for id in 2..8 {
        run(&executor, add(body(id))).await;
    }
    let mut next = request.clone();
    next.service.update_version = 1;
    next.replica.id += 1;
    run(&executor, add(next)).await;
    assert_eq!(executor.statistics().effects, 8);
    assert!(matches!(
        executor.submit(add(request)),
        Err(Failure::Capacity)
    ));
    let actions = effects(&executor).split_off(6);
    assert!(matches!(
        actions.as_slice(),
        [
            Effect::UpdateServiceDescription { .. },
            Effect::Close { .. }
        ]
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_one_ft_ack_reuses_description_helper_and_ignores_old_description_not_ack() {
    // Actual consumer RA.cpp:3334–3418. Required when Add changes retained SD
    // after a dropped report was captured; no full-upload support is enabled.
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    make_ready(&executor, &request).await;
    let mut greater = request.clone();
    greater.replica.id += 1;
    run(&executor, add(greater)).await;
    effects(&executor);
    close_done(&executor, 0).await;
    effects(&executor);
    run(&executor, Command::RetryReport(request.unit.id.clone())).await;
    let Effect::ReplicaUp(report) = effects(&executor).remove(0) else {
        panic!("report");
    };
    let mut equal = request.clone();
    equal.service.update_version = 4;
    equal.service.target = 6;
    run(&executor, add(equal)).await;
    effects(&executor);
    assert_eq!(state(&executor).body.service.update_version, 4);
    run(&executor, ack(*report, false)).await;
    assert_eq!(state(&executor).fm_stage, FmStage::None);
    assert!(state(&executor).local_deleted);
    assert_eq!(state(&executor).body.service.update_version, 4);
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::RemoveReply(_)]
    ));
    finish(&store, &executor).await;
}

#[tokio::test]
async fn native_opening_replacement_is_ignored_before_after_registration_and_host_crash() {
    // Direct SL transitioning constituent: Test.StateMachine.cpp:3592–3604;
    // shared opening-host-loss helper: RA.cpp:1138–1160, FT.cpp:5258–5279.
    let (store, executor, _) = setup(WAIT).await;
    let request = body(1);
    let initial = executor.submit(add(request.clone())).unwrap();
    initial.wait(WAIT).await.unwrap();
    effects(&executor);
    let mut incoming = request.clone();
    incoming.replica.id += 1;
    incoming.replica.instance += 1;
    incoming.service.update_version = 9;
    incoming.service.target = 9;
    assert_eq!(
        run(&executor, add(incoming.clone())).await,
        Disposition::IgnoredPending
    );
    assert_eq!(state(&executor).body.service, request.service);
    registered(&executor, &request, initial.id).await;
    assert_eq!(
        run(&executor, add(incoming.clone())).await,
        Disposition::IgnoredPending
    );
    assert!(effects(&executor).is_empty());
    let usage = state(&executor).usage;
    run(
        &executor,
        Command::HostDown(Box::new(HostDown {
            unit: request.unit.id.clone(),
            host: registration().host,
        })),
    )
    .await;
    assert_eq!(state(&executor).phase, Phase::Opening);
    assert_eq!(state(&executor).usage, usage);
    assert!(state(&executor).runtime.is_none());
    assert!(matches!(
        effects(&executor).as_slice(),
        [Effect::ReleaseRegistration(_)]
    ));
    assert_eq!(
        run(&executor, add(incoming)).await,
        Disposition::IgnoredPending
    );
    assert_eq!(state(&executor).body.service, request.service);
    assert!(effects(&executor).is_empty());
    run(&executor, Command::RetryOpen(request.unit.id)).await;
    assert!(
        matches!(effects(&executor).as_slice(), [Effect::Open { usage: retained, .. }]
        if *retained==usage)
    );
    finish(&store, &executor).await;
}

fn description_ack(executor: &Executor, update: u64) -> Command {
    let current = state(executor);
    let mut service = current.body.service.clone();
    service.update_version = update;
    service.target += 1;
    let mut report = ReplicaUp::default();
    report.dropped.0.push(FailoverUnitInfo {
        service,
        unit: current.body.unit.clone(),
        replicas: fube_transport::serialization::serde::ObjectArray(vec![ReplicaInfo {
            description: current.body.replica.clone(),
            intermediate_role: 1,
        }]),
        ..FailoverUnitInfo::default()
    });
    ack(report, false)
}

#[tokio::test]
async fn native_pending_description_survives_opening_host_loss_but_actual_send_requires_runtime() {
    // Existing one-FT ack consumer RA.cpp:3334–3353; pending/retry helper plus
    // ACTUAL send consumer RA.cpp:3582–3614 (null STR silently suppresses send).
    // This is a shared-helper-derived scenario, not new all-message support.
    for terminal in [false, true] {
        let (store, executor, _) = setup(WAIT).await;
        let request = body(1);
        let initial = executor.submit(add(request.clone())).unwrap();
        initial.wait(WAIT).await.unwrap();
        effects(&executor);
        registered(&executor, &request, initial.id).await;
        run(&executor, description_ack(&executor, 2)).await;
        assert!(state(&executor).service_description_pending);
        assert!(matches!(
            effects(&executor).as_slice(),
            [Effect::UpdateServiceDescription { .. }]
        ));
        let late = description_completion(&executor);
        assert_eq!(
            run(
                &executor,
                Command::CompleteServiceDescription(Box::new(late))
            )
            .await,
            Disposition::IgnoredStale
        ); // generic IPC excludes Opening
        if terminal {
            executor
                .config()
                .set(RetryState::Opening, Threshold::Drop, 1);
        }
        run(
            &executor,
            Command::HostDown(Box::new(HostDown {
                unit: request.unit.id.clone(),
                host: registration().host,
            })),
        )
        .await;
        effects(&executor);
        assert_eq!(state(&executor).service_description_pending, !terminal);
        run(
            &executor,
            Command::RetryServiceDescription(request.unit.id.clone()),
        )
        .await;
        assert!(effects(&executor).is_empty()); // no fabricated runtime / panic
        if !terminal {
            run(&executor, Command::RetryOpen(request.unit.id.clone())).await;
            effects(&executor);
            let current = state(&executor).body.clone();
            registered(&executor, &current, initial.id).await;
            run(
                &executor,
                Command::RetryServiceDescription(request.unit.id.clone()),
            )
            .await;
            assert!(matches!(
                effects(&executor).as_slice(),
                [Effect::UpdateServiceDescription { .. }]
            ));
            run(
                &executor,
                Command::CompleteOpen(Box::new(complete(&current, initial.id))),
            )
            .await;
            effects(&executor);
            let accepted = description_completion(&executor);
            run(
                &executor,
                Command::CompleteServiceDescription(Box::new(accepted.clone())),
            )
            .await;
            assert!(!state(&executor).service_description_pending);
            let mut replace = request.clone();
            replace.replica.id += 1;
            run(&executor, add(replace)).await;
            effects(&executor);
            assert_eq!(
                run(
                    &executor,
                    Command::CompleteServiceDescription(Box::new(accepted.clone()))
                )
                .await,
                Disposition::IgnoredStale
            );
            close_done(&executor, 0).await;
            effects(&executor);
            assert_eq!(
                run(
                    &executor,
                    Command::CompleteServiceDescription(Box::new(accepted))
                )
                .await,
                Disposition::IgnoredStale
            );
        }
        finish(&store, &executor).await;
    }
}

#[tokio::test]
async fn replacement_close_and_recreation_store_failures_keep_resources_and_proposals_private() {
    // Rust-only failures in the existing P1 path, now reached via native Drop.
    for stateful in [false, true] {
        for recreating in [false, true] {
            for (unknown, fail) in [(false, true), (true, false), (true, true)] {
                let (store, executor, hooks) = setup(if unknown {
                    Duration::from_millis(50)
                } else {
                    WAIT
                })
                .await;
                let request = if stateful { primary_body() } else { body(1) };
                let create = |body| {
                    if stateful {
                        primary_add(body)
                    } else {
                        add(body)
                    }
                };
                make_ready(&executor, &request).await;
                let mut next = request.clone();
                next.replica.id += 1;
                next.replica.instance += 1;
                run(&executor, create(next.clone())).await;
                effects(&executor);
                if recreating {
                    close_done(&executor, 0).await;
                    effects(&executor);
                }
                let old = state(&executor);
                let node_before = executor.service_types();
                let writes = operations(&hooks).len();
                let command = if recreating {
                    create(next)
                } else {
                    Command::CompleteClose(Box::new(close_completion(&executor, 0)))
                };
                let (entered, release) = hooks.pause(fail);
                let ticket = executor.submit(command).unwrap();
                entered.await.unwrap();
                assert!(Arc::ptr_eq(&old, &state(&executor)));
                assert_eq!(executor.service_types(), node_before);
                assert!(effects(&executor).is_empty());
                if unknown {
                    assert_eq!(ticket.wait(WAIT).await, Err(Failure::Unknown));
                    tokio::time::timeout(WAIT, async {
                        while store.stats().lifecycle != fube_ra_store::Lifecycle::Fenced {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert!(Arc::ptr_eq(&old, &state(&executor)));
                    assert_eq!(executor.service_types(), node_before);
                    assert!(effects(&executor).is_empty());
                }
                release.send(()).unwrap();
                let outcome = ticket.resolve(WAIT).await;
                if fail {
                    assert!(matches!(outcome, Err(Failure::Store(_))));
                    assert!(Arc::ptr_eq(&old, &state(&executor)));
                    assert_eq!(executor.service_types(), node_before);
                    assert!(effects(&executor).is_empty());
                } else {
                    assert_eq!(outcome, Ok(Disposition::Applied));
                    if recreating {
                        assert_eq!(state(&executor).phase, Phase::Opening);
                        assert!(matches!(
                            effects(&executor).as_slice(),
                            [Effect::Open { .. }]
                        ));
                    } else {
                        assert_eq!(state(&executor).phase, Phase::Closed);
                        assert!(executor.service_types().is_empty());
                        assert!(matches!(
                            effects(&executor).as_slice(),
                            [
                                Effect::ReleaseRegistration(_),
                                Effect::ReleaseUsage { .. },
                                Effect::RequestReportRetry,
                                Effect::Health(HealthEvent::Close)
                            ]
                        ));
                    }
                }
                assert_eq!(operations(&hooks).len(), writes + 1);
                finish(&store, &executor).await;
            }
        }
    }
}

#[tokio::test]
async fn removal_and_finish_close_keep_private_proposals_and_same_unknown_ticket() {
    // Rust-specific real P1 Store invariants, not native test translations.
    for stateful in [false, true] {
        for finish_stage in [false, true] {
            for (unknown, fail) in [(false, true), (true, false), (true, true)] {
                let (store, executor, hooks) = setup(if unknown {
                    Duration::from_millis(50)
                } else {
                    WAIT
                })
                .await;
                let request = if stateful { primary_body() } else { body(1) };
                let deletion = |request| {
                    if stateful {
                        primary_delete(request, false)
                    } else {
                        remove(request, false)
                    }
                };
                make_ready(&executor, &request).await;
                if finish_stage {
                    run(&executor, deletion(request.clone())).await;
                    effects(&executor);
                }
                let old = state(&executor);
                let writes = operations(&hooks).len();
                let command = if finish_stage {
                    Command::CompleteClose(Box::new(close_completion(&executor, 0)))
                } else {
                    deletion(request.clone())
                };
                let (entered, release) = hooks.pause(fail);
                let ticket = executor.submit(command).unwrap();
                entered.await.unwrap();
                let queued = executor.submit(deletion(request.clone())).unwrap();
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
                            Effect::RemoveReply(_) | Effect::DeleteReplicaReply(_)
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
