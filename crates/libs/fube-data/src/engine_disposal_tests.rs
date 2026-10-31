use super::*;
use fube_runtime::extensions::CatchUpResult;

#[tokio::test]
async fn actual_owner_and_peer_ack_progress_while_entire_application_executor_is_stalled() {
    let partition = Uuid::new_v4();
    let secondary = engine(partition, 2, Arc::new(Provider::default()));
    secondary
        .change_role(EPOCH, ReplicaRole::IdleSecondary, cancel())
        .await
        .unwrap();
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let primary = endpoint(&network, partition, 1);
    network.register_peer(&secondary.endpoint()).unwrap();
    let target = secondary.clone();
    let (go, begin) = std_mpsc::sync_channel(0);
    let (completed, result) = std_mpsc::sync_channel(1);
    // Independent loopback peer harness, not a fube user-callback executor.
    let peer = thread::spawn(move || {
        begin
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            raw_send(
                &network,
                &primary,
                &target,
                Body::StartCopy {
                    epoch: EPOCH,
                    target: 2,
                    start: 1,
                },
            );
            timeout(std::time::Duration::from_secs(1), incoming.control.recv())
                .await
                .unwrap()
                .unwrap();
            raw_send(
                &network,
                &primary,
                &target,
                Body::Copy {
                    epoch: EPOCH,
                    target: 2,
                    sequence: 1,
                    data: vec![vec![7]],
                    last: false,
                },
            );
            timeout(std::time::Duration::from_secs(1), async {
                loop {
                    let packet = incoming
                        .control
                        .recv()
                        .await
                        .expect("peer closed before ACK");
                    if matches!(
                        packet.body,
                        Body::Ack {
                            copy_received: 1,
                            copy_applied: 0,
                            ..
                        }
                    ) {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            completed.send(()).unwrap();
        });
        network.shutdown_checked().unwrap();
    });
    go.send(()).unwrap();
    // No application scheduler poll can run between this send and the actual
    // peer ACK. Receiving data is not an explicit application-data ACK.
    result
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    peer.join().unwrap();
    let operation = secondary
        .copy_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.sequence, 1);
    operation.acknowledge().unwrap();
    secondary.close(cancel()).await.unwrap();
}

struct ExecutionView(ExecutionRuntime);

struct ReplayFinalDrop {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<std_mpsc::Receiver<()>>,
}
impl ReplayCheckpointProvider for ReplayFinalDrop {
    fn replication_checkpoint(
        &self,
        _: i64,
        _: Cancellation,
    ) -> ServiceFuture<'_, Option<ReplayOperation>> {
        Box::pin(async { Ok(None) })
    }
}
impl Drop for ReplayFinalDrop {
    fn drop(&mut self) {
        assert_eq!(thread::current().name(), Some("fube-owned-cleanup"));
        self.entered
            .get_mut()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        self.release
            .get_mut()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separately_optional_replay_final_arc_is_owned_until_actual_cleanup_not_close_admission() {
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let (entered, dropped) = oneshot::channel();
    let (release, wait) = std_mpsc::channel();
    let engine = Engine::bind_with_capabilities(
        Actor {
            partition: Uuid::new_v4(),
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        Arc::new(Provider::default()),
        Some(Arc::new(ReplayFinalDrop {
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(wait),
        })),
        Settings::default(),
        Some(Arc::new(ExecutionView(execution.clone()))),
        crate::extensions::StateProviderCapabilities::default(),
    )
    .unwrap();
    let owner = engine.clone();
    let mut close = tokio::spawn(async move { owner.close(cancel()).await });
    timeout(std::time::Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert!(timeout(std::time::Duration::from_millis(30), &mut close)
        .await
        .is_err());
    execution
        .try_reserve()
        .unwrap()
        .run_owned(|| Ok(()))
        .await
        .unwrap();
    release.send(()).unwrap();
    close.await.unwrap().unwrap();
    drop(engine);
    execution.finish_standalone().await.unwrap();
}

impl NativeControlView for ExecutionView {
    fn execution(&self) -> Result<Option<ExecutionRuntime>> {
        Ok(Some(self.0.clone()))
    }
    fn status(&self) -> Result<ReplicaStatus> {
        Ok(ReplicaStatus {
            lifecycle: ReplicaLifecycle::Open,
            service_role: ReplicaRole::Unknown,
            replicator_role: ReplicaRole::Unknown,
            replicator_open: true,
            current_role: ReplicaRole::Unknown,
            current_epoch: EPOCH,
            previous_epoch: Epoch::INVALID,
            configuration_stage: ConfigurationStage::Current,
            catch_up: CatchUpResult::NotStarted,
            minimum_replica_set_size: 1,
            configuration: Vec::new(),
            service_address: String::new(),
            replication_address: String::new(),
            progress: ReplicationProgress::EMPTY,
        })
    }
    fn report_fault(
        &self,
        _: fube_runtime::FaultType,
    ) -> ServiceFuture<'_, fube_runtime::extensions::FaultDelivery> {
        Box::pin(async { Err(Error::Closed) })
    }
}
struct DropStream {
    immediate: bool,
    polled: Arc<AtomicBool>,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<std_mpsc::Receiver<()>>,
}
impl IOperationDataStream for DropStream {
    fn get_next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        self.polled.store(true, Ordering::Release);
        if self.immediate {
            return Box::pin(async { Ok(Some(OperationData::new(vec![vec![1]])?)) });
        }
        Box::pin(async move {
            cancellation.cancelled().await;
            Err(Error::Closed)
        })
    }
}
impl Drop for DropStream {
    fn drop(&mut self) {
        assert_eq!(thread::current().name(), Some("fube-owned-cleanup"));
        self.entered
            .get_mut()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        self.release
            .get_mut()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
    }
}
struct DropProvider {
    base: Provider,
    stream: Mutex<Option<Arc<dyn IOperationDataStream>>>,
}
impl IStateProvider for DropProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.base.get_last_committed_sequence_number()
    }
    fn update_epoch(&self, e: Epoch, n: i64, c: Cancellation) -> ServiceFuture<'_, ()> {
        self.base.update_epoch(e, n, c)
    }
    fn on_data_loss(&self, c: Cancellation) -> ServiceFuture<'_, bool> {
        self.base.on_data_loss(c)
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        self.base.get_copy_context()
    }
    fn get_copy_state(
        &self,
        _: i64,
        context: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        assert!(context.is_none());
        Ok(self.stream.lock().unwrap().take().unwrap())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_waits_own_actual_stream_drop_but_unrelated_configurations_and_owner_progress_continue(
) {
    delayed_command_disposal(false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configuration_waits_its_retired_idle_stream_but_not_unrelated_commands() {
    delayed_command_disposal(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configuration_cancellation_retains_actual_disposal_and_never_becomes_success() {
    delayed_command_disposal(true, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_execution_close_waits_retained_idle_stream_before_terminal_completion() {
    delayed_command_disposal(true, false, true).await;
}

async fn delayed_command_disposal(configure: bool, cancelled: bool, closing: bool) {
    // Shared execution accounting only; this deliberately does not manufacture
    // native invocation authority or claim deployed qualification.
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let view: Arc<dyn NativeControlView> = Arc::new(ExecutionView(execution.clone()));
    let partition = Uuid::new_v4();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std_mpsc::sync_channel(1);
    let polled = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(DropProvider {
        base: Provider::default(),
        stream: Mutex::new(Some(Arc::new(DropStream {
            immediate: configure,
            polled: polled.clone(),
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(release_rx),
        }))),
    });
    let settings = Settings {
        queue_operations: 1,
        ..Settings::default()
    };
    let primary = Engine::bind_with_native_control(
        Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider,
        settings,
        Some(view.clone()),
    )
    .unwrap();
    let unrelated = Engine::bind_with_native_control(
        Actor {
            partition: Uuid::new_v4(),
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        Arc::new(Provider::default()),
        Settings::default(),
        Some(view),
    )
    .unwrap();
    initial_primary(&primary).await;
    initial_primary(&unrelated).await;
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let remote = endpoint(&network, partition, 2);
    let mut replica = descriptor(&primary);
    replica.replica_id = 2;
    replica.address = remote.to_string();
    replica.role = ReplicaRole::ActiveSecondary;
    if configure {
        primary
            .update_current_configuration(configuration(vec![replica.clone()], true, 1), cancel())
            .await
            .unwrap();
    }
    let building = primary.clone();
    let building_replica = replica.clone();
    let build =
        tokio::spawn(async move { building.build_replica(building_replica, cancel()).await });
    loop {
        let packet = timeout(std::time::Duration::from_secs(1), incoming.control.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(packet.body, Body::StartCopy { .. }) {
            break;
        }
    }
    raw_send(
        &network,
        &remote,
        &primary,
        Body::Ack {
            received: 0,
            applied: 0,
            copy_received: 0,
            copy_applied: 0,
            error: 0,
        },
    );
    until(|| polled.load(Ordering::Acquire)).await;
    if configure {
        // The sole payload charge fills queue_operations. No next poll can
        // start; this is a genuinely idle, retained user stream, not a canceled
        // observer mistaken for disposal.
        timeout(std::time::Duration::from_secs(1), incoming.data.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let removing = primary.clone();
    let cancellation = cancel();
    let command_cancel = cancellation.clone();
    let mut remove = tokio::spawn(async move {
        if closing {
            removing.close(command_cancel).await
        } else if configure {
            removing
                .update_current_configuration(configuration(vec![], true, 1), command_cancel)
                .await
        } else {
            removing
                .remove_replica(replica, ReplicaRemoval::BuildRetryCleanup, command_cancel)
                .await
        }
    });
    timeout(std::time::Duration::from_secs(1), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(timeout(std::time::Duration::from_millis(10), &mut remove)
        .await
        .is_err());
    timeout(
        std::time::Duration::from_millis(100),
        unrelated.update_current_configuration(configuration(vec![], true, 1), cancel()),
    )
    .await
    .unwrap()
    .unwrap();
    if !closing {
        timeout(
            std::time::Duration::from_millis(100),
            primary.update_current_configuration(configuration(vec![], true, 1), cancel()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert!(!remove.is_finished());
    if cancelled {
        cancellation.cancel();
        assert!(timeout(std::time::Duration::from_millis(10), &mut remove)
            .await
            .is_err());
    }
    release.send(()).unwrap();
    let result = remove.await.unwrap();
    if cancelled {
        assert!(matches!(result, Err(Error::Closed)));
    } else {
        result.unwrap();
    }
    assert!(build.await.unwrap().is_err());
    abort_owned(&primary).await;
    abort_owned(&unrelated).await;
    network.shutdown();
    drop((primary, unrelated));
    execution.finish_standalone().await.unwrap();
}

struct ReentryProvider {
    engine: Mutex<Weak<Engine>>,
    checked: Arc<AtomicBool>,
    dropped: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_remove_transfers_accepted_cleanup_at_full_shared64_after_enumeration() {
    let execution = ExecutionRuntime::standalone(tokio::runtime::Handle::current()).unwrap();
    let view: Arc<dyn NativeControlView> = Arc::new(ExecutionView(execution.clone()));
    let partition = Uuid::new_v4();
    let (dropped, drop_rx) = tokio::sync::oneshot::channel();
    let (release_drop, drop_gate) = std_mpsc::sync_channel(1);
    let polled = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(DropProvider {
        base: Provider::default(),
        stream: Mutex::new(Some(Arc::new(DropStream {
            immediate: false,
            polled: polled.clone(),
            entered: Mutex::new(Some(dropped)),
            release: Mutex::new(drop_gate),
        }))),
    });
    let primary = Engine::bind_with_native_control(
        Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider,
        Settings::default(),
        Some(view),
    )
    .unwrap();
    initial_primary(&primary).await;
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let remote = endpoint(&network, partition, 2);
    let mut replica = descriptor(&primary);
    replica.replica_id = 2;
    replica.address = remote.to_string();
    let building = primary.clone();
    let building_replica = replica.clone();
    let build =
        tokio::spawn(async move { building.build_replica(building_replica, cancel()).await });
    timeout(std::time::Duration::from_secs(1), incoming.control.recv())
        .await
        .unwrap()
        .unwrap();
    raw_send(
        &network,
        &remote,
        &primary,
        Body::Ack {
            received: 0,
            applied: 0,
            copy_received: 0,
            copy_applied: 0,
            error: 0,
        },
    );
    until(|| polled.load(Ordering::Acquire)).await;
    let (paused, paused_rx) = std_mpsc::sync_channel(1);
    let (resume, resume_rx) = std_mpsc::sync_channel(1);
    *primary.shared.copy_job_pause.lock().unwrap() = Some((paused, resume_rx));
    let removing = primary.clone();
    let remove = execution.try_reserve().unwrap().run_future(async move {
        removing
            .remove_replica(replica, ReplicaRemoval::BuildRetryCleanup, cancel())
            .await
    });
    // Controlled source seam: the provider root is physically disposed and
    // observed, but the owner has not yet applied its CopyNext result.
    paused_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let mut held = Vec::new();
    for (lane, count) in [(CallLane::Native, 31), (CallLane::Provider, 32)] {
        for _ in 0..count {
            held.push(
                execution
                    .reserve(
                        lane,
                        CallOrigin::default(),
                        std::time::Duration::from_secs(30),
                        false,
                    )
                    .unwrap(),
            );
        }
    }
    assert!(matches!(
        execution.try_reserve(),
        Err(Error::ResourceLimit(_))
    ));
    resume.send(()).unwrap();
    timeout(std::time::Duration::from_secs(1), drop_rx)
        .await
        .unwrap()
        .unwrap();
    // This destructor cannot have acquired a 65th independent record.
    assert!(matches!(
        execution.try_reserve(),
        Err(Error::ResourceLimit(_))
    ));
    release_drop.send(()).unwrap();
    remove.await.unwrap();
    assert!(build.await.unwrap().is_err());
    drop(held);
    // Successful fresh admission also proves authority was not terminally
    // retired by a mistaken parentless full64 release.
    execution
        .try_reserve()
        .unwrap()
        .run_owned(|| Ok(()))
        .await
        .unwrap();
    primary.close(cancel()).await.unwrap();
    network.shutdown_checked().unwrap();
    drop(primary);
    execution.finish_standalone().await.unwrap();
}

impl IStateProvider for ReentryProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        Ok(0)
    }
    fn update_epoch(&self, _: Epoch, _: i64, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            let engine = self.engine.lock().unwrap().upgrade().unwrap();
            let before = engine.shared.revision.load(Ordering::Acquire);
            assert!(matches!(
                engine.close(cancel()).await,
                Err(Error::InvalidInput(_))
            ));
            assert!(!engine.shared.stopped.load(Ordering::Acquire));
            assert_eq!(engine.shared.revision.load(Ordering::Acquire), before);
            assert!(engine.shared.execution.check_constructor().is_err());
            self.checked.store(true, Ordering::Release);
            Ok(false)
        })
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        Err(Error::Closed)
    }
    fn get_copy_state(
        &self,
        _: i64,
        _: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        Err(Error::Closed)
    }
}
impl Drop for ReentryProvider {
    fn drop(&mut self) {
        let engine = self.engine.get_mut().unwrap().upgrade().unwrap();
        let before = engine.shared.revision.load(Ordering::Acquire);
        assert!(matches!(engine.shutdown(), Err(Error::InvalidInput(_))));
        assert_eq!(engine.shared.revision.load(Ordering::Acquire), before);
        self.dropped
            .get_mut()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_provider_callback_and_destructor_reentry_are_rejected_before_shutdown_mutation() {
    let (dropped, drop_rx) = tokio::sync::oneshot::channel();
    let checked = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(ReentryProvider {
        engine: Mutex::new(Weak::new()),
        checked: checked.clone(),
        dropped: Mutex::new(Some(dropped)),
    });
    let engine = Engine::bind(
        Actor {
            partition: Uuid::new_v4(),
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider.clone(),
        Settings::default(),
    )
    .unwrap();
    *provider.engine.lock().unwrap() = Arc::downgrade(&engine);
    drop(provider);
    initial_primary(&engine).await;
    assert!(!engine.on_data_loss(cancel()).await.unwrap());
    assert!(checked.load(Ordering::Acquire));
    engine.close(cancel()).await.unwrap();
    drop_rx.await.unwrap();
    assert!(engine.snapshot().closed);
}
