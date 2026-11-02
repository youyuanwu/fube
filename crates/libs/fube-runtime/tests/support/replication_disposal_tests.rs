use super::*;
use std::sync::atomic::AtomicUsize;

struct DropGate {
    entered: Option<oneshot::Sender<()>>,
    release: Mutex<std_mpsc::Receiver<()>>,
    dropped: Arc<AtomicUsize>,
}
impl Drop for DropGate {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().name(), Some("fube-owned-cleanup"));
        let _ = self.entered.take().unwrap().send(());
        self.release
            .get_mut()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct GatedStream {
    inner: Arc<dyn IOperationDataStream>,
    _gate: DropGate,
}
impl IOperationDataStream for GatedStream {
    fn get_next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        self.inner.get_next(cancellation)
    }
}
struct GateProvider {
    memory: Arc<MemoryProvider>,
    gate: Mutex<Option<DropGate>>,
}
impl IStateProvider for GateProvider {
    fn get_last_committed_sequence_number(&self) -> Result<i64> {
        self.memory.get_last_committed_sequence_number()
    }
    fn update_epoch(&self, e: Epoch, n: i64, c: Cancellation) -> ServiceFuture<'_, ()> {
        self.memory.update_epoch(e, n, c)
    }
    fn on_data_loss(&self, c: Cancellation) -> ServiceFuture<'_, bool> {
        self.memory.on_data_loss(c)
    }
    fn get_copy_context(&self) -> Result<Arc<dyn IOperationDataStream>> {
        self.memory.get_copy_context()
    }
    fn get_copy_state(
        &self,
        n: i64,
        c: Option<Arc<dyn IOperationDataStream>>,
    ) -> Result<Arc<dyn IOperationDataStream>> {
        let inner = self.memory.get_copy_state(n, c)?;
        Ok(match self.gate.lock().unwrap().take() {
            Some(gate) => Arc::new(GatedStream { inner, _gate: gate }),
            None => inner,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issued_canonical_sync_configuration_and_remove_wait_exact_physical_disposal_not_owner_admission(
) {
    for root in [false, true] {
        for removing in [false, true] {
            let partition = uuid::Uuid::new_v4();
            let memory = MemoryProvider::new();
            let provider = Arc::new(GateProvider {
                memory: memory.clone(),
                gate: Mutex::new(None),
            });
            let primary = Fixture::using(
                partition,
                source(1),
                ReplicaRole::Primary,
                root,
                (
                    memory,
                    provider.clone(),
                    Settings {
                        queue_operations: 1,
                        ..Settings::default()
                    },
                ),
            )
            .await;
            primary
                .host
                .configure(config(EPOCH, vec![], 1))
                .await
                .unwrap();
            let target =
                Fixture::new(partition, source(2), ReplicaRole::IdleSecondary, false).await;
            let idle = target.descriptor(ReplicaRole::IdleSecondary);
            if removing {
                let pump = target.pump();
                primary
                    .host
                    .build(idle.clone(), Cancellation::default())
                    .await
                    .unwrap();
                pump.abort();
                let _ = pump.await;
            }
            let (entered, entered_rx) = oneshot::channel();
            let (release, drop_release) = std_mpsc::sync_channel(1);
            let dropped = Arc::new(AtomicUsize::new(0));
            let mut native_remove = None;
            let mut remove_release = None;
            if removing {
                let (captured, receive) = oneshot::channel();
                let (unpause, pause) = std_mpsc::sync_channel(1);
                *primary.engine.native_remove_pause.lock().unwrap() = Some((captured, pause));
                let node = primary.host.clone();
                let peer = idle.clone();
                native_remove = Some(tokio::spawn(async move { node.remove(peer).await }));
                let proof = receive.await.unwrap();
                assert_eq!(
                    proof.kind(),
                    NativeInvocationKind::Remove(ReplicaRemoval::Idle)
                );
                // Private algorithm setup only: the genuine native-issued
                // Remove is paused before owner admission. No public standalone
                // authority or fabricated invocation is involved.
                primary
                    .engine
                    .remove_replica(
                        idle.clone(),
                        ReplicaRemoval::BuildRetryCleanup,
                        Cancellation::default(),
                    )
                    .await
                    .unwrap();
                remove_release = Some(unpause);
            }
            let endpoint = target.engine.endpoint();
            target.engine.close(Cancellation::default()).await.unwrap();
            let (network, mut incoming) =
                PeerTransport::bind(&endpoint.address.to_string(), Settings::default()).unwrap();
            network.register_peer(&primary.engine.endpoint()).unwrap();
            let mut owned_peer = idle.clone();
            if !removing {
                owned_peer.role = ReplicaRole::ActiveSecondary;
                owned_peer.ready = true;
                // Arrange an idle retained stream with the existing private
                // ownership seam; the tested configuration below is canonical
                // and uses the real runtime's immutable native selection.
                primary
                    .engine
                    .update_current_configuration(
                        config(EPOCH, vec![owned_peer.clone()], 1),
                        Cancellation::default(),
                    )
                    .await
                    .unwrap();
            }
            *provider.gate.lock().unwrap() = Some(DropGate {
                entered: Some(entered),
                release: Mutex::new(drop_release),
                dropped: dropped.clone(),
            });
            let engine = primary.engine.clone();
            let build = tokio::spawn(async move {
                engine
                    .build_replica(owned_peer, Cancellation::default())
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(1), incoming.control.recv())
                .await
                .unwrap()
                .unwrap();
            network
                .send(Packet {
                    from: endpoint.clone(),
                    to: primary.engine.endpoint().actor,
                    body: Body::Ack {
                        received: 0,
                        applied: 0,
                        copy_received: 0,
                        copy_applied: 0,
                        error: 0,
                    },
                })
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(1), incoming.data.recv())
                .await
                .unwrap()
                .unwrap();
            let mut configuration = None;
            if let Some(release) = remove_release {
                release.send(()).unwrap();
            } else {
                // Issue a distinct native full-epoch configuration, rather than
                // replaying a consumed/latest ledger record outside dispatch.
                // The canonical SDK callback is still synchronous and must
                // wait for the same physical stream destruction.
                let host = primary.host.clone();
                configuration = Some(tokio::spawn(async move {
                    host.configure(config(
                        Epoch {
                            configuration_number: EPOCH.configuration_number + 1,
                            ..EPOCH
                        },
                        vec![],
                        1,
                    ))
                    .await
                }));
            }
            let entered = tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await;
            if !matches!(entered, Ok(Ok(()))) {
                let status = primary.native.status();
                let snapshot = primary.engine.snapshot();
                release.send(()).unwrap();
                let result = match configuration.take() {
                    Some(call) => Some(call.await.unwrap()),
                    None => None,
                };
                provider.gate.lock().unwrap().take();
                primary.engine.close(Cancellation::default()).await.unwrap();
                let _ = build.await;
                network.shutdown_checked().unwrap();
                panic!("canonical disposal did not enter: {entered:?}; configuration {result:?}; native {status:?}; lower {snapshot:?}");
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            if let Some(call) = configuration.as_mut() {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(10), call)
                        .await
                        .is_err()
                );
            }
            if let Some(call) = native_remove.as_mut() {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(10), call)
                        .await
                        .is_err()
                );
            }
            primary.write("owner-progress-during-disposal").await;
            assert_eq!(primary.engine.snapshot().progress.committed, 1);
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            if let Some(call) = configuration {
                call.await.unwrap().unwrap();
            }
            if let Some(call) = native_remove {
                call.await.unwrap().unwrap();
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(build.await.unwrap().is_err());
            network.shutdown_checked().unwrap();
        }
    }
}
