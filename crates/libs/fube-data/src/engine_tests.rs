// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Rust-only Fabric TCP source-contract fixtures; not native interoperability.

use super::*;
use crate::{MemoryProvider, Mutation};
use std::sync::atomic::{AtomicI64, AtomicUsize};
use uuid::Uuid;

const EPOCH: Epoch = Epoch {
    data_loss: 1,
    configuration: 0x1_0000_0000,
};
const NEXT_EPOCH: Epoch = Epoch {
    data_loss: 1,
    configuration: 0x2_0000_0000,
};

#[tokio::test]
async fn promoted_prefix_without_retained_suffix_accepts_bounded_existing_peer_receipt() {
    let partition = Uuid::new_v4();
    let primary = engine(
        partition,
        1,
        Arc::new(Provider {
            last: AtomicI64::new(20),
            ..Provider::default()
        }),
    );
    primary
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let (transport, _incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let remote = endpoint(&transport, partition, 2);
    transport.register_peer(&primary.endpoint()).unwrap();
    let peer = ReplicaInformation {
        node_id: [2; 16],
        node_instance: 1,
        replica_id: 2,
        incarnation: 1,
        role: ReplicaRole::Secondary,
        previous_role: ReplicaRole::Secondary,
        up: true,
        ready: true,
        address: remote.to_string(),
        progress: ReplicationProgress { first: 0, last: 20 },
        must_catch_up: false,
    };
    let cc = configuration(vec![peer], true, 2);
    primary
        .update_current_configuration(cc.clone(), cancel())
        .await
        .unwrap();
    until(|| primary.snapshot().peers.len() == 1).await;
    assert_eq!(primary.snapshot().progress.completed, 20);
    assert_eq!(
        primary.snapshot().peers[0].maximum_sent,
        20,
        "the reconciled prefix bounds receipts even when no retained operation needs resending"
    );
    raw_send(
        &transport,
        &remote,
        &primary,
        Body::Ack {
            received: 20,
            applied: 20,
            copy_received: -1,
            copy_applied: -1,
            error: 0,
        },
    );
    until(|| primary.snapshot().peers[0].applied == 20).await;
    primary
        .wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    raw_send(
        &transport,
        &remote,
        &primary,
        Body::Ack {
            received: 21,
            applied: 21,
            copy_received: -1,
            copy_applied: -1,
            error: 0,
        },
    );
    tokio::time::sleep(Settings::default().retry * 2).await;
    assert_eq!(primary.snapshot().peers[0].applied, 20);
    assert_eq!(primary.snapshot().progress.assigned, 20);
    primary
        .change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    primary
        .change_role(NEXT_EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let mut next = cc;
    next.epoch = NEXT_EPOCH;
    primary
        .update_current_configuration(next, cancel())
        .await
        .unwrap();
    until(|| {
        let snapshot = primary.snapshot();
        snapshot.role == ReplicaRole::Primary
            && snapshot.epoch == Some(NEXT_EPOCH)
            && snapshot.peers.len() == 1
            && snapshot.peers[0].configured
    })
    .await;
    assert_eq!(
        primary.snapshot().peers[0].maximum_sent,
        20,
        "retained peer promotion needs the same reconciled prefix bound"
    );
    raw_send(
        &transport,
        &remote,
        &primary,
        Body::Ack {
            received: 20,
            applied: 20,
            copy_received: -1,
            copy_applied: -1,
            error: 0,
        },
    );
    until(|| primary.snapshot().peers[0].applied == 20).await;
    primary
        .wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    primary.abort();
    transport.shutdown();
}

#[tokio::test]
async fn public_open_endpoint_includes_native_required_empty_transport_suffix() {
    let node = engine(Uuid::new_v4(), 1, Arc::new(Provider::default()));
    let published = node.open(cancel()).await.unwrap();
    // Native ReplicaInformation first finds the actor-incarnation ';', then
    // requires another ';' delimiting the transport ID (which may be empty).
    let first = published.find(';').unwrap();
    let second = published[first + 1..].find(';').unwrap() + first + 1;
    assert_eq!(second, published.len() - 1);
    assert_eq!(published.parse::<Endpoint>().unwrap(), node.endpoint());
    assert_eq!(node.endpoint().transport_id.as_deref(), Some(""));
    node.close(cancel()).await.unwrap();
}

#[cfg(feature = "qualification")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qualification_packets_do_not_assign_change_membership_or_apply_obsolete_traffic() {
    let partition = Uuid::new_v4();
    let pp = Arc::new(Provider::default());
    let sp = Arc::new(Provider::default());
    let primary = engine(partition, 1, pp.clone());
    let secondary = engine(partition, 2, sp.clone());
    primary
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    primary
        .update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    secondary
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    primary
        .build_replica(descriptor(&secondary), cancel())
        .await
        .unwrap();
    secondary
        .change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    let mut member = descriptor(&secondary);
    member.role = ReplicaRole::Secondary;
    member.ready = true;
    primary
        .update_current_configuration(configuration(vec![member], true, 2), cancel())
        .await
        .unwrap();
    let before = primary.snapshot();
    for kind in ["stale_epoch", "wrong_sender", "wrong_target"] {
        let receipt = primary.qualification_probe(kind, Some(2)).await.unwrap();
        assert_eq!(receipt["kind"], kind);
        assert_eq!(receipt["admission_only"], true);
    }
    for kind in ["stale_copy_ack", "wrong_sender", "wrong_target"] {
        secondary.qualification_probe(kind, Some(1)).await.unwrap();
    }
    assert!(primary
        .qualification_probe("arbitrary", None)
        .await
        .is_err());
    assert!(primary
        .qualification_probe("stale_epoch", Some(99))
        .await
        .is_err());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(primary.snapshot().progress, before.progress);
    assert_eq!(primary.snapshot().peers.len(), before.peers.len());
    assert_eq!(secondary.snapshot().progress.received, 0);
    assert!(primary.snapshot().fault.is_none());
    assert!(secondary.snapshot().fault.is_none());
    let mut write = primary
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(write.sequence, 1);
    pp.last.store(1, Ordering::Release);
    let operation = secondary
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.sequence, 1);
    sp.last.store(1, Ordering::Release);
    operation.acknowledge().unwrap();
    drop(operation);
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    primary.abort();
    secondary.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_idle_build_retains_continuing_writes_until_cc_adoption() {
    let partition = Uuid::new_v4();
    let provider = Arc::new(SnapshotProvider::default());
    let primary = engine(partition, 1, provider.clone());
    let target_provider = Arc::new(Provider::default());
    let target = engine(partition, 2, target_provider.clone());
    primary
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    primary
        .update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    target
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let completed_build = cancel();
    primary
        .build_replica(descriptor(&target), completed_build.clone())
        .await
        .unwrap();
    // RAP retires a completed invocation's cancellation scope independently of
    // the established idle/configured session's lifetime.
    completed_build.cancel();
    tokio::time::sleep(Settings::default().retry * 2).await;
    assert!(!primary.snapshot().peers[0].cleanup_required);
    let started = std::time::Instant::now();
    let written_before = primary.transport.diagnostics()["events"]["written_data"][0]
        .as_u64()
        .unwrap_or(0);
    for sequence in 1..=100 {
        let mut accepted = primary
            .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
            .await
            .unwrap();
        provider.record(accepted.sequence);
        assert_eq!(accepted.wait(cancel()).await.unwrap(), sequence);
        let operation = timeout(
            std::time::Duration::from_secs(2),
            target.replication_stream().next(cancel()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "idle handoff stalled at {sequence}: primary={:?}; target={:?}; pnet={}; tnet={}",
                primary.snapshot(),
                target.snapshot(),
                primary.transport.diagnostics(),
                target.transport.diagnostics()
            )
        })
        .unwrap()
        .unwrap();
        assert_eq!(operation.sequence, sequence);
        if sequence == 1 {
            until(|| target.snapshot().progress.received == sequence).await;
            assert_eq!(
                target.progress().unwrap(),
                ReplicationProgress { first: 0, last: 1 },
                "receive is not completed replay capability"
            );
        }
        let progress = primary.snapshot();
        assert!(progress.progress.first_retained <= sequence, "{progress:?}");
        assert!(!progress.peers[0].cleanup_required);
        target_provider.last.store(sequence, Ordering::Release);
        operation.acknowledge().unwrap();
        drop(operation);
        until(|| primary.snapshot().peers[0].applied == sequence).await;
        assert_eq!(
            primary.progress().unwrap(),
            ReplicationProgress {
                first: 0,
                last: sequence
            },
            "released committed history is not available replay data"
        );
    }
    let written_after = primary.transport.diagnostics()["events"]["written_data"][0]
        .as_u64()
        .unwrap_or(0);
    // At most one outstanding operation here. Legitimate timer retries scale
    // with elapsed time, not with the count of duplicate cumulative receipts.
    let retry_allowance =
        8 * (1 + started.elapsed().as_millis() / Settings::default().retry.as_millis());
    assert!(
        (written_after - written_before) as u128 <= 100 + retry_allowance,
        "duplicate receipts amplified data: {}",
        primary.transport.diagnostics()
    );
    let mut member = descriptor(&target);
    member.role = ReplicaRole::Secondary;
    member.ready = true;
    target
        .change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    primary
        .update_current_configuration(configuration(vec![member], true, 2), cancel())
        .await
        .unwrap();
    primary
        .wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    primary.abort();
    target.abort();
}

struct SnapshotProvider {
    last: AtomicI64,
    history: Mutex<BTreeMap<i64, u64>>,
    snapshots: AtomicBool,
    requested: Mutex<Vec<i64>>,
    eos: Arc<Semaphore>,
}

impl Default for SnapshotProvider {
    fn default() -> Self {
        Self {
            last: AtomicI64::new(0),
            history: Mutex::new(BTreeMap::new()),
            snapshots: AtomicBool::new(false),
            requested: Mutex::new(Vec::new()),
            eos: Arc::new(Semaphore::new(0)),
        }
    }
}

impl SnapshotProvider {
    fn record(&self, sequence: i64) {
        self.history
            .lock()
            .unwrap()
            .insert(sequence, sequence as u64 * 10);
        self.last.store(sequence, Ordering::Release);
    }
}

struct SnapshotStream {
    snapshot: OperationData,
    next: AtomicUsize,
    eos: Arc<Semaphore>,
}

impl OperationDataStream for SnapshotStream {
    fn next(&self, _: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if self.next.fetch_add(1, Ordering::AcqRel) == 0 {
                Ok(Some(self.snapshot.clone()))
            } else {
                let _permit = self
                    .eos
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Closed)?;
                Ok(None)
            }
        })
    }
}

impl StateProvider for SnapshotProvider {
    fn last_committed(&self) -> Result<i64> {
        Ok(self.last.load(Ordering::Acquire))
    }
    fn update_epoch(&self, _: Epoch, previous: i64, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.history
                .lock()
                .unwrap()
                .retain(|sequence, _| *sequence <= previous);
            self.last.store(previous, Ordering::Release);
            Ok(())
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        Ok(Arc::new(Empty))
    }
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        assert!(context.is_none());
        self.requested.lock().unwrap().push(up_to);
        if !self.snapshots.load(Ordering::Acquire) {
            return Ok(Arc::new(Empty));
        }
        let history = self.history.lock().unwrap();
        if up_to > self.last.load(Ordering::Acquire)
            || (1..=up_to).any(|sequence| !history.contains_key(&sequence))
        {
            return Err(Error::ResourceLimit("fixture history unavailable"));
        }
        let mut bytes = up_to.to_le_bytes().to_vec();
        for (&sequence, &value) in history.range(..=up_to) {
            bytes.extend_from_slice(&sequence.to_le_bytes());
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Ok(Arc::new(SnapshotStream {
            snapshot: OperationData::new(vec![bytes])?,
            next: AtomicUsize::new(0),
            eos: self.eos.clone(),
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonempty_live_copy_uses_previous_quorum_horizon_and_applied_finish_fence_not_latest() {
    let partition = Uuid::new_v4();
    let source = Arc::new(SnapshotProvider::default());
    let p = engine(partition, 1, source.clone());
    let slow = engine(partition, 2, Arc::new(Provider::default()));
    let fast_provider = Arc::new(Provider::default());
    let fast = engine(partition, 3, fast_provider.clone());
    let target_provider = Arc::new(Provider::default());
    let target = engine(partition, 4, target_provider.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    for secondary in [&slow, &fast] {
        secondary
            .change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        p.build_replica(descriptor(secondary), cancel())
            .await
            .unwrap();
    }
    p.update_catch_up_configuration(
        configuration(vec![descriptor(&slow)], true, 2),
        configuration(vec![descriptor(&fast)], false, 1),
        cancel(),
    )
    .await
    .unwrap();
    for _ in 1..=10 {
        let write = p
            .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
            .await
            .unwrap();
        source.record(write.sequence);
    }
    for expected in 1..=10 {
        let operation = fast
            .replication_stream()
            .next(cancel())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.sequence, expected);
        fast_provider.last.store(expected, Ordering::Release);
        operation.acknowledge().unwrap();
    }
    until(|| {
        p.snapshot()
            .peers
            .iter()
            .any(|peer| peer.replica == 3 && peer.applied == 10)
    })
    .await;
    assert_eq!(p.snapshot().progress.committed, 0);
    source.snapshots.store(true, Ordering::Release);
    target
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let build = p.build_replica(descriptor(&target), cancel());
    tokio::pin!(build);
    assert!(timeout(std::time::Duration::from_millis(40), &mut build)
        .await
        .is_err());
    let snapshot = target.copy_stream().next(cancel()).await.unwrap().unwrap();
    let bytes = snapshot.data.to_segments().remove(0);
    let horizon = i64::from_le_bytes(bytes[..8].try_into().unwrap());
    assert_eq!(horizon, 10);
    assert_eq!(bytes.len(), 8 + 10 * 16);
    assert_eq!(*source.requested.lock().unwrap().last().unwrap(), 10);
    let copied: Vec<_> = bytes[8..]
        .as_chunks::<16>()
        .0
        .iter()
        .map(|entry| {
            (
                i64::from_le_bytes(entry[..8].try_into().unwrap()),
                u64::from_le_bytes(entry[8..].try_into().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        copied,
        (1..=10).map(|n| (n, n as u64 * 10)).collect::<Vec<_>>()
    );
    for expected in 11..=110 {
        let write = p
            .submit(
                OperationData::new(vec![vec![expected as u8]]).unwrap(),
                cancel(),
            )
            .await
            .unwrap();
        assert_eq!(write.sequence, expected);
        source.record(write.sequence);
    }
    source.eos.add_permits(1);
    until(|| {
        p.snapshot()
            .peers
            .iter()
            .any(|peer| peer.replica == 4 && peer.copy_finish == Some(110))
    })
    .await;
    until(|| target.snapshot().progress.received == 110).await;
    assert!(!target.snapshot().copy_done);
    assert_eq!(target.snapshot().progress.applied, 0);
    assert!(timeout(std::time::Duration::from_millis(40), &mut build)
        .await
        .is_err());
    for expected in 111..=115 {
        let write = p
            .submit(
                OperationData::new(vec![vec![expected as u8]]).unwrap(),
                cancel(),
            )
            .await
            .unwrap();
        source.record(write.sequence);
    }
    // Adoption during the build transfers ownership away from RemoveIdle.
    p.update_catch_up_configuration(
        configuration(vec![descriptor(&slow), descriptor(&target)], true, 2),
        configuration(vec![descriptor(&fast)], false, 1),
        cancel(),
    )
    .await
    .unwrap();
    p.remove_replica(descriptor(&target), ReplicaRemoval::Idle, cancel())
        .await
        .unwrap();
    target_provider.last.store(horizon, Ordering::Release);
    snapshot.acknowledge().unwrap();
    drop(snapshot);
    until(|| target.snapshot().copy_done).await;
    assert!(target.copy_stream().next(cancel()).await.unwrap().is_none());
    for expected in 11..=110 {
        let operation = target
            .replication_stream()
            .next(cancel())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.sequence, expected);
        target_provider.last.store(expected, Ordering::Release);
        operation.acknowledge().unwrap();
    }
    timeout(std::time::Duration::from_secs(3), &mut build)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.snapshot().progress.assigned, 115);
    until(|| {
        p.snapshot()
            .peers
            .iter()
            .any(|peer| peer.replica == 4 && peer.applied == 110)
    })
    .await;
    p.abort();
    slow.abort();
    fast.abort();
    target.abort();
}

struct FailureStream {
    mode: usize,
    active: Arc<AtomicUsize>,
    disposed: Arc<AtomicUsize>,
}

impl OperationDataStream for FailureStream {
    fn next(&self, _: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::AcqRel);
            let _guard = CallbackGuard {
                active: self.active.clone(),
                disposed: self.disposed.clone(),
            };
            match self.mode {
                1 => Err(Error::Protocol("copy enumeration failed")),
                2 => panic!("copy enumeration panicked"),
                _ => std::future::pending().await,
            }
        })
    }
}

struct CopyFailureProvider {
    base: Provider,
    stream: Arc<FailureStream>,
}

impl StateProvider for CopyFailureProvider {
    fn last_committed(&self) -> Result<i64> {
        self.base.last_committed()
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.base.update_epoch(epoch, previous, cancellation)
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        self.base.on_data_loss(cancellation)
    }
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        self.base.copy_context()
    }
    fn copy_state(
        &self,
        _: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        assert!(context.is_none());
        if self.stream.mode == 4 {
            return Err(Error::ResourceLimit(
                "required provider history unavailable",
            ));
        }
        Ok(self.stream.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_provider_error_panic_deadline_and_unavailable_history_fence_and_dispose() {
    for mode in [1, 2, 3, 4] {
        let stream = Arc::new(FailureStream {
            mode,
            active: Arc::new(AtomicUsize::new(0)),
            disposed: Arc::new(AtomicUsize::new(0)),
        });
        let source = Arc::new(CopyFailureProvider {
            base: Provider::default(),
            stream: stream.clone(),
        });
        let partition = Uuid::new_v4();
        let settings = Settings {
            callback_timeout: std::time::Duration::from_millis(150),
            ..Settings::default()
        };
        let p = Engine::bind(
            Actor {
                partition,
                replica: 1,
                incarnation: Uuid::new_v4(),
            },
            "127.0.0.1:0",
            source,
            settings,
        )
        .unwrap();
        let s = engine(partition, 2, Arc::new(Provider::default()));
        p.change_role(EPOCH, ReplicaRole::Primary, cancel())
            .await
            .unwrap();
        p.update_current_configuration(configuration(vec![], true, 1), cancel())
            .await
            .unwrap();
        s.change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        assert!(p.build_replica(descriptor(&s), cancel()).await.is_err());
        until(|| p.snapshot().closed).await;
        assert!(!p.snapshot().ready);
        assert_eq!(stream.active.load(Ordering::Acquire), 0);
        assert_eq!(
            stream.disposed.load(Ordering::Acquire),
            usize::from(mode != 4)
        );
        p.abort();
        s.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_sixty_four_copy_callbacks_are_bounded_cancel_cleanup_joins_and_control_is_reserved() {
    let stream = Arc::new(FailureStream {
        mode: 3,
        active: Arc::new(AtomicUsize::new(0)),
        disposed: Arc::new(AtomicUsize::new(0)),
    });
    let source = Arc::new(CopyFailureProvider {
        base: Provider::default(),
        stream: stream.clone(),
    });
    let partition = Uuid::new_v4();
    let settings = Settings {
        operation_bytes: 1024,
        operation_segments: 1,
        ..Settings::default()
    };
    let p = Engine::bind(
        Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        source,
        settings,
    )
    .unwrap();
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    network.register_peer(&p.endpoint()).unwrap();
    let template = descriptor(&p);
    let mut builds = JoinSet::new();
    let mut replicas = Vec::new();
    let mut cancellations = Vec::new();
    for id in 2..=65 {
        let destination = endpoint(&network, partition, id);
        let mut replica = template.clone();
        replica.replica_id = id;
        replica.address = destination.to_string();
        let owner = p.clone();
        let cancellation = cancel();
        let owned_cancellation = cancellation.clone();
        let owned_replica = replica.clone();
        builds.spawn(async move { owner.build_replica(owned_replica, owned_cancellation).await });
        replicas.push((replica, destination));
        cancellations.push(cancellation);
    }
    timeout(std::time::Duration::from_secs(5), async {
        while stream.active.load(Ordering::Acquire) < 64 {
            tokio::select! {
                Some(packet) = incoming.control.recv() => {
                    if let Body::StartCopy { target, .. } = packet.body {
                        let destination = &replicas[(target - 2) as usize].1;
                        raw_send(&network, destination, &p, Body::Ack {
                            received: 0, applied: 0, copy_received: 0, copy_applied: 0, error: 0,
                        });
                    }
                },
                _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(stream.active.load(Ordering::Acquire), 64);
    assert!(p.snapshot().provider_callbacks <= 65);
    let mut excess = template;
    excess.replica_id = 66;
    excess.address = endpoint(&network, partition, 66).to_string();
    assert!(p.build_replica(excess, cancel()).await.is_err());
    assert!(!p.on_data_loss(cancel()).await.unwrap());
    assert_eq!(stream.active.load(Ordering::Acquire), 64);
    cancellations[0].cancel();
    p.remove_replica(
        replicas[0].0.clone(),
        ReplicaRemoval::BuildRetryCleanup,
        cancel(),
    )
    .await
    .unwrap();
    until(|| stream.active.load(Ordering::Acquire) == 63).await;
    assert_eq!(stream.disposed.load(Ordering::Acquire), 1);
    p.abort();
    assert_eq!(stream.active.load(Ordering::Acquire), 0);
    assert_eq!(stream.disposed.load(Ordering::Acquire), 64);
    assert_eq!(
        p.shared.budget.operations.available_permits(),
        p.settings.queue_operations
    );
    while let Some(result) = builds.join_next().await {
        assert!(result.unwrap().is_err());
    }
    network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unfinished_copy_rejects_epoch_update_and_promotion_without_success() {
    for promote in [false, true] {
        let source = Arc::new(SnapshotProvider::default());
        source.snapshots.store(true, Ordering::Release);
        let partition = Uuid::new_v4();
        let p = engine(partition, 1, source);
        let s = engine(partition, 2, Arc::new(Provider::default()));
        p.change_role(EPOCH, ReplicaRole::Primary, cancel())
            .await
            .unwrap();
        p.update_current_configuration(configuration(vec![], true, 1), cancel())
            .await
            .unwrap();
        s.change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        let cancellation = cancel();
        let build = p.build_replica(descriptor(&s), cancellation.clone());
        tokio::pin!(build);
        assert!(timeout(std::time::Duration::from_millis(40), &mut build)
            .await
            .is_err());
        let pending = s.copy_stream().next(cancel()).await.unwrap().unwrap();
        let next = Epoch {
            configuration: EPOCH.configuration * 2,
            ..EPOCH
        };
        let result = if promote {
            s.change_role(next, ReplicaRole::Primary, cancel()).await
        } else {
            s.update_epoch(next, cancel()).await
        };
        assert!(result.is_err());
        until(|| s.snapshot().closed).await;
        assert!(!s.snapshot().ready);
        assert!(pending.acknowledge().is_err());
        cancellation.cancel();
        assert!(build.await.is_err());
        p.remove_replica(descriptor(&s), ReplicaRemoval::BuildRetryCleanup, cancel())
            .await
            .unwrap();
        p.abort();
        s.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promotion_reconciles_then_discards_nondispatched_and_out_of_order_tail() {
    let provider = Arc::new(Provider::default());
    let (secondary, network, primary, mut incoming) =
        initialized_raw_secondary(provider.clone()).await;
    let drainer = tokio::spawn(async move {
        loop {
            tokio::select! {
                packet = incoming.control.recv() => if packet.is_none() { break; },
                packet = incoming.data.recv() => if packet.is_none() { break; },
            }
        }
    });
    for sequence in 1..=64 {
        raw_send(
            &network,
            &primary,
            &secondary,
            replication(EPOCH, EPOCH, sequence, sequence as u8),
        );
    }
    let stream = secondary.replication_stream();
    let mut held = Vec::new();
    for expected in 1..=64 {
        let operation = stream.next(cancel()).await.unwrap().unwrap();
        assert_eq!(operation.sequence, expected);
        provider.last.store(expected, Ordering::Release);
        operation.acknowledge().unwrap();
        held.push(operation);
    }
    until(|| secondary.snapshot().progress.applied == 64).await;
    assert_eq!(secondary.shared.callbacks.available_permits(), 0);
    raw_send(
        &network,
        &primary,
        &secondary,
        replication(EPOCH, EPOCH, 65, 65),
    );
    raw_send(
        &network,
        &primary,
        &secondary,
        replication(EPOCH, EPOCH, 67, 67),
    );
    until(|| secondary.snapshot().progress.assigned == 67).await;
    assert_eq!(secondary.snapshot().progress.received, 65);
    let next = Epoch {
        configuration: EPOCH.configuration * 2,
        ..EPOCH
    };
    secondary
        .change_role(next, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    until(|| secondary.snapshot().role == ReplicaRole::Primary).await;
    assert_eq!(provider.last_committed().unwrap(), 64);
    assert!(provider.epochs.lock().unwrap().contains(&(next, 64)));
    assert_eq!(secondary.snapshot().progress.assigned, 64);
    assert_eq!(secondary.snapshot().progress.received, 64);
    let mut current = configuration(vec![], true, 1);
    current.epoch = next;
    secondary
        .update_current_configuration(current, cancel())
        .await
        .unwrap();
    let write = secondary
        .submit(OperationData::new(vec![vec![99]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(write.sequence, 65);
    drop(held);
    secondary.abort();
    network.shutdown();
    drainer.abort();
    let _ = drainer.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_directed_planned_swap_preserves_stream_and_reconciled_sequence() {
    let partition = Uuid::new_v4();
    let pp = Arc::new(Provider::default());
    let sp = Arc::new(Provider::default());
    let p = engine(partition, 1, pp.clone());
    let s = engine(partition, 2, sp.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    s.change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    p.build_replica(descriptor(&s), cancel()).await.unwrap();
    s.change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![descriptor(&s)], true, 2), cancel())
        .await
        .unwrap();
    for expected in 1..=10 {
        let mut write = p
            .submit(
                OperationData::new(vec![vec![expected as u8]]).unwrap(),
                cancel(),
            )
            .await
            .unwrap();
        pp.last.store(expected, Ordering::Release);
        let operation = s
            .replication_stream()
            .next(cancel())
            .await
            .unwrap()
            .unwrap();
        sp.last.store(expected, Ordering::Release);
        operation.acknowledge().unwrap();
        write.wait(cancel()).await.unwrap();
    }
    let next = Epoch {
        configuration: EPOCH.configuration * 2,
        ..EPOCH
    };
    p.change_role(next, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    s.change_role(next, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let mut current = configuration(vec![descriptor(&p)], true, 2);
    current.epoch = next;
    s.update_current_configuration(current, cancel())
        .await
        .unwrap();
    let mut write = s
        .submit(OperationData::new(vec![vec![11]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(write.sequence, 11);
    sp.last.store(11, Ordering::Release);
    let operation = timeout(
        std::time::Duration::from_secs(3),
        p.replication_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(operation.sequence, 11);
    assert_eq!(operation.epoch, next);
    pp.last.store(11, Ordering::Release);
    operation.acknowledge().unwrap();
    assert_eq!(write.wait(cancel()).await.unwrap(), 11);
    p.abort();
    s.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_build_cleanup_allows_retry_but_actual_remove_tombstones_native_incarnation() {
    let stream = Arc::new(FailureStream {
        mode: 3,
        active: Arc::new(AtomicUsize::new(0)),
        disposed: Arc::new(AtomicUsize::new(0)),
    });
    let source = Arc::new(CopyFailureProvider {
        base: Provider::default(),
        stream: stream.clone(),
    });
    let partition = Uuid::new_v4();
    let p = engine(partition, 1, source);
    let s = engine(partition, 2, Arc::new(Provider::default()));
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    s.change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    for reason in [ReplicaRemoval::BuildRetryCleanup, ReplicaRemoval::Idle] {
        let cancellation = cancel();
        let build = p.build_replica(descriptor(&s), cancellation.clone());
        tokio::pin!(build);
        assert!(timeout(std::time::Duration::from_millis(40), &mut build)
            .await
            .is_err());
        until(|| stream.active.load(Ordering::Acquire) == 1).await;
        cancellation.cancel();
        assert!(build.await.is_err());
        assert!(p.build_replica(descriptor(&s), cancel()).await.is_err());
        p.remove_replica(descriptor(&s), reason, cancel())
            .await
            .unwrap();
        assert_eq!(stream.active.load(Ordering::Acquire), 0);
    }
    assert_eq!(stream.disposed.load(Ordering::Acquire), 2);
    assert!(p.build_replica(descriptor(&s), cancel()).await.is_err());
    // A changed wire UUID cannot launder the removed native incarnation.
    let mut stale = descriptor(&s);
    let mut stale_endpoint = s.endpoint();
    stale_endpoint.actor.incarnation = Uuid::new_v4();
    stale.address = stale_endpoint.to_string();
    assert!(p.build_replica(stale, cancel()).await.is_err());
    p.remove_replica(descriptor(&s), ReplicaRemoval::Idle, cancel())
        .await
        .unwrap();
    p.abort();
    s.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_gap_empty_data_duplicate_and_legacy_eos_require_every_explicit_apply() {
    let partition = Uuid::new_v4();
    let secondary = engine(partition, 2, Arc::new(Provider::default()));
    secondary
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let primary = endpoint(&network, partition, 1);
    network.register_peer(&secondary.endpoint()).unwrap();
    raw_send(
        &network,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch: EPOCH,
            target: 2,
            start: 1,
        },
    );
    timeout(std::time::Duration::from_secs(2), incoming.control.recv())
        .await
        .unwrap()
        .unwrap();
    for (sequence, data, last) in [
        (2, vec![vec![]], false),
        (3, vec![], true),
        (1, vec![vec![9]], false),
    ] {
        raw_send(
            &network,
            &primary,
            &secondary,
            Body::Copy {
                epoch: EPOCH,
                target: 2,
                sequence,
                data,
                last,
            },
        );
    }
    let stream = secondary.copy_stream();
    let one = timeout(std::time::Duration::from_secs(2), stream.next(cancel()))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let two = stream.next(cancel()).await.unwrap().unwrap();
    assert_eq!((one.sequence, two.sequence), (1, 2));
    assert_eq!(two.data.to_segments(), vec![Vec::<u8>::new()]);
    two.acknowledge().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!secondary.snapshot().copy_done);
    assert!(
        timeout(std::time::Duration::from_millis(20), stream.next(cancel()))
            .await
            .is_err()
    );
    one.acknowledge().unwrap();
    until(|| secondary.snapshot().copy_done).await;
    assert!(stream.next(cancel()).await.unwrap().is_none());
    raw_send(
        &network,
        &primary,
        &secondary,
        Body::Copy {
            epoch: EPOCH,
            target: 2,
            sequence: 3,
            data: vec![],
            last: true,
        },
    );
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(secondary.snapshot().fault.is_none());
    secondary.abort();
    network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_service_apply_deadline_fences_without_receipt_as_commit_and_disposes_owned_permits(
) {
    let partition = Uuid::new_v4();
    let p = engine(partition, 1, Arc::new(Provider::default()));
    let s = Engine::bind(
        Actor {
            partition,
            replica: 2,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        Arc::new(Provider::default()),
        Settings {
            callback_timeout: std::time::Duration::from_millis(200),
            ..Settings::default()
        },
    )
    .unwrap();
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    s.change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    p.build_replica(descriptor(&s), cancel()).await.unwrap();
    p.update_current_configuration(configuration(vec![descriptor(&s)], true, 2), cancel())
        .await
        .unwrap();
    let mut write = p
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    let operation = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    until(|| s.snapshot().closed).await;
    assert_eq!(p.snapshot().progress.committed, 0);
    assert!(s.snapshot().fault.unwrap().contains("deadline"));
    assert!(operation.acknowledge().is_err());
    let canceled = cancel();
    canceled.cancel();
    assert!(write.wait(canceled).await.is_err());
    assert_eq!(s.shared.callbacks.available_permits(), 63);
    assert_eq!(s.snapshot().owned_operations, 1);
    drop(operation);
    assert_eq!(s.shared.callbacks.available_permits(), 64);
    assert_eq!(s.snapshot().owned_operations, 0);
    p.abort();
    s.abort();
}

struct Empty;
impl OperationDataStream for Empty {
    fn next(&self, _: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Default)]
struct Provider {
    last: AtomicI64,
    epochs: Mutex<Vec<(Epoch, i64)>>,
    horizons: Mutex<Vec<i64>>,
}

impl StateProvider for Provider {
    fn last_committed(&self) -> Result<i64> {
        Ok(self.last.load(Ordering::Acquire))
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.epochs.lock().unwrap().push((epoch, previous));
            self.last.store(previous, Ordering::Release);
            Ok(())
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        Ok(Arc::new(Empty))
    }
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        assert!(context.is_none());
        self.horizons.lock().unwrap().push(up_to);
        Ok(Arc::new(Empty))
    }
}

fn cancel() -> Cancellation {
    Cancellation::default()
}

fn engine(partition: Uuid, id: i64, provider: Arc<dyn StateProvider>) -> Arc<Engine> {
    engine_with_settings(partition, id, provider, Settings::default())
}

fn engine_with_settings(
    partition: Uuid,
    id: i64,
    provider: Arc<dyn StateProvider>,
    settings: Settings,
) -> Arc<Engine> {
    Engine::bind(
        Actor {
            partition,
            replica: id,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider,
        settings,
    )
    .unwrap()
}

fn descriptor(engine: &Engine) -> ReplicaInformation {
    ReplicaInformation {
        node_id: [engine.endpoint.actor.replica as u8; 16],
        node_instance: 1,
        replica_id: engine.endpoint.actor.replica,
        incarnation: 1,
        role: ReplicaRole::Secondary,
        previous_role: ReplicaRole::Secondary,
        up: true,
        ready: true,
        address: engine.endpoint.to_string(),
        progress: ReplicationProgress::EMPTY,
        must_catch_up: false,
    }
}

fn configuration(
    replicas: Vec<ReplicaInformation>,
    local: bool,
    quorum: usize,
) -> ReplicaSetConfiguration {
    ReplicaSetConfiguration {
        epoch: EPOCH,
        replicas,
        write_quorum: quorum,
        local_member: local,
    }
}

// Runtime status shapes from stateful_reconfiguration.rs:admit_epoch,
// configuration and catch_up. These are source fixtures, not a native node.
struct SourceNativeView(Mutex<Result<ReplicaStatus>>);

impl NativeControlView for SourceNativeView {
    fn status(&self) -> Result<ReplicaStatus> {
        self.0.lock().unwrap().clone()
    }
    fn report_fault(
        &self,
        _: mssf_runtime::FaultType,
    ) -> ServiceFuture<'_, mssf_runtime::FaultDelivery> {
        Box::pin(async { Err(Error::Closed) })
    }
}

fn prewrite_status() -> ReplicaStatus {
    ReplicaStatus {
        lifecycle: ReplicaLifecycle::Open,
        service_role: ReplicaRole::Primary,
        replicator_role: ReplicaRole::Primary,
        replicator_open: true,
        current_role: ReplicaRole::Secondary,
        current_epoch: Epoch {
            configuration: NEXT_EPOCH.configuration + 7,
            ..NEXT_EPOCH
        },
        previous_epoch: Epoch {
            configuration: EPOCH.configuration + 3,
            ..EPOCH
        },
        configuration_stage: ConfigurationStage::PreWriteRevoke,
        catch_up: mssf_runtime::CatchUpResult::NotStarted,
        minimum_replica_set_size: 1,
        configuration: vec![],
        service_address: String::new(),
        replication_address: String::new(),
        progress: ReplicationProgress::EMPTY,
    }
}

fn primary_status() -> ReplicaStatus {
    ReplicaStatus {
        current_role: ReplicaRole::Primary,
        current_epoch: EPOCH,
        previous_epoch: Epoch::INVALID,
        configuration_stage: ConfigurationStage::Current,
        catch_up: mssf_runtime::CatchUpResult::Completed,
        ..prewrite_status()
    }
}

fn engine_with_native_view(
    partition: Uuid,
    id: i64,
    provider: Arc<dyn StateProvider>,
    view: Arc<SourceNativeView>,
) -> Arc<Engine> {
    Engine::bind_with_native_control(
        Actor {
            partition,
            replica: id,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider,
        Settings::default(),
        Some(view),
    )
    .unwrap()
}

#[tokio::test]
async fn native_singleton_requires_stable_runtime_membership_not_primary_role_alone() {
    let mut native = primary_status();
    native.minimum_replica_set_size = 2;
    let view = Arc::new(SourceNativeView(Mutex::new(Ok(native))));
    let provider = Arc::new(Provider::default());
    let node = engine_with_native_view(Uuid::new_v4(), 1, provider.clone(), view.clone());
    node.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    tokio::time::sleep(Settings::default().retry * 2).await;
    assert!(
        !node.snapshot().ready,
        "min2 cannot become a singleton from role"
    );
    let variants: &[fn(&mut ReplicaStatus)] = &[
        |s: &mut ReplicaStatus| s.lifecycle = ReplicaLifecycle::Closing,
        |s: &mut ReplicaStatus| s.configuration_stage = ConfigurationStage::CatchUp,
        |s: &mut ReplicaStatus| s.catch_up = mssf_runtime::CatchUpResult::NotStarted,
        |s: &mut ReplicaStatus| s.current_epoch = NEXT_EPOCH,
        |s: &mut ReplicaStatus| s.replicator_open = false,
        |s: &mut ReplicaStatus| s.service_role = ReplicaRole::Secondary,
    ];
    for mutate in variants {
        let mut status = primary_status();
        mutate(&mut status);
        *view.0.lock().unwrap() = Ok(status);
        tokio::time::sleep(Settings::default().retry * 2).await;
        assert!(!node.snapshot().ready);
    }
    *view.0.lock().unwrap() = Ok(primary_status());
    until(|| node.snapshot().ready).await;
    let mut write = node
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    provider.last.store(write.sequence, Ordering::Release);
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    node.abort();
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_role_alone_is_not_readiness_and_singleton_admission_is_owned() {
    let provider = Arc::new(Provider::default());
    let engine = engine(Uuid::new_v4(), 1, provider);
    assert!(!engine.snapshot().ready);
    assert_eq!(
        engine.open(cancel()).await.unwrap(),
        engine.endpoint().to_string()
    );
    engine
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    assert!(!engine.snapshot().ready);
    engine
        .update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    until(|| engine.snapshot().ready).await;
    let mut replication = engine
        .submit(OperationData::new(vec![vec![42]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(replication.sequence, 1);
    assert_eq!(replication.wait(cancel()).await.unwrap(), 1);
    assert!(serde_json::to_value(engine.snapshot()).unwrap()["ready"]
        .as_bool()
        .unwrap());
    engine.close(cancel()).await.unwrap();
    assert!(engine.snapshot().closed);
    assert!(engine
        .submit(OperationData::new(vec![]).unwrap(), cancel())
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_loopback_engines_require_explicit_apply_for_one_hundred_writes() {
    let partition = Uuid::new_v4();
    let p = engine(partition, 1, Arc::new(Provider::default()));
    let s1p = Arc::new(Provider::default());
    let s2p = Arc::new(Provider::default());
    let s1 = engine(partition, 2, s1p.clone());
    let s2 = engine(partition, 3, s2p.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    for secondary in [&s1, &s2] {
        secondary
            .change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        p.build_replica(descriptor(secondary), cancel())
            .await
            .unwrap();
        secondary
            .change_role(EPOCH, ReplicaRole::Secondary, cancel())
            .await
            .unwrap();
    }
    p.update_current_configuration(
        configuration(vec![descriptor(&s1), descriptor(&s2)], true, 2),
        cancel(),
    )
    .await
    .unwrap();
    let mut accepted = Vec::new();
    for n in 1..=100_i64 {
        accepted.push(
            p.submit(
                OperationData::new(vec![vec![], n.to_le_bytes().to_vec()]).unwrap(),
                cancel(),
            )
            .await
            .unwrap(),
        );
    }
    until(|| s1.snapshot().progress.received >= 64 && s2.snapshot().progress.received >= 64).await;
    assert_eq!(s1.snapshot().progress.applied, 0);
    assert_eq!(p.snapshot().progress.committed, 0);
    let stream1 = s1.replication_stream();
    let stream2 = s2.replication_stream();
    for expected in 1..=100 {
        for (stream, provider) in [(&stream1, &s1p), (&stream2, &s2p)] {
            let operation = timeout(std::time::Duration::from_secs(5), stream.next(cancel()))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(operation.sequence, expected);
            assert_eq!(
                operation.data.to_segments(),
                vec![vec![], expected.to_le_bytes().to_vec()]
            );
            provider.last.store(expected, Ordering::Release);
            operation.acknowledge().unwrap();
        }
    }
    for mut operation in accepted {
        assert_eq!(operation.wait(cancel()).await.unwrap(), operation.sequence);
    }
    until(|| {
        s1.snapshot().progress.applied == 100
            && s2.snapshot().progress.applied == 100
            && p.snapshot().progress.committed == 100
    })
    .await;
    assert_eq!(p.snapshot().progress.committed, 100);
    p.abort();
    s1.abort();
    s2.abort();
}

async fn pair() -> (Arc<Engine>, Arc<Engine>, Arc<Provider>) {
    pair_with_native_view(None).await
}

async fn pair_with_native_view(
    view: Option<Arc<SourceNativeView>>,
) -> (Arc<Engine>, Arc<Engine>, Arc<Provider>) {
    let partition = Uuid::new_v4();
    let primary = engine(partition, 1, Arc::new(Provider::default()));
    let provider = Arc::new(Provider::default());
    let secondary = match view {
        Some(view) => engine_with_native_view(partition, 2, provider.clone(), view),
        None => engine(partition, 2, provider.clone()),
    };
    primary
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    primary
        .update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    secondary
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    primary
        .build_replica(descriptor(&secondary), cancel())
        .await
        .unwrap();
    secondary
        .change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    (primary, secondary, provider)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_prewrite_source_sequence_keeps_old_epoch_receipts_until_update_epoch_and_swap() {
    let partition = Uuid::new_v4();
    let pp = Arc::new(Provider::default());
    let sp = Arc::new(Provider::default());
    let view = Arc::new(SourceNativeView(Mutex::new(Ok(primary_status()))));
    let p = engine_with_native_view(partition, 1, pp.clone(), view.clone());
    let s = engine(partition, 2, sp.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    s.change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    p.build_replica(descriptor(&s), cancel()).await.unwrap();
    s.change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    let previous = configuration(vec![descriptor(&s)], true, 2);
    p.update_current_configuration(previous.clone(), cancel())
        .await
        .unwrap();
    let mut write = p
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    pp.last.store(1, Ordering::Release);
    let operation = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.epoch, EPOCH);
    let provider_epochs = pp.epochs.lock().unwrap().clone();
    let revision = p.shared.revision.load(Ordering::Acquire);
    let mut current = previous.clone();
    current.epoch = NEXT_EPOCH;
    let mut status = prewrite_status();
    status.configuration = vec![descriptor(&s)];
    status.configuration[0].role = ReplicaRole::Primary;
    *view.0.lock().unwrap() = Ok(status.clone());
    p.update_catch_up_configuration(current.clone(), previous.clone(), cancel())
        .await
        .unwrap();
    until(|| !p.snapshot().ready).await;
    assert_eq!(p.snapshot().epoch, Some(EPOCH));
    assert_eq!(pp.epochs.lock().unwrap().as_slice(), provider_epochs);
    assert_eq!(p.shared.revision.load(Ordering::Acquire), revision);
    assert!(p
        .submit(OperationData::new(vec![vec![2]]).unwrap(), cancel())
        .await
        .is_err());
    assert_eq!(p.snapshot().progress.assigned, 1);
    let first_all = p.wait_for_catch_up(CatchUpMode::All, cancel());
    tokio::pin!(first_all);
    assert!(
        timeout(std::time::Duration::from_millis(40), &mut first_all)
            .await
            .is_err()
    );
    // ACK has no epoch: the existing E1 session can drain the first native ALL,
    // even though installed membership metadata already describes E2.
    sp.last.store(1, Ordering::Release);
    operation.acknowledge().unwrap();
    drop(operation);
    timeout(std::time::Duration::from_secs(2), &mut first_all)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    assert_eq!(p.snapshot().epoch, Some(EPOCH));
    assert!(!p.snapshot().ready);
    assert_eq!(s.snapshot().epoch, Some(EPOCH));
    assert_eq!(pp.epochs.lock().unwrap().as_slice(), provider_epochs);

    status.configuration_stage = ConfigurationStage::CatchUp;
    assert_eq!(
        status.access().1,
        mssf_runtime::AccessStatus::ReconfigurationPending
    );
    *view.0.lock().unwrap() = Ok(status);
    p.update_epoch(NEXT_EPOCH, cancel()).await.unwrap();
    until(|| p.snapshot().epoch == Some(NEXT_EPOCH) && p.snapshot().ready).await;
    assert!(pp.epochs.lock().unwrap().contains(&(NEXT_EPOCH, 1)));
    // Engine readiness after UpdateEpoch is not the independent native gate.
    assert_eq!(
        view.status().unwrap().access().1,
        mssf_runtime::AccessStatus::ReconfigurationPending
    );
    p.update_catch_up_configuration(current, previous, cancel())
        .await
        .unwrap();
    p.wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    let mut status = view.status().unwrap();
    status.catch_up = mssf_runtime::CatchUpResult::Completed;
    *view.0.lock().unwrap() = Ok(status);
    p.change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    let mut status = view.status().unwrap();
    status.service_role = ReplicaRole::Secondary;
    status.replicator_role = ReplicaRole::Secondary;
    status.configuration_stage = ConfigurationStage::Current;
    *view.0.lock().unwrap() = Ok(status);
    s.change_role(NEXT_EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let mut current = configuration(vec![descriptor(&p)], true, 2);
    current.epoch = NEXT_EPOCH;
    s.update_catch_up_configuration(
        current,
        configuration(vec![descriptor(&p)], true, 2),
        cancel(),
    )
    .await
    .unwrap();
    let mut write = s
        .submit(OperationData::new(vec![vec![2]]).unwrap(), cancel())
        .await
        .unwrap();
    sp.last.store(2, Ordering::Release);
    let operation = p
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.sequence, 2);
    assert_eq!(operation.epoch, NEXT_EPOCH);
    assert!(s.snapshot().progress.committed < write.sequence);
    pp.last.store(2, Ordering::Release);
    operation.acknowledge().unwrap();
    drop(operation);
    assert_eq!(write.wait(cancel()).await.unwrap(), 2);
    assert!(!p.snapshot().ready);
    p.abort();
    s.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_prewrite_rejects_wrong_views_future_pc_and_stale_cc_without_mutation() {
    let partition = Uuid::new_v4();
    let provider = Arc::new(Provider::default());
    let view = Arc::new(SourceNativeView(Mutex::new(Ok(primary_status()))));
    let p = engine_with_native_view(partition, 1, provider.clone(), view.clone());
    let s = engine(partition, 2, Arc::new(Provider::default()));
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let previous = configuration(vec![], true, 1);
    p.update_current_configuration(previous.clone(), cancel())
        .await
        .unwrap();
    until(|| p.snapshot().ready).await;
    let before = p.snapshot();
    let epochs = provider.epochs.lock().unwrap().clone();
    let revision = p.shared.revision.load(Ordering::Acquire);
    let mut current = configuration(vec![descriptor(&s)], true, 2);
    current.epoch = NEXT_EPOCH;
    let variants: &[fn(&mut ReplicaStatus)] = &[
        |s| s.configuration_stage = ConfigurationStage::Current,
        |s| s.configuration_stage = ConfigurationStage::CatchUp,
        |s| s.current_role = ReplicaRole::Primary,
        |s| s.current_role = ReplicaRole::Idle,
        |s| s.service_role = ReplicaRole::Secondary,
        |s| s.replicator_role = ReplicaRole::Secondary,
        |s| s.replicator_open = false,
        |s| s.lifecycle = ReplicaLifecycle::Opening,
        |s| s.lifecycle = ReplicaLifecycle::Closing,
        |s| s.lifecycle = ReplicaLifecycle::Closed,
        |s| s.current_epoch = EPOCH,
        |s| s.current_epoch.configuration += 0x1_0000_0000,
        |s| s.current_epoch.data_loss += 1,
        |s| s.previous_epoch = NEXT_EPOCH,
        |s| s.previous_epoch = Epoch::INVALID,
    ];
    for (index, change) in variants.iter().enumerate() {
        let mut status = prewrite_status();
        change(&mut status);
        *view.0.lock().unwrap() = Ok(status);
        assert!(
            p.update_catch_up_configuration(current.clone(), previous.clone(), cancel())
                .await
                .is_err(),
            "accepted invalid source view {index}"
        );
        // PC-less controls still require the complete stage/role/CC proof.
        if index < variants.len() - 2 {
            assert!(p
                .update_current_configuration(current.clone(), cancel())
                .await
                .is_err());
        }
        let after = p.snapshot();
        assert_eq!(after.progress, before.progress);
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.role, before.role);
        assert!(after.ready);
        assert!(after.peers.is_empty());
        assert!(after.fault.is_none());
    }
    *view.0.lock().unwrap() = Err(Error::Closed);
    assert!(p
        .update_current_configuration(previous.clone(), cancel())
        .await
        .is_err());
    *view.0.lock().unwrap() = Ok(prewrite_status());
    for epoch in [
        NEXT_EPOCH,
        Epoch {
            data_loss: 2,
            configuration: 0,
        },
        Epoch {
            data_loss: -1,
            configuration: EPOCH.configuration,
        },
        Epoch {
            data_loss: 1,
            configuration: -1,
        },
        Epoch::INVALID,
    ] {
        let mut invalid_pc = previous.clone();
        invalid_pc.epoch = epoch;
        assert!(p
            .update_catch_up_configuration(previous.clone(), invalid_pc, cancel())
            .await
            .is_err());
        assert!(p.snapshot().peers.is_empty());
        assert_eq!(p.snapshot().progress, before.progress);
        assert!(p.snapshot().ready);
    }
    // The active PC must match native provenance even when it is an older,
    // otherwise valid epoch.
    let mut wrong_pc = previous.clone();
    wrong_pc.epoch.data_loss = 0;
    assert!(p
        .update_catch_up_configuration(current.clone(), wrong_pc, cancel())
        .await
        .is_err());
    p.update_catch_up_configuration(current.clone(), previous.clone(), cancel())
        .await
        .unwrap();
    p.update_current_configuration(current.clone(), cancel())
        .await
        .unwrap();
    until(|| !p.snapshot().ready && p.snapshot().peers.len() == 1).await;
    // E1 is still the engine epoch, but cannot undo installed E2 metadata.
    assert!(p
        .update_current_configuration(previous, cancel())
        .await
        .is_err());
    let mut future = current.clone();
    future.epoch.configuration += 0x1_0000_0000;
    assert!(p
        .update_current_configuration(future, cancel())
        .await
        .is_err());
    assert!(p.snapshot().peers[0].configured);
    assert!(!p.snapshot().ready);
    assert_eq!(p.snapshot().progress, before.progress);
    assert_eq!(p.snapshot().epoch, Some(EPOCH));
    assert_eq!(provider.epochs.lock().unwrap().as_slice(), epochs);
    assert_eq!(p.shared.revision.load(Ordering::Acquire), revision);
    p.abort();
    s.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unqualified_configuration_cannot_advance_or_rewind_native_epoch() {
    let p = engine(Uuid::new_v4(), 1, Arc::new(Provider::default()));
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    until(|| p.snapshot().ready).await;
    for epoch in [
        NEXT_EPOCH,
        Epoch {
            data_loss: 2,
            configuration: 0,
        },
        Epoch {
            data_loss: 0,
            configuration: NEXT_EPOCH.configuration,
        },
    ] {
        let mut current = configuration(vec![], true, 1);
        current.epoch = epoch;
        assert!(p
            .update_current_configuration(current.clone(), cancel())
            .await
            .is_err());
        assert!(p
            .update_catch_up_configuration(current, configuration(vec![], true, 1), cancel())
            .await
            .is_err());
        assert!(p.snapshot().ready);
        assert_eq!(p.snapshot().epoch, Some(EPOCH));
        assert_eq!(p.snapshot().progress.assigned, 0);
    }
    p.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_primary_e2_cc_and_e1_pc_keep_separate_ack_quorums_and_current_catchup() {
    let mut status = primary_status();
    status.service_role = ReplicaRole::Secondary;
    status.replicator_role = ReplicaRole::Secondary;
    status.current_role = ReplicaRole::Secondary;
    let view = Arc::new(SourceNativeView(Mutex::new(Ok(status))));
    let (old, p, provider) = pair_with_native_view(Some(view.clone())).await;
    old.change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    p.change_role(NEXT_EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    let mut status = primary_status();
    status.current_epoch = NEXT_EPOCH;
    status.previous_epoch = EPOCH;
    status.configuration_stage = ConfigurationStage::CatchUp;
    status.catch_up = mssf_runtime::CatchUpResult::NotStarted;
    status.configuration = vec![descriptor(&old)];
    *view.0.lock().unwrap() = Ok(status);
    let mut current = configuration(vec![], true, 1);
    current.epoch = NEXT_EPOCH;
    let mut inactive = configuration(vec![], false, 0);
    inactive.epoch = Epoch::INVALID;
    p.update_catch_up_configuration(current.clone(), inactive, cancel())
        .await
        .unwrap();
    // Epoch ordering is lexicographic: a lower data-loss epoch may carry a
    // larger configuration number without becoming a future PC.
    let mut older = configuration(vec![], true, 1);
    older.epoch = Epoch {
        data_loss: 0,
        configuration: NEXT_EPOCH.configuration + 0x1_0000_0000,
    };
    p.update_catch_up_configuration(current.clone(), older, cancel())
        .await
        .unwrap();
    let previous = configuration(vec![descriptor(&old)], false, 1);
    p.update_catch_up_configuration(current.clone(), previous.clone(), cancel())
        .await
        .unwrap();
    let mut accepted = p
        .submit(OperationData::new(vec![vec![7]]).unwrap(), cancel())
        .await
        .unwrap();
    provider.last.store(1, Ordering::Release);
    let operation = old
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.epoch, NEXT_EPOCH);
    until(|| p.snapshot().peers[0].received == 1).await;
    assert_eq!(p.snapshot().progress.committed, 0);
    assert!(timeout(
        std::time::Duration::from_millis(40),
        accepted.wait(cancel())
    )
    .await
    .is_err());
    // Native catch-up uses CC plus the retained fence, not PC write commitment.
    p.wait_for_catch_up(CatchUpMode::WriteQuorum, cancel())
        .await
        .unwrap();
    let mut required = current.clone();
    let mut target = descriptor(&old);
    target.must_catch_up = true;
    required.replicas.push(target);
    p.update_catch_up_configuration(required, previous.clone(), cancel())
        .await
        .unwrap();
    for mode in [CatchUpMode::WriteQuorum, CatchUpMode::All] {
        assert!(timeout(
            std::time::Duration::from_millis(40),
            p.wait_for_catch_up(mode, cancel())
        )
        .await
        .is_err());
    }
    let mut unavailable = previous.clone();
    unavailable.write_quorum = 2;
    p.update_catch_up_configuration(current.clone(), unavailable, cancel())
        .await
        .unwrap();
    until(|| !p.snapshot().ready).await;
    assert!(p
        .submit(OperationData::new(vec![vec![8]]).unwrap(), cancel())
        .await
        .is_err());
    assert_eq!(p.snapshot().progress.assigned, 1);
    p.wait_for_catch_up(CatchUpMode::WriteQuorum, cancel())
        .await
        .unwrap();
    p.wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    let mut previous = previous;
    previous.local_member = true;
    p.update_catch_up_configuration(current, previous, cancel())
        .await
        .unwrap();
    assert_eq!(
        p.snapshot().progress.committed,
        0,
        "PC q1 cannot self-commit"
    );
    operation.acknowledge().unwrap();
    drop(operation);
    assert_eq!(accepted.wait(cancel()).await.unwrap(), 1);
    p.abort();
    old.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joint_quorums_previous_one_excludes_primary_and_catchup_is_not_write_commit() {
    let (p, s, provider) = pair().await;
    let mut remote = descriptor(&s);
    remote.address = Endpoint {
        transport_id: Some("fixture-transport-id".into()),
        ..s.endpoint()
    }
    .to_string();
    p.update_catch_up_configuration(
        configuration(vec![], true, 1),
        configuration(vec![remote.clone()], false, 1),
        cancel(),
    )
    .await
    .unwrap();
    let mut accepted = p
        .submit(OperationData::new(vec![vec![7]]).unwrap(), cancel())
        .await
        .unwrap();
    until(|| s.snapshot().progress.received == 1).await;
    assert_eq!(p.snapshot().progress.committed, 0);
    // Native WRITE_QUORUM uses CC and the prior catch-up fence, not both
    // configurations' write quorums.
    p.wait_for_catch_up(CatchUpMode::WriteQuorum, cancel())
        .await
        .unwrap();
    let mut required = configuration(vec![remote.clone()], true, 1);
    required.replicas[0].must_catch_up = true;
    p.update_catch_up_configuration(
        required,
        configuration(vec![remote.clone()], false, 1),
        cancel(),
    )
    .await
    .unwrap();
    assert!(timeout(
        std::time::Duration::from_millis(40),
        p.wait_for_catch_up(CatchUpMode::WriteQuorum, cancel())
    )
    .await
    .is_err());
    assert!(timeout(
        std::time::Duration::from_millis(40),
        p.wait_for_catch_up(CatchUpMode::All, cancel())
    )
    .await
    .is_err());
    // A native idle cleanup cannot remove a configured member to manufacture
    // a smaller PC, nor can a quorum larger than its available PC self-commit.
    p.remove_replica(remote.clone(), ReplicaRemoval::Idle, cancel())
        .await
        .unwrap();
    p.update_catch_up_configuration(
        configuration(vec![], true, 1),
        configuration(vec![remote.clone()], false, 2),
        cancel(),
    )
    .await
    .unwrap();
    until(|| !p.snapshot().ready).await;
    p.wait_for_catch_up(CatchUpMode::WriteQuorum, cancel())
        .await
        .unwrap();
    assert!(p
        .submit(OperationData::new(vec![vec![8]]).unwrap(), cancel())
        .await
        .is_err());
    assert_eq!(p.snapshot().progress.assigned, 1);
    p.update_catch_up_configuration(
        configuration(vec![], true, 1),
        configuration(vec![remote.clone()], true, 1),
        cancel(),
    )
    .await
    .unwrap();
    assert_eq!(p.snapshot().progress.committed, 0);
    let operation = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    provider.last.store(1, Ordering::Release);
    operation.acknowledge().unwrap();
    assert_eq!(accepted.wait(cancel()).await.unwrap(), 1);
    p.wait_for_catch_up(CatchUpMode::All, cancel())
        .await
        .unwrap();
    p.abort();
    s.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_cancel_is_ambiguous_resource_rejection_consumes_no_lsn_and_old_captures_are_fenced(
) {
    let (p, s, provider) = pair().await;
    p.update_current_configuration(configuration(vec![descriptor(&s)], true, 2), cancel())
        .await
        .unwrap();
    let canceled = cancel();
    canceled.cancel();
    assert!(p
        .submit(OperationData::new(vec![vec![0]]).unwrap(), canceled.clone())
        .await
        .is_err());
    let mut one = p
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    assert!(one.wait(canceled).await.is_err());
    drop(one);
    let mut two = p
        .submit(OperationData::new(vec![vec![2]]).unwrap(), cancel())
        .await
        .unwrap();
    let stream = s.replication_stream();
    let op1 = stream.next(cancel()).await.unwrap().unwrap();
    let op2 = stream.next(cancel()).await.unwrap().unwrap();
    op2.acknowledge().unwrap();
    until(|| s.snapshot().progress.received == 2).await;
    assert_eq!(s.snapshot().progress.applied, 0);
    provider.last.store(2, Ordering::Release);
    op1.acknowledge().unwrap();
    assert_eq!(two.wait(cancel()).await.unwrap(), 2);
    drop(op1);
    drop(op2);
    let three = p
        .submit(OperationData::new(vec![vec![3]]).unwrap(), cancel())
        .await
        .unwrap();
    let late = stream.next(cancel()).await.unwrap().unwrap();
    let before = s.shared.callbacks.available_permits();
    assert!(before < s.settings.provider_operations);
    s.abort();
    assert!(late.acknowledge().is_err());
    assert_eq!(s.shared.callbacks.available_permits(), before);
    drop(late);
    assert_eq!(
        s.shared.callbacks.available_permits(),
        s.settings.provider_operations
    );
    assert_eq!(
        s.shared.budget.operations.available_permits(),
        s.settings.queue_operations
    );
    drop(three);
    p.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_admission_and_callback_capture_exhaustion_preserve_control_capacity() {
    let partition = Uuid::new_v4();
    let settings = Settings {
        queue_operations: 2,
        provider_operations: 1,
        ..Settings::default()
    };
    let p = Engine::bind(
        Actor {
            partition,
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        Arc::new(Provider::default()),
        settings,
    )
    .unwrap();
    let s = engine(partition, 2, Arc::new(Provider::default()));
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    s.change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    p.build_replica(descriptor(&s), cancel()).await.unwrap();
    p.update_current_configuration(configuration(vec![descriptor(&s)], true, 2), cancel())
        .await
        .unwrap();
    let a = p
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    let b = p
        .submit(OperationData::new(vec![vec![2]]).unwrap(), cancel())
        .await
        .unwrap();
    assert!(matches!(
        p.submit(OperationData::new(vec![vec![3]]).unwrap(), cancel())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(p.snapshot().progress.assigned, 2);
    let stream = s.replication_stream();
    for expected in [1, 2] {
        let operation = stream.next(cancel()).await.unwrap().unwrap();
        assert_eq!(operation.sequence, expected);
        operation.acknowledge().unwrap();
    }
    let mut a = a;
    let mut b = b;
    a.wait(cancel()).await.unwrap();
    b.wait(cancel()).await.unwrap();
    until(|| p.shared.budget.operations.available_permits() == 2).await;
    let c = p
        .submit(OperationData::new(vec![vec![3]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(c.sequence, 3);
    assert_eq!(p.progress().unwrap().last, 2);
    p.abort();
    s.abort();
}

fn endpoint(transport: &PeerTransport, partition: Uuid, replica: i64) -> Endpoint {
    Endpoint {
        address: transport.address().to_owned(),
        actor: Actor {
            partition,
            replica,
            incarnation: Uuid::new_v4(),
        },
        transport_id: None,
    }
}

fn raw_send(transport: &PeerTransport, from: &Endpoint, to: &Engine, body: Body) {
    transport
        .send(Packet {
            from: from.clone(),
            to: to.endpoint.actor,
            body,
        })
        .unwrap();
}

async fn initialized_raw_secondary(
    provider: Arc<dyn StateProvider>,
) -> (Arc<Engine>, PeerTransport, Endpoint, Incoming) {
    let partition = Uuid::new_v4();
    let secondary = engine(partition, 2, provider);
    secondary
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let (transport, incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let primary = endpoint(&transport, partition, 1);
    transport.register_peer(&secondary.endpoint()).unwrap();
    raw_send(
        &transport,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch: EPOCH,
            target: 2,
            start: 1,
        },
    );
    until(|| secondary.snapshot().epoch == Some(EPOCH)).await;
    // The independently sequenced empty EOS is not delivered as provider data.
    raw_send(
        &transport,
        &primary,
        &secondary,
        Body::Copy {
            epoch: EPOCH,
            target: 2,
            sequence: 1,
            data: vec![],
            last: true,
        },
    );
    until(|| secondary.snapshot().copy_done).await;
    (secondary, transport, primary, incoming)
}

fn replication(epoch: Epoch, original: Epoch, sequence: i64, data: u8) -> Body {
    Body::Replication {
        primary_epoch: epoch,
        operation_epoch: original,
        operations: vec![WireOperation {
            sequence,
            kind: 1,
            atomic_group: -1,
            data: vec![vec![data]],
        }],
        batch_end: sequence,
        completed: 0,
    }
}

struct CallbackGuard {
    active: Arc<AtomicUsize>,
    disposed: Arc<AtomicUsize>,
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.disposed.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct ControlledProvider {
    base: Provider,
    epoch_mode: AtomicUsize,
    loss_mode: AtomicUsize,
    changed: AtomicBool,
    recovered: AtomicI64,
    active: Arc<AtomicUsize>,
    disposed: Arc<AtomicUsize>,
    entered: AtomicUsize,
    release: tokio::sync::Notify,
}

impl ControlledProvider {
    async fn callback(&self, mode: usize) -> Result<()> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.active.fetch_add(1, Ordering::AcqRel);
        let _guard = CallbackGuard {
            active: self.active.clone(),
            disposed: self.disposed.clone(),
        };
        match mode {
            1 => Err(Error::Protocol("injected provider failure")),
            2 => panic!("injected provider panic"),
            3 => {
                self.release.notified().await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl StateProvider for ControlledProvider {
    fn last_committed(&self) -> Result<i64> {
        self.base.last_committed()
    }
    fn update_epoch(&self, epoch: Epoch, previous: i64, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.callback(self.epoch_mode.load(Ordering::Acquire))
                .await?;
            self.base.epochs.lock().unwrap().push((epoch, previous));
            self.base.last.store(previous, Ordering::Release);
            Ok(())
        })
    }
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            self.callback(self.loss_mode.load(Ordering::Acquire))
                .await?;
            let changed = self.changed.load(Ordering::Acquire);
            if changed {
                self.base
                    .last
                    .store(self.recovered.load(Ordering::Acquire), Ordering::Release);
            }
            Ok(changed)
        })
    }
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        self.base.copy_context()
    }
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        self.base.copy_state(up_to, context)
    }
}

struct FaultNativeView {
    faults: Mutex<Vec<mssf_runtime::FaultType>>,
    pending: bool,
    active: Arc<AtomicUsize>,
    disposed: Arc<AtomicUsize>,
}

impl FaultNativeView {
    fn new(pending: bool) -> Self {
        Self {
            faults: Mutex::new(Vec::new()),
            pending,
            active: Arc::new(AtomicUsize::new(0)),
            disposed: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl NativeControlView for FaultNativeView {
    fn status(&self) -> Result<ReplicaStatus> {
        Ok(primary_status())
    }
    fn report_fault(
        &self,
        fault: mssf_runtime::FaultType,
    ) -> ServiceFuture<'_, mssf_runtime::FaultDelivery> {
        Box::pin(async move {
            self.faults.lock().unwrap().push(fault);
            self.active.fetch_add(1, Ordering::AcqRel);
            let _capture = CallbackGuard {
                active: self.active.clone(),
                disposed: self.disposed.clone(),
            };
            if self.pending {
                std::future::pending::<()>().await;
            }
            Ok(mssf_runtime::FaultDelivery {
                fault,
                attempts: 1,
                last_error: None,
                cancelled: false,
                superseded_transient: false,
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn internal_provider_fault_notifies_once_and_pending_receipt_cannot_block_joined_abort() {
    for mode in [1, 2] {
        for pending in [false, true] {
            let provider = Arc::new(ControlledProvider::default());
            let provider_weak = Arc::downgrade(&provider);
            let view = Arc::new(FaultNativeView::new(pending));
            let partition = Uuid::new_v4();
            let secondary = Engine::bind_with_native_control(
                Actor {
                    partition,
                    replica: 2,
                    incarnation: Uuid::new_v4(),
                },
                "127.0.0.1:0",
                provider.clone(),
                Settings::default(),
                Some(view.clone()),
            )
            .unwrap();
            secondary
                .change_role(EPOCH, ReplicaRole::Idle, cancel())
                .await
                .unwrap();
            let (transport, _incoming) =
                PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
            let primary = endpoint(&transport, partition, 1);
            transport.register_peer(&secondary.endpoint()).unwrap();
            raw_send(
                &transport,
                &primary,
                &secondary,
                Body::StartCopy {
                    epoch: EPOCH,
                    target: 2,
                    start: 1,
                },
            );
            until(|| secondary.snapshot().epoch == Some(EPOCH)).await;
            raw_send(
                &transport,
                &primary,
                &secondary,
                Body::Copy {
                    epoch: EPOCH,
                    target: 2,
                    sequence: 1,
                    data: vec![],
                    last: true,
                },
            );
            until(|| secondary.snapshot().copy_done).await;
            let previous_disposed = provider.disposed.load(Ordering::Acquire);
            provider.epoch_mode.store(mode, Ordering::Release);
            // No native control callback is active: peer data triggers the
            // internal epoch barrier whose provider completion faults the owner.
            raw_send(
                &transport,
                &primary,
                &secondary,
                replication(NEXT_EPOCH, NEXT_EPOCH, 1, 7),
            );
            until(|| view.faults.lock().unwrap().len() == 1).await;
            assert!(secondary.snapshot().fault.is_some());
            assert!(!secondary.snapshot().ready);
            let active = provider.active.clone();
            let disposed = provider.disposed.clone();
            drop(provider);
            let closing = secondary.clone();
            timeout(
                std::time::Duration::from_secs(2),
                tokio::task::spawn_blocking(move || closing.abort()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                *view.faults.lock().unwrap(),
                vec![mssf_runtime::FaultType::Transient]
            );
            assert_eq!(view.active.load(Ordering::Acquire), 0);
            assert_eq!(view.disposed.load(Ordering::Acquire), 1);
            assert_eq!(active.load(Ordering::Acquire), 0);
            assert_eq!(disposed.load(Ordering::Acquire), previous_disposed + 1);
            assert!(provider_weak.upgrade().is_none());
            assert!(secondary.snapshot().closed);
            secondary.close(cancel()).await.unwrap();
            assert_eq!(view.faults.lock().unwrap().len(), 1);
            transport.shutdown();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_shutdown_never_notifies_fault_and_standalone_has_no_native_view() {
    for abort in [false, true] {
        let view = Arc::new(FaultNativeView::new(true));
        let engine = Engine::bind_with_native_control(
            Actor {
                partition: Uuid::new_v4(),
                replica: 1,
                incarnation: Uuid::new_v4(),
            },
            "127.0.0.1:0",
            Arc::new(Provider::default()),
            Settings::default(),
            Some(view.clone()),
        )
        .unwrap();
        if abort {
            engine.abort();
        } else {
            engine.close(cancel()).await.unwrap();
        }
        assert!(view.faults.lock().unwrap().is_empty());
        assert_eq!(view.disposed.load(Ordering::Acquire), 0);
    }
    let standalone = engine(Uuid::new_v4(), 1, Arc::new(Provider::default()));
    assert!(standalone.native_control.is_none());
    standalone.abort();
    let view = Arc::new(FaultNativeView::new(true));
    let startup_error = Engine::bind_with_native_control(
        Actor {
            partition: Uuid::new_v4(),
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        Arc::new(Provider {
            last: AtomicI64::new(-1),
            ..Provider::default()
        }),
        Settings::default(),
        Some(view.clone()),
    );
    assert!(startup_error.is_err());
    assert!(view.faults.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_epoch_barrier_waits_for_explicit_apply_then_provider_disposal() {
    let provider = Arc::new(ControlledProvider::default());
    let (secondary, transport, primary, _incoming) =
        initialized_raw_secondary(provider.clone()).await;
    let next = Epoch {
        configuration: EPOCH.configuration * 2,
        ..EPOCH
    };
    provider.epoch_mode.store(3, Ordering::Release);
    let initial_calls = provider.entered.load(Ordering::Acquire);
    raw_send(
        &transport,
        &primary,
        &secondary,
        replication(next, EPOCH, 1, 1),
    );
    raw_send(
        &transport,
        &primary,
        &secondary,
        replication(next, next, 2, 2),
    );
    let stream = secondary.replication_stream();
    let first = stream.next(cancel()).await.unwrap().unwrap();
    until(|| secondary.snapshot().progress.received == 2).await;
    assert_eq!(provider.entered.load(Ordering::Acquire), initial_calls);
    assert!(
        timeout(std::time::Duration::from_millis(40), stream.next(cancel()))
            .await
            .is_err()
    );
    provider.base.last.store(1, Ordering::Release);
    first.acknowledge().unwrap();
    until(|| provider.active.load(Ordering::Acquire) == 1).await;
    assert!(
        timeout(std::time::Duration::from_millis(40), stream.next(cancel()))
            .await
            .is_err()
    );
    // No second epoch/loss callback is admitted behind an active barrier.
    assert!(secondary.update_epoch(next, cancel()).await.is_err());
    provider.release.notify_one();
    let second = timeout(std::time::Duration::from_secs(2), stream.next(cancel()))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.sequence, 2);
    assert_eq!(provider.active.load(Ordering::Acquire), 0);
    assert!(provider.base.epochs.lock().unwrap().contains(&(next, 1)));
    provider.base.last.store(2, Ordering::Release);
    second.acknowledge().unwrap();
    until(|| secondary.snapshot().progress.applied == 2).await;
    secondary.abort();
    transport.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_epoch_and_data_loss_errors_panics_deadlines_are_terminal_and_disposed() {
    for loss in [false, true] {
        for mode in [1, 2, 3] {
            let provider = Arc::new(ControlledProvider::default());
            let settings = Settings {
                callback_timeout: std::time::Duration::from_millis(100),
                ..Settings::default()
            };
            let e = Engine::bind(
                Actor {
                    partition: Uuid::new_v4(),
                    replica: 1,
                    incarnation: Uuid::new_v4(),
                },
                "127.0.0.1:0",
                provider.clone(),
                settings,
            )
            .unwrap();
            e.change_role(EPOCH, ReplicaRole::Primary, cancel())
                .await
                .unwrap();
            e.update_current_configuration(configuration(vec![], true, 1), cancel())
                .await
                .unwrap();
            let result = if loss {
                provider.loss_mode.store(mode, Ordering::Release);
                e.on_data_loss(cancel()).await.map(|_| ())
            } else {
                provider.epoch_mode.store(mode, Ordering::Release);
                e.update_epoch(
                    Epoch {
                        configuration: EPOCH.configuration * 2,
                        ..EPOCH
                    },
                    cancel(),
                )
                .await
            };
            assert!(result.is_err());
            until(|| e.snapshot().closed).await;
            assert!(!e.snapshot().ready);
            assert!(e.snapshot().fault.is_some());
            assert_eq!(provider.active.load(Ordering::Acquire), 0);
            assert_eq!(provider.disposed.load(Ordering::Acquire), 2);
            assert!(e
                .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
                .await
                .is_err());
            e.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_epoch_drain_faults_and_changed_data_loss_resets_assignment_from_provider() {
    let provider = Arc::new(Provider::default());
    let (s, transport, primary, _incoming) = initialized_raw_secondary(provider).await;
    raw_send(&transport, &primary, &s, replication(EPOCH, EPOCH, 1, 1));
    let outstanding = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    let cancellation = cancel();
    let update = s.update_epoch(
        Epoch {
            configuration: EPOCH.configuration * 2,
            ..EPOCH
        },
        cancellation.clone(),
    );
    tokio::pin!(update);
    assert!(timeout(std::time::Duration::from_millis(40), &mut update)
        .await
        .is_err());
    cancellation.cancel();
    assert!(update.await.is_err());
    until(|| s.snapshot().closed).await;
    assert!(outstanding.acknowledge().is_err());
    s.abort();
    transport.shutdown();

    let provider = Arc::new(ControlledProvider::default());
    let p = engine(Uuid::new_v4(), 1, provider.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    for expected in 1..=4 {
        let mut write = p
            .submit(
                OperationData::new(vec![vec![expected as u8]]).unwrap(),
                cancel(),
            )
            .await
            .unwrap();
        provider.base.last.store(expected, Ordering::Release);
        write.wait(cancel()).await.unwrap();
    }
    assert!(!p.on_data_loss(cancel()).await.unwrap());
    assert_eq!(p.snapshot().progress.assigned, 4);
    provider.recovered.store(1, Ordering::Release);
    provider.changed.store(true, Ordering::Release);
    assert!(p.on_data_loss(cancel()).await.unwrap());
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    let write = p
        .submit(OperationData::new(vec![vec![9]]).unwrap(), cancel())
        .await
        .unwrap();
    assert_eq!(write.sequence, 2);
    p.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retransmission_recovers_after_actual_tcp_disconnect_without_send_as_commit() {
    let partition = Uuid::new_v4();
    let p = engine(partition, 1, Arc::new(Provider::default()));
    let (peer, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let destination = endpoint(&peer, partition, 2);
    let mut remote = descriptor(&p);
    remote.replica_id = 2;
    remote.address = destination.to_string();
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![remote], true, 2), cancel())
        .await
        .unwrap();
    let mut write = p
        .submit(OperationData::new(vec![vec![4, 5, 6]]).unwrap(), cancel())
        .await
        .unwrap();
    let first = timeout(std::time::Duration::from_secs(3), incoming.data.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.body, Body::Replication { .. }));
    assert_eq!(p.snapshot().progress.committed, 0);
    let address = peer.address().to_owned();
    peer.shutdown();
    let (replacement, mut incoming) = PeerTransport::bind(&address, Settings::default()).unwrap();
    replacement.register_peer(&p.endpoint()).unwrap();
    let retry = timeout(std::time::Duration::from_secs(4), incoming.data.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(retry.body, Body::Replication { ref operations, .. } if operations[0].sequence == 1 && operations[0].data == vec![vec![4,5,6]])
    );
    raw_send(
        &replacement,
        &destination,
        &p,
        Body::Ack {
            received: 1,
            applied: 1,
            copy_received: -1,
            copy_applied: -1,
            error: 0,
        },
    );
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    p.abort();
    replacement.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_wire_gaps_duplicates_stale_epochs_actors_and_impossible_receipts() {
    let provider = Arc::new(Provider::default());
    let (s, transport, primary, _incoming) = initialized_raw_secondary(provider.clone()).await;
    raw_send(&transport, &primary, &s, replication(EPOCH, EPOCH, 2, 2));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(s.snapshot().progress.received, 0);
    let older = Epoch {
        configuration: 0,
        ..EPOCH
    };
    raw_send(&transport, &primary, &s, replication(older, older, 1, 1));
    let mut impostor = primary.clone();
    impostor.actor.incarnation = Uuid::new_v4();
    raw_send(&transport, &impostor, &s, replication(EPOCH, EPOCH, 1, 1));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(s.snapshot().progress.received, 0);
    raw_send(&transport, &primary, &s, replication(EPOCH, EPOCH, 1, 1));
    raw_send(&transport, &primary, &s, replication(EPOCH, EPOCH, 2, 2));
    let stream = s.replication_stream();
    let first = stream.next(cancel()).await.unwrap().unwrap();
    let second = stream.next(cancel()).await.unwrap().unwrap();
    assert_eq!((first.sequence, second.sequence), (1, 2));
    until(|| s.snapshot().progress.received == 2).await;
    second.acknowledge().unwrap();
    assert_eq!(s.progress().unwrap().last, 2);
    assert_eq!(s.snapshot().progress.applied, 0);
    provider.last.store(2, Ordering::Release);
    first.acknowledge().unwrap();
    until(|| s.snapshot().progress.applied == 2).await;
    s.abort();
    transport.shutdown();

    let (p, s, _) = pair().await;
    p.update_current_configuration(configuration(vec![descriptor(&s)], true, 2), cancel())
        .await
        .unwrap();
    let accepted = p
        .submit(OperationData::new(vec![vec![1]]).unwrap(), cancel())
        .await
        .unwrap();
    until(|| s.snapshot().progress.received == 1).await;
    let ack = |received, applied| Body::Ack {
        received,
        applied,
        copy_received: -1,
        copy_applied: -1,
        error: 0,
    };
    s.transport
        .send(Packet {
            from: s.endpoint(),
            to: p.endpoint.actor,
            body: ack(100, 100),
        })
        .unwrap();
    let mut old = s.endpoint();
    old.actor.incarnation = Uuid::new_v4();
    s.transport
        .send(Packet {
            from: old,
            to: p.endpoint.actor,
            body: ack(1, 1),
        })
        .unwrap();
    s.transport
        .send(Packet {
            from: s.endpoint(),
            to: p.endpoint.actor,
            body: ack(0, 0),
        })
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert_eq!(p.snapshot().progress.committed, 0);
    assert_eq!(p.snapshot().peers[0].received, 1);
    drop(accepted);
    p.abort();
    s.abort();
}

// R1-R5 regressions promoted from reviews/phase2_repros.rs. The unsafe swap
// now exercises the explicit rejection policy rather than assuming success.
async fn initial_primary(primary: &Engine) {
    primary
        .change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    primary
        .update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
}

async fn build_memory(primary: &Engine, secondary: &Arc<Engine>, provider: &Arc<MemoryProvider>) {
    secondary
        .change_role(EPOCH, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let target = secondary.clone();
    let provider = provider.clone();
    let copier = tokio::spawn(async move {
        while let Some(op) = target.copy_stream().next(cancel()).await.unwrap() {
            provider.apply_copy(&op.data).unwrap();
            op.acknowledge().unwrap();
        }
    });
    primary
        .build_replica(descriptor(secondary), cancel())
        .await
        .unwrap();
    copier.await.unwrap();
    secondary
        .change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
}

fn mutation(id: &str, value: &str) -> OperationData {
    Mutation {
        id: id.into(),
        key: "key".into(),
        value: value.into(),
    }
    .operation()
    .unwrap()
}

async fn apply_memory(engine: &Engine, provider: &MemoryProvider, sequence: i64) -> Operation {
    let op = timeout(
        std::time::Duration::from_secs(2),
        engine.replication_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(op.sequence, sequence);
    provider.apply_replication(op.sequence, &op.data).unwrap();
    op.acknowledge().unwrap();
    op
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn demotion_does_not_turn_an_ambiguous_tail_into_a_false_apply_ack() {
    let partition = Uuid::new_v4();
    let ap = MemoryProvider::new();
    let bp = MemoryProvider::new();
    let a = engine(partition, 1, ap.clone());
    let b = Engine::bind(
        Actor {
            partition,
            replica: 2,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        bp.clone(),
        Settings {
            provider_operations: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    initial_primary(&a).await;
    build_memory(&a, &b, &bp).await;
    a.update_current_configuration(configuration(vec![descriptor(&b)], true, 2), cancel())
        .await
        .unwrap();
    let one = mutation("one", "committed");
    let mut write = a.submit(one.clone(), cancel()).await.unwrap();
    ap.apply_replication(write.sequence, &one).unwrap();
    let held = apply_memory(&b, &bp, 1).await;
    write.wait(cancel()).await.unwrap();

    // Keep the already-ACKed capture, exactly as in the review reproduction:
    // B receives the old tail but its one-capture budget prevents dispatch.
    let old = mutation("old-tail", "old");
    let mut ambiguous = a.submit(old.clone(), cancel()).await.unwrap();
    ap.apply_replication(2, &old).unwrap();
    until(|| b.snapshot().progress.received == 2).await;
    assert_eq!(b.snapshot().progress.applied, 1);
    assert_eq!(a.snapshot().progress.committed, 1);
    assert!(a
        .change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .is_err());
    assert_eq!(a.snapshot().role, ReplicaRole::Primary);
    assert_eq!(a.snapshot().epoch, Some(EPOCH));
    assert_eq!(a.snapshot().progress.assigned, 2);
    assert!(a.snapshot().owned_operations > 0);
    assert!(a.snapshot().fault.is_none());

    b.change_role(NEXT_EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    until(|| b.snapshot().role == ReplicaRole::Primary).await;
    assert_eq!(b.snapshot().progress.assigned, 1);
    let mut current = configuration(vec![descriptor(&a)], true, 2);
    current.epoch = NEXT_EPOCH;
    b.update_current_configuration(current, cancel())
        .await
        .unwrap();
    let replacement = mutation("new-tail", "new");
    let mut committed = b.submit(replacement.clone(), cancel()).await.unwrap();
    assert_eq!(committed.sequence, 2);
    bp.apply_replication(2, &replacement).unwrap();
    // The divergent real maps may exist as ambiguous local history, but can
    // never acquire the successful two-member result from the original repro.
    assert_ne!(ap.snapshot().values, bp.snapshot().values);
    assert!(timeout(
        std::time::Duration::from_millis(150),
        committed.wait(cancel())
    )
    .await
    .is_err());
    assert!(timeout(
        std::time::Duration::from_millis(40),
        ambiguous.wait(cancel())
    )
    .await
    .is_err());
    assert_eq!(a.snapshot().progress.committed, 1);
    assert_eq!(b.snapshot().progress.committed, 1);
    assert_eq!(ap.snapshot().values["key"], "old");
    drop(held);
    a.abort();
    b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_demotion_preserves_accepted_completion_for_drain_and_retry() {
    let partition = Uuid::new_v4();
    let ap = MemoryProvider::new();
    let bp = MemoryProvider::new();
    let a = engine(partition, 1, ap.clone());
    let b = engine(partition, 2, bp.clone());
    initial_primary(&a).await;
    build_memory(&a, &b, &bp).await;
    a.update_current_configuration(configuration(vec![descriptor(&b)], true, 2), cancel())
        .await
        .unwrap();
    let one = mutation("accepted", "one");
    let mut write = a.submit(one.clone(), cancel()).await.unwrap();
    ap.apply_replication(1, &one).unwrap();
    until(|| b.snapshot().progress.received == 1).await;
    assert!(a
        .change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .is_err());
    let canceled = cancel();
    canceled.cancel();
    assert!(write.wait(canceled).await.is_err());
    drop(apply_memory(&b, &bp, 1).await);
    assert_eq!(
        timeout(std::time::Duration::from_secs(2), write.wait(cancel()))
            .await
            .unwrap()
            .unwrap(),
        1
    );
    a.change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    until(|| a.snapshot().role == ReplicaRole::Secondary).await;
    assert_eq!(a.snapshot().progress.applied, 1);
    assert_eq!(ap.snapshot().values, bp.snapshot().values);
    assert_eq!(ap.snapshot().results, bp.snapshot().results);
    a.abort();
    b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuing_third_secondary_accepts_the_new_primary() {
    let partition = Uuid::new_v4();
    let ap = MemoryProvider::new();
    let bp = MemoryProvider::new();
    let cp = MemoryProvider::new();
    let a = engine(partition, 1, ap.clone());
    let b = engine(partition, 2, bp.clone());
    let c = engine(partition, 3, cp.clone());
    initial_primary(&a).await;
    build_memory(&a, &b, &bp).await;
    build_memory(&a, &c, &cp).await;
    a.update_current_configuration(
        configuration(vec![descriptor(&b), descriptor(&c)], true, 3),
        cancel(),
    )
    .await
    .unwrap();
    let one = mutation("one", "committed");
    let mut write = a.submit(one.clone(), cancel()).await.unwrap();
    ap.apply_replication(1, &one).unwrap();
    drop(apply_memory(&b, &bp, 1).await);
    drop(apply_memory(&c, &cp, 1).await);
    write.wait(cancel()).await.unwrap();
    a.change_role(NEXT_EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    b.change_role(NEXT_EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    c.update_epoch(NEXT_EPOCH, cancel()).await.unwrap();
    let mut current = configuration(vec![descriptor(&a), descriptor(&c)], true, 3);
    current.epoch = NEXT_EPOCH;
    b.update_current_configuration(current, cancel())
        .await
        .unwrap();
    let two = mutation("two", "new-primary");
    let mut write = b.submit(two.clone(), cancel()).await.unwrap();
    bp.apply_replication(2, &two).unwrap();
    drop(apply_memory(&a, &ap, 2).await);
    drop(apply_memory(&c, &cp, 2).await);
    assert_eq!(
        timeout(std::time::Duration::from_secs(2), write.wait(cancel()))
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert_eq!(ap.snapshot().values, bp.snapshot().values);
    assert_eq!(cp.snapshot().values, bp.snapshot().values);
    assert_eq!(ap.snapshot().results, cp.snapshot().results);
    until(|| a.snapshot().progress.applied == 2 && c.snapshot().progress.applied == 2).await;
    assert_eq!(a.snapshot().progress.applied, 2);
    assert_eq!(c.snapshot().progress.applied, 2);
    a.abort();
    b.abort();
    c.abort();
}

async fn copying_raw_secondary(
    epoch: Epoch,
    provider: Arc<dyn StateProvider>,
    settings: Settings,
) -> (Arc<Engine>, PeerTransport, Endpoint, Incoming) {
    let partition = Uuid::new_v4();
    let secondary = Engine::bind(
        Actor {
            partition,
            replica: 2,
            incarnation: Uuid::new_v4(),
        },
        "127.0.0.1:0",
        provider,
        settings,
    )
    .unwrap();
    secondary
        .change_role(epoch, ReplicaRole::Idle, cancel())
        .await
        .unwrap();
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let primary = endpoint(&network, partition, 1);
    network.register_peer(&secondary.endpoint()).unwrap();
    raw_send(
        &network,
        &primary,
        &secondary,
        Body::StartCopy {
            epoch,
            target: 2,
            start: 1,
        },
    );
    timeout(std::time::Duration::from_secs(2), incoming.control.recv())
        .await
        .unwrap()
        .unwrap();
    (secondary, network, primary, incoming)
}

fn copy_item(epoch: Epoch, sequence: i64, data: Vec<Vec<u8>>, last: bool) -> Body {
    Body::Copy {
        epoch,
        target: 2,
        sequence,
        data,
        last,
    }
}

fn replication_data(primary: Epoch, original: Epoch, sequence: i64, data: &OperationData) -> Body {
    Body::Replication {
        primary_epoch: primary,
        operation_epoch: original,
        operations: vec![WireOperation {
            sequence,
            kind: 1,
            atomic_group: -1,
            data: data.to_segments(),
        }],
        batch_end: sequence,
        completed: 0,
    }
}

async fn retry_raw_operation(
    secondary: &Engine,
    network: &PeerTransport,
    primary: &Endpoint,
    body: Body,
    stream: Arc<OperationStream>,
) -> Operation {
    let next = stream.next(cancel());
    tokio::pin!(next);
    timeout(std::time::Duration::from_secs(5), async {
        let mut retry = interval(std::time::Duration::from_millis(20));
        loop {
            tokio::select! {
                result = &mut next => return result.unwrap().unwrap(),
                _ = retry.tick() => raw_send(network, primary, secondary, body.clone()),
            }
        }
    })
    .await
    .unwrap()
}

async fn retry_raw_eos(secondary: &Engine, network: &PeerTransport, primary: &Endpoint, end: i64) {
    timeout(std::time::Duration::from_secs(5), async {
        while !secondary.snapshot().copy_done {
            raw_send(
                network,
                primary,
                secondary,
                copy_item(EPOCH, end, vec![], true),
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_rebinding_requires_node_accepted_newer_epoch_and_fences_old_actor() {
    let provider = Arc::new(Provider::default());
    let (s, old_network, old_primary, _old_incoming) =
        initialized_raw_secondary(provider.clone()).await;
    s.change_role(EPOCH, ReplicaRole::Secondary, cancel())
        .await
        .unwrap();
    let (new_network, _new_incoming) =
        PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    let new_primary = endpoint(&new_network, s.endpoint.actor.partition, 3);
    new_network.register_peer(&s.endpoint()).unwrap();
    for (epoch, original) in [(EPOCH, EPOCH), (NEXT_EPOCH, EPOCH)] {
        raw_send(
            &new_network,
            &new_primary,
            &s,
            replication(epoch, original, 1, 3),
        );
    }
    assert!(timeout(
        std::time::Duration::from_millis(60),
        s.replication_stream().next(cancel())
    )
    .await
    .is_err());
    assert_eq!(s.snapshot().progress.received, 0);
    s.update_epoch(NEXT_EPOCH, cancel()).await.unwrap();
    raw_send(
        &old_network,
        &old_primary,
        &s,
        replication(EPOCH, EPOCH, 1, 1),
    );
    let future = Epoch {
        configuration: NEXT_EPOCH.configuration + EPOCH.configuration,
        ..EPOCH
    };
    raw_send(
        &new_network,
        &new_primary,
        &s,
        replication(future, EPOCH, 1, 3),
    );
    assert!(timeout(
        std::time::Duration::from_millis(60),
        s.replication_stream().next(cancel())
    )
    .await
    .is_err());
    assert_eq!(s.snapshot().progress.received, 0);
    raw_send(
        &new_network,
        &new_primary,
        &s,
        replication(NEXT_EPOCH, EPOCH, 1, 3),
    );
    let op = timeout(
        std::time::Duration::from_secs(2),
        s.replication_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(op.data.to_segments(), vec![vec![3]]);
    provider.last.store(1, Ordering::Release);
    op.acknowledge().unwrap();
    until(|| s.snapshot().progress.applied == 1).await;
    // Even the formerly legitimate actor cannot take the binding back by
    // claiming the same accepted epoch after the new actor has been selected.
    raw_send(
        &old_network,
        &old_primary,
        &s,
        replication(NEXT_EPOCH, EPOCH, 2, 1),
    );
    assert!(timeout(
        std::time::Duration::from_millis(60),
        s.replication_stream().next(cancel())
    )
    .await
    .is_err());
    raw_send(
        &new_network,
        &new_primary,
        &s,
        replication(NEXT_EPOCH, EPOCH, 2, 3),
    );
    let op = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(op.sequence, 2);
    assert_eq!(op.data.to_segments(), vec![vec![3]]);
    assert!(s.snapshot().fault.is_none());
    s.abort();
    old_network.shutdown();
    new_network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_operation_epoch_may_precede_start_copy_primary_epoch() {
    // Exercise the real provider's monotonic epoch check, not a permissive
    // provider that accepts an accidental E2 -> E1 callback regression.
    let future = Epoch {
        configuration: NEXT_EPOCH.configuration + EPOCH.configuration,
        ..EPOCH
    };
    for (original, advance_node) in [
        (EPOCH, false),
        (EPOCH, true),
        (NEXT_EPOCH, false),
        (future, true),
    ] {
        let provider = MemoryProvider::new();
        let (s, network, primary, _incoming) =
            copying_raw_secondary(NEXT_EPOCH, provider.clone(), Settings::default()).await;
        assert_eq!(provider.snapshot().epoch_configuration, 0);
        raw_send(
            &network,
            &primary,
            &s,
            copy_item(NEXT_EPOCH, 1, vec![], true),
        );
        until(|| s.snapshot().copy_done).await;
        // A node epoch control (including a duplicate) is not a provider
        // original-epoch barrier. Stale primary packets are still rejected.
        let accepted = if advance_node { future } else { NEXT_EPOCH };
        s.update_epoch(accepted, cancel()).await.unwrap();
        assert_eq!(provider.snapshot().epoch_configuration, 0);
        let one = mutation("historical", "one");
        let stale = if advance_node { NEXT_EPOCH } else { EPOCH };
        raw_send(
            &network,
            &primary,
            &s,
            replication_data(stale, original.min(stale), 1, &one),
        );
        assert!(timeout(
            std::time::Duration::from_millis(40),
            s.replication_stream().next(cancel())
        )
        .await
        .is_err());
        raw_send(
            &network,
            &primary,
            &s,
            replication_data(accepted, original, 1, &one),
        );
        let first = timeout(
            std::time::Duration::from_secs(2),
            s.replication_stream().next(cancel()),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(first.epoch, original);
        assert_eq!(first.sequence, 1);
        assert_eq!(
            provider.snapshot().epoch_configuration,
            if original > NEXT_EPOCH {
                original.configuration
            } else {
                0
            }
        );
        let two = mutation("successor", "two");
        raw_send(
            &network,
            &primary,
            &s,
            replication_data(future, future, 2, &two),
        );
        until(|| s.snapshot().progress.received == 2).await;
        if original < future {
            assert!(timeout(
                std::time::Duration::from_millis(40),
                s.replication_stream().next(cancel())
            )
            .await
            .is_err());
            assert_eq!(provider.snapshot().epoch_configuration, 0);
        }
        provider.apply_replication(1, &first.data).unwrap();
        first.acknowledge().unwrap();
        drop(first);
        let second = timeout(
            std::time::Duration::from_secs(2),
            s.replication_stream().next(cancel()),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(second.sequence, 2);
        assert_eq!(
            provider.snapshot().epoch_configuration,
            future.configuration
        );
        provider.apply_replication(2, &second.data).unwrap();
        second.acknowledge().unwrap();
        until(|| s.snapshot().progress.applied == 2).await;
        assert_eq!(provider.snapshot().values["key"], "two");
        assert!(s.snapshot().fault.is_none());
        s.abort();
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_replication_cannot_starve_copy_eos_forever() {
    let (s, network, primary, _incoming) = copying_raw_secondary(
        EPOCH,
        Arc::new(Provider::default()),
        Settings {
            queue_operations: 2,
            ..Settings::default()
        },
    )
    .await;
    for sequence in 1..=2 {
        raw_send(
            &network,
            &primary,
            &s,
            replication(EPOCH, EPOCH, sequence, 42),
        );
    }
    until(|| s.snapshot().progress.received == 1).await;
    assert_eq!(s.shared.budget.operations.available_permits(), 1);
    for _ in 0..3 {
        raw_send(&network, &primary, &s, copy_item(EPOCH, 1, vec![], true));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    until(|| s.snapshot().copy_done).await;
    assert_eq!(s.snapshot().progress.applied, 0);
    assert_eq!(s.snapshot().progress.received, 1);
    assert!(s.snapshot().owned_operations <= 2);
    assert!(!s.snapshot().closed);
    raw_send(&network, &primary, &s, replication(EPOCH, EPOCH, 2, 42));
    until(|| s.snapshot().progress.received == 2).await;
    let op = s
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(op.sequence, 1);
    s.abort();
    network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_replication_cannot_starve_copy_items_under_operation_or_byte_exhaustion() {
    for (operation_limit, bytes_exhausted) in [(1, false), (2, false), (8, true)] {
        let settings = if bytes_exhausted {
            Settings {
                operation_bytes: 2048,
                queue_bytes: 4096,
                queue_operations: operation_limit,
                ..Settings::default()
            }
        } else {
            Settings {
                queue_operations: operation_limit,
                ..Settings::default()
            }
        };
        let bytes = if bytes_exhausted { 1800 } else { 1 };
        let data = OperationData::new(vec![vec![42; bytes]]).unwrap();
        let provider = Arc::new(Provider::default());
        let (s, network, primary, _incoming) =
            copying_raw_secondary(EPOCH, provider.clone(), settings.clone()).await;
        let total = operation_limit.min(2) as i64;
        let staged = if bytes_exhausted {
            0
        } else {
            operation_limit.saturating_sub(1).min(2) as i64
        };
        let sending_epoch = if bytes_exhausted { NEXT_EPOCH } else { EPOCH };
        for sequence in 1..=total {
            raw_send(
                &network,
                &primary,
                &s,
                replication_data(sending_epoch, EPOCH, sequence, &data),
            );
        }
        let copy = retry_raw_operation(
            &s,
            &network,
            &primary,
            copy_item(EPOCH, 1, data.to_segments(), false),
            s.copy_stream(),
        )
        .await;
        assert_eq!(copy.data, data);
        assert_eq!(s.snapshot().progress.received, staged);
        assert!(!s.snapshot().copy_done);
        assert_eq!(s.snapshot().owned_operations, staged as usize + 1);
        assert!(s.snapshot().owned_operations <= settings.queue_operations);
        assert!(s.snapshot().owned_bytes <= settings.queue_bytes);
        raw_send(&network, &primary, &s, copy_item(EPOCH, 2, vec![], true));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!s.snapshot().copy_done);
        assert_eq!(s.snapshot().progress.applied, 0);
        copy.acknowledge().unwrap();
        until(|| s.snapshot().dispatched_unacknowledged == 0).await;
        assert!(s.snapshot().owned_bytes >= data.retained_bytes());
        drop(copy);
        retry_raw_eos(&s, &network, &primary, 2).await;
        assert!(s.copy_stream().next(cancel()).await.unwrap().is_none());
        for sequence in 1..=total {
            let op = retry_raw_operation(
                &s,
                &network,
                &primary,
                replication_data(sending_epoch, EPOCH, sequence, &data),
                s.replication_stream(),
            )
            .await;
            assert_eq!(op.sequence, sequence);
            assert_eq!(op.data, data);
            provider.last.store(sequence, Ordering::Release);
            op.acknowledge().unwrap();
        }
        until(|| s.snapshot().progress.applied == total).await;
        assert!(s.snapshot().owned_operations <= settings.queue_operations);
        assert!(s.snapshot().owned_bytes <= settings.queue_bytes);
        assert!(s.snapshot().fault.is_none());
        s.abort();
        assert_eq!(s.snapshot().owned_operations, 0);
        assert_eq!(s.snapshot().owned_bytes, 0);
        network.shutdown();
    }
}

async fn aggregate_copy_snapshot(settings: Settings, bytes: usize) -> EngineSnapshot {
    let (secondary, network, primary, _incoming) =
        copying_raw_secondary(EPOCH, MemoryProvider::new(), settings).await;
    let data = OperationData::new(vec![vec![42; bytes]]).unwrap();
    for sequence in 1..=2 {
        raw_send(
            &network,
            &primary,
            &secondary,
            replication_data(EPOCH, EPOCH, sequence, &data),
        );
    }
    let capture = retry_raw_operation(
        &secondary,
        &network,
        &primary,
        copy_item(EPOCH, 1, data.to_segments(), false),
        secondary.copy_stream(),
    )
    .await;
    let snapshot = secondary.snapshot();
    assert_eq!(capture.data, data);
    assert!(!snapshot.copy_done);
    assert_eq!(snapshot.progress.applied, 0);
    drop(capture);
    secondary.abort();
    assert_eq!(secondary.snapshot().owned_operations, 0);
    assert_eq!(secondary.snapshot().owned_bytes, 0);
    network.shutdown();
    snapshot
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_engine_operation_limit_is_not_doubled_during_copy() {
    let snapshot = aggregate_copy_snapshot(
        Settings {
            queue_operations: 2,
            ..Settings::default()
        },
        1,
    )
    .await;
    assert!(snapshot.owned_operations <= 2, "{snapshot:?}");
    assert_eq!(snapshot.progress.received, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_engine_byte_limit_is_not_doubled_during_copy() {
    let snapshot = aggregate_copy_snapshot(
        Settings {
            operation_bytes: 2048,
            queue_bytes: 4096,
            queue_operations: 8,
            ..Settings::default()
        },
        1800,
    )
    .await;
    assert!(snapshot.owned_bytes <= 4096, "{snapshot:?}");
    assert_eq!(snapshot.progress.received, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submit_attempts_cannot_consume_incoming_copy_reservation_or_capture_charge() {
    for bytes_exhausted in [false, true] {
        let settings = if bytes_exhausted {
            Settings {
                operation_bytes: 2048,
                operation_segments: 64,
                queue_bytes: 4096,
                queue_operations: 8,
                ..Settings::default()
            }
        } else {
            Settings {
                queue_operations: 2,
                ..Settings::default()
            }
        };
        let (s, network, primary, _incoming) =
            copying_raw_secondary(EPOCH, MemoryProvider::new(), settings.clone()).await;
        let data =
            OperationData::new(vec![vec![42; if bytes_exhausted { 600 } else { 1 }]]).unwrap();
        let copy_data = if bytes_exhausted {
            let mut segments = vec![vec![42; settings.operation_bytes]];
            segments.resize(settings.operation_segments, vec![]);
            OperationData::with_settings(segments, &settings).unwrap()
        } else {
            data.clone()
        };
        raw_send(
            &network,
            &primary,
            &s,
            replication_data(EPOCH, EPOCH, 1, &data),
        );
        until(|| s.snapshot().progress.received == 1).await;
        let start = Arc::new(tokio::sync::Barrier::new(9));
        let captured = Arc::new(tokio::sync::Barrier::new(9));
        let mut attempts = JoinSet::new();
        for _ in 0..8 {
            let s = s.clone();
            let data = data.clone();
            let start = start.clone();
            let captured = captured.clone();
            let settings = settings.clone();
            attempts.spawn(async move {
                start.wait().await;
                for phase in 0..2 {
                    if phase == 1 {
                        captured.wait().await;
                    }
                    for _ in 0..32 {
                        assert!(s.submit(data.clone(), cancel()).await.is_err());
                        let snapshot = s.snapshot();
                        assert!(snapshot.owned_operations <= settings.queue_operations);
                        assert!(snapshot.owned_bytes <= settings.queue_bytes);
                        tokio::task::yield_now().await;
                    }
                }
            });
        }
        start.wait().await;
        raw_send(
            &network,
            &primary,
            &s,
            replication_data(EPOCH, EPOCH, 2, &data),
        );
        let copy = retry_raw_operation(
            &s,
            &network,
            &primary,
            copy_item(EPOCH, 1, copy_data.to_segments(), false),
            s.copy_stream(),
        )
        .await;
        assert_eq!(copy.data, copy_data);
        assert_eq!(s.snapshot().progress.received, 1);
        captured.wait().await;
        while let Some(result) = attempts.join_next().await {
            result.unwrap();
        }
        copy.acknowledge().unwrap();
        until(|| s.snapshot().dispatched_unacknowledged == 0).await;
        assert_eq!(s.snapshot().owned_operations, 2);
        assert_eq!(
            s.snapshot().owned_bytes,
            data.retained_bytes() + copy_data.retained_bytes()
        );
        drop(copy);
        retry_raw_eos(&s, &network, &primary, 2).await;
        s.abort();
        assert_eq!(s.snapshot().owned_operations, 0);
        assert_eq!(s.snapshot().owned_bytes, 0);
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_budget_repro_requires_eos_or_explicit_preassignment_write_rejection() {
    let partition = Uuid::new_v4();
    let source = Arc::new(SnapshotProvider::default());
    source.snapshots.store(true, Ordering::Release);
    let settings = Settings {
        operation_bytes: 2048,
        operation_segments: 1024,
        queue_bytes: 4096,
        queue_operations: 8,
        retry: std::time::Duration::from_millis(20),
        ..Settings::default()
    };
    let p = engine_with_settings(partition, 1, source.clone(), settings.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    let (network, mut incoming) = PeerTransport::bind("127.0.0.1:0", Settings::default()).unwrap();
    network.register_peer(&p.endpoint()).unwrap();
    let target = endpoint(&network, partition, 2);
    let mut replica = descriptor(&p);
    replica.replica_id = 2;
    replica.address = target.to_string();
    let owner = p.clone();
    let build = tokio::spawn(async move { owner.build_replica(replica, cancel()).await });
    timeout(std::time::Duration::from_secs(2), async {
        loop {
            if matches!(
                incoming.control.recv().await.unwrap().body,
                Body::StartCopy { .. }
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let ack = |received, copy_received| {
        raw_send(
            &network,
            &target,
            &p,
            Body::Ack {
                received,
                applied: received,
                copy_received,
                copy_applied: copy_received,
                error: 0,
            },
        )
    };
    ack(0, 0);
    let first = timeout(std::time::Duration::from_secs(2), incoming.data.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        first.body,
        Body::Copy {
            sequence: 1,
            last: false,
            ..
        }
    ));
    let data = OperationData::new(vec![vec![7; 1800]]).unwrap();
    let accepted = match p.submit(data.clone(), cancel()).await {
        Ok(mut write) => {
            assert_eq!(write.sequence, 1);
            source.record(1);
            assert_eq!(write.wait(cancel()).await.unwrap(), 1);
            Some(write)
        }
        Err(Error::ResourceLimit(_)) => {
            assert_eq!(p.snapshot().progress.assigned, 0);
            assert_eq!(p.snapshot().progress.committed, 0);
            None
        }
        Err(error) => panic!("unexpected admission failure: {error}"),
    };
    let assigned = i64::from(accepted.is_some());
    ack(0, 1);
    source.eos.add_permits(1);
    // This is the R8 oracle: withhold the replication receipt until actual EOS,
    // not indefinitely calling a stable permit cycle acceptable backpressure.
    timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = p.snapshot();
            assert!(snapshot.owned_operations <= settings.queue_operations);
            assert!(snapshot.owned_bytes <= settings.queue_bytes);
            tokio::select! {
                Some(packet) = incoming.data.recv() => {
                    if matches!(packet.body, Body::Copy { sequence: 2, last: true, .. }) {
                        break;
                    }
                },
                _ = incoming.control.recv() => {},
            }
        }
    })
    .await
    .expect("retained write starved outgoing EOS enumeration");
    assert_eq!(p.snapshot().peers[0].copy_finish, Some(assigned));
    if accepted.is_some() {
        timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Body::Replication { operations, .. } =
                    incoming.data.recv().await.unwrap().body
                {
                    if operations
                        .iter()
                        .any(|op| op.sequence == 1 && op.data == data.to_segments())
                    {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
    }
    ack(assigned, -1);
    timeout(std::time::Duration::from_secs(2), build)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    until(|| p.snapshot().owned_operations == 0).await;
    let mut next = p.submit(data, cancel()).await.unwrap();
    assert_eq!(
        next.sequence,
        assigned + 1,
        "rejected write consumed an LSN"
    );
    source.record(next.sequence);
    assert_eq!(next.wait(cancel()).await.unwrap(), assigned + 1);
    p.abort();
    assert_eq!(p.snapshot().owned_bytes, 0);
    network.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_copy_and_real_idle_ack_progress_with_admitted_or_rejected_live_writes() {
    for (queue_bytes, operation_segments, queue_operations, admits) in [
        (8192, 1, 8, true),
        (4096, 1024, 8, false),
        (8192, 1, 2, false),
    ] {
        let settings = Settings {
            operation_bytes: 2048,
            operation_segments,
            queue_bytes,
            queue_operations,
            retry: std::time::Duration::from_millis(20),
            ..Settings::default()
        };
        let partition = Uuid::new_v4();
        let source = Arc::new(SnapshotProvider::default());
        source.snapshots.store(true, Ordering::Release);
        let target_provider = Arc::new(Provider::default());
        let p = engine_with_settings(partition, 1, source.clone(), settings.clone());
        let s = engine_with_settings(partition, 2, target_provider.clone(), settings.clone());
        p.change_role(EPOCH, ReplicaRole::Primary, cancel())
            .await
            .unwrap();
        p.update_current_configuration(configuration(vec![], true, 1), cancel())
            .await
            .unwrap();
        s.change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        let owner = p.clone();
        let replica = descriptor(&s);
        let build = tokio::spawn(async move { owner.build_replica(replica, cancel()).await });
        let copy = timeout(
            std::time::Duration::from_secs(2),
            s.copy_stream().next(cancel()),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(copy.sequence, 1);
        // The gated EOS callback holds its full pre-enumeration charge while
        // real write admission competes with it.
        until(|| {
            p.snapshot().provider_callbacks == 1
                && p.snapshot().owned_bytes == maximum_copy_bytes(&settings)
        })
        .await;
        let data = OperationData::new(vec![vec![7; 1800]]).unwrap();
        let assigned = match p.submit(data.clone(), cancel()).await {
            Ok(mut write) => {
                assert!(admits);
                assert_eq!(write.sequence, 1);
                source.record(1);
                assert_eq!(write.wait(cancel()).await.unwrap(), 1);
                assert_eq!(p.snapshot().progress.completed, 0);
                1
            }
            Err(Error::ResourceLimit(_)) => {
                assert!(!admits);
                assert_eq!(p.snapshot().progress.assigned, 0);
                assert_eq!(p.snapshot().progress.committed, 0);
                0
            }
            Err(error) => panic!("unexpected admission failure: {error}"),
        };
        source.eos.add_permits(1);
        until(|| p.snapshot().peers[0].copy_finish == Some(assigned)).await;
        assert!(!s.snapshot().copy_done);
        assert!(!build.is_finished());
        for owner in [&p, &s] {
            let snapshot = owner.snapshot();
            assert!(snapshot.owned_bytes <= queue_bytes);
            assert!(snapshot.owned_operations <= queue_operations);
        }
        copy.acknowledge().unwrap();
        drop(copy);
        assert!(timeout(
            std::time::Duration::from_secs(2),
            s.copy_stream().next(cancel())
        )
        .await
        .unwrap()
        .unwrap()
        .is_none());
        if admits {
            let op = timeout(
                std::time::Duration::from_secs(2),
                s.replication_stream().next(cancel()),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(op.sequence, 1);
            assert_eq!(op.data, data);
            target_provider.last.store(1, Ordering::Release);
            op.acknowledge().unwrap();
        }
        timeout(std::time::Duration::from_secs(2), build)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        until(|| p.snapshot().owned_operations == 0).await;
        let mut next = p.submit(data.clone(), cancel()).await.unwrap();
        assert_eq!(next.sequence, assigned + 1);
        source.record(next.sequence);
        assert_eq!(next.wait(cancel()).await.unwrap(), assigned + 1);
        let op = timeout(
            std::time::Duration::from_secs(2),
            s.replication_stream().next(cancel()),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(op.sequence, assigned + 1);
        assert_eq!(op.data, data);
        target_provider.last.store(op.sequence, Ordering::Release);
        op.acknowledge().unwrap();
        drop(op);
        p.abort();
        s.abort();
        assert_eq!(p.snapshot().owned_bytes, 0);
        assert_eq!(s.snapshot().owned_bytes, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_future_copy_headroom_survives_concurrent_build_and_write_admission() {
    let partition = Uuid::new_v4();
    let source = Arc::new(SnapshotProvider::default());
    let follower_provider = Arc::new(Provider::default());
    let target_provider = Arc::new(Provider::default());
    let settings = Settings {
        operation_bytes: 2048,
        operation_segments: 1,
        queue_bytes: 4096,
        queue_operations: 8,
        retry: std::time::Duration::from_millis(20),
        ..Settings::default()
    };
    let p = engine_with_settings(partition, 1, source.clone(), settings.clone());
    let follower = engine(partition, 2, follower_provider.clone());
    let target = engine_with_settings(partition, 3, target_provider.clone(), settings.clone());
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    for secondary in [&follower, &target] {
        secondary
            .change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
    }
    p.build_replica(descriptor(&follower), cancel())
        .await
        .unwrap();
    p.update_current_configuration(
        configuration(vec![descriptor(&follower)], true, 2),
        cancel(),
    )
    .await
    .unwrap();
    let data = OperationData::new(vec![vec![7; 1700]]).unwrap();
    let mut write = p.submit(data.clone(), cancel()).await.unwrap();
    source.record(1);
    let held = follower
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    // No outgoing enumeration is active yet. Retained primary writes still
    // cannot consume the next copy slot, even before a future build is known.
    assert!(matches!(
        p.submit(data.clone(), cancel()).await,
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(p.snapshot().progress.assigned, 1);
    assert_eq!(p.snapshot().owned_bytes, data.retained_bytes());
    source.snapshots.store(true, Ordering::Release);
    let start = Arc::new(tokio::sync::Barrier::new(10));
    let mut attempts = JoinSet::new();
    for _ in 0..8 {
        let owner = p.clone();
        let data = data.clone();
        let start = start.clone();
        let settings = settings.clone();
        attempts.spawn(async move {
            start.wait().await;
            for _ in 0..32 {
                assert!(matches!(
                    owner.submit(data.clone(), cancel()).await,
                    Err(Error::ResourceLimit(_))
                ));
                let snapshot = owner.snapshot();
                assert!(snapshot.owned_operations <= settings.queue_operations);
                assert!(snapshot.owned_bytes <= settings.queue_bytes);
                tokio::task::yield_now().await;
            }
        });
    }
    let owner = p.clone();
    let replica = descriptor(&target);
    let build_start = start.clone();
    let build = tokio::spawn(async move {
        build_start.wait().await;
        owner.build_replica(replica, cancel()).await
    });
    start.wait().await;
    let copy = timeout(
        std::time::Duration::from_secs(2),
        target.copy_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    until(|| {
        p.snapshot().provider_callbacks == 1
            && p.snapshot().owned_bytes == data.retained_bytes() + maximum_copy_bytes(&settings)
    })
    .await;
    assert_eq!(p.snapshot().progress.committed, 0);
    source.eos.add_permits(1);
    until(|| {
        p.snapshot()
            .peers
            .iter()
            .any(|peer| peer.replica == 3 && peer.copy_finish == Some(1))
    })
    .await;
    assert!(!target.snapshot().copy_done);
    copy.acknowledge().unwrap();
    drop(copy);
    assert!(timeout(
        std::time::Duration::from_secs(2),
        target.copy_stream().next(cancel())
    )
    .await
    .unwrap()
    .unwrap()
    .is_none());
    let tail = timeout(
        std::time::Duration::from_secs(2),
        target.replication_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(tail.sequence, 1);
    assert_eq!(tail.data, data);
    target_provider.last.store(1, Ordering::Release);
    tail.acknowledge().unwrap();
    drop(tail);
    timeout(std::time::Duration::from_secs(2), build)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    while let Some(attempt) = attempts.join_next().await {
        attempt.unwrap();
    }
    assert_eq!(p.snapshot().progress.assigned, 1);
    assert_eq!(p.snapshot().progress.committed, 0);
    follower_provider.last.store(1, Ordering::Release);
    held.acknowledge().unwrap();
    drop(held);
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    until(|| p.snapshot().owned_operations == 0).await;
    let mut next = p.submit(data.clone(), cancel()).await.unwrap();
    assert_eq!(next.sequence, 2, "concurrent rejected writes consumed LSNs");
    source.record(2);
    let op = timeout(
        std::time::Duration::from_secs(2),
        follower.replication_stream().next(cancel()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(op.sequence, 2);
    assert_eq!(op.data, data);
    follower_provider.last.store(2, Ordering::Release);
    op.acknowledge().unwrap();
    drop(op);
    assert_eq!(next.wait(cancel()).await.unwrap(), 2);
    p.abort();
    follower.abort();
    target.abort();
    for owner in [&p, &follower, &target] {
        assert_eq!(owner.snapshot().owned_operations, 0);
        assert_eq!(owner.snapshot().owned_bytes, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_build_admission_counts_pending_writes_before_installing_any_peer() {
    for byte_limit in [false, true] {
        let settings = Settings {
            operation_bytes: 2048,
            operation_segments: if byte_limit { 1024 } else { 1 },
            queue_bytes: if byte_limit { 4096 } else { 8192 },
            queue_operations: if byte_limit { 8 } else { 2 },
            ..Settings::default()
        };
        let partition = Uuid::new_v4();
        let provider = Arc::new(Provider::default());
        let p = engine_with_settings(partition, 1, provider.clone(), settings.clone());
        let s = engine(partition, 2, Arc::new(Provider::default()));
        p.change_role(EPOCH, ReplicaRole::Primary, cancel())
            .await
            .unwrap();
        p.update_current_configuration(configuration(vec![], true, 1), cancel())
            .await
            .unwrap();
        s.change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
        let data = OperationData::new(vec![vec![7; 1800]]).unwrap();
        let mut replies = Vec::new();
        let (reply, result) = oneshot::channel();
        {
            let _admission = p.shared.admission.lock().unwrap();
            // Schedule the build first but keep the owner behind admission.
            // These are already-charged, not-yet-assigned Submit messages, just
            // as if producers won admission while the owner selected Build.
            p.commands
                .try_send(Command::Build(descriptor(&s), cancel(), reply))
                .unwrap_or_else(|_| panic!("build command queue unexpectedly full"));
            for _ in 0..if byte_limit { 1 } else { 2 } {
                let charge = p
                    .shared
                    .budget
                    .acquire_reserved(
                        data.retained_bytes(),
                        0,
                        if byte_limit {
                            0
                        } else {
                            maximum_copy_bytes(&settings)
                        },
                    )
                    .unwrap();
                let (reply, result) = oneshot::channel();
                p.writes
                    .try_send(Submit {
                        data: data.clone(),
                        charge,
                        cancellation: cancel(),
                        reply,
                    })
                    .unwrap_or_else(|_| panic!("pending write queue unexpectedly full"));
                replies.push(result);
            }
            assert_eq!(p.snapshot().progress.assigned, 0);
        }
        assert!(matches!(
            timeout(std::time::Duration::from_secs(2), result)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::ResourceLimit(_))
        ));
        let mut sequence = 0;
        for reply in replies {
            let mut write = reply.await.unwrap().unwrap();
            sequence += 1;
            assert_eq!(write.sequence, sequence);
            provider.last.store(sequence, Ordering::Release);
            assert_eq!(write.wait(cancel()).await.unwrap(), sequence);
        }
        until(|| p.snapshot().owned_operations == 0).await;
        assert!(
            p.snapshot().peers.is_empty(),
            "failed build partially installed a peer"
        );
        assert!(!s.snapshot().copy_done);
        // No cleanup obligation/tombstone was installed by the resource error.
        timeout(
            std::time::Duration::from_secs(2),
            p.build_replica(descriptor(&s), cancel()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(p.snapshot().progress.assigned, sequence);
        let mut next = p.submit(data, cancel()).await.unwrap();
        assert_eq!(next.sequence, sequence + 1);
        provider.last.store(next.sequence, Ordering::Release);
        next.wait(cancel()).await.unwrap();
        p.abort();
        s.abort();
        assert_eq!(p.snapshot().owned_bytes, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_build_on_retained_write_budget_fails_without_losing_write_or_cleanup_state() {
    let partition = Uuid::new_v4();
    let provider = Arc::new(Provider::default());
    let follower_provider = Arc::new(Provider::default());
    let settings = Settings {
        operation_bytes: 2048,
        operation_segments: 1024,
        queue_bytes: 4096,
        queue_operations: 8,
        retry: std::time::Duration::from_millis(20),
        ..Settings::default()
    };
    let p = engine_with_settings(partition, 1, provider.clone(), settings);
    let follower = engine(partition, 2, follower_provider.clone());
    let target = engine(partition, 3, Arc::new(Provider::default()));
    p.change_role(EPOCH, ReplicaRole::Primary, cancel())
        .await
        .unwrap();
    p.update_current_configuration(configuration(vec![], true, 1), cancel())
        .await
        .unwrap();
    for secondary in [&follower, &target] {
        secondary
            .change_role(EPOCH, ReplicaRole::Idle, cancel())
            .await
            .unwrap();
    }
    p.build_replica(descriptor(&follower), cancel())
        .await
        .unwrap();
    p.update_current_configuration(
        configuration(vec![descriptor(&follower)], true, 2),
        cancel(),
    )
    .await
    .unwrap();
    let data = OperationData::new(vec![vec![7; 1800]]).unwrap();
    let mut write = p.submit(data.clone(), cancel()).await.unwrap();
    provider.last.store(1, Ordering::Release);
    let capture = follower
        .replication_stream()
        .next(cancel())
        .await
        .unwrap()
        .unwrap();
    let canceled = cancel();
    canceled.cancel();
    assert!(matches!(write.wait(canceled).await, Err(Error::Closed)));
    assert!(matches!(
        timeout(
            std::time::Duration::from_secs(2),
            p.build_replica(descriptor(&target), cancel())
        )
        .await
        .unwrap(),
        Err(Error::ResourceLimit(_))
    ));
    let snapshot = p.snapshot();
    assert_eq!(snapshot.progress.assigned, 1);
    assert_eq!(snapshot.progress.committed, 0);
    assert_eq!(snapshot.owned_bytes, data.retained_bytes());
    assert_eq!(snapshot.peers.len(), 1);
    assert!(!snapshot.peers[0].cleanup_required);
    assert!(!target.snapshot().copy_done);
    follower_provider.last.store(1, Ordering::Release);
    capture.acknowledge().unwrap();
    drop(capture);
    assert_eq!(write.wait(cancel()).await.unwrap(), 1);
    until(|| p.snapshot().owned_operations == 0).await;
    timeout(
        std::time::Duration::from_secs(2),
        p.build_replica(descriptor(&target), cancel()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(p.snapshot().peers.len(), 2);
    assert_eq!(p.snapshot().progress.assigned, 1);
    p.abort();
    follower.abort();
    target.abort();
    assert_eq!(p.snapshot().owned_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_disposes_operations_still_owned_by_stream_channels() {
    let (a, b, _) = pair().await;
    a.update_current_configuration(configuration(vec![descriptor(&b)], true, 2), cancel())
        .await
        .unwrap();
    let _one = a
        .submit(OperationData::new(vec![vec![42]]).unwrap(), cancel())
        .await
        .unwrap();
    let _two = a
        .submit(OperationData::new(vec![vec![42]]).unwrap(), cancel())
        .await
        .unwrap();
    until(|| b.snapshot().operation_captures == 2).await;
    let stream = b.replication_stream();
    b.abort();
    let snapshot = b.snapshot();
    assert_eq!(snapshot.operation_captures, 0, "{snapshot:?}");
    assert_eq!(snapshot.owned_operations, 0);
    assert_eq!(snapshot.owned_bytes, 0);
    assert!(stream.next(cancel()).await.unwrap().is_none());
    a.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_stream_shutdown_drains_buffer_but_preserves_application_owned_capture() {
    for hold_capture in [false, true] {
        let (s, network, primary, _incoming) =
            copying_raw_secondary(EPOCH, Arc::new(Provider::default()), Settings::default()).await;
        for sequence in 1..=2 {
            raw_send(
                &network,
                &primary,
                &s,
                copy_item(EPOCH, sequence, vec![vec![42]], false),
            );
        }
        until(|| s.snapshot().operation_captures == 2).await;
        let stream = s.copy_stream();
        let held = if hold_capture {
            Some(stream.next(cancel()).await.unwrap().unwrap())
        } else {
            None
        };
        s.abort();
        let snapshot = s.snapshot();
        assert_eq!(snapshot.operation_captures, usize::from(hold_capture));
        assert_eq!(snapshot.owned_operations, usize::from(hold_capture));
        assert_eq!(
            snapshot.owned_bytes,
            held.as_ref().map_or(0, |op| op.data.retained_bytes())
        );
        assert!(stream.next(cancel()).await.unwrap().is_none());
        if let Some(op) = held.as_ref() {
            assert!(op.acknowledge().is_err());
        }
        drop(held);
        assert_eq!(s.snapshot().operation_captures, 0);
        assert_eq!(s.snapshot().owned_operations, 0);
        assert_eq!(s.snapshot().owned_bytes, 0);
        network.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_shutdown_wakes_pending_polls_and_linearizes_concurrent_publication() {
    let e = engine(Uuid::new_v4(), 1, Arc::new(Provider::default()));
    let mut polls = Vec::new();
    for stream in [e.copy_stream(), e.replication_stream()] {
        for _ in 0..2 {
            let stream = stream.clone();
            let pending = Arc::new(AtomicBool::new(false));
            let ready = pending.clone();
            let poll = tokio::spawn(async move {
                let next = stream.next(cancel());
                tokio::pin!(next);
                std::future::poll_fn(|cx| {
                    let result = std::future::Future::poll(next.as_mut(), cx);
                    if result.is_pending() {
                        ready.store(true, Ordering::Release);
                    }
                    result
                })
                .await
            });
            polls.push(poll);
            until(|| pending.load(Ordering::Acquire)).await;
        }
    }
    e.abort();
    for poll in polls {
        assert!(timeout(std::time::Duration::from_secs(2), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
    }

    let (a, b, _) = pair().await;
    a.update_current_configuration(configuration(vec![descriptor(&b)], true, 2), cancel())
        .await
        .unwrap();
    for _ in 0..8 {
        a.submit(OperationData::new(vec![vec![42]]).unwrap(), cancel())
            .await
            .unwrap();
    }
    until(|| b.snapshot().operation_captures == 8).await;
    let mut polls = Vec::new();
    for _ in 0..8 {
        let stream = b.replication_stream();
        polls.push(tokio::spawn(async move { stream.next(cancel()).await }));
    }
    b.abort();
    let mut held = Vec::new();
    for poll in polls {
        if let Some(op) = timeout(std::time::Duration::from_secs(2), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            assert!(op.acknowledge().is_err());
            held.push(op);
        }
    }
    assert_eq!(b.snapshot().operation_captures, held.len());
    assert_eq!(b.snapshot().owned_operations, held.len());
    assert_eq!(
        b.snapshot().owned_bytes,
        held.iter()
            .map(|op| op.data.retained_bytes())
            .sum::<usize>()
    );
    drop(held);
    assert_eq!(b.snapshot().operation_captures, 0);
    assert_eq!(b.snapshot().owned_operations, 0);
    assert_eq!(b.snapshot().owned_bytes, 0);
    a.abort();
}
