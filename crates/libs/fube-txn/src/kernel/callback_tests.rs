// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Expected K01–K03/C01–C04 branches are from SF 51a7703, not native execution.
//! Local registry fixtures use source-format bytes and opaque lookup; they do
//! not implement or qualify the complete registry metadata codec.

use super::{
    checkpoint::{
        CheckpointCoordinator, CheckpointIo, CheckpointPhase, FlushReason, PhysicalRecord,
        VoidFuture,
    },
    copy::{drain_state, CopyOperation, CopyStageStream},
    named,
    processor::{ControlledLower, KernelManager, ProcessingRole},
    provider::{
        CallbackKind, CallbackPlan, CopiedMetadata, LocalStateCallbacks, PointEvent, PointProvider,
        ProviderDirectory, ProviderMode, RegistryMetadata, REGISTRY_ID,
    },
    record::{Record, RecordKind},
    status::Status,
    transaction::{own, RetryRuntime},
    version::Completion,
};
use crate::{
    internal::IStateProviderManager, ApiFuture, Cancellation, IOperationDataStream,
    IStateProvider2, ITransaction, OperationData, Result, TransactionBase,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

type Journal = Arc<Mutex<Vec<String>>>;

fn event(journal: &Journal, value: impl Into<String>) {
    journal.lock().unwrap().push(value.into());
}
fn events(journal: &Journal) -> Vec<String> {
    journal.lock().unwrap().clone()
}
fn version() -> OperationData {
    OperationData::new(vec![vec![1, 0, 0, 0, 0]]).unwrap()
}
fn empty() -> OperationData {
    OperationData::new(Vec::new()).unwrap()
}

// CheckpointFile.cpp:645–697; ASCII lengths fit one VarInt byte. Native null
// init=-1, root parent=0; each chunk has trailing StateProviderMetadata opcode1.
fn metadata_packet(metadata: &[(i64, String, ProviderMode)]) -> OperationData {
    let mut packet = Vec::new();
    for (id, name, mode) in metadata {
        assert!(name.len() < 128);
        let mut record = vec![0; 4];
        record.push(name.len() as u8);
        record.extend_from_slice(name.as_bytes());
        record.push(5);
        record.extend_from_slice(b"point");
        record.extend_from_slice(&1i64.to_le_bytes());
        record.extend_from_slice(&(-1i64).to_le_bytes());
        record.extend_from_slice(&(-1i32).to_le_bytes());
        record.extend_from_slice(
            &(match mode {
                ProviderMode::Active => 0i32,
                ProviderMode::DelayDelete => 1,
                ProviderMode::Stale => 2,
            })
            .to_le_bytes(),
        );
        record.extend_from_slice(&id.to_le_bytes());
        record.extend_from_slice(&0i64.to_le_bytes());
        let size = record.len() as u32;
        record[..4].copy_from_slice(&size.to_le_bytes());
        packet.extend(record);
    }
    packet.push(1);
    OperationData::new(vec![packet]).unwrap()
}

struct TestStream {
    name: String,
    journal: Journal,
    data: Mutex<VecDeque<Result<Option<OperationData>>>>,
}
impl TestStream {
    fn new(name: &str, journal: &Journal, data: Vec<OperationData>) -> Arc<Self> {
        let mut data: VecDeque<_> = data.into_iter().map(|data| Ok(Some(data))).collect();
        data.push_back(Ok(None));
        Arc::new(Self {
            name: name.into(),
            journal: journal.clone(),
            data: Mutex::new(data),
        })
    }
}
impl IOperationDataStream for TestStream {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            event(
                &self.journal,
                format!("{}:Next:{}", self.name, cancellation.is_cancelled()),
            );
            self.data.lock().unwrap().pop_front().unwrap_or(Ok(None))
        })
    }
    fn dispose(&self) -> Result<()> {
        event(&self.journal, format!("{}:Dispose", self.name));
        Ok(())
    }
}

struct GatedEosStream {
    journal: Journal,
    gate: Completion<()>,
}
impl IOperationDataStream for GatedEosStream {
    fn get_next(&self, cancellation: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        let journal = self.journal.clone();
        let gate = self.gate.clone();
        own(Box::pin(async move {
            event(
                &journal,
                format!("gated:Next:{}", cancellation.is_cancelled()),
            );
            gate.wait().await.as_ref().clone()?;
            event(&journal, "gated:EOS");
            Ok(None)
        }))
    }
    fn dispose(&self) -> Result<()> {
        event(&self.journal, "gated:Dispose");
        Ok(())
    }
}

struct Local {
    journal: Journal,
    failures: Mutex<BTreeMap<&'static str, Result<()>>>,
    decoded: Mutex<Vec<(OperationData, Vec<CopiedMetadata>)>>,
    snapshots: Mutex<Vec<Vec<(i64, String, ProviderMode)>>>,
    targets: Mutex<Vec<i64>>,
}
impl Local {
    fn new(journal: &Journal) -> Arc<Self> {
        Arc::new(Self {
            journal: journal.clone(),
            failures: Mutex::new(BTreeMap::new()),
            decoded: Mutex::new(Vec::new()),
            snapshots: Mutex::new(Vec::new()),
            targets: Mutex::new(Vec::new()),
        })
    }
    fn call(&self, kind: &'static str) -> Result<()> {
        event(&self.journal, kind);
        self.failures.lock().unwrap().remove(kind).unwrap_or(Ok(()))
    }
    fn packet(&self, metadata: Vec<(i64, String, ProviderMode)>) -> OperationData {
        let packet = metadata_packet(&metadata);
        self.decoded.lock().unwrap().push((
            packet.clone(),
            metadata
                .into_iter()
                .map(|(id, _, mode)| CopiedMetadata { id, mode })
                .collect(),
        ));
        packet
    }
}
impl LocalStateCallbacks for Local {
    fn prepare(&self, _: i64, metadata: &[RegistryMetadata]) -> Result<()> {
        self.snapshots.lock().unwrap().push(
            metadata
                .iter()
                .map(|entry| (entry.id, entry.name.clone(), entry.mode))
                .collect(),
        );
        self.call("Local:Prepare")
    }
    fn perform(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { self.call("Local:Perform") })
    }
    fn complete(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { self.call("Local:Complete") })
    }
    fn cleanup(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { self.call("Local:Cleanup") })
    }
    fn get_current_state(
        &self,
        target: i64,
        metadata: &[RegistryMetadata],
    ) -> Result<Arc<dyn IOperationDataStream>> {
        self.call("Local:Get")?;
        self.targets.lock().unwrap().push(target);
        let mut data = vec![version()];
        if !metadata.is_empty() {
            data.push(
                self.packet(
                    metadata
                        .iter()
                        .map(|entry| (entry.id, entry.name.clone(), entry.mode))
                        .collect(),
                ),
            );
        }
        Ok(TestStream::new("registry", &self.journal, data))
    }
    fn begin(&self) -> Result<()> {
        self.call("Local:Begin")
    }
    fn set(&self, number: i64, data: OperationData) -> ApiFuture<'_, Vec<CopiedMetadata>> {
        Box::pin(async move {
            self.call("Local:Set")?;
            event(&self.journal, format!("Local:Set:{number}"));
            if data == version() {
                return Ok(Vec::new());
            }
            self.decoded
                .lock()
                .unwrap()
                .iter()
                .find(|(packet, _)| *packet == data)
                .map(|(_, decoded)| decoded.clone())
                .ok_or_else(|| Status::InternalDbCorruption.error())
        })
    }
    fn end(&self) -> ApiFuture<'_, ()> {
        Box::pin(async { self.call("Local:End") })
    }
}

struct Io {
    journal: Journal,
    fence: Completion<()>,
    stable: Completion<()>,
    begin_status: Mutex<Result<()>>,
    rename_status: Mutex<Result<()>>,
    void_record_flush_failed: Mutex<bool>,
    role: Mutex<ProcessingRole>,
    tail: Arc<dyn Fn() -> i64 + Send + Sync>,
}
impl Io {
    fn new(journal: &Journal, tail: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        let fence = Completion::new();
        fence.complete(Ok(()));
        Self::with_fence(journal, tail, fence)
    }
    fn with_fence(
        journal: &Journal,
        tail: Arc<dyn Fn() -> i64 + Send + Sync>,
        fence: Completion<()>,
    ) -> Arc<Self> {
        Arc::new(Self {
            journal: journal.clone(),
            fence,
            stable: Completion::new(),
            begin_status: Mutex::new(Ok(())),
            rename_status: Mutex::new(Ok(())),
            tail,
            void_record_flush_failed: Mutex::new(false),
            role: Mutex::new(ProcessingRole::Secondary),
        })
    }
}
impl CheckpointIo for Io {
    fn dispatcher_fence(&self) -> VoidFuture<'_> {
        Box::pin(async {
            event(&self.journal, "Fence");
            let _ = self.fence.wait().await;
        })
    }
    fn begin_record_flush(&self) -> ApiFuture<'_, ()> {
        Box::pin(async {
            event(&self.journal, "Begin:FlushStatus");
            self.begin_status.lock().unwrap().clone()
        })
    }
    fn stability_permission(&self, lsn: i64) -> VoidFuture<'_> {
        Box::pin(async move {
            event(&self.journal, format!("Stable:Wait:{lsn}"));
            let _ = self.stable.wait().await;
        })
    }
    fn tail(&self) -> i64 {
        (self.tail)()
    }
    fn role(&self) -> ProcessingRole {
        *self.role.lock().unwrap()
    }
    fn append(&self, record: PhysicalRecord) -> Result<()> {
        event(&self.journal, format!("Append:{record:?}"));
        Ok(())
    }
    fn flush(&self, reason: FlushReason) -> VoidFuture<'_> {
        Box::pin(async move {
            event(&self.journal, format!("Flush:{reason:?}"));
            if *self.void_record_flush_failed.lock().unwrap() {
                event(&self.journal, "Physical:FailedStatusNotReturnedByVoidFlush");
            }
        })
    }
    fn rename_copy_log(&self) -> ApiFuture<'_, ()> {
        Box::pin(async {
            event(&self.journal, "Rename");
            self.rename_status.lock().unwrap().clone()
        })
    }
}

struct Fixture {
    journal: Journal,
    local: Arc<Local>,
    directory: Arc<ProviderDirectory>,
    points: Vec<Arc<PointProvider>>,
}
fn fixture(names: &[&str]) -> Fixture {
    let journal = Arc::new(Mutex::new(Vec::new()));
    let local = Local::new(&journal);
    let points: Vec<_> = names
        .iter()
        .map(|name| PointProvider::preinitialized(name))
        .collect();
    let directory = ProviderDirectory::preinitialized(
        points.iter().enumerate().map(|(index, provider)| {
            (
                7 + index as i64,
                provider.clone() as Arc<dyn IStateProvider2>,
            )
        }),
        None,
    )
    .unwrap();
    directory.set_local_callbacks(local.clone());
    Fixture {
        journal,
        local,
        directory,
        points,
    }
}
fn plan(
    journal: &Journal,
    label: &str,
    gate: Option<Completion<()>>,
    result: Result<()>,
) -> CallbackPlan {
    let journal = journal.clone();
    let label = label.to_owned();
    CallbackPlan {
        gate,
        result,
        on_enter: Some(Arc::new(move || event(&journal, label.clone()))),
    }
}
fn entered(point: &PointProvider, kind: CallbackKind) -> bool {
    point.events().contains(&PointEvent::Callback {
        kind,
        completed: false,
    })
}
fn completed(point: &PointProvider, kind: CallbackKind) -> bool {
    point.events().contains(&PointEvent::Callback {
        kind,
        completed: true,
    })
}
async fn observe<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("bounded callback completion")
}
async fn wait_for(predicate: impl Fn() -> bool) {
    observe(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn checkpoint(fixture: &Fixture) {
    fixture.directory.prepare_checkpoint(10).unwrap();
    observe(
        fixture
            .directory
            .perform_checkpoint(Cancellation::default()),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn k01_checkpoint_fence_stability_all_provider_awaits_and_physical_partial_order() {
    let f = fixture(&["fabric:/a", "fabric:/z", "fabric:/deleted"]);
    f.directory.set_mode(9, ProviderMode::DelayDelete, None);
    for (index, point) in f.points.iter().take(2).enumerate() {
        point.inject_callback(
            CallbackKind::Prepare,
            plan(&f.journal, &format!("p{index}:Prepare"), None, Ok(())),
        );
    }
    let perform0 = Completion::new();
    let perform1 = Completion::new();
    let complete0 = Completion::new();
    let complete1 = Completion::new();
    for (index, gate) in [perform0.clone(), perform1.clone()].into_iter().enumerate() {
        f.points[index].inject_callback(
            CallbackKind::Perform,
            plan(&f.journal, &format!("p{index}:Perform"), Some(gate), Ok(())),
        );
    }
    for (index, gate) in [complete0.clone(), complete1.clone()]
        .into_iter()
        .enumerate()
    {
        f.points[index].inject_callback(
            CallbackKind::Complete,
            plan(
                &f.journal,
                &format!("p{index}:Complete"),
                Some(gate),
                Ok(()),
            ),
        );
    }
    let io = Io::new(&f.journal, Arc::new(|| 12));
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io.clone(), f.directory.faults.clone());
    let activity = coordinator.start(10, false).unwrap();
    assert_eq!(activity.phase(), CheckpointPhase::Ready);
    assert_eq!(
        events(&f.journal),
        vec![
            "Fence",
            "Begin:FlushStatus",
            "Local:Prepare",
            "p0:Prepare",
            "p1:Prepare",
            "Stable:Wait:10"
        ]
    );
    assert!(!entered(&f.points[0], CallbackKind::Perform));
    assert_eq!(f.local.snapshots.lock().unwrap()[0].len(), 3);
    assert!(coordinator.start(11, false).is_err());
    io.stable.complete(Ok(()));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        entered(&f.points[0], CallbackKind::Perform)
            && entered(&f.points[1], CallbackKind::Perform)
    );
    perform1.complete(Ok(()));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(completed(&f.points[1], CallbackKind::Perform));
    assert!(!events(&f.journal).contains(&"Local:Perform".into()));
    perform0.complete(Ok(()));
    observe(activity.phase1.wait())
        .await
        .as_ref()
        .clone()
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        entered(&f.points[0], CallbackKind::Complete)
            && entered(&f.points[1], CallbackKind::Complete)
    );
    assert_eq!(f.directory.checkpoint_metadata().len(), 3);
    complete0.complete(Ok(()));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!events(&f.journal).contains(&"Local:Complete".into()));
    complete1.complete(Ok(()));
    observe(activity.finished.wait())
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(
        events(&f.journal),
        vec![
            "Fence",
            "Begin:FlushStatus",
            "Local:Prepare",
            "p0:Prepare",
            "p1:Prepare",
            "Stable:Wait:10",
            "p0:Perform",
            "p1:Perform",
            "Local:Perform",
            "Append:EndCheckpoint",
            "Flush:EndCheckpoint",
            "p0:Complete",
            "p1:Complete",
            "Local:Complete",
            "Local:Cleanup",
            "Append:CompleteCheckpoint",
            "Flush:CompleteCheckpoint",
        ]
    );
    assert!(!entered(&f.points[2], CallbackKind::Prepare));
    assert!(f.directory.faults.reported().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn k02_begin_flush_local_prepare_and_sequential_provider_prepare_fail_stop() {
    for failure in ["flush", "local", "provider"] {
        let f = fixture(&["fabric:/a", "fabric:/b"]);
        let io = Io::new(&f.journal, Arc::new(|| 12));
        io.stable.complete(Ok(()));
        match failure {
            "flush" => *io.begin_status.lock().unwrap() = Err(Status::ObjectClosed.error()),
            "local" => {
                f.local
                    .failures
                    .lock()
                    .unwrap()
                    .insert("Local:Prepare", Err(Status::ObjectClosed.error()));
            }
            _ => f.points[0].inject_callback(
                CallbackKind::Prepare,
                CallbackPlan {
                    result: Err(Status::ObjectClosed.error()),
                    ..Default::default()
                },
            ),
        }
        let coordinator =
            CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
        let activity = coordinator.start(10, false).unwrap();
        assert!(observe(activity.finished.wait()).await.is_err());
        assert_eq!(activity.phase(), CheckpointPhase::Faulted);
        assert!(!entered(&f.points[1], CallbackKind::Prepare));
        assert!(!entered(&f.points[0], CallbackKind::Perform));
        assert!(!events(&f.journal).contains(&"Append:EndCheckpoint".into()));
        assert_eq!(f.directory.faults.reported().len(), 1);
        assert!(f.directory.faults.service_error().is_none());
        if failure == "flush" {
            assert!(f.local.snapshots.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn k02_post_fence_service_latch_suppresses_callbacks_and_failed_flush_takes_precedence() {
    for failed_flush in [false, true] {
        let f = fixture(&["fabric:/a", "fabric:/b"]);
        let fence = Completion::new();
        let io = Io::with_fence(&f.journal, Arc::new(|| 10), fence.clone());
        io.stable.complete(Ok(()));
        let service_error = Status::ObjectClosed.error();
        let flush_error = Status::Timeout.error();
        if failed_flush {
            *io.begin_status.lock().unwrap() = Err(flush_error.clone());
        }
        let coordinator =
            CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
        let activity = coordinator.start(10, false).unwrap();
        assert_eq!(activity.phase(), CheckpointPhase::Invalid);
        assert_eq!(events(&f.journal), vec!["Fence"]);
        assert!(f.directory.faults.service_error().is_none());
        f.directory
            .faults
            .process_service_error(ProcessingRole::Secondary, service_error.clone());
        assert!(!activity.phase1.is_completed() && !activity.finished.is_completed());
        fence.complete(Ok(()));
        let expected = if failed_flush {
            flush_error
        } else {
            service_error.clone()
        };
        for result in [
            observe(activity.phase1.wait()).await,
            observe(activity.finished.wait()).await,
        ] {
            assert_eq!(
                result.as_ref().clone().unwrap_err().native_code(),
                expected.native_code()
            );
        }
        assert_eq!(activity.phase(), CheckpointPhase::Faulted);
        assert_eq!(events(&f.journal), vec!["Fence", "Begin:FlushStatus"]);
        assert!(f.local.snapshots.lock().unwrap().is_empty());
        assert!(f.directory.checkpoint_metadata().is_empty());
        assert!(f.points.iter().all(|point| point.events().is_empty()));
        assert_eq!(
            f.directory.faults.service_error().unwrap().native_code(),
            service_error.native_code()
        );
        assert_eq!(
            f.directory
                .faults
                .observed()
                .iter()
                .map(|error| error.native_code())
                .collect::<Vec<_>>(),
            vec![service_error.native_code(), expected.native_code()]
        );
        assert_eq!(
            f.directory
                .faults
                .reported()
                .iter()
                .map(|error| error.native_code())
                .collect::<Vec<_>>(),
            vec![service_error.native_code()]
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_dropped_finish_observer_retains_phase1_and_physical_completion() {
    let f = fixture(&["fabric:/point"]);
    let perform = Completion::new();
    f.points[0].inject_callback(
        CallbackKind::Perform,
        plan(&f.journal, "point:Perform", Some(perform.clone()), Ok(())),
    );
    let io = Io::new(&f.journal, Arc::new(|| 10));
    io.stable.complete(Ok(()));
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
    let activity = coordinator.start(10, true).unwrap();
    assert!(entered(&f.points[0], CallbackKind::Perform));
    let observer = coordinator.finish_first_copy(activity.clone(), 10);
    drop(observer);
    drop(coordinator);
    assert!(!activity.finished.is_completed());
    assert!(!entered(&f.points[0], CallbackKind::Complete));
    perform.complete(Ok(()));
    observe(activity.finished.wait())
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(activity.phase(), CheckpointPhase::Completed);
    assert!(completed(&f.points[0], CallbackKind::Perform));
    assert!(completed(&f.points[0], CallbackKind::Complete));
    assert_eq!(
        events(&f.journal),
        vec![
            "Fence",
            "Begin:FlushStatus",
            "Local:Prepare",
            "Stable:Wait:10",
            "point:Perform",
            "Flush:CopyDrain",
            "Local:Perform",
            "Append:EndCheckpoint",
            "Flush:EndCheckpoint",
            "Flush:BeforeRename",
            "Rename",
            "Local:Complete",
            "Local:Cleanup",
            "Append:CompleteCheckpoint",
            "Flush:CompleteCheckpoint",
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_dropped_callback_observers_retain_peer_drain_and_local_callbacks() {
    let f = fixture(&["fabric:/a", "fabric:/b"]);
    f.directory.prepare_checkpoint(10).unwrap();
    for kind in [CallbackKind::Perform, CallbackKind::Complete] {
        let first = Completion::new();
        let second = Completion::new();
        for (index, gate) in [first.clone(), second.clone()].into_iter().enumerate() {
            f.points[index].inject_callback(
                kind,
                CallbackPlan {
                    gate: Some(gate),
                    ..Default::default()
                },
            );
        }
        let observer = match kind {
            CallbackKind::Perform => f.directory.perform_checkpoint(Cancellation::default()),
            CallbackKind::Complete => f.directory.complete_checkpoint(Cancellation::default()),
            _ => unreachable!(),
        };
        assert!(entered(&f.points[0], kind) && entered(&f.points[1], kind));
        drop(observer);
        second.complete(Ok(()));
        wait_for(|| completed(&f.points[1], kind)).await;
        let local = if kind == CallbackKind::Perform {
            "Local:Perform"
        } else {
            "Local:Complete"
        };
        assert!(!events(&f.journal).contains(&local.into()));
        first.complete(Ok(()));
        if kind == CallbackKind::Perform {
            wait_for(|| f.directory.checkpoint_metadata().len() == 2).await;
        } else {
            wait_for(|| events(&f.journal).contains(&"Local:Cleanup".into())).await;
        }
        assert!(completed(&f.points[0], kind) && completed(&f.points[1], kind));
        assert!(events(&f.journal).contains(&local.into()));
    }
    assert!(f
        .points
        .iter()
        .all(|point| point.completed_checkpoint().is_some()));
    assert_eq!(
        events(&f.journal),
        vec![
            "Local:Prepare",
            "Local:Perform",
            "Local:Complete",
            "Local:Cleanup"
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn k02_perform_and_complete_errors_wait_peers_and_keep_first_array_error() {
    for kind in [CallbackKind::Perform, CallbackKind::Complete] {
        let f = fixture(&["fabric:/a", "fabric:/b"]);
        let peer = Completion::new();
        let first_error = Status::ObjectClosed.error();
        f.points[0].inject_callback(
            kind,
            CallbackPlan {
                result: Err(first_error.clone()),
                ..Default::default()
            },
        );
        f.points[1].inject_callback(
            kind,
            CallbackPlan {
                gate: Some(peer.clone()),
                result: Err(Status::Timeout.error()),
                ..Default::default()
            },
        );
        let io = Io::new(&f.journal, Arc::new(|| 10));
        io.stable.complete(Ok(()));
        let coordinator =
            CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
        let activity = coordinator.start(10, false).unwrap();
        assert!(entered(&f.points[0], kind) && entered(&f.points[1], kind));
        assert!(!activity.finished.is_completed());
        peer.complete(Ok(()));
        assert_eq!(
            observe(activity.finished.wait())
                .await
                .as_ref()
                .clone()
                .unwrap_err()
                .native_code(),
            first_error.native_code()
        );
        assert!(!events(&f.journal).contains(&"Local:Complete".into()));
        assert!(!events(&f.journal).contains(&"Append:CompleteCheckpoint".into()));
        if kind == CallbackKind::Perform {
            assert_eq!(activity.phase(), CheckpointPhase::Faulted);
            assert!(!events(&f.journal).contains(&"Local:Perform".into()));
            assert!(!entered(&f.points[1], CallbackKind::Complete));
        } else {
            assert_eq!(activity.phase(), CheckpointPhase::Completed);
            assert!(events(&f.journal).contains(&"Append:EndCheckpoint".into()));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn k02_local_perform_complete_cleanup_failure_preserves_prior_effects() {
    for kind in ["Local:Perform", "Local:Complete", "Local:Cleanup"] {
        let f = fixture(&["fabric:/a"]);
        f.local
            .failures
            .lock()
            .unwrap()
            .insert(kind, Err(Status::ObjectClosed.error()));
        let io = Io::new(&f.journal, Arc::new(|| 10));
        io.stable.complete(Ok(()));
        let coordinator =
            CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
        let activity = coordinator.start(10, false).unwrap();
        assert!(observe(activity.finished.wait()).await.is_err());
        assert!(completed(&f.points[0], CallbackKind::Perform));
        assert!(!events(&f.journal).contains(&"Append:CompleteCheckpoint".into()));
        if kind == "Local:Perform" {
            assert!(f.directory.checkpoint_metadata().is_empty());
            assert!(!entered(&f.points[0], CallbackKind::Complete));
        } else {
            assert!(completed(&f.points[0], CallbackKind::Complete));
            assert!(f.points[0].completed_checkpoint().is_some());
            assert_eq!(
                events(&f.journal).contains(&"Local:Cleanup".into()),
                kind == "Local:Cleanup"
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn k03_first_copy_phase1_delayed_finish_uses_actual_tail_and_rename_order() {
    let f = fixture(&["fabric:/point"]);
    let manager = KernelManager::established(
        f.directory.clone(),
        Arc::new(ControlledLower::new(2)),
        ProcessingRole::Secondary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let record = Record {
        lsn: 12,
        kind: RecordKind::Barrier { last_stable_lsn: 8 },
    };
    let barrier = manager
        .replay_record(record.encode().unwrap(), None)
        .unwrap();
    // The supported replay map checks contiguous records; emit prefix as well.
    for lsn in 2..12 {
        manager
            .replay_record(
                Record {
                    lsn,
                    kind: RecordKind::Barrier { last_stable_lsn: 5 },
                }
                .encode()
                .unwrap(),
                None,
            )
            .unwrap();
    }
    manager.flush(Ok(()));
    observe(barrier.processing.wait()).await;
    assert_eq!(
        (manager.tail(), manager.stable(), manager.version()),
        (12, 8, 12)
    );
    let tail_manager = manager.clone();
    let io = Io::new(&f.journal, Arc::new(move || tail_manager.tail()));
    let perform_gate = Completion::new();
    f.points[0].inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(perform_gate.clone()),
            ..Default::default()
        },
    );
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io.clone(), manager.faults.clone());
    let activity = coordinator.start(8, true).unwrap();
    io.stable.complete(Ok(()));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(entered(&f.points[0], CallbackKind::Perform));
    assert!(coordinator
        .finish_first_copy(activity.clone(), manager.stable())
        .await
        .is_err());
    let finish = coordinator.finish_first_copy(activity.clone(), manager.tail());
    assert!(!activity.phase1.is_completed());
    assert!(events(&f.journal).contains(&"Flush:CopyDrain".into()));
    assert!(!events(&f.journal).contains(&"Append:EndCheckpoint".into()));
    perform_gate.complete(Ok(()));
    observe(finish).await.unwrap();
    assert_eq!(
        events(&f.journal),
        vec![
            "Fence",
            "Begin:FlushStatus",
            "Local:Prepare",
            "Stable:Wait:8",
            "Flush:CopyDrain",
            "Local:Perform",
            "Append:EndCheckpoint",
            "Flush:EndCheckpoint",
            "Flush:BeforeRename",
            "Rename",
            "Local:Complete",
            "Local:Cleanup",
            "Append:CompleteCheckpoint",
            "Flush:CompleteCheckpoint",
        ]
    );
    assert!(activity.finished.is_completed());
}

#[tokio::test(flavor = "current_thread")]
async fn k03_interruption_waits_phase1_faults_without_end_complete_or_rename() {
    let f = fixture(&["fabric:/point"]);
    let gate = Completion::new();
    f.points[0].inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(gate.clone()),
            ..Default::default()
        },
    );
    let io = Io::new(&f.journal, Arc::new(|| 9));
    io.stable.complete(Ok(()));
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
    let activity = coordinator.start(8, true).unwrap();
    let cancel = coordinator.cancel_first_copy(activity.clone(), 12);
    assert!(!activity.finished.is_completed());
    gate.complete(Ok(()));
    assert!(Status::InvalidOperation.matches(&observe(cancel).await.unwrap_err()));
    assert!(activity.phase1.is_completed() && completed(&f.points[0], CallbackKind::Perform));
    assert!(!entered(&f.points[0], CallbackKind::Complete));
    assert!(!events(&f.journal)
        .iter()
        .any(|event| event.starts_with("Append:") || event == "Rename"));
    assert!(f.directory.faults.is_closing());
}

#[tokio::test(flavor = "current_thread")]
async fn k03_phase1_failure_and_rename_error_never_complete_provider() {
    for phase1_failure in [true, false] {
        let f = fixture(&["fabric:/point"]);
        if phase1_failure {
            f.points[0].inject_callback(
                CallbackKind::Perform,
                CallbackPlan {
                    result: Err(Status::ObjectClosed.error()),
                    ..Default::default()
                },
            );
        }
        let io = Io::new(&f.journal, Arc::new(|| 12));
        io.stable.complete(Ok(()));
        *io.rename_status.lock().unwrap() = Err(Status::ObjectClosed.error());
        let coordinator =
            CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
        let activity = coordinator.start(8, true).unwrap();
        observe(activity.phase1.wait()).await;
        assert!(!entered(&f.points[0], CallbackKind::Complete));
        assert!(!events(&f.journal).contains(&"Append:EndCheckpoint".into()));
        assert!(observe(coordinator.finish_first_copy(activity.clone(), 12))
            .await
            .is_err());
        assert!(!entered(&f.points[0], CallbackKind::Complete));
        assert!(!events(&f.journal).contains(&"Append:CompleteCheckpoint".into()));
        assert_eq!(
            events(&f.journal).contains(&"Rename".into()),
            !phase1_failure
        );
    }
}

struct Operation {
    data: OperationData,
    id: usize,
    journal: Journal,
}
impl CopyOperation for Operation {
    fn data(&self) -> &OperationData {
        &self.data
    }
    fn acknowledge(&self) {
        event(&self.journal, format!("ACK:{}", self.id));
    }
}
struct StageStream {
    operations: Mutex<VecDeque<StageResult>>,
    on_transition: Option<Arc<dyn Fn() + Send + Sync>>,
}
type StageResult = Result<Option<Arc<dyn CopyOperation>>>;
impl CopyStageStream for StageStream {
    fn get_operation(&self) -> ApiFuture<'_, Option<Arc<dyn CopyOperation>>> {
        Box::pin(async {
            let result = self
                .operations
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(None));
            if let Ok(Some(operation)) = &result {
                if operation
                    .data()
                    .segments()
                    .last()
                    .is_some_and(|stage| stage.as_ref() == 4u32.to_le_bytes())
                {
                    if let Some(callback) = &self.on_transition {
                        callback();
                    }
                }
            }
            result
        })
    }
}
fn operation(
    journal: &Journal,
    id: usize,
    data: OperationData,
    stage: u32,
) -> Arc<dyn CopyOperation> {
    let mut segments = data.to_segments();
    segments.push(stage.to_le_bytes().to_vec());
    Arc::new(Operation {
        data: OperationData::new(segments).unwrap(),
        id,
        journal: journal.clone(),
    })
}
fn stages(operations: &[Arc<dyn CopyOperation>]) -> StageStream {
    StageStream {
        operations: Mutex::new(
            operations
                .iter()
                .cloned()
                .map(|operation| Ok(Some(operation)))
                .collect(),
        ),
        on_transition: None,
    }
}
fn registry_operations(f: &Fixture, providers: &[i64]) -> Vec<Arc<dyn CopyOperation>> {
    let packet = f.local.packet(
        providers
            .iter()
            .map(|id| {
                (
                    *id,
                    f.directory.entry(*id).unwrap().provider.get_name().into(),
                    ProviderMode::Active,
                )
            })
            .collect(),
    );
    vec![
        operation(
            &f.journal,
            0,
            named::encode_copy(REGISTRY_ID, &version()).unwrap(),
            3,
        ),
        operation(
            &f.journal,
            1,
            named::encode_copy(REGISTRY_ID, &packet).unwrap(),
            3,
        ),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn c01_registry_first_descending_names_null_and_header_only_then_explicit_dispose() {
    let f = fixture(&["fabric:/a", "fabric:/z", "fabric:/m"]);
    f.points[0].set_copy_stream(None);
    f.points[1].set_copy_stream(Some(vec![empty()]));
    f.points[2].set_copy_stream(Some(vec![PointProvider::payload(1, 2)]));
    checkpoint(&f).await;
    let stream = f.directory.get_current_state(88).unwrap();
    assert_eq!(*f.local.targets.lock().unwrap(), vec![88]);
    let mut output = Vec::new();
    while let Some(data) = observe(stream.get_next(Cancellation::default()))
        .await
        .unwrap()
    {
        output.push(named::decode_copy(&data).unwrap());
    }
    assert_eq!(
        output.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![REGISTRY_ID, REGISTRY_ID, 8, 9]
    );
    assert_eq!(output[0].1, version());
    assert!(output[2].1.segments().is_empty());
    assert!(!events(&f.journal)
        .iter()
        .any(|event| event.ends_with(":Dispose")));
    // Repeated EOS hits the source empty-remaining-collection status quirk.
    assert!(stream.get_next(Cancellation::default()).await.is_err());
    stream.dispose().unwrap();
    stream.dispose().unwrap();
    assert_eq!(
        events(&f.journal)
            .iter()
            .filter(|event| event.as_str() == "registry:Dispose")
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn c01_constituent_error_keeps_cursor_token_and_dispose_order_includes_unvisited() {
    let journal = Arc::new(Mutex::new(Vec::new()));
    let registry = TestStream::new("registry", &journal, Vec::new());
    let z = TestStream::new("z", &journal, vec![empty()]);
    z.data
        .lock()
        .unwrap()
        .push_front(Err(Status::Timeout.error()));
    let a = TestStream::new("a", &journal, Vec::new());
    let stream = super::copy::NamedStateStream::new(registry, vec![(8, z), (7, a)]);
    let cancellation = Cancellation::default();
    cancellation.cancel();
    assert!(Status::Timeout.matches(&stream.get_next(cancellation.clone()).await.unwrap_err()));
    let data = stream.get_next(cancellation).await.unwrap().unwrap();
    assert_eq!(named::decode_copy(&data).unwrap(), (8, empty()));
    stream.dispose().unwrap();
    assert_eq!(
        events(&journal),
        vec![
            "registry:Next:true",
            "z:Next:true",
            "z:Next:true",
            "registry:Dispose",
            "z:Dispose",
            "a:Dispose",
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn c01_dropped_named_eos_observer_retains_self_and_advances_to_different_child() {
    let journal = Arc::new(Mutex::new(Vec::new()));
    let gate = Completion::new();
    let registry = Arc::new(GatedEosStream {
        journal: journal.clone(),
        gate: gate.clone(),
    });
    let first = PointProvider::payload(1, 10);
    let second = PointProvider::payload(2, 20);
    let child = TestStream::new("point", &journal, vec![first, second.clone()]);
    let stream = super::copy::NamedStateStream::new(registry, vec![(7, child)]);
    let retained = Arc::downgrade(&stream);
    let observer = stream.get_next(Cancellation::default());
    assert_eq!(events(&journal), vec!["gated:Next:false"]);
    drop(observer);
    drop(stream);
    let stream = retained
        .upgrade()
        .expect("accepted named enumeration owns self");
    gate.complete(Ok(()));
    wait_for(|| events(&journal).contains(&"point:Next:false".into())).await;
    let data = observe(stream.get_next(Cancellation::default()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(named::decode_copy(&data).unwrap(), (7, second));
    assert_eq!(
        events(&journal),
        vec![
            "gated:Next:false",
            "gated:EOS",
            "point:Next:false",
            "point:Next:false"
        ]
    );
    assert!(stream
        .get_next(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    assert!(!events(&journal)
        .iter()
        .any(|event| event.ends_with(":Dispose")));
    stream.dispose().unwrap();
    assert_eq!(
        events(&journal)
            .iter()
            .rev()
            .take(2)
            .cloned()
            .collect::<Vec<_>>(),
        vec!["point:Dispose", "gated:Dispose"]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn c02_dropped_drain_set_observer_retains_ack_transition_poll_and_end() {
    let f = fixture(&["fabric:/point"]);
    let set_gate = Completion::new();
    let end_gate = Completion::new();
    f.points[0].inject_callback(
        CallbackKind::SetCopy,
        plan(&f.journal, "point:Set", Some(set_gate.clone()), Ok(())),
    );
    f.points[0].inject_callback(
        CallbackKind::EndCopy,
        plan(&f.journal, "point:End", Some(end_gate.clone()), Ok(())),
    );
    let mut operations = registry_operations(&f, &[7]);
    operations.push(operation(
        &f.journal,
        2,
        named::encode_copy(7, &PointProvider::payload(4, 40)).unwrap(),
        3,
    ));
    operations.push(operation(&f.journal, 3, empty(), 4));
    let transition_seen = Completion::new();
    let mut stream = stages(&operations);
    let transition_signal = transition_seen.clone();
    stream.on_transition = Some(Arc::new(move || {
        transition_signal.complete(Ok(()));
    }));
    let stream = Arc::new(stream);
    let retained = Arc::downgrade(&stream);
    let observer = drain_state(f.directory.clone(), stream.clone());
    assert!(entered(&f.points[0], CallbackKind::SetCopy));
    assert!(!completed(&f.points[0], CallbackKind::SetCopy));
    assert!(!events(&f.journal).contains(&"ACK:2".into()));
    drop(observer);
    drop(stream);
    assert!(retained.upgrade().is_some());
    assert!(!entered(&f.points[0], CallbackKind::EndCopy));
    set_gate.complete(Ok(()));
    observe(transition_seen.wait())
        .await
        .as_ref()
        .clone()
        .unwrap();
    wait_for(|| entered(&f.points[0], CallbackKind::EndCopy)).await;
    assert!(completed(&f.points[0], CallbackKind::SetCopy));
    assert!(f.points[0].copied().is_empty());
    assert_eq!(f.directory.copy_progress(), Some(vec![7]));
    assert!(!completed(&f.points[0], CallbackKind::EndCopy));
    assert_eq!(
        events(&f.journal),
        vec![
            "Local:Begin",
            "Local:Set",
            "Local:Set:0",
            "ACK:0",
            "Local:Set",
            "Local:Set:1",
            "ACK:1",
            "point:Set",
            "ACK:2",
            "Local:End",
            "point:End"
        ]
    );
    end_gate.complete(Ok(()));
    wait_for(|| f.directory.copy_progress().is_none() && retained.upgrade().is_none()).await;
    assert!(completed(&f.points[0], CallbackKind::EndCopy));
    assert_eq!(f.points[0].copied(), BTreeMap::from([(4, 40)]));
    assert!(!events(&f.journal).contains(&"ACK:3".into()));
    assert!(retained.upgrade().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn c02_c04_registry_begins_null_stream_provider_global_numbers_and_same_transition() {
    let f = fixture(&["fabric:/null", "fabric:/data", "fabric:/late"]);
    f.points[0].set_copy_stream(None);
    let mut operations = registry_operations(&f, &[7, 8]);
    operations.push(operation(
        &f.journal,
        2,
        named::encode_copy(8, &PointProvider::payload(4, 40)).unwrap(),
        3,
    ));
    let transition = operation(
        &f.journal,
        3,
        OperationData::new(vec![vec![9, 8, 7]]).unwrap(),
        4,
    );
    operations.push(transition.clone());
    let late_directory = f.directory.clone();
    let mut stream = stages(&operations);
    stream.on_transition = Some(Arc::new(move || {
        // Initialized late replay admission, not copied registry Begin.
        late_directory.set_mode(9, ProviderMode::Active, None);
    }));
    let result = observe(drain_state(f.directory.clone(), Arc::new(stream)))
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&result, &transition));
    assert!(
        completed(&f.points[0], CallbackKind::BeginCopy)
            && completed(&f.points[0], CallbackKind::EndCopy)
    );
    assert!(
        completed(&f.points[1], CallbackKind::BeginCopy)
            && completed(&f.points[1], CallbackKind::EndCopy)
    );
    assert!(!entered(&f.points[2], CallbackKind::EndCopy));
    assert_eq!(f.directory.entry(9).unwrap().mode, ProviderMode::Active);
    assert!(f.points[1].events().contains(&PointEvent::CopySet {
        number: 2,
        data: PointProvider::payload(4, 40),
        cancelled: false,
    }));
    assert_eq!(f.points[1].copied(), BTreeMap::from([(4, 40)]));
    assert!(f.points[1].versions(4).is_empty()); // Copy installation is not transaction publication.
    assert_eq!(f.directory.copy_progress(), None);
    assert_eq!(
        events(&f.journal),
        vec![
            "Local:Begin",
            "Local:Set",
            "Local:Set:0",
            "ACK:0",
            "Local:Set",
            "Local:Set:1",
            "ACK:1",
            "ACK:2",
            "Local:End"
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn c03_zero_first_is_success_interrupted_eos_and_poll_failure_never_end() {
    let f = fixture(&["fabric:/point"]);
    assert!(drain_state(f.directory.clone(), Arc::new(stages(&[])))
        .await
        .unwrap()
        .is_none());
    assert!(events(&f.journal).is_empty() && f.directory.copy_progress().is_none());
    for poll_failure in [false, true] {
        let f = fixture(&["fabric:/point"]);
        let operations = registry_operations(&f, &[7]);
        let stream = stages(&operations);
        if poll_failure {
            stream
                .operations
                .lock()
                .unwrap()
                .push_back(Err(Status::Timeout.error()));
        }
        let error = observe(drain_state(f.directory.clone(), Arc::new(stream)))
            .await
            .err()
            .unwrap();
        assert!((if poll_failure {
            Status::Timeout
        } else {
            Status::Cancelled
        })
        .matches(&error));
        assert_eq!(f.directory.copy_progress(), Some(vec![7]));
        assert!(completed(&f.points[0], CallbackKind::BeginCopy));
        assert!(!entered(&f.points[0], CallbackKind::EndCopy));
        assert!(!events(&f.journal).contains(&"Local:End".into()));
        assert!(events(&f.journal).contains(&"ACK:1".into()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn c03_set_failure_no_failing_ack_no_end_retains_prior_progress() {
    for local_failure in [false, true] {
        let f = fixture(&["fabric:/point"]);
        let mut operations = registry_operations(&f, &[7]);
        operations.push(operation(
            &f.journal,
            2,
            named::encode_copy(7, &PointProvider::payload(4, 40)).unwrap(),
            3,
        ));
        operations.push(operation(&f.journal, 3, empty(), 4));
        if local_failure {
            f.local
                .failures
                .lock()
                .unwrap()
                .insert("Local:Set", Err(Status::ObjectClosed.error()));
        } else {
            f.points[0].inject_callback(
                CallbackKind::SetCopy,
                CallbackPlan {
                    result: Err(Status::ObjectClosed.error()),
                    ..Default::default()
                },
            );
        }
        assert!(observe(drain_state(
            f.directory.clone(),
            Arc::new(stages(&operations))
        ))
        .await
        .is_err());
        assert!(!events(&f.journal).contains(&"ACK:2".into()));
        assert!(!events(&f.journal).contains(&"Local:End".into()));
        assert!(!entered(&f.points[0], CallbackKind::EndCopy));
        assert_eq!(events(&f.journal).contains(&"ACK:1".into()), !local_failure);
        assert_eq!(
            f.directory.copy_progress(),
            Some(if local_failure { vec![] } else { vec![7] })
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn c03_end_failure_keeps_progress_and_local_precedes_all_begun_peers() {
    for local_failure in [false, true] {
        let f = fixture(&["fabric:/a", "fabric:/b"]);
        let mut operations = registry_operations(&f, &[7, 8]);
        operations.push(operation(&f.journal, 2, empty(), 4));
        let peer = Completion::new();
        if local_failure {
            f.local
                .failures
                .lock()
                .unwrap()
                .insert("Local:End", Err(Status::ObjectClosed.error()));
        } else {
            f.points[0].inject_callback(
                CallbackKind::EndCopy,
                plan(
                    &f.journal,
                    "p0:End",
                    None,
                    Err(Status::ObjectClosed.error()),
                ),
            );
            f.points[1].inject_callback(
                CallbackKind::EndCopy,
                plan(&f.journal, "p1:End", Some(peer.clone()), Ok(())),
            );
        }
        let stream = stages(&operations);
        let work = drain_state(f.directory.clone(), Arc::new(stream));
        tokio::pin!(work);
        if !local_failure {
            assert!(tokio::time::timeout(Duration::from_millis(5), &mut work)
                .await
                .is_err());
            assert!(entered(&f.points[1], CallbackKind::EndCopy));
            assert_eq!(
                events(&f.journal)
                    .iter()
                    .rev()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>(),
                vec!["p1:End", "p0:End", "Local:End"]
            );
            peer.complete(Ok(()));
        }
        assert!(observe(work).await.is_err());
        assert_eq!(f.directory.copy_progress(), Some(vec![7, 8]));
        assert!(!events(&f.journal).contains(&"ACK:2".into()));
        assert_eq!(entered(&f.points[0], CallbackKind::EndCopy), !local_failure);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn copy_registry_begin_failure_admission_modes_and_begin_local_failure_preserve_effects() {
    let f = fixture(&["fabric:/a", "fabric:/b"]);
    f.points[1].inject_callback(
        CallbackKind::BeginCopy,
        CallbackPlan {
            result: Err(Status::ObjectClosed.error()),
            ..Default::default()
        },
    );
    let operations = registry_operations(&f, &[7, 8]);
    assert!(
        drain_state(f.directory.clone(), Arc::new(stages(&operations)))
            .await
            .is_err()
    );
    assert_eq!(f.directory.copy_progress(), Some(vec![7]));
    assert_eq!(f.directory.entry(8).unwrap().mode, ProviderMode::Active);
    assert!(!events(&f.journal).contains(&"ACK:1".into()));
    assert!(!entered(&f.points[0], CallbackKind::EndCopy));
    f.directory.set_mode(7, ProviderMode::DelayDelete, None);
    let data = named::encode_copy(7, &empty()).unwrap();
    assert!(Status::ObjectClosed.matches(
        &f.directory
            .set_current_state(-7, data.clone())
            .await
            .unwrap_err()
    ));
    f.directory.set_mode(7, ProviderMode::Stale, None);
    assert!(
        Status::NotImplemented.matches(&f.directory.set_current_state(-7, data).await.unwrap_err())
    );

    let f = fixture(&["fabric:/a"]);
    f.local
        .failures
        .lock()
        .unwrap()
        .insert("Local:Begin", Err(Status::ObjectClosed.error()));
    assert!(f.directory.begin_setting_current_state().await.is_err());
    assert_eq!(f.directory.copy_progress(), Some(vec![]));
    assert_eq!(f.directory.entry(7).unwrap().mode, ProviderMode::Stale);
    assert!(!entered(&f.points[0], CallbackKind::BeginCopy));
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_captures_registry_before_mutation_and_point_snapshot_not_later_writes() {
    let f = fixture(&["fabric:/point"]);
    let manager = KernelManager::established(
        f.directory.clone(),
        Arc::new(ControlledLower::new(2)),
        ProcessingRole::Primary,
        0,
        Arc::new(RetryRuntime::default()),
    );
    let writer = manager.new_transaction().unwrap();
    f.points[0].write(writer.as_ref(), 7, 4, 40).unwrap();
    let commit = writer.commit();
    manager.barrier(0).unwrap();
    manager.flush(Ok(()));
    manager.replication(2, Ok(()));
    manager.replication(3, Ok(()));
    observe(commit).await.unwrap();
    let gate = Completion::new();
    f.points[0].inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(gate.clone()),
            ..Default::default()
        },
    );
    f.directory.prepare_checkpoint(3).unwrap();
    let perform = f.directory.perform_checkpoint(Cancellation::default());
    // A real transaction can continue while checkpoint Perform is suspended.
    let later = manager.new_transaction().unwrap();
    f.points[0].write(later.as_ref(), 7, 4, 99).unwrap();
    let later_commit = later.commit();
    manager.barrier(0).unwrap();
    manager.flush(Ok(()));
    manager.replication(4, Ok(()));
    manager.replication(5, Ok(()));
    observe(later_commit).await.unwrap();
    assert_eq!(f.points[0].versions(4).last().unwrap().value, 99);
    assert!(!completed(&f.points[0], CallbackKind::Perform));
    f.directory.set_mode(7, ProviderMode::DelayDelete, None);
    gate.complete(Ok(()));
    observe(perform).await.unwrap();
    observe(f.directory.complete_checkpoint(Cancellation::default()))
        .await
        .unwrap();
    assert_eq!(
        f.directory.checkpoint_metadata()[0].mode,
        ProviderMode::Active
    );
    assert_eq!(
        f.points[0].completed_checkpoint(),
        Some(BTreeMap::from([(4, 40)]))
    );
    assert!(completed(&f.points[0], CallbackKind::Complete));
    writer.dispose().unwrap();
    later.dispose().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn k01_void_physical_flush_does_not_invent_checked_record_status_branch() {
    let f = fixture(&["fabric:/point"]);
    let io = Io::new(&f.journal, Arc::new(|| 10));
    io.stable.complete(Ok(()));
    *io.void_record_flush_failed.lock().unwrap() = true;
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
    let activity = coordinator.start(10, false).unwrap();
    observe(activity.finished.wait())
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert!(completed(&f.points[0], CallbackKind::Complete));
    assert!(events(&f.journal).contains(&"Append:CompleteCheckpoint".into()));
    assert_eq!(
        events(&f.journal)
            .iter()
            .filter(|event| event.as_str() == "Physical:FailedStatusNotReturnedByVoidFlush")
            .count(),
        2
    );
    // The side-effect seam made no physical durability promise.
}

#[tokio::test(flavor = "current_thread")]
async fn c01_factory_failure_explicitly_disposes_already_created_registry_stream() {
    let f = fixture(&["fabric:/point"]);
    checkpoint(&f).await;
    f.points[0].inject_callback(
        CallbackKind::GetCopy,
        CallbackPlan {
            result: Err(Status::ObjectClosed.error()),
            ..Default::default()
        },
    );
    assert!(f.directory.get_current_state(4).is_err());
    assert_eq!(
        events(&f.journal)
            .iter()
            .rev()
            .take(2)
            .cloned()
            .collect::<Vec<_>>(),
        vec!["registry:Dispose", "Local:Get"]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_primary_first_copy_and_negative_lsn_reject_before_callbacks() {
    let f = fixture(&["fabric:/point"]);
    let io = Io::new(&f.journal, Arc::new(|| 10));
    *io.role.lock().unwrap() = ProcessingRole::Primary;
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
    assert!(coordinator.start(10, true).is_err());
    assert!(coordinator.start(-1, false).is_err());
    assert!(events(&f.journal).is_empty() && f.points[0].events().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_dispatch_fence_blocks_prepare_and_faults_even_in_recovery_without_service_latch(
) {
    let f = fixture(&["fabric:/point"]);
    let fence = Completion::new();
    let io = Io::with_fence(&f.journal, Arc::new(|| 10), fence.clone());
    *io.role.lock().unwrap() = ProcessingRole::Recovery;
    io.stable.complete(Ok(()));
    f.points[0].inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            result: Err(Status::ObjectClosed.error()),
            ..Default::default()
        },
    );
    let coordinator =
        CheckpointCoordinator::new(f.directory.clone(), io, f.directory.faults.clone());
    let activity = coordinator.start(10, false).unwrap();
    assert_eq!(events(&f.journal), vec!["Fence"]);
    assert_eq!(activity.phase(), CheckpointPhase::Invalid);
    fence.complete(Ok(()));
    assert!(observe(activity.finished.wait()).await.is_err());
    assert_eq!(f.directory.faults.reported().len(), 1);
    assert!(f.directory.faults.service_error().is_none() && f.directory.faults.is_closing());
}

#[tokio::test(flavor = "current_thread")]
async fn copy_signed_provider_record_and_header_only_payload_are_not_eos() {
    let f = fixture(&["fabric:/point"]);
    let packet = f
        .local
        .packet(vec![(7, "fabric:/point".into(), ProviderMode::Active)]);
    f.directory.begin_setting_current_state().await.unwrap();
    f.directory
        .set_current_state(0, named::encode_copy(REGISTRY_ID, &packet).unwrap())
        .await
        .unwrap();
    f.directory
        .set_current_state(-7, named::encode_copy(7, &empty()).unwrap())
        .await
        .unwrap();
    assert!(f.points[0].events().contains(&PointEvent::CopySet {
        number: -7,
        data: empty(),
        cancelled: false
    }));
    assert!(completed(&f.points[0], CallbackKind::SetCopy));
    f.directory.end_setting_current_state().await.unwrap();
    assert!(completed(&f.points[0], CallbackKind::EndCopy));
}

#[tokio::test(flavor = "current_thread")]
async fn copy_uninitialized_registry_provider_rejects_without_fake_lifecycle_or_ack() {
    let f = fixture(&["fabric:/point"]);
    let packet = f.local.packet(vec![(
        99,
        "fabric:/uninitialized".into(),
        ProviderMode::Active,
    )]);
    let operations = vec![operation(
        &f.journal,
        0,
        named::encode_copy(REGISTRY_ID, &packet).unwrap(),
        3,
    )];
    let error = drain_state(f.directory.clone(), Arc::new(stages(&operations)))
        .await
        .err()
        .unwrap();
    assert!(Status::NotImplemented.matches(&error));
    assert_eq!(f.directory.copy_progress(), Some(vec![]));
    assert!(f.points[0].events().is_empty());
    assert!(!events(&f.journal).contains(&"ACK:0".into()));
    assert!(!events(&f.journal).contains(&"Local:End".into()));
}
