//! Independent upper admission ownership over the ONE standard lower engine.
//! Native TransactionManager.cpp:328–478 distinguishes admission from completion.
//! Application captures never enter lower Submit or its owner/destructor locks.
use super::{
    map::RecordNode,
    processor::{Activity, KernelManager},
    record::{Record, RecordKind},
    version::Completion,
};
use crate::{ApiFuture, Cancellation, Error, OperationContext, Result, TransactionBase};
use fube_data::{extensions::ReplicatorDiagnostics, FabricReplicator, IStateReplicator};
use fube_runtime::extensions::{
    CallLane, CallOrigin, DisposalTracker, ExecutionRuntime, ObjectQuota,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, Mutex, Weak,
};
use std::time::{Duration, Instant};
#[cfg(test)]
type PublicationPause = (i64, mpsc::SyncSender<()>, mpsc::Receiver<()>);
#[cfg(test)]
type GroupAttempt = (u64, Result<i64>);
fn allocation(error: Error) -> Error {
    if error.kind() == fube_runtime::ErrorKind::ResourceExhausted {
        // Explicit finite Rust allocator boundary, not native writer throttle.
        super::status::Status::InsufficientResources.error()
    } else {
        error
    }
}

pub(crate) struct RuntimeLower {
    self_weak: Weak<Self>,
    #[cfg(test)]
    pub(crate) publication_pause: Arc<Mutex<Option<PublicationPause>>>,
    #[cfg(test)]
    pub(crate) group_attempts: Mutex<Vec<GroupAttempt>>,
    #[cfg(test)]
    pub(crate) group_pause: Mutex<Option<Completion<()>>>,
    lower: Arc<FabricReplicator>,
    execution: ExecutionRuntime,
    drivers: tokio::runtime::Handle,
    payloads: ObjectQuota,
    barriers_enabled: AtomicBool,
    first_primary: tokio::sync::Mutex<bool>,
    group: Mutex<GroupCommit>,
    disposal: DisposalTracker,
    live: Arc<Live>,
    publication: Arc<PublicationState>,
}
struct PublicationState {
    state: Mutex<(bool, usize)>, // accepting, not-yet-flushed dispositions
    changed: tokio::sync::Notify,
}
struct PublicationGuard(Arc<PublicationState>);
impl Drop for PublicationGuard {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().1 -= 1;
        self.0.changed.notify_waiters();
    }
}
struct GroupCommit {
    needed: bool,
    running: bool,
    delay_ms: u64,
}
#[derive(Default)]
struct Live {
    count: AtomicUsize,
    changed: tokio::sync::Notify,
}
struct Driver(Arc<Live>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

struct Payload {
    record: Record,
    transaction: Option<Arc<dyn TransactionBase>>,
    context: Option<Arc<dyn OperationContext>>,
    memory: Option<Arc<super::memory_log::Retention>>,
}

struct Admission {
    initial: Completion<Arc<Activity>>,
    synchronous: Mutex<Option<mpsc::SyncSender<Result<Arc<Activity>>>>>,
}
impl Admission {
    fn publish(&self, result: Result<Arc<Activity>>) {
        if self.initial.complete(result.clone()) {
            if let Some(sender) = self.synchronous.lock().expect("admission poisoned").take() {
                let _ = sender.send(result);
            }
        }
    }
}

impl RuntimeLower {
    pub(crate) fn new(lower: Arc<FabricReplicator>) -> Result<Arc<Self>> {
        let payloads = ObjectQuota::new(32)?;
        Ok(Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            #[cfg(test)]
            publication_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            group_attempts: Mutex::new(Vec::new()),
            #[cfg(test)]
            group_pause: Mutex::new(None),
            execution: lower.execution_runtime(),
            lower,
            drivers: tokio::runtime::Handle::current(),
            payloads,
            barriers_enabled: AtomicBool::new(false),
            first_primary: tokio::sync::Mutex::new(false),
            group: Mutex::new(GroupCommit {
                needed: false,
                running: false,
                delay_ms: 2,
            }),
            disposal: DisposalTracker::default(),
            live: Arc::new(Live::default()),
            publication: Arc::new(PublicationState {
                state: Mutex::new((true, 0)),
                changed: tokio::sync::Notify::new(),
            }),
        }))
    }
    fn self_owner(&self) -> Arc<Self> {
        self.self_weak.upgrade().expect("runtime owner")
    }

    /// Enable only native upper scheduling state; lower control remains
    /// the role/quorum authority. The first replicated record is still emitted
    /// lazily by the native first-write election gate, not by read-only commit.
    pub(crate) fn enable_barriers(&self, _: &Arc<KernelManager>) -> Result<()> {
        let mut publication = self.publication.state.lock().unwrap();
        if !publication.0 && self.live.count.load(Ordering::Acquire) != 0 {
            return Err(Error::InvalidInput(
                "previous scheduling generation is not drained",
            ));
        }
        self.barriers_enabled.store(true, Ordering::Release);
        publication.0 = true;
        Ok(())
    }

    fn reserve_publication(&self) -> Result<PublicationGuard> {
        let mut state = self.publication.state.lock().unwrap();
        if !state.0 {
            return Err(Error::Closed);
        }
        state.1 += 1;
        Ok(PublicationGuard(self.publication.clone()))
    }

    fn fence_publication(&self) {
        let mut state = self.publication.state.lock().unwrap();
        state.0 = false;
        self.barriers_enabled.store(false, Ordering::Release);
    }

    async fn publications_drained(&self) {
        loop {
            let changed = self.publication.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.publication.state.lock().unwrap().1 == 0 {
                return;
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_publications(&self) {
        self.publications_drained().await;
    }

    async fn ensure_first(&self, manager: Arc<KernelManager>, asynchronous: bool) -> Result<()> {
        let mut established = self.first_primary.lock().await;
        if *established {
            return Ok(());
        }
        let first = self
            .admit_async(
                manager.clone(),
                Record {
                    lsn: -1,
                    kind: RecordKind::Barrier {
                        last_stable_lsn: manager.stable(),
                    },
                },
                None,
                None,
            )
            .await?;
        *established = true;
        drop(established);
        // Native sync Begin/Add only owns the barrier continuation. The first
        // async winner waits its processing; subsequent established callers do
        // not invent a shared serialized transaction coordinator.
        if asynchronous {
            first.processing.wait().await.as_ref().clone()?;
        }
        Ok(())
    }

    fn schedule_group(&self, manager: Arc<KernelManager>) {
        if !self.barriers_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut group = self.group.lock().unwrap();
        group.needed = true;
        if group.running {
            return;
        }
        group.running = true;
        drop(group);
        let runtime = self.self_owner();
        self.live.count.fetch_add(1, Ordering::AcqRel);
        let driver = Driver(self.live.clone());
        drop(self.drivers.spawn(async move {
            let _driver = driver;
            loop {
                #[cfg(test)]
                {
                    let paused = runtime.group_pause.lock().unwrap().clone();
                    if let Some(paused) = paused {
                        let _ = paused.wait().await;
                    }
                }
                let delay = runtime.group.lock().unwrap().delay_ms;
                tokio::time::sleep(Duration::from_millis(delay)).await;
                if !runtime.barriers_enabled.load(Ordering::Acquire)
                    || manager.faults.is_closing()
                    || manager.gate().is_err()
                {
                    runtime.group.lock().unwrap().running = false;
                    break;
                }
                runtime.group.lock().unwrap().needed = false;
                let barrier = runtime
                    .admit_async(
                        manager.clone(),
                        Record {
                            lsn: -1,
                            kind: RecordKind::Barrier {
                                last_stable_lsn: manager.stable(),
                            },
                        },
                        None,
                        None,
                    )
                    .await;
                #[cfg(test)]
                {
                    let mut attempts = runtime.group_attempts.lock().unwrap();
                    if attempts.len() == 64 {
                        let _ = attempts.remove(0);
                    }
                    attempts.push((
                        delay,
                        barrier.as_ref().map(|a| a.node.lsn()).map_err(Clone::clone),
                    ));
                }
                let barrier = match barrier {
                    Ok(barrier) => barrier,
                    Err(_) => {
                        let mut group = runtime.group.lock().unwrap();
                        // This is not TransactionBase's retry whitelist.
                        // Native GroupCommit retries failed admission only while
                        // Primary/not-closing; unexpected errors fault separately.
                        if !runtime.barriers_enabled.load(Ordering::Acquire)
                            || manager.role() != super::processor::ProcessingRole::Primary
                            || manager.faults.is_closing()
                        {
                            group.running = false;
                            break;
                        }
                        group.delay_ms = (group.delay_ms * 2).min(5000);
                        continue;
                    }
                };
                // Native clears the scheduling task BEFORE Apply/quorum waits:
                // another accepted End may need a dispatch Barrier even while
                // this group's quorum observation remains pending.
                runtime.group.lock().unwrap().running = false;
                let result = barrier.result.wait().await;
                let consecutive = result.is_ok()
                    && barrier.previous_stable.load(Ordering::Acquire) == barrier.node.lsn() - 1;
                let mut group = runtime.group.lock().unwrap();
                group.delay_ms = 2;
                // Native CheckpointManager::GroupCommit additional idle barrier advertises the
                // newly stable frontier, then stops on a consecutive barrier.
                if group.running {
                    break;
                }
                if !group.needed && consecutive {
                    group.running = false;
                    break;
                }
                group.running = true;
            }
        }));
    }

    pub(crate) fn admit(
        &self,
        manager: Arc<KernelManager>,
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> Result<Arc<Activity>> {
        let deadline = self.execution.blocking_deadline()?;
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        let (send, receive) = mpsc::sync_channel(1);
        self.start(
            manager,
            record,
            transaction,
            context,
            Some(send),
            Some(deadline),
        )?;
        // Never use OwnedCall::wait/recv_timeout here. A queued native Begin/Add
        // must reconcile its first cache using the known disposition.
        receive.recv().map_err(|_| Error::Closed)?
    }

    pub(crate) fn admit_async(
        &self,
        manager: Arc<KernelManager>,
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
    ) -> ApiFuture<'static, Arc<Activity>> {
        match self.start(manager, record, transaction, context, None, None) {
            Ok(admission) => {
                Box::pin(async move { admission.initial.wait().await.as_ref().clone() })
            }
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn start(
        &self,
        manager: Arc<KernelManager>,
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
        synchronous: Option<mpsc::SyncSender<Result<Arc<Activity>>>>,
        deadline: Option<Instant>,
    ) -> Result<Arc<Admission>> {
        let started = self.start_inner(
            manager.clone(),
            record,
            transaction,
            context,
            synchronous,
            deadline,
        );
        if let Err(error) = &started {
            if super::status::Status::InsufficientResources.matches(error) {
                // FUBE ADAPTATION: bounded pre-record allocation failure.
                // Native creation can throw; this fallible Rust boundary faults,
                // not a counterfeit successful/queue-full Barrier attempt.
                manager.faults.replication_error(error);
            }
        }
        started
    }

    fn start_inner(
        &self,
        manager: Arc<KernelManager>,
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
        synchronous: Option<mpsc::SyncSender<Result<Arc<Activity>>>>,
        deadline: Option<Instant>,
    ) -> Result<Arc<Admission>> {
        ExecutionRuntime::reject_engine_reentry(self.lower.execution_identity())?;
        // Atomic with generation retirement, before quotas/capture/driver spawn.
        // The guard ends after disposition + ordered append + successful flush,
        // NOT after Apply/quorum (which may need the retiring final boundary).
        let publication = self.reserve_publication()?;
        if record.transaction_id().is_some_and(|id| id <= 0) {
            return Err(super::status::Status::NotImplemented.error());
        }
        if transaction
            .as_ref()
            .is_some_and(|owner| record.transaction_id() != Some(owner.get_transaction_id()))
            || (context.is_some()
                && !matches!(
                    record.kind,
                    RecordKind::Begin { .. } | RecordKind::Operation { .. }
                ))
        {
            return Err(Error::InvalidInput(
                "invalid record ownership before lower admission",
            ));
        }
        let wire = record.encode()?;
        // Every record class uses the same finite allocator AND ordinary lower
        // replicate. End/Barrier are not priority/loan-backed admissions.
        let payload = Arc::new(
            self.execution
                .reserve_object_with_origin(&self.payloads, &self.disposal, CallOrigin::default())
                .map_err(allocation)?
                .publish(Mutex::new(None)),
        );
        let first_needed = self.barriers_enabled.load(Ordering::Acquire)
            && matches!(
                record.kind,
                RecordKind::Begin { .. } | RecordKind::Operation { .. } | RecordKind::End { .. }
            );
        let latency_sensitive = matches!(
            record.kind,
            RecordKind::Begin { single: true, .. } | RecordKind::End { .. }
        );
        let memory = manager.reserve_memory(&record)?;
        let mut capture = payload.get().lock().unwrap();
        *capture = Some(Payload {
            record,
            transaction,
            context,
            memory,
        });
        drop(capture);
        let receipt = payload.receipt();
        let budget = deadline.map_or(Duration::from_secs(30), |until| {
            until
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(30))
        });
        // Provider lane is the distinct supervised upper-log execution lane,
        // not a lower owner callback or a recursive Native reservation.
        let permit = self
            .execution
            .reserve(CallLane::Provider, CallOrigin::default(), budget, false)
            .map_err(allocation)?;
        let admission = Arc::new(Admission {
            initial: Completion::new(),
            synchronous: Mutex::new(synchronous),
        });
        let published = admission.clone();
        let asynchronous = published.synchronous.lock().unwrap().is_none();
        let lower = self.lower.clone();
        let continuation = manager.clone();
        #[cfg(test)]
        let publication_pause = self.publication_pause.clone();
        let body = async move {
            let admitted = lower.replicate(wire, Cancellation::default()).await;
            let accepted = match admitted {
                Ok(accepted) => accepted,
                Err(error) => {
                    manager.faults.replication_error(&error);
                    // ManagedObject Drop transfers captures to supervised cleanup.
                    // The lower has never owned them. Await physical retirement
                    // before announcing rejection to either observer.
                    drop(payload);
                    let error = receipt.wait().await.err().unwrap_or(error);
                    published.publish(Err(error.clone()));
                    return Err(error);
                }
            };
            #[cfg(test)]
            {
                let pause = {
                    let mut slot = publication_pause.lock().unwrap();
                    if slot
                        .as_ref()
                        .is_some_and(|pause| pause.0 == accepted.sequence_number)
                    {
                        slot.take()
                    } else {
                        None
                    }
                };
                if let Some((_, entered, release)) = pause {
                    let _ = entered.send(());
                    release
                        .recv_timeout(Duration::from_secs(2))
                        .expect("publication probe released");
                }
            }
            let mut captured = payload
                .get()
                .lock()
                .expect("payload poisoned")
                .take()
                .unwrap();
            captured.record.lsn = accepted.sequence_number;
            let activity =
                ExecutionRuntime::engine_publication(lower.execution_identity(), || {
                    let node =
                        RecordNode::new(captured.record, captured.transaction, captured.context)?;
                    let activity = Activity::new(node, true);
                    if let Some(retention) = captured.memory {
                        activity.node.retain_memory(retention);
                    }
                    manager.append(activity.clone())?;
                    Ok(activity)
                })?;
            // No transaction-cache lock or provider callback in publication.
            published.publish(Ok(activity.clone()));
            // Admission is a bounded invocation. A pending multi-transaction
            // begin must not inherit a callback deadline for its entire lifetime.
            // Dormant payload/continuation capacity remains owned until processing.
            Ok((activity, accepted, payload))
        };
        let initial_runtime = self.self_owner();
        let initial_manager = continuation.clone();
        let work: ApiFuture<'static, _> = Box::pin(async move {
            if first_needed {
                initial_runtime
                    .ensure_first(initial_manager, asynchronous)
                    .await?;
            }
            permit.run_future(body).await
        });
        let observation = admission.clone();
        self.live.count.fetch_add(1, Ordering::AcqRel);
        let driver = Driver(self.live.clone());
        let runtime = self.self_owner();
        // This bounded driver retains OwnedCall until physical completion. Client
        // Drop never reaches OwnedCall::drop or cancels accepted admission/retry.
        drop(self.drivers.spawn(async move {
            let _driver = driver;
            match work.await {
                Ok((activity, mut accepted, payload)) => {
                    continuation.start_completion(activity.clone());
                    continuation.flush(Ok(()));
                    if latency_sensitive {
                        runtime.schedule_group(continuation.clone());
                    }
                    drop(publication);
                    let quorum = accepted
                        .completion
                        .wait(Cancellation::default())
                        .await
                        .map(|_| ());
                    continuation.complete_replication(&activity, quorum);
                    let _ = activity.result.wait().await;
                    let _ = activity.processing.wait().await;
                    let receipt = payload.receipt();
                    drop(payload);
                    let _ = receipt.wait().await;
                }
                Err(error) => {
                    observation.publish(Err(error));
                }
            }
        }));
        Ok(admission)
    }

    pub(crate) async fn drained(&self) {
        loop {
            let changed = self.live.changed.notified();
            if self.live.count.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
        self.disposal.drained().await;
    }

    pub(crate) async fn stop(&self) {
        self.fence_publication();
        self.publications_drained().await;
        {
            let mut group = self.group.lock().unwrap();
            group.needed = false;
        }
        self.drained().await;
        // Reset only after every old-generation accepted continuation retires.
        // A subsequent primary must pass native Elected→Established again.
        *self.first_primary.lock().await = false;
        *self.group.lock().unwrap() = GroupCommit {
            needed: false,
            running: false,
            delay_ms: 2,
        };
        self.disposal.drained().await;
    }

    pub(crate) async fn deactivate_for_role(
        &self,
        manager: &Arc<KernelManager>,
        role: crate::ReplicaRole,
    ) -> Result<()> {
        // Provider/log drains are Phase2; this local native boundary drains only
        // the accepted processor records before changing their processing role.
        let event = if role == crate::ReplicaRole::None {
            7 // RemovingState, LoggingReplicatorImpl.cpp:1470–1485
        } else if role == crate::ReplicaRole::Unknown {
            4 // Closed
        } else if manager.role() == super::processor::ProcessingRole::Primary
            && matches!(
                role,
                crate::ReplicaRole::ActiveSecondary | crate::ReplicaRole::IdleSecondary
            )
        {
            5
        } else if role == crate::ReplicaRole::Primary {
            3
        } else {
            4
        };
        self.finish_generation(manager, event).await?;
        manager.set_role(role);
        if role == crate::ReplicaRole::Primary {
            self.enable_barriers(manager)?;
        }
        Ok(())
    }

    /// Native PrepareForReplicaRoleTransitionAsync waits LSN ordering before
    /// Information (LoggingReplicatorImpl.cpp:1459–1498). Here actual lower
    /// acceptance and upper publication are separately owned, so fence/wait
    /// those dispositions before placing the last processor dispatch boundary.
    pub(crate) async fn finish_generation(
        &self,
        manager: &Arc<KernelManager>,
        event: i32,
    ) -> Result<()> {
        self.fence_publication();
        self.publications_drained().await;
        let boundary = manager.local_boundary(12, event)?;
        let flushed = boundary.flush.wait().await.as_ref().clone();
        let applied = boundary.apply.wait().await.as_ref().clone();
        let processed = boundary.processing.wait().await.as_ref().clone();
        self.stop().await;
        // Native terminal processing completion is not successful flush/Apply.
        flushed.and(applied).and(processed)
    }

    #[cfg(test)]
    pub(crate) fn publications_fenced(&self) -> bool {
        !self.publication.state.lock().unwrap().0
    }

    #[cfg(test)]
    pub(crate) async fn group_idle(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.group.lock().unwrap().running || self.live.count.load(Ordering::Acquire) != 0
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("native group commit reaches idle");
    }
}
