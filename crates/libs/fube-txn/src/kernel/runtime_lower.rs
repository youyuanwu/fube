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
use fube_data::{
    extensions::{LogReservation, ReplicatorDiagnostics, ReservedLogReplication},
    FabricReplicator, IStateReplicator,
};
use fube_runtime::extensions::{
    CallLane, CallOrigin, DisposalTracker, ExecutionRuntime, ManagedObject, ObjectQuota,
};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, Mutex, Weak,
};
use std::time::{Duration, Instant};
const GLOBAL_BARRIERS: usize = 18; // first plus idle continuations for 16 live owners
const DISPATCH_BARRIERS: usize = 16;
const BARRIER_SLOTS: usize = GLOBAL_BARRIERS + DISPATCH_BARRIERS;
#[cfg(test)]
type PublicationPause = (i64, mpsc::SyncSender<()>, mpsc::Receiver<()>);
fn throttle(error: Error) -> Error {
    if error.kind() == fube_runtime::ErrorKind::ResourceExhausted {
        super::status::Status::ServiceTooBusy.error()
    } else {
        error
    }
}

pub(crate) struct RuntimeLower {
    self_weak: Weak<Self>,
    #[cfg(test)]
    pub(crate) publication_pause: Arc<Mutex<Option<PublicationPause>>>,
    lower: Arc<FabricReplicator>,
    execution: ExecutionRuntime,
    drivers: tokio::runtime::Handle,
    payloads: ObjectQuota,
    endings: ObjectQuota,
    end_loans: Mutex<BTreeMap<i64, Arc<CriticalSlot>>>,
    barriers: ObjectQuota,
    barrier_pool: Mutex<Option<BarrierPool>>,
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
struct CriticalSlot {
    payload: Arc<ManagedObject<Mutex<Option<Payload>>>>,
    lower: Arc<LogReservation>,
    memory: Option<Arc<super::memory_log::Retention>>,
    dispatch: Option<Arc<BarrierGrant>>,
}
struct BarrierPool {
    payloads: Vec<Arc<ManagedObject<Mutex<Option<Payload>>>>>,
    loans: Vec<Arc<LogReservation>>,
    memory: Vec<Option<Arc<super::memory_log::MemoryCredit>>>,
    busy: Vec<bool>,
}
struct BarrierGrant {
    index: usize,
    slot: Arc<CriticalSlot>,
    runtime: Weak<RuntimeLower>,
    submitted: AtomicBool,
}
impl Drop for BarrierGrant {
    fn drop(&mut self) {
        if !self.submitted.load(Ordering::Acquire) {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.release_barrier(self.index);
            }
        }
    }
}
struct GroupCommit {
    needed: bool,
    running: bool,
    delay_ms: u64,
    pending: Vec<Arc<BarrierGrant>>,
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
        let endings = ObjectQuota::new(16)?;
        let barriers = ObjectQuota::new(BARRIER_SLOTS)?;
        Ok(Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            #[cfg(test)]
            publication_pause: Arc::new(Mutex::new(None)),
            execution: lower.execution_runtime(),
            lower,
            drivers: tokio::runtime::Handle::current(),
            payloads,
            endings,
            barriers,
            barrier_pool: Mutex::new(None),
            barriers_enabled: AtomicBool::new(false),
            first_primary: tokio::sync::Mutex::new(false),
            group: Mutex::new(GroupCommit {
                needed: false,
                running: false,
                delay_ms: 2,
                pending: Vec::new(),
            }),
            end_loans: Mutex::new(BTreeMap::new()),
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

    /// Establish only upper scheduling capacity; native lower control remains
    /// the role/quorum authority. The first replicated record is still emitted
    /// lazily by the native first-write election gate, not by read-only commit.
    pub(crate) fn enable_barriers(&self, manager: &Arc<KernelManager>) -> Result<()> {
        let mut publication = self.publication.state.lock().unwrap();
        if !publication.0 && self.live.count.load(Ordering::Acquire) != 0 {
            return Err(Error::InvalidInput(
                "previous scheduling generation is not drained",
            ));
        }
        let mut pool = self.barrier_pool.lock().unwrap();
        if pool.is_none() {
            let mut payloads = Vec::new();
            let mut loans = Vec::new();
            let mut memory = Vec::new();
            for _ in 0..BARRIER_SLOTS {
                let payload = self
                    .execution
                    .reserve_object_with_origin(
                        &self.barriers,
                        &self.disposal,
                        CallOrigin::default(),
                    )?
                    .publish(Mutex::new(None));
                loans.push(self.lower.reserve_barrier_operation(208)?);
                memory.push(
                    manager
                        .memory()
                        .map(|log| {
                            log.lock().unwrap().reserve_reusable(&Record {
                                lsn: -1,
                                kind: RecordKind::Barrier { last_stable_lsn: 0 },
                            })
                        })
                        .transpose()?,
                );
                payloads.push(Arc::new(payload));
            }
            *pool = Some(BarrierPool {
                payloads,
                loans,
                memory,
                busy: vec![false; BARRIER_SLOTS],
            });
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

    fn claim_barrier(&self, dispatch: bool) -> Result<(usize, Arc<CriticalSlot>)> {
        let mut pool = self.barrier_pool.lock().unwrap();
        let pool = pool
            .as_mut()
            .ok_or(Error::InvalidInput("barrier capacity not established"))?;
        let range = if dispatch {
            GLOBAL_BARRIERS..BARRIER_SLOTS
        } else {
            0..GLOBAL_BARRIERS
        };
        for index in range {
            if pool.busy[index]
                || !pool.loans[index].is_available()
                || pool.memory[index]
                    .as_ref()
                    .is_some_and(|credit| !credit.available())
            {
                continue;
            }
            let lower = pool.loans[index].clone();
            let memory = pool.memory[index]
                .as_ref()
                .map(|credit| credit.take())
                .transpose()?;
            pool.busy[index] = true;
            return Ok((
                index,
                Arc::new(CriticalSlot {
                    payload: pool.payloads[index].clone(),
                    lower,
                    memory,
                    dispatch: None,
                }),
            ));
        }
        Err(super::status::Status::ServiceTooBusy.error())
    }

    fn release_barrier(&self, index: usize) {
        if let Some(pool) = self.barrier_pool.lock().unwrap().as_mut() {
            pool.busy[index] = false;
        }
    }
    fn reserve_dispatch(&self) -> Result<Arc<BarrierGrant>> {
        let (index, slot) = self.claim_barrier(true)?;
        Ok(Arc::new(BarrierGrant {
            index,
            slot,
            runtime: self.self_weak.clone(),
            submitted: AtomicBool::new(false),
        }))
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

    fn schedule_group(&self, manager: Arc<KernelManager>, grant: Arc<BarrierGrant>) {
        if !self.barriers_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut group = self.group.lock().unwrap();
        group.pending.push(grant);
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
                let mut grants = std::mem::take(&mut runtime.group.lock().unwrap().pending);
                let selected = grants.pop();
                let barrier = if let Some(grant) = selected.clone() {
                    runtime.admit_granted(manager.clone(), grant).await
                } else {
                    runtime
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
                        .await
                };
                let barrier = match barrier {
                    Ok(barrier) => {
                        drop(grants);
                        barrier
                    }
                    Err(_) => {
                        let mut group = runtime.group.lock().unwrap();
                        if let Some(grant) = selected {
                            grants.push(grant);
                        }
                        group.pending.extend(grants);
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
                // Native GroupCommit.cpp additional idle barrier advertises the
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
        let begin = match record.kind {
            RecordKind::Begin {
                transaction_id,
                single: false,
                ..
            } => Some(transaction_id),
            _ => None,
        };
        let started = self.start_inner(
            manager,
            record,
            transaction,
            context,
            synchronous,
            deadline,
            None,
        );
        if started.is_err() {
            if let Some(id) = begin {
                self.end_loans.lock().unwrap().remove(&id);
            }
        }
        started
    }

    fn admit_granted(
        &self,
        manager: Arc<KernelManager>,
        grant: Arc<BarrierGrant>,
    ) -> ApiFuture<'static, Arc<Activity>> {
        let record = Record {
            lsn: -1,
            kind: RecordKind::Barrier {
                last_stable_lsn: manager.stable(),
            },
        };
        match self.start_inner(manager, record, None, None, None, None, Some(grant)) {
            Ok(admission) => {
                Box::pin(async move { admission.initial.wait().await.as_ref().clone() })
            }
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "native admission parameters plus pre-reserved dispatch ownership"
    )]
    fn start_inner(
        &self,
        manager: Arc<KernelManager>,
        record: Record,
        transaction: Option<Arc<dyn TransactionBase>>,
        context: Option<Arc<dyn OperationContext>>,
        synchronous: Option<mpsc::SyncSender<Result<Arc<Activity>>>>,
        deadline: Option<Instant>,
        supplied: Option<Arc<BarrierGrant>>,
    ) -> Result<Arc<Admission>> {
        ExecutionRuntime::reject_engine_reentry(self.lower.execution_identity())?;
        // Atomic with generation retirement, before quotas/capture/driver spawn.
        // The guard ends after disposition + ordered append + successful flush,
        // NOT after Apply/quorum (which may need the retiring final boundary).
        let publication = self.reserve_publication()?;
        let wire = record.encode()?;
        let begin_id = match record.kind {
            RecordKind::Begin {
                transaction_id,
                single: false,
                ..
            } => Some(transaction_id),
            _ => None,
        };
        if let Some(id) = begin_id {
            let mut loans = self.end_loans.lock().unwrap();
            if let std::collections::btree_map::Entry::Vacant(entry) = loans.entry(id) {
                let reservation = self
                    .execution
                    .reserve_object_with_origin(
                        &self.endings,
                        &self.disposal,
                        CallOrigin::default(),
                    )
                    .map_err(throttle)?;
                let lower = self.lower.reserve_log_operation(229)?;
                let memory = manager.reserve_memory(&Record {
                    lsn: -1,
                    kind: RecordKind::End {
                        transaction_id: id,
                        committed: true,
                    },
                })?;
                let dispatch = if self.barriers_enabled.load(Ordering::Acquire) {
                    Some(self.reserve_dispatch()?)
                } else {
                    None
                };
                entry.insert(Arc::new(CriticalSlot {
                    payload: Arc::new(reservation.publish(Mutex::new(None))),
                    lower,
                    memory,
                    dispatch,
                }));
            }
        }
        let end_id = match record.kind {
            RecordKind::End { transaction_id, .. } => Some(transaction_id),
            _ => None,
        };
        let barrier = if matches!(record.kind, RecordKind::Barrier { .. })
            && self.barriers_enabled.load(Ordering::Acquire)
        {
            Some(if let Some(grant) = &supplied {
                (grant.index, grant.slot.clone())
            } else {
                self.claim_barrier(false)?
            })
        } else {
            None
        };
        let critical = if let Some((_, slot)) = &barrier {
            Some(slot.clone())
        } else {
            end_id
                .map(|id| {
                    self.end_loans
                        .lock()
                        .unwrap()
                        .get(&id)
                        .cloned()
                        .ok_or(Error::InvalidInput("end lacks accepted begin reservation"))
                })
                .transpose()?
        };
        let payload = if let Some(slot) = &critical {
            slot.payload.clone()
        } else {
            Arc::new(
                self.execution
                    .reserve_object_with_origin(
                        &self.payloads,
                        &self.disposal,
                        CallOrigin::default(),
                    )
                    .map_err(throttle)?
                    .publish(Mutex::new(None)),
            )
        };
        let first_needed = self.barriers_enabled.load(Ordering::Acquire)
            && matches!(
                record.kind,
                RecordKind::Begin { .. } | RecordKind::Operation { .. } | RecordKind::End { .. }
            );
        let latency_sensitive = matches!(
            record.kind,
            RecordKind::Begin { single: true, .. } | RecordKind::End { .. }
        );
        let dispatch = if latency_sensitive && self.barriers_enabled.load(Ordering::Acquire) {
            if let Some(slot) = &critical {
                slot.dispatch.clone()
            } else {
                Some(self.reserve_dispatch()?)
            }
        } else {
            None
        };
        let memory = if let Some(slot) = &critical {
            slot.memory.clone()
        } else {
            manager.reserve_memory(&record)?
        };
        let mut capture = payload.get().lock().unwrap();
        if capture.is_some() {
            return Err(super::status::Status::ServiceTooBusy.error());
        }
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
        let permit = if critical.is_none() {
            Some(
                self.execution
                    .reserve(CallLane::Provider, CallOrigin::default(), budget, false)
                    .map_err(throttle)?,
            )
        } else {
            None
        };
        let admission = Arc::new(Admission {
            initial: Completion::new(),
            synchronous: Mutex::new(synchronous),
        });
        let published = admission.clone();
        let asynchronous = published.synchronous.lock().unwrap().is_none();
        let lower = self.lower.clone();
        let continuation = manager.clone();
        let rejected_runtime = self.self_owner();
        let barrier_index = barrier.as_ref().map(|(index, _)| *index);
        let reusable = critical.is_some();
        let reservation = critical.as_ref().map(|slot| slot.lower.clone());
        #[cfg(test)]
        let publication_pause = self.publication_pause.clone();
        let body = async move {
            let admitted = if let Some(reservation) = reservation {
                lower
                    .replicate_reserved(wire, reservation, Cancellation::default())
                    .await
            } else {
                lower.replicate(wire, Cancellation::default()).await
            };
            let accepted = match admitted {
                Ok(accepted) => accepted,
                Err(error) => {
                    manager.faults.replication_error(&error);
                    // ManagedObject Drop transfers captures to supervised cleanup.
                    // The lower has never owned them. Await physical retirement
                    // before announcing rejection to either observer.
                    let error = if reusable {
                        // End contains only a concrete transaction already held
                        // by its pending Begin, never application operation data/
                        // context. Its dormant slot remains reserved across retry.
                        drop(payload.get().lock().unwrap().take());
                        error
                    } else {
                        drop(payload);
                        receipt.wait().await.err().unwrap_or(error)
                    };
                    if let Some(id) = begin_id {
                        rejected_runtime.end_loans.lock().unwrap().remove(&id);
                    }
                    if let Some(index) = barrier_index {
                        rejected_runtime.release_barrier(index);
                    }
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
        // Termination is pure internal log work with pre-reserved dormant
        // capacity: no application/provider code runs during its admission.
        // It cannot be starved by ordinary callback-lane records.
        let initial_runtime = self.self_owner();
        let initial_manager = continuation.clone();
        let work: ApiFuture<'static, _> = Box::pin(async move {
            if first_needed {
                initial_runtime
                    .ensure_first(initial_manager, asynchronous)
                    .await?;
            }
            if let Some(permit) = permit {
                permit.run_future(body).await
            } else {
                body.await
            }
        });
        let observation = admission.clone();
        self.live.count.fetch_add(1, Ordering::AcqRel);
        let driver = Driver(self.live.clone());
        let runtime = self.self_owner();
        if let Some(grant) = &supplied {
            grant.submitted.store(true, Ordering::Release);
        }
        // This bounded driver retains OwnedCall until physical completion. Client
        // Drop never reaches OwnedCall::drop or cancels accepted admission/retry.
        drop(self.drivers.spawn(async move {
            let _driver = driver;
            match work.await {
                Ok((activity, mut accepted, payload)) => {
                    if let Some(id) = end_id {
                        runtime.end_loans.lock().unwrap().remove(&id);
                    }
                    continuation.start_completion(activity.clone());
                    continuation.flush(Ok(()));
                    if latency_sensitive {
                        if let Some(grant) = dispatch {
                            runtime.schedule_group(continuation.clone(), grant);
                        }
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
                    if !reusable {
                        let _ = receipt.wait().await;
                    }
                    if let Some(index) = barrier_index {
                        runtime.release_barrier(index);
                    }
                }
                Err(error) => {
                    if let Some(index) = barrier_index {
                        runtime.release_barrier(index);
                    }
                    if let Some(id) = begin_id {
                        runtime.end_loans.lock().unwrap().remove(&id);
                    }
                    observation.publish(Err(error));
                }
            }
            drop(supplied);
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
            group.pending.clear();
        }
        self.drained().await;
        // Reset only after every old-generation accepted continuation retires.
        // A subsequent primary must pass native Elected→Established again.
        *self.first_primary.lock().await = false;
        *self.group.lock().unwrap() = GroupCommit {
            needed: false,
            running: false,
            delay_ms: 2,
            pending: Vec::new(),
        };
        let endings = std::mem::take(&mut *self.end_loans.lock().unwrap());
        let barriers = self.barrier_pool.lock().unwrap().take();
        drop((endings, barriers));
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
        manager
            .local_boundary(12, event)?
            .processing
            .wait()
            .await
            .as_ref()
            .clone()?;
        self.stop().await;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn publications_fenced(&self) -> bool {
        !self.publication.state.lock().unwrap().0
    }

    #[cfg(test)]
    pub(crate) fn barrier_loan(&self, index: usize) -> Arc<LogReservation> {
        self.barrier_pool.lock().unwrap().as_ref().unwrap().loans[index].clone()
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
