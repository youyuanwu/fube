//! Independent upper admission ownership over the ONE standard lower engine.
//! Native TransactionManager.cpp:328–478 distinguishes admission from completion.
//! Application captures never enter lower Submit or its owner/destructor locks.
use super::{
    map::RecordNode,
    processor::{Activity, KernelManager},
    record::Record,
    version::Completion,
};
use crate::{ApiFuture, Cancellation, Error, OperationContext, Result, TransactionBase};
use fube_data::{extensions::ReplicatorDiagnostics, FabricReplicator, IStateReplicator};
use fube_runtime::extensions::{
    CallLane, CallOrigin, DisposalTracker, ExecutionRuntime, ObjectQuota,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::{Duration, Instant};
#[cfg(test)]
type PublicationPause = (i64, mpsc::SyncSender<()>, mpsc::Receiver<()>);

pub(crate) struct RuntimeLower {
    #[cfg(test)]
    pub(crate) publication_pause: Arc<Mutex<Option<PublicationPause>>>,
    lower: Arc<FabricReplicator>,
    execution: ExecutionRuntime,
    drivers: tokio::runtime::Handle,
    payloads: ObjectQuota,
    disposal: DisposalTracker,
    live: Arc<Live>,
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
        Ok(Arc::new(Self {
            #[cfg(test)]
            publication_pause: Arc::new(Mutex::new(None)),
            execution: lower.execution_runtime(),
            lower,
            drivers: tokio::runtime::Handle::current(),
            payloads: ObjectQuota::new(64)?,
            disposal: DisposalTracker::default(),
            live: Arc::new(Live::default()),
        }))
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
        ExecutionRuntime::reject_engine_reentry(self.lower.execution_identity())?;
        let wire = record.encode()?;
        let reservation = self.execution.reserve_object_with_origin(
            &self.payloads,
            &self.disposal,
            CallOrigin::default(),
        )?;
        let payload = reservation.publish(Mutex::new(Some(Payload {
            record,
            transaction,
            context,
        })));
        let receipt = payload.receipt();
        let budget = deadline.map_or(Duration::from_secs(30), |until| {
            until
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(30))
        });
        // Provider lane is the distinct supervised upper-log execution lane,
        // not a lower owner callback or a recursive Native reservation.
        let permit =
            self.execution
                .reserve(CallLane::Provider, CallOrigin::default(), budget, false)?;
        let admission = Arc::new(Admission {
            initial: Completion::new(),
            synchronous: Mutex::new(synchronous),
        });
        let published = admission.clone();
        let lower = self.lower.clone();
        let continuation = manager.clone();
        #[cfg(test)]
        let publication_pause = self.publication_pause.clone();
        let work = permit.run_future(async move {
            let admitted = lower.replicate(wire, Cancellation::default()).await;
            let accepted = match admitted {
                Ok(accepted) => accepted,
                Err(error) => {
                    // ManagedObject Drop transfers captures to supervised cleanup.
                    // The lower has never owned them. Await physical retirement
                    // before announcing rejection to either observer.
                    drop(payload);
                    let cleanup = receipt.wait().await;
                    let error = cleanup.err().unwrap_or(error);
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
                    manager.append(activity.clone())?;
                    Ok(activity)
                })?;
            // No transaction-cache lock or provider callback in publication.
            published.publish(Ok(activity.clone()));
            // Admission is a bounded invocation. A pending multi-transaction
            // begin must not inherit a callback deadline for its entire lifetime.
            // Dormant payload/continuation capacity remains owned until processing.
            Ok((activity, accepted, payload))
        });
        let observation = admission.clone();
        self.live.count.fetch_add(1, Ordering::AcqRel);
        let driver = Driver(self.live.clone());
        // This bounded driver retains OwnedCall until physical completion. Client
        // Drop never reaches OwnedCall::drop or cancels accepted admission/retry.
        drop(self.drivers.spawn(async move {
            let _driver = driver;
            match work.await {
                Ok((activity, mut accepted, payload)) => {
                    continuation.start_completion(activity.clone());
                    continuation.flush(Ok(()));
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
}
