//! Adapter tests use controlled callbacks, not a purported empty-state policy.
//! The actual sample custom replicator has its own controlled-peer test gate.
use super::*;
#[path = "stateful_control_tests.rs"]
mod control_tests;
use crate::extensions::{NativeIntegrationProvider, NativeRuntimeIntegration};
use crate::{
    ApiFuture as ServiceFuture, IPrimaryReplicator, IReplicator, IStatefulServiceFactory,
    IStatefulServiceReplica, NativeReplicaSetConfiguration as ReplicaSetConfiguration, OpenMode,
    ReplicaRole, ReplicaSetConfig, ReplicaSetQuorumMode as CatchUpMode,
};

#[derive(Default)]
struct StatefulCalls {
    events: Mutex<Vec<&'static str>>,
    partitions: Mutex<Vec<NativeRuntimeIntegration>>,
    provider: Mutex<Option<NativeIntegrationProvider>>,
    open_entered: Notify,
    release: Notify,
    catch_up_release: Notify,
    build_release: Notify,
    configuration_release: Notify,
    remove_release: Notify,
    configuration_failure: AtomicBool,
    configurations: Mutex<Vec<(ReplicaSetConfiguration, Option<ReplicaSetConfiguration>)>>,
    builds: Mutex<Vec<crate::ReplicaInformation>>,
    removals: Mutex<Vec<crate::ReplicaRemoval>>,
    epochs: Mutex<Vec<crate::Epoch>>,
    access_events: Mutex<
        Vec<(
            &'static str,
            (
                crate::ServicePartitionAccessStatus,
                crate::ServicePartitionAccessStatus,
            ),
        )>,
    >,
}
impl StatefulCalls {
    fn record(&self, event: &'static str) {
        // A callback must be able to inspect its published snapshot.
        for partition in self.partitions.lock().unwrap().iter() {
            if let Ok(status) = partition.status() {
                self.access_events
                    .lock()
                    .unwrap()
                    .push((event, status.access()));
            }
        }
        self.events.lock().unwrap().push(event);
    }
}
struct StatefulFactoryFixture(Arc<StatefulCalls>);
struct StatefulFixture {
    calls: Arc<StatefulCalls>,
    mode: u8,
    native: NativeRuntimeIntegration,
}
struct ReplicatorFixture(Arc<StatefulCalls>, u8, NativeRuntimeIntegration);
pub(crate) fn destructor_model_factory(
    provider: NativeIntegrationProvider,
) -> Box<dyn IStatefulServiceFactory> {
    let calls = Arc::new(StatefulCalls::default());
    *calls.provider.lock().unwrap() = Some(provider);
    Box::new(StatefulFactoryFixture(calls))
}
fn blocking_participant_drop() {
    use std::io::Write;
    println!("STATEFUL_PARTICIPANT_DROP_ENTERED");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
}
impl Drop for StatefulFixture {
    fn drop(&mut self) {
        if matches!(self.mode, 232 | 233 | 236) {
            blocking_participant_drop();
        }
    }
}
impl Drop for ReplicatorFixture {
    fn drop(&mut self) {
        if matches!(self.1, 151 | 152) {
            self.0.record("replicatorFinalDropEntered");
            std::thread::sleep(std::time::Duration::from_millis(if self.1 == 151 {
                30
            } else {
                700
            }));
            self.0.record("replicatorFinalDropComplete");
        }
        if matches!(self.1, 234 | 235) {
            blocking_participant_drop();
        }
    }
}
impl IStatefulServiceFactory for StatefulFactoryFixture {
    fn create_replica(
        &self,
        _: String,
        _: String,
        initialization: &[u8],
        partition: uuid::Uuid,
        replica: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        assert_eq!(replica, 100);
        self.0.record("create");
        Ok(Box::new(StatefulFixture {
            calls: self.0.clone(),
            mode: initialization.first().copied().unwrap_or(0),
            native: self
                .0
                .provider
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .capture(partition, replica)?,
        }) as Box<dyn IStatefulServiceReplica>)
    }
}
impl IStatefulServiceReplica for StatefulFixture {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn crate::IStatefulServicePartition>,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            assert_eq!(mode, OpenMode::New);
            let _ = partition.get_partition_information()?;
            self.calls.record("serviceOpen");
            self.calls
                .partitions
                .lock()
                .unwrap()
                .push(self.native.clone());
            self.calls.open_entered.notify_one();
            let kinds = [
                crate::ErrorKind::InvalidArgument,
                crate::ErrorKind::Unsupported,
                crate::ErrorKind::ObjectClosed,
                crate::ErrorKind::InvalidState,
                crate::ErrorKind::InvalidExecutionContext,
                crate::ErrorKind::NotPrimary,
                crate::ErrorKind::ReconfigurationPending,
                crate::ErrorKind::NoWriteQuorum,
                crate::ErrorKind::Canceled,
                crate::ErrorKind::Timeout,
                crate::ErrorKind::ResourceExhausted,
            ];
            if (61..=71).contains(&self.mode) {
                return Err(Error::new(
                    kinds[(self.mode - 61) as usize],
                    "closed canceled not-primary words are not classification",
                ));
            }
            if self.mode == 72 {
                return Err(Error::with_cause(
                    crate::ErrorKind::Provider,
                    "controlled provider failure",
                    Error::Remote {
                        code: -887,
                        message: "actual native cause".into(),
                    },
                ));
            }
            if self.mode == 28
                && self
                    .calls
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| **e == "serviceOpen")
                    .count()
                    == 1
            {
                return Err(Error::Remote {
                    code: -456,
                    message: "first Open fails for retry test".into(),
                });
            }
            match self.mode {
                1 | 233 => {
                    return Err(Error::Remote {
                        code: -456,
                        message: "controlled open failure".into(),
                    })
                }
                8 => tokio::select! {
                    _ = self.calls.release.notified() => {},
                    _ = cancellation.cancelled() => return Err(Error::Closed),
                },
                _ => {}
            }
            Ok(Box::new(ReplicatorFixture(
                self.calls.clone(),
                self.mode,
                self.native.clone(),
            )) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.calls.record(match role {
                ReplicaRole::Primary => "servicePrimary",
                ReplicaRole::IdleSecondary => "serviceIdle",
                ReplicaRole::ActiveSecondary => "serviceSecondary",
                ReplicaRole::None => "serviceNone",
                ReplicaRole::Unknown => return Err(Error::Protocol("unexpected unknown role")),
            });
            if self.mode == 30 && role == ReplicaRole::Primary {
                self.calls.release.notified().await;
            }
            Ok(if role == ReplicaRole::Primary {
                "model://stateful/client"
            } else {
                ""
            }
            .into())
        })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.calls.record("serviceClose");
            if self.mode == 31 {
                self.calls.release.notified().await;
            }
            if self.mode == 2 {
                return Err(Error::Protocol("controlled close failure"));
            }
            Ok(())
        })
    }
    fn abort(&self) {
        self.calls.record("serviceAbort");
        assert_ne!(self.mode, 11, "controlled stateful abort panic");
    }
}
impl IReplicator for ReplicatorFixture {
    fn open(&self, _: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.0.record("replicatorOpen");
            Ok("model://stateful/control".into())
        })
    }
    fn change_role(
        &self,
        epoch: crate::Epoch,
        role: ReplicaRole,
        _: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.0.epochs.lock().unwrap().push(epoch);
            self.0.record(match role {
                ReplicaRole::Primary => "replicatorPrimary",
                ReplicaRole::IdleSecondary => "replicatorIdle",
                ReplicaRole::ActiveSecondary => "replicatorSecondary",
                ReplicaRole::None => "replicatorNone",
                ReplicaRole::Unknown => return Err(Error::Protocol("unexpected unknown role")),
            });
            Ok(())
        })
    }
    fn update_epoch(&self, epoch: crate::Epoch, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if self.1 < 20 {
                return Err(Error::Protocol("unexpected epoch in lifecycle fixture"));
            }
            self.0.record("epoch");
            self.0.epochs.lock().unwrap().push(epoch);
            if self.1 == 34
                && self
                    .0
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| **e == "epoch")
                    .count()
                    == 1
            {
                return Err(Error::Remote {
                    code: -996,
                    message: "first UpdateEpoch fails".into(),
                });
            }
            Ok(())
        })
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        self.0.record("progress");
        if self.1 == 24 {
            return Err(Error::Remote {
                code: -999,
                message: "controlled progress failure".into(),
            });
        }
        Ok(0)
    }
    fn get_current_progress(&self) -> Result<i64> {
        if self.1 == 24 {
            return Err(Error::Remote {
                code: -999,
                message: "controlled progress failure".into(),
            });
        }
        Ok(0)
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.0.record("replicatorClose");
            Ok(())
        })
    }
    fn abort(&self) {
        self.0.record("replicatorAbort");
    }
}
impl IPrimaryReplicator for ReplicatorFixture {
    fn on_data_loss(&self, _: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            self.0.record("dataLoss");
            if self.1 == 29 {
                return Err(Error::Remote {
                    code: -995,
                    message: "controlled data-loss failure".into(),
                });
            }
            Ok(self.1 == 25)
        })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        let proof = self.2.capture_configuration(&current, Some(&previous))?;
        let (current, previous) = proof.configuration().unwrap();
        let current = current.descriptor()?;
        let previous = previous.unwrap().descriptor()?;
        if self.1 < 20 {
            return Err(Error::Protocol(
                "unexpected configuration in lifecycle fixture",
            ));
        }
        self.0.record("catchupConfiguration");
        self.0
            .configurations
            .lock()
            .unwrap()
            .push((current, Some(previous)));
        if matches!(self.1, 36 | 37) && self.0.configuration_failure.load(Ordering::Acquire) {
            return Err(Error::Remote {
                code: -994,
                message: "persistent configuration proof failure".into(),
            });
        }
        let count = self.0.configurations.lock().unwrap().len();
        if (self.1 == 23 && count == 1) || (matches!(self.1, 35 | 37 | 151 | 152) && count == 2) {
            let calls = self.0.clone();
            self.2
                .execution()
                .reserve(
                    crate::extensions::CallLane::Provider,
                    crate::extensions::CallOrigin::default(),
                    Duration::from_secs(30),
                    false,
                )?
                .run_future(async move {
                    calls.configuration_release.notified().await;
                    Ok(())
                })
                .wait()?;
        }
        Ok(())
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        let proof = self.2.capture_configuration(&current, None)?;
        let current = proof.configuration().unwrap().0.descriptor()?;
        if self.1 < 20 {
            return Err(Error::Protocol(
                "unexpected current configuration in lifecycle fixture",
            ));
        }
        self.0.record("currentConfiguration");
        self.0.configurations.lock().unwrap().push((current, None));
        Ok(())
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: CatchUpMode,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if self.1 < 20 {
                return Err(Error::Protocol("unexpected catchup in lifecycle fixture"));
            }
            self.0.record(if mode == CatchUpMode::All {
                "waitAll"
            } else {
                "waitQuorum"
            });
            if matches!(self.1, 21 | 35 | 151 | 152) {
                tokio::select! {
                    _ = self.0.catch_up_release.notified() => {},
                    _ = cancellation.cancelled() => return Err(Error::Closed),
                }
            }
            if self.1 == 33 {
                self.0.catch_up_release.notified().await;
            }
            Ok(())
        })
    }
    fn build_replica(
        &self,
        replica: crate::ReplicaInformation,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if self.1 < 20 {
                return Err(Error::Protocol("unexpected build in lifecycle fixture"));
            }
            self.0.record("build");
            self.0.builds.lock().unwrap().push(replica);
            if matches!(self.1, 40 | 42) && self.0.builds.lock().unwrap().len() == 1 {
                return Err(Error::Remote {
                    code: -993,
                    message: "partial build resources require cleanup".into(),
                });
            }
            if self.1 == 22 {
                tokio::select! {
                    _ = self.0.build_release.notified() => {},
                    _ = cancellation.cancelled() => return Err(Error::Closed),
                }
            }
            Ok(())
        })
    }
    fn remove_replica(&self, replica_id: i64) -> Result<()> {
        let proof = self.2.capture_pending_removal(replica_id)?;
        let crate::extensions::NativeInvocationKind::Remove(reason) = proof.kind() else {
            unreachable!()
        };
        if self.1 < 20 {
            return Err(Error::Protocol("unexpected remove in lifecycle fixture"));
        }
        self.0.record("remove");
        self.0.removals.lock().unwrap().push(reason);
        if matches!(self.1, 41 | 42)
            && self
                .0
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "remove")
                .count()
                == 1
        {
            return Err(Error::Remote {
                code: -992,
                message: "partial removal requires retry".into(),
            });
        }
        if self.1 == 37 {
            let calls = self.0.clone();
            self.2
                .execution()
                .reserve(
                    crate::extensions::CallLane::Provider,
                    crate::extensions::CallOrigin::default(),
                    Duration::from_secs(30),
                    false,
                )?
                .run_future(async move {
                    calls.remove_release.notified().await;
                    Ok(())
                })
                .wait()?;
        }
        Ok(())
    }
}
fn register(h: &Harness) -> Arc<StatefulCalls> {
    let calls = Arc::new(StatefulCalls::default());
    *calls.provider.lock().unwrap() = Some(h.engine.native_integration_provider());
    h.engine
        .insert_stateful_factory(
            "ModelStatefulType".into(),
            Arc::new(StatefulFactoryFixture(calls.clone())),
        )
        .unwrap();
    calls
}
#[tokio::test]
async fn classified_sdk_callback_failures_retain_exact_native_codes_without_display_parsing() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let expected = [
        0x80070057u32,
        0x80004001,
        0x80071bfe,
        0x80071bfd,
        0x80071bfd,
        0x80071bc6,
        0x80071bca,
        0x80071bc5,
        0x80004004,
        0x80071bff,
        0x8007000e,
    ];
    for (index, code) in expected.into_iter().enumerate() {
        let open = request(&h, (40 + index) as u8, 1, (61 + index) as u8);
        h.send("ReplicaOpen", &open).await;
        let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
        assert_eq!(reply.error.error, code as i32 as i64);
        assert!(reply.local.service_endpoint.is_empty());
        assert!(calls
            .partitions
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .validate()
            .is_err());
    }
    let open = request(&h, 60, 1, 72);
    h.send("ReplicaOpen", &open).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, -887);
    assert!(reply.local.service_endpoint.is_empty());
}
fn request(h: &Harness, partition: u8, incarnation: i64, mode: u8) -> ProxyRequest {
    let mut request = h.request(partition, incarnation, mode);
    request.service.stateful = true;
    request.service.service_type.name = "ModelStatefulType".into();
    request.unit.current.configuration = (1 << 32) + 1;
    request
}

#[tokio::test]
async fn owned_stateful_fault_uses_native_identity_epochs_and_generation_fences() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let sibling_request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &sibling_request).await;
    h.receive("ReplicaOpenReply").await;
    let mut open = request(&h, 2, 17, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let mut current = open.clone();
    current.unit.current.configuration += 7;
    current.unit.previous = open.unit.current.clone();
    current.remote = ObjectArray(vec![open.local.clone()]);
    current.runtime.clear();
    current.service = native_default_service(current.service);
    h.send("UpdateConfiguration", &current).await;
    h.receive("UpdateConfigurationReply").await;
    let partition = calls.partitions.lock().unwrap()[0].clone();
    // Neither mutable copies of identity nor role diagnostics retarget reports.
    let mut observational = partition.identity().clone();
    observational.partition_id = uuid::Uuid::nil();
    observational.replica_id = -1;
    observational.incarnation = -1;
    assert_ne!(&observational, partition.identity());
    let (result, message) = tokio::join!(
        partition.report_fault(FaultType::Transient),
        h.receive("RAReportFault")
    );
    assert_eq!(result.unwrap().attempts, 1);
    assert_eq!(message.properties().unwrap().actor, 12);
    let fault: FaultBody = from_slice(&message.body).unwrap();
    assert_eq!(fault.fault, 1);
    assert_eq!(to_vec(&fault.unit).unwrap(), to_vec(&current.unit).unwrap());
    assert_eq!((fault.local.id, fault.local.instance), (100, 17));
    assert_eq!(fault.local.node.id.0, current.local.node.id.0);
    assert_eq!(fault.local.node.instance, current.local.node.instance);
    assert_eq!(fault.activity.kind, 2);
    assert_eq!(
        partition
            .report_fault(FaultType::Transient)
            .await
            .unwrap()
            .attempts,
        1
    );
    h.no_reply().await;
    open.unit = current.unit.clone();
    h.send("ReplicaClose", &open).await;
    h.receive("ReplicaCloseReply").await;
    assert!(partition.fault_delivery().unwrap().cancelled);
    assert!(matches!(
        partition.report_fault(FaultType::Transient).await,
        Err(Error::Closed)
    ));
    let sibling = h.calls.partitions.lock().unwrap()[0].clone();
    assert!(sibling.ensure_valid().is_ok());
    assert!(sibling.fault_delivery().is_none());
    open.local.instance += 1;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    let replacement = calls.partitions.lock().unwrap().last().unwrap().clone();
    assert!(matches!(
        partition.report_fault(FaultType::Permanent).await,
        Err(Error::Closed)
    ));
    let (result, message) = tokio::join!(
        replacement.report_fault(FaultType::Transient),
        h.receive("RAReportFault")
    );
    result.unwrap();
    assert_eq!(
        from_slice::<FaultBody>(&message.body)
            .unwrap()
            .local
            .instance,
        18
    );
}

#[tokio::test]
async fn stateful_fault_retry_outlives_callers_executor_and_stops_with_unit() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut open = request(&h, 2, 9, 0);
    open.local.current_role = 2;
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let partition = calls.partitions.lock().unwrap()[0].clone();
    assert_eq!(
        partition.access_status().unwrap(),
        (
            crate::ServicePartitionAccessStatus::NotPrimary,
            crate::ServicePartitionAccessStatus::NotPrimary
        )
    );
    let caller = partition.clone();
    let permit = h.engine.shared.reports.acquire_many(16).await.unwrap();
    // The owner executor cannot run while this current-thread test joins the
    // engine-like executor. The enqueued worker must survive that executor.
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let fault = caller.report_fault(FaultType::Transient);
            tokio::pin!(fault);
            poll_pending_once(fault.as_mut()).await;
        });
    })
    .join()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while partition.fault_delivery().unwrap().attempts == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(partition.fault_delivery().unwrap().last_error.is_some());
    drop(permit);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    // Native callback polls now use owned OS workers; virtual-time auto
    // advancement must not expire I/O/Close observation before they can run.
    tokio::time::resume();
    let message = h.receive("RAReportFault").await;
    let body: FaultBody = from_slice(&message.body).unwrap();
    assert_eq!(
        (body.local.id, body.local.instance, body.fault),
        (100, 9, 1)
    );
    while partition.fault_delivery().unwrap().attempts < 2 {
        tokio::task::yield_now().await;
    }
    let backend = partition.reporting_for_test();
    let running_references = Arc::strong_count(&backend);
    h.send("ReplicaClose", &open).await;
    h.receive("ReplicaCloseReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(Arc::strong_count(&backend), running_references - 1);
    let attempts = partition.fault_delivery().unwrap().attempts;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(partition.fault_delivery().unwrap().attempts, attempts);
    assert!(partition.fault_delivery().unwrap().cancelled);
    h.no_reply().await;
}

#[tokio::test]
async fn queued_stateful_fault_is_fenced_after_authority_retirement() {
    use tokio::io::AsyncReadExt;

    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 7, 0);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    let partition = calls.partitions.lock().unwrap()[0].clone();
    let connection = h.engine.shared.connection.clone();
    let blocker = connection.send_one_way_fenced(
        12,
        "FixtureBlocked",
        vec![0; fube_transport::frame::MAX_FRAME_SIZE - 4096],
        deadline_after(Duration::from_secs(5)).unwrap(),
        fube_transport::connection::SendStatus::default(),
    );
    tokio::pin!(blocker);
    poll_pending_once(blocker.as_mut()).await;
    let mut fixed = [0; fube_transport::frame::FIXED_HEADER_SIZE];
    h.peer.read_exact(&mut fixed).await.unwrap();
    let fault = partition.report_fault(FaultType::Transient);
    tokio::pin!(fault);
    poll_pending_once(fault.as_mut()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.reports.available_permits() != 15 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(partition.fault_delivery().unwrap().attempts, 0);
    h.engine.shared.authority.retire();
    let size = u32::from_le_bytes(fixed[..4].try_into().unwrap()) as usize;
    h.peer
        .read_exact(&mut vec![0; size - fixed.len()])
        .await
        .unwrap();
    blocker.await.unwrap();
    assert!(matches!(fault.await, Err(Error::Closed)));
    assert!(partition.fault_delivery().unwrap().cancelled);
    assert!(matches!(
        partition.report_fault(FaultType::Permanent).await,
        Err(Error::Closed)
    ));
    h.no_reply().await;
    assert_eq!(partition.fault_delivery().unwrap().attempts, 1);
}

#[tokio::test]
async fn stateful_open_order_endpoints_access_duplicate_and_graceful_isolation() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let stateless = h.request(1, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let request = request(&h, 2, 1, 0);
    h.send("ReplicaOpen", &request).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert_eq!(reply.local.service_endpoint, "model://stateful/client");
    assert_eq!(reply.local.replication_endpoint, "model://stateful/control");
    assert_eq!(
        *calls.events.lock().unwrap(),
        vec![
            "create",
            "serviceOpen",
            "replicatorOpen",
            "replicatorPrimary",
            "servicePrimary"
        ]
    );
    let partition = calls.partitions.lock().unwrap()[0].clone();
    assert_eq!(
        partition.access_status().unwrap(),
        (
            crate::ServicePartitionAccessStatus::Granted,
            crate::ServicePartitionAccessStatus::Granted
        )
    );
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert_eq!(calls.events.lock().unwrap().len(), 5);
    h.send("RAPQuery", &request).await;
    let query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(
        (
            query.detail.kind,
            query.detail.replica_id,
            query.detail.instance_id
        ),
        (2, 100, -1)
    );
    assert_eq!(
        (query.detail.read_status, query.detail.write_status),
        (1, 1)
    );
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert!(matches!(partition.status(), Err(Error::Closed)));
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        ["replicatorClose", "serviceClose"]
    );
}

#[tokio::test]
async fn stateful_idle_open_never_grants_primary_access_and_drop_orders_none_before_close() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 3, 1, 0);
    request.local.current_role = 2;
    h.send("ReplicaOpen", &request).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(reply.local.service_endpoint.is_empty());
    assert_eq!(
        calls.partitions.lock().unwrap()[0].access_status().unwrap(),
        (
            crate::ServicePartitionAccessStatus::NotPrimary,
            crate::ServicePartitionAccessStatus::NotPrimary
        )
    );
    request.flags = 8;
    request.local.current_role = 1; // Native Close-only reconstructed descriptor.
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        [
            "replicatorNone",
            "serviceNone",
            "replicatorClose",
            "serviceClose"
        ]
    );
}

#[tokio::test]
async fn stateful_failed_open_fences_handle_and_abort_preserves_stateless_sibling() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let stateless = h.request(1, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let failed = request(&h, 2, 1, 1);
    h.send("ReplicaOpen", &failed).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, -456);
    assert!(calls.partitions.lock().unwrap()[0].status().is_err());
    assert_eq!(
        *calls.events.lock().unwrap(),
        ["create", "serviceOpen", "serviceAbort"]
    );
    let mut next = request(&h, 2, 2, 0);
    h.send("ReplicaOpen", &next).await;
    h.receive("ReplicaOpenReply").await;
    next.flags = 4;
    next.local.current_role = 1;
    h.send("ReplicaClose", &next).await;
    h.receive("ReplicaCloseReply").await;
    assert!(h.calls.partitions.lock().unwrap()[0].ensure_valid().is_ok());
    assert_eq!(
        &calls.events.lock().unwrap()[8..],
        [
            "replicatorNone",
            "serviceNone",
            "replicatorAbort",
            "serviceAbort"
        ]
    );
    assert_eq!(h.engine.metrics().snapshot().abort_callbacks, 2);
}

#[tokio::test]
async fn stateful_abort_cancels_open_owner_and_fences_old_incarnation() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 2, 1, 8);
    h.send("ReplicaOpen", &request).await;
    tokio::time::timeout(Duration::from_secs(2), calls.open_entered.notified())
        .await
        .unwrap();
    request.flags = 4;
    h.send("ReplicaClose", &request).await;
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(failed.error.error, 0);
    assert!(calls.partitions.lock().unwrap()[0].status().is_err());
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    request.flags = 0;
    request.local.instance = 2;
    request.service.initialization.0 = vec![0];
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let old = calls.partitions.lock().unwrap()[0].clone();
    let new = calls.partitions.lock().unwrap()[1].clone();
    assert!(old.validate().is_err());
    assert!(new.validate().is_ok());
    request.local.instance = 1;
    h.send("ReplicaClose", &request).await;
    h.no_reply().await;
    assert!(new.validate().is_ok());
}

#[tokio::test]
async fn stateful_type_and_partition_cross_kind_takeover_are_rejected() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    assert!(h
        .engine
        .insert_stateful_factory("ModelType".into(), Arc::new(StatefulFactoryFixture(calls)))
        .is_err());
    assert!(h
        .engine
        .insert_factory(
            "ModelStatefulType".into(),
            Arc::new(Factory(h.calls.clone()))
        )
        .is_err());
    let stateless = h.request(2, 1, 0);
    h.send("ReplicaOpen", &stateless).await;
    h.receive("ReplicaOpenReply").await;
    let stateful = request(&h, 2, 2, 0);
    h.send("ReplicaOpen", &stateful).await;
    h.engine.shared.connection.closed().await;
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn stateful_close_error_is_normalized_only_after_abort_and_panic_never_acks() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let request = request(&h, 2, 1, 2);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(
        &calls.events.lock().unwrap()[5..],
        [
            "replicatorClose",
            "serviceClose",
            "replicatorAbort",
            "serviceAbort"
        ]
    );
    let mut request = request;
    request.local.instance = 2;
    request.service.initialization.0 = vec![11];
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    request.flags = 4;
    h.send("ReplicaClose", &request).await;
    h.engine.shared.connection.closed().await;
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn stateful_close_before_open_tombstone_and_newer_owner_are_isolated() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let mut request = request(&h, 2, 1, 0);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    h.send("ReplicaOpen", &request).await;
    let closed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(closed.error.error, 0);
    assert!(calls.events.lock().unwrap().is_empty());
    request.local.instance = 2;
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert!(calls.partitions.lock().unwrap()[0].validate().is_ok());
}

#[tokio::test]
async fn active_stateful_callback_prevents_successful_host_quiescence() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let request = request(&h, 2, 1, 8);
    h.send("ReplicaOpen", &request).await;
    tokio::time::timeout(Duration::from_secs(2), calls.open_entered.notified())
        .await
        .unwrap();
    assert!(!calls.partitions.lock().unwrap().is_empty());
    h.engine.shared.authority.retire(); // Runtime::shutdown's first action.
    assert!(h.engine.quiesce().await.is_err());
    assert!(calls.partitions.lock().unwrap()[0].validate().is_err());
    calls.release.notify_waiters();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_between_gate_and_generation_install_cannot_be_overwritten() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let hook = h.engine.shared.stateful.pause_open_admission();
    let mut request = request(&h, 2, 1, 0);
    h.send("ReplicaOpen", &request).await;
    hook.reached.notified().await;
    request.flags = 4;
    h.send("ReplicaClose", &request).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.engine.shared.stateful.close_through([2; 16]) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    hook.release.notify_one();
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_ne!(reply.error.error, 0);
    assert!(calls.events.lock().unwrap().is_empty());
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    request.flags = 0;
    request.local.instance = 2;
    h.send("ReplicaOpen", &request).await;
    let next: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(next.error.error, 0);
    assert!(calls.partitions.lock().unwrap()[0].validate().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_becoming_stale_before_revocation_cannot_touch_replacement() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let old = request(&h, 2, 1, 0);
    h.send("ReplicaOpen", &old).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    h.send("ReplicaClose", &old).await;
    h.receive("ReplicaCloseReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let hook = h.engine.shared.stateful.pause_close_admission();
    h.send("ReplicaClose", &old).await;
    hook.reached.notified().await;
    let mut newer = old.clone();
    newer.local.instance = 2;
    h.send("ReplicaOpen", &newer).await;
    let opened: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(opened.error.error, 0);
    let replacement = calls.partitions.lock().unwrap()[1].clone();
    assert!(replacement.validate().is_ok());
    hook.release.notify_one();
    h.no_reply().await;
    assert!(replacement.validate().is_ok());
    assert_eq!(
        replacement.access_status().unwrap(),
        (
            crate::ServicePartitionAccessStatus::Granted,
            crate::ServicePartitionAccessStatus::Granted
        )
    );
    assert_eq!(h.engine.shared.stateful.close_through([2; 16]), 1);
}

fn native_default_service(mut service: Service) -> Service {
    service.name.clear();
    service.instance = 0;
    service.service_type.name.clear();
    service.service_type.package.name.clear();
    service.service_type.package.application.type_name.clear();
    service.service_type.package.application.number = 0;
    service.application.clear();
    service.version.instance = 0;
    service.version.version.application.value.major = 0;
    service.version.version.application.value.minor = 0;
    service.version.version.value.major = 0;
    service.version.version.value.minor = 0;
    service.partitions = 0;
    service.target = 0;
    service.minimum = 0;
    service.stateful = false;
    service.persisted = false;
    service.initialization.0.clear();
    service.group = false;
    service.correlations.0.clear();
    service.constraints.clear();
    service.scaleout = 0;
    service.metrics.0.clear();
    service.move_cost = 0;
    service.restart_wait = i64::MIN;
    service.quorum_wait = i64::MIN;
    service.update_version = 0;
    service.placement.0.clear();
    service.standby_keep = i64::MIN;
    service.activation_mode = 0;
    service.dns.clear();
    service
}

#[tokio::test]
async fn stateful_query_default_metadata_is_not_ready_until_replicator_owner_exists() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let request = request(&h, 2, 1, 8);
    h.send("ReplicaOpen", &request).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.partitions.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut query = request.clone();
    query.service = native_default_service(query.service);
    h.send("RAPQuery", &query).await;
    let early: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(early.error, 0x80071bc7u32 as i32 as i64);
    calls.release.notify_one();
    h.receive("ReplicaOpenReply").await;
    h.send("RAPQuery", &query).await;
    let ready: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(ready.error, 0);
    assert_eq!(
        (
            ready.detail.kind,
            ready.detail.replica_id,
            ready.detail.instance_id
        ),
        (2, 100, -1)
    );
}
