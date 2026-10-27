use super::*;
use mssf_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
};

#[derive(Default)]
struct Calls {
    opens: AtomicUsize,
    closes: AtomicUsize,
    aborts: AtomicUsize,
    partitions: Mutex<Vec<Partition>>,
    release: Notify,
    abort_release: AtomicBool,
    abort_running: AtomicUsize,
}
struct Factory(Arc<Calls>);
struct Instance {
    calls: Arc<Calls>,
    mode: u8,
}
impl StatelessFactory for Factory {
    fn create(&self, context: ServiceContext) -> ServiceFuture<'_, Arc<dyn StatelessService>> {
        Box::pin(async move {
            assert_eq!(context.identity.instance_id, 100);
            let mode = context.initialization_data.first().copied().unwrap_or(0);
            if mode == 9 {
                return Err(Error::Remote {
                    code: -123,
                    message: "factory failure".into(),
                });
            }
            Ok(Arc::new(Instance {
                calls: self.0.clone(),
                mode,
            }) as Arc<dyn StatelessService>)
        })
    }
}
impl StatelessService for Instance {
    fn open(&self, partition: Partition, _cancel: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.calls.opens.fetch_add(1, Ordering::SeqCst);
            self.calls
                .partitions
                .lock()
                .unwrap()
                .push(partition.clone());
            if self.mode == 8 {
                self.calls.release.notified().await;
            }
            if self.mode == 1 || self.mode == 5 {
                return Err(Error::Remote {
                    code: -456,
                    message: "open failure".into(),
                });
            }
            assert!(self.mode != 3, "callback panic regression");
            partition.ensure_valid()?;
            Ok(format!("model://{}", partition.identity.partition_id))
        })
    }
    fn close(&self, _cancel: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.calls.closes.fetch_add(1, Ordering::SeqCst);
            if self.mode == 7 {
                self.calls.release.notified().await;
            }
            if self.mode == 2 || self.mode == 6 {
                return Err(Error::Remote {
                    code: -789,
                    message: "close failure".into(),
                });
            }
            Ok(())
        })
    }
    fn abort(&self) {
        self.calls.aborts.fetch_add(1, Ordering::SeqCst);
        self.calls.abort_running.fetch_add(1, Ordering::SeqCst);
        struct Running<'a>(&'a AtomicUsize);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _running = Running(&self.calls.abort_running);
        assert_ne!(self.mode, 11, "intentional Abort panic");
        if matches!(self.mode, 4..=6 | 12) {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !self.calls.abort_release.load(Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
struct Harness {
    engine: Engine,
    peer: TcpStream,
    calls: Arc<Calls>,
    index: u32,
}

#[tokio::test]
async fn reporting_native_paths_identity_health_results_and_close_fences() {
    let mut h = Harness::new().await;
    let mut request = h.request(1, 1, 0);
    request.service.metrics.0.push(Metric {
        name: "Work".into(),
        weight: 1,
        primary: 0,
        secondary: 0,
    });
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let mut partition = h.calls.partitions.lock().unwrap()[0].clone();
    // Public snapshots must not be usable to retarget a report.
    partition.identity.partition_id = uuid::Uuid::nil();
    partition.identity.instance_id = -1;
    let metrics = vec![crate::LoadMetric {
        name: "Work".into(),
        value: 731,
    }];
    let (result, message) =
        tokio::join!(partition.report_load(&metrics), h.receive("RAPReportLoad"));
    result.unwrap();
    assert_eq!(message.properties().unwrap().actor, 12);
    let load: LoadBody = from_slice(&message.body).unwrap();
    assert_eq!(load.reports.0[0].partition, request.unit.id.id);
    assert_eq!(load.reports.0[0].secondary.0[0].value, 731);
    assert_eq!(load.reports.0[0].secondary.0[0].weighted_sum, 731.0);
    assert_eq!(load.reports.0[0].nodes.0[0].node.0, request.local.node.id.0);
    assert!(load.reports.0[0].primary.0.is_empty());
    let (result, message) = tokio::join!(
        partition.report_move_cost(crate::MoveCost::High),
        h.receive("RAPReportLoad")
    );
    result.unwrap();
    let cost: LoadBody = from_slice(&message.body).unwrap();
    assert_eq!(cost.reports.0[0].secondary.0[0].name, "_MoveCost_");
    assert_eq!(cost.reports.0[0].secondary.0[0].value, 3);
    h.send("RAPQuery", &request).await;
    let query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(query.detail.loads.0.len(), 2);

    for code in [0, 0x80071c47u32 as i32 as i64] {
        let report = crate::HealthReport {
            source: "User".into(),
            property: "Ready".into(),
            state: crate::HealthState::Warning,
            description: "detail".into(),
            time_to_live: None,
            sequence: crate::HealthSequence::Explicit(45),
            remove_when_expired: true,
        };
        let p = partition.clone();
        let operation = tokio::spawn(async move { p.report_health(&report).await });
        let message = h.receive("ReportHealth").await;
        assert_eq!(message.properties().unwrap().actor, 7);
        let body: HealthBody = from_slice(&message.body).unwrap();
        let report = &body.reports.0[0];
        assert_eq!(report.ttl, i64::MAX);
        assert_eq!(report.state, 2);
        let entity = &report.entity.0.as_ref().unwrap().0;
        assert_eq!(entity.instance, 100);
        assert_eq!(entity.kind, 2);
        assert_eq!(entity.partition, request.unit.id.id);
        write_message(
            &mut h.peer,
            &Message {
                headers: vec![
                    Header::actor(16).unwrap(),
                    Header::action("OperationSuccessAction").unwrap(),
                    Header::relates_to(message.properties().unwrap().id).unwrap(),
                ],
                body: to_vec(&HealthReply {
                    results: ObjectArray(vec![HealthResult {
                        kind: 2,
                        source: report.source.clone(),
                        sequence: report.sequence,
                        entity_property: format!(
                            "{}+100+Ready",
                            uuid::Uuid::from_bytes_le(request.unit.id.id.0)
                        ),
                        error: code,
                    }]),
                    streams: ObjectArray(vec![]),
                })
                .unwrap(),
            },
        )
        .await
        .unwrap();
        let result = operation.await.unwrap();
        if code == 0 {
            assert_eq!(result.unwrap().sequence, 45);
        } else {
            assert!(matches!(result, Err(Error::Remote { code: actual, .. }) if actual == code));
        }
    }
    let (result, message) = tokio::join!(
        partition.report_fault(FaultType::Transient),
        h.receive("RAReportFault")
    );
    assert_eq!(result.unwrap().attempts, 1);
    let fault: FaultBody = from_slice(&message.body).unwrap();
    assert_eq!(fault.fault, 1); // public COM Transient=2 must NOT leak here
    assert_eq!(fault.local.instance, 1);
    assert_eq!(fault.activity.kind, 2);
    assert_eq!(
        partition
            .report_fault(FaultType::Transient)
            .await
            .unwrap()
            .attempts,
        1
    );
    let (result, message) = tokio::join!(
        partition.report_fault(FaultType::Permanent),
        h.receive("RAReportFault")
    );
    result.unwrap();
    assert_eq!(from_slice::<FaultBody>(&message.body).unwrap().fault, 2);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert!(partition.fault_delivery().unwrap().cancelled);
    assert!(matches!(
        partition.report_load(&metrics).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        partition.report_move_cost(crate::MoveCost::Low).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        partition.report_fault(FaultType::Transient).await,
        Err(Error::Closed)
    ));
    request.local.instance += 1;
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert!(partition.report_load(&metrics).await.is_err());
    let current = h.calls.partitions.lock().unwrap().last().unwrap().clone();
    let (result, _) = tokio::join!(current.report_load(&metrics), h.receive("RAPReportLoad"));
    result.unwrap();
    h.engine.shared.authority.retire();
    assert!(current.report_load(&metrics).await.is_err());
    assert!(current
        .report_move_cost(crate::MoveCost::High)
        .await
        .is_err());
    assert!(current.report_health(&health_report()).await.is_err());
    assert!(current.report_fault(FaultType::Transient).await.is_err());
}

fn health_report() -> crate::HealthReport {
    crate::HealthReport {
        source: "User".into(),
        property: "Ready".into(),
        state: crate::HealthState::Ok,
        description: "detail".into(),
        time_to_live: Some(Duration::from_secs(3)),
        sequence: crate::HealthSequence::Auto,
        remove_when_expired: false,
    }
}

#[tokio::test]
async fn health_saturation_leaves_poll_admission_and_rejects_overload() {
    let mut h = Harness::new().await;
    let mut reports = Vec::new();
    for id in 1..=16 {
        let request = h.request(id, 1, 0);
        h.send("ReplicaOpen", &request).await;
        h.receive("ReplicaOpenReply").await;
        let p = h.calls.partitions.lock().unwrap().last().unwrap().clone();
        reports.push(tokio::spawn(async move {
            p.report_health(&health_report()).await
        }));
        h.receive("ReportHealth").await; // withhold HM results
    }
    let request = h.request(17, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let p = h.calls.partitions.lock().unwrap().last().unwrap().clone();
    assert!(matches!(
        p.report_health(&health_report()).await,
        Err(Error::ResourceLimit("runtime report admission"))
    ));
    let connection = h.engine.shared.connection.clone();
    let poll = tokio::spawn(async move {
        connection
            .request(
                "GetProcessLeaseTTLRequest",
                15,
                vec![],
                vec![],
                deadline_after(Duration::from_secs(1)).unwrap(),
            )
            .await
    });
    let message = read_message(&mut h.peer).await.unwrap();
    assert_eq!(message.properties().unwrap().actor, 15);
    write_message(
        &mut h.peer,
        &Message {
            headers: vec![Header::relates_to(message.properties().unwrap().id).unwrap()],
            body: vec![],
        },
    )
    .await
    .unwrap();
    poll.await.unwrap().unwrap();
    for task in reports {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn health_validation_and_unmatched_results_never_acknowledge() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let p = h.calls.partitions.lock().unwrap()[0].clone();
    for ttl in [Duration::ZERO, Duration::from_nanos(99), Duration::MAX] {
        let mut report = health_report();
        report.time_to_live = Some(ttl);
        assert!(p.report_health(&report).await.is_err());
    }
    for sequence in [-1, 0] {
        let mut report = health_report();
        report.sequence = crate::HealthSequence::Explicit(sequence);
        assert!(p.report_health(&report).await.is_err());
    }
    let mut report = health_report();
    report.source = "System.Forged".into();
    assert!(p.report_health(&report).await.is_err());
    for wrong in 0..5 {
        let p = p.clone();
        let task = tokio::spawn(async move { p.report_health(&health_report()).await });
        let message = h.receive("ReportHealth").await;
        let sent: HealthBody = from_slice(&message.body).unwrap();
        let sent = &sent.reports.0[0];
        assert_eq!(sent.ttl, 30_000_000);
        assert!(sent.sequence > 0);
        let mut result = HealthResult {
            kind: 2,
            source: sent.source.clone(),
            sequence: sent.sequence,
            entity_property: format!(
                "{}+100+Ready",
                uuid::Uuid::from_bytes_le(request.unit.id.id.0)
            ),
            error: 0,
        };
        match wrong {
            0 => result.kind = 3,
            1 => result.source = "other".into(),
            2 => result.sequence -= 1,
            3 => result.entity_property = "other".into(),
            _ => {}
        }
        write_message(
            &mut h.peer,
            &Message {
                headers: vec![
                    Header::actor(16).unwrap(),
                    Header::action("OperationSuccessAction").unwrap(),
                    Header::relates_to(message.properties().unwrap().id).unwrap(),
                ],
                body: to_vec(&HealthReply {
                    results: ObjectArray(if wrong == 4 { vec![] } else { vec![result] }),
                    streams: ObjectArray(vec![]),
                })
                .unwrap(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(task.await.unwrap(), Err(Error::Protocol(_))));
    }
}

async fn poll_pending_once<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn fault_pending_duplicates_and_permanent_upgrade_share_one_worker() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let p = h.calls.partitions.lock().unwrap()[0].clone();
    let first = p.report_fault(FaultType::Transient);
    let duplicate = p.report_fault(FaultType::Transient);
    let upgrade = p.report_fault(FaultType::Permanent);
    tokio::pin!(first, duplicate, upgrade);
    poll_pending_once(first.as_mut()).await;
    poll_pending_once(duplicate.as_mut()).await;
    poll_pending_once(upgrade.as_mut()).await;
    let message = h.receive("RAReportFault").await;
    assert_eq!(from_slice::<FaultBody>(&message.body).unwrap().fault, 2);
    assert!(matches!(first.await, Err(Error::InvalidInput(_))));
    assert!(matches!(duplicate.await, Err(Error::InvalidInput(_))));
    assert!(upgrade.await.unwrap().superseded_transient);
    assert_eq!(p.fault_delivery().unwrap().attempts, 1);
}

#[tokio::test]
async fn fault_initial_result_survives_retry_failure_for_unpolled_waiter() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let p = h.calls.partitions.lock().unwrap()[0].clone();
    let first = p.report_fault(FaultType::Transient);
    tokio::pin!(first);
    poll_pending_once(first.as_mut()).await;
    h.receive("RAReportFault").await;
    while p.fault_delivery().unwrap().attempts < 1 {
        tokio::task::yield_now().await;
    }
    let permit = h.engine.shared.reports.acquire_many(16).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    while p.fault_delivery().unwrap().attempts < 2 {
        tokio::task::yield_now().await;
    }
    assert!(p.fault_delivery().unwrap().last_error.is_some());
    assert_eq!(first.await.unwrap().attempts, 1);
    drop(permit);
}

#[tokio::test]
async fn failed_open_invalidates_reports_before_blocked_abort_finishes() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 5);
    h.send("ReplicaOpen", &request).await;
    while h.calls.abort_running.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let p = h.calls.partitions.lock().unwrap()[0].clone();
    assert!(p.ensure_valid().is_err());
    assert!(p.report_fault(FaultType::Transient).await.is_err());
    h.calls.abort_release.store(true, Ordering::Release);
    h.receive("ReplicaOpenReply").await;
}

#[tokio::test]
async fn fault_initial_failure_retry_and_cancellation_remain_observable() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let p = h.calls.partitions.lock().unwrap()[0].clone();
    let permit = h.engine.shared.reports.acquire_many(16).await.unwrap();
    assert!(matches!(
        p.report_fault(FaultType::Transient).await,
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(p.fault_delivery().unwrap().attempts, 1);
    assert!(p.fault_delivery().unwrap().last_error.is_some());
    assert!(p.report_fault(FaultType::Transient).await.is_err());
    drop(permit);
    // Escalation wakes the one retained worker; it is not blocked by the old
    // stage's report admission and carries the correct internal fault enum.
    let (result, message) = tokio::join!(
        p.report_fault(FaultType::Permanent),
        h.receive("RAReportFault")
    );
    assert!(result.unwrap().superseded_transient);
    assert_eq!(from_slice::<FaultBody>(&message.body).unwrap().fault, 2);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert!(p.fault_delivery().unwrap().cancelled);
}

#[tokio::test]
async fn reporting_bounds_and_current_description_validation() {
    let mut h = Harness::new().await;
    let mut request = h.request(1, 1, 0);
    request.service.metrics.0.push(Metric {
        name: "Work".into(),
        weight: 1,
        primary: 0,
        secondary: 0,
    });
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let partition = h.calls.partitions.lock().unwrap()[0].clone();
    for metrics in [
        vec![],
        vec![crate::LoadMetric {
            name: "No".into(),
            value: 1,
        }],
        vec![crate::LoadMetric {
            name: "Work".into(),
            value: i32::MAX as u32,
        }],
        vec![
            crate::LoadMetric {
                name: "Work".into(),
                value: 1
            };
            65
        ],
        vec![
            crate::LoadMetric {
                name: "Work".into(),
                value: 1
            };
            2
        ],
    ] {
        assert!(partition.report_load(&metrics).await.is_err());
    }
    let _permit = partition.reporting.admission_for_test();
    assert!(matches!(
        partition.report_move_cost(crate::MoveCost::High).await,
        Err(Error::ResourceLimit(_))
    ));
    drop(_permit);
    request.service.metrics.0.clear();
    request.service.update_version += 1;
    h.send("ProxyUpdateServiceDescription", &request).await;
    h.receive("ProxyUpdateServiceDescriptionReply").await;
    assert!(partition
        .report_load(&[crate::LoadMetric {
            name: "Work".into(),
            value: 1
        }])
        .await
        .is_err());
}
impl Harness {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let env = [
            (
                "Fabric_ApplicationHostId",
                "443de6bf-b823-40d2-915d-0826843aeddb",
            ),
            ("Fabric_ApplicationHostType", "Activated_SingleCodePackage"),
            ("Fabric_ApplicationId", "Model_App0"),
            ("Fabric_ServicePackageName", "ModelService"),
            ("Fabric_CodePackageName", "Code"),
            ("Fabric_CodePackageInstanceSeqNum", "1"),
            ("Fabric_ServicePackageInstanceSeqNum", "2"),
            ("Fabric_ServicePackageVersionInstance", "1.0:1.0:3"),
            ("Fabric_ApplicationName", "fabric:/Model"),
            ("Fabric_Folder_App_Work", "/work"),
            ("Fabric_Folder_App_Log", "/log"),
            ("Fabric_Folder_App_Temp", "/temp"),
            ("Fabric_NodeIPOrFQDN", "127.0.0.1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .chain([(
            "Fabric_RuntimeConnectionAddress".into(),
            address.to_string(),
        )])
        .collect();
        let activation = ActivationContext::parse(&env).unwrap();
        let (tx, rx) = mpsc::channel(32);
        let handler = Arc::new(move |r| {
            tx.try_send(r)
                .map_err(|_| Error::ResourceLimit("model ingress"))
        });
        let (client, peer) = tokio::join!(
            Connection::connect_ipc_clear(
                address,
                deadline_after(Duration::from_secs(2)).unwrap(),
                &activation.host_id,
                handler
            ),
            async {
                let (mut stream, _) = listener.accept().await.unwrap();
                read_message(&mut stream).await.unwrap();
                write_message(
                    &mut stream,
                    &Message::listen_instance(
                        MessageId {
                            guid: [1; 16],
                            index: 1,
                        },
                        &ListenInstance::default(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
                stream
            }
        );
        let authority = Authority::new(std::time::Instant::now(), 3_000_000_000).unwrap();
        let engine = Engine::start(activation, authority, client.unwrap(), rx);
        let calls = Arc::new(Calls::default());
        engine
            .insert_factory("ModelType".into(), Arc::new(Factory(calls.clone())))
            .unwrap();
        Self {
            engine,
            peer,
            calls,
            index: 2,
        }
    }
    fn request(&self, partition: u8, incarnation: i64, mode: u8) -> ProxyRequest {
        let context = &self.engine.shared.activation.runtime;
        let version = context.code.version.clone();
        ProxyRequest {
            runtime: context.id.clone(),
            unit: FailoverUnit {
                id: PartitionId {
                    id: Guid([partition; 16]),
                },
                consistency: ConsistencyUnit {
                    id: PartitionId {
                        id: Guid([partition; 16]),
                    },
                    low: 0,
                    high: 0,
                    name: String::new(),
                    kind: 1,
                },
                current: Epoch {
                    configuration: 1,
                    data_loss: 1,
                },
                previous: Epoch {
                    configuration: 0,
                    data_loss: 0,
                },
                target: 1,
                minimum: 1,
            },
            local: Replica {
                node: Node {
                    id: mssf_transport::serialization::serde::ByteArray(vec![1; 16]),
                    instance: 3,
                },
                id: 100,
                instance: incarnation,
                current_role: 4,
                previous_role: 0,
                up: true,
                last_lsn: -1,
                first_lsn: -1,
                state: 2,
                version: version.clone(),
                service_endpoint: String::new(),
                replication_endpoint: String::new(),
            },
            remote: EmptyObjectArray,
            service: Service {
                name: "fabric:/Model/Service".into(),
                instance: 1,
                service_type: ServiceType {
                    package: context.code.id.package.package.clone(),
                    name: "ModelType".into(),
                },
                application: "fabric:/Model".into(),
                version,
                partitions: 1,
                target: 1,
                minimum: 1,
                stateful: false,
                persisted: false,
                initialization: mssf_transport::serialization::serde::ByteArray(vec![mode]),
                group: false,
                correlations: mssf_transport::serialization::serde::ObjectArray(vec![]),
                constraints: String::new(),
                scaleout: 0,
                metrics: mssf_transport::serialization::serde::ObjectArray(vec![]),
                move_cost: 0,
                restart_wait: 0,
                quorum_wait: 0,
                update_version: 1,
                placement: mssf_transport::serialization::serde::ObjectArray(vec![]),
                standby_keep: 0,
                activation_mode: 0,
                dns: String::new(),
                scaling: EmptyObjectArray,
            },
            flags: 0,
        }
    }
    async fn send(&mut self, action: &str, request: &ProxyRequest) -> MessageId {
        let mut request = request.clone();
        if action == "RAPQuery" {
            request.flags = 0;
        }
        let id = MessageId {
            guid: [7; 16],
            index: self.index,
        };
        self.index += 1;
        let mut headers = vec![
            Header::actor(12).unwrap(),
            Header::action(action).unwrap(),
            Header::message_id(id).unwrap(),
        ];
        if action == "RAPQuery" {
            headers.push(Header::boolean(mssf_transport::message::EXPECTS_REPLY, true).unwrap())
        }
        write_message(
            &mut self.peer,
            &Message {
                headers,
                body: to_vec(&request).unwrap(),
            },
        )
        .await
        .unwrap();
        id
    }
    async fn receive(&mut self, action: &str) -> Message {
        let msg = tokio::time::timeout(Duration::from_secs(2), read_message(&mut self.peer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.properties().unwrap().action, action);
        assert_eq!(
            msg.properties().unwrap().actor,
            if action == "ReportHealth" { 7 } else { 12 }
        );
        msg
    }
    async fn no_reply(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), read_message(&mut self.peer))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn open_readiness_query_duplicate_close_and_partition_invalidation() {
    let mut h = Harness::new().await;
    let mut request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(!reply.local.service_endpoint.is_empty());
    let partition = h.calls.partitions.lock().unwrap()[0].clone();
    assert!(partition.report_load(&[]).await.is_err());
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert_eq!(h.calls.opens.load(Ordering::SeqCst), 1);
    let old_epoch = request.clone();
    request.unit.current.configuration += 1;
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    h.send("ReplicaClose", &old_epoch).await;
    h.no_reply().await;
    let mut native_query = request.clone();
    native_query.service.name.clear();
    native_query.service.application.clear();
    native_query.service.service_type.name.clear();
    native_query.service.service_type.package.name.clear();
    native_query
        .service
        .service_type
        .package
        .application
        .type_name
        .clear();
    native_query.service.service_type.package.application.number = 0;
    native_query.service.restart_wait = i64::MIN;
    native_query.service.quorum_wait = i64::MIN;
    native_query.service.standby_keep = i64::MIN;
    native_query.unit.consistency.kind = 0;
    let id = h.send("RAPQuery", &native_query).await;
    let response = h.receive("RAPQueryReply").await;
    assert_eq!(response.properties().unwrap().relates_to, id);
    let query: QueryReply = from_slice(&response.body).unwrap();
    assert_eq!(query.error, 0);
    assert_eq!(query.detail.instance_id, 100);
    assert_eq!(query.detail.operation, 1);
    native_query.local.instance += 1;
    h.send("RAPQuery", &native_query).await;
    let newer_query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(
        newer_query.error, 0,
        "native query permits newer request incarnation without mutating authority"
    );
    native_query.local.instance = 0;
    h.send("RAPQuery", &native_query).await;
    let stale_query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_ne!(stale_query.error, 0);
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(h.calls.closes.load(Ordering::SeqCst), 1);
    assert!(partition.ensure_valid().is_err());
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    assert_eq!(h.calls.closes.load(Ordering::SeqCst), 1);
    h.send("ReplicaOpen", &request).await;
    let closed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(closed.error.error, OBJECT_CLOSED);
    h.engine.quiesce().await.unwrap();
}

#[tokio::test]
async fn independent_units_progress_and_abort_fences_late_open() {
    let mut h = Harness::new().await;
    let blocked = h.request(1, 1, 8);
    h.send("ReplicaOpen", &blocked).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.calls.opens.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.send("RAPQuery", &blocked).await;
    let opening: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
    assert_eq!(opening.detail.operation, 2);
    assert!(opening.local.service_endpoint.is_empty());
    let other = h.request(2, 1, 0);
    h.send("ReplicaOpen", &other).await;
    let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(reply.unit.id.id, Guid([2; 16]));
    let mut abort = blocked.clone();
    abort.flags = 4;
    h.send("ReplicaClose", &abort).await;
    let failed: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
    assert_eq!(failed.error.error, OBJECT_CLOSED);
    assert!(failed.local.service_endpoint.is_empty());
    assert_eq!(h.calls.aborts.load(Ordering::SeqCst), 1);
    // Native retry after incompatible Abort/Open overlap.
    h.send("ReplicaClose", &abort).await;
    h.receive("ReplicaCloseReply").await;
    h.engine.quiesce().await.unwrap();
}

#[tokio::test]
async fn stale_incarnations_updates_and_callback_errors_are_not_success() {
    let mut h = Harness::new().await;
    for (part, mode, expected) in [(1, 1, -456), (2, 9, -123)] {
        let request = h.request(part, 2, mode);
        h.send("ReplicaOpen", &request).await;
        let reply: ProxyReply = from_slice(&h.receive("ReplicaOpenReply").await.body).unwrap();
        assert_eq!(reply.error.error, expected);
        assert!(reply.local.service_endpoint.is_empty());
    }
    assert_eq!(h.calls.aborts.load(Ordering::SeqCst), 1);
    let mut request = h.request(3, 2, 2);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    let mut stale = request.clone();
    stale.local.instance = 1;
    h.send("ReplicaClose", &stale).await;
    h.no_reply().await;
    request.service.update_version = 2;
    request.service.constraints = "accepted-v2".into();
    h.send("ProxyUpdateServiceDescription", &request).await;
    let update: UpdateServiceReply =
        from_slice(&h.receive("ProxyUpdateServiceDescriptionReply").await.body).unwrap();
    assert_eq!(update.service.update_version, 2);
    let mut duplicate = request.clone();
    duplicate.service.constraints = "must-not-apply".into();
    h.send("ProxyUpdateServiceDescription", &duplicate).await;
    let same: UpdateServiceReply =
        from_slice(&h.receive("ProxyUpdateServiceDescriptionReply").await.body).unwrap();
    assert_eq!(same.service.constraints, "accepted-v2");
    request.service.update_version = 3;
    request.service.constraints = "accepted-v3".into();
    h.send("ProxyUpdateServiceDescription", &request).await;
    let newer: UpdateServiceReply =
        from_slice(&h.receive("ProxyUpdateServiceDescriptionReply").await.body).unwrap();
    assert_eq!(newer.service.constraints, "accepted-v3");
    let mut old = request.clone();
    old.service.update_version = 1;
    h.send("ProxyUpdateServiceDescription", &old).await;
    h.no_reply().await;
    h.send("ReplicaClose", &request).await;
    let close: ProxyReply = from_slice(&h.receive("ReplicaCloseReply").await.body).unwrap();
    assert_eq!(close.error.error, 0);
    assert_eq!(h.calls.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.calls.aborts.load(Ordering::SeqCst),
        2,
        "failed close must abort before success ACK"
    );
    h.engine.quiesce().await.unwrap();
}

#[tokio::test]
async fn callback_panic_and_live_unit_overflow_retire_host() {
    let mut h = Harness::new().await;
    let panic = h.request(1, 1, 3);
    h.send("ReplicaOpen", &panic).await;
    tokio::time::timeout(Duration::from_secs(2), h.engine.shared.connection.closed())
        .await
        .unwrap();
    assert!(h.engine.shared.authority.check().is_err());
    let mut h = Harness::new().await;
    for partition in 1..=32 {
        let r = h.request(partition, 1, 0);
        h.send("ReplicaOpen", &r).await;
        h.receive("ReplicaOpenReply").await;
    }
    let overflow = h.request(33, 1, 0);
    h.send("ReplicaOpen", &overflow).await;
    tokio::time::timeout(Duration::from_secs(2), h.engine.shared.connection.closed())
        .await
        .unwrap();
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn tombstones_and_factory_registration_have_exact_limits() {
    let h = Harness::new().await;
    for n in 1..=31 {
        h.engine
            .insert_factory(format!("Type{n}"), Arc::new(Factory(h.calls.clone())))
            .unwrap()
    }

    assert!(h
        .engine
        .insert_factory("overflow".into(), Arc::new(Factory(h.calls.clone())))
        .is_err());
    assert!(h
        .engine
        .insert_factory("ModelType".into(), Arc::new(Factory(h.calls.clone())))
        .is_err());
    for n in 0..4096u32 {
        let mut r = h.request(1, 1, 0);
        r.unit.id.id.0[..4].copy_from_slice(&n.to_le_bytes());
        let u = unit(&h.engine.shared, &r, true).unwrap().unwrap();
        u.state.lock().unwrap().phase = Phase::Closed;
    }
    let r = h.request(2, 1, 0);
    assert!(matches!(
        unit(&h.engine.shared, &r, true),
        Err(Error::ResourceLimit("service tombstones"))
    ));
}

#[tokio::test]
async fn callback_admission_at_32_and_overflow_is_terminal() {
    let mut h = Harness::new().await;
    for n in 1..=32 {
        let blocked = h.request(n, 1, 8);
        h.send("ReplicaOpen", &blocked).await;
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.calls.opens.load(Ordering::SeqCst) != 32 {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    assert!(!h.engine.shared.connection.is_closed());
    let overflow = h.request(33, 1, 8);
    h.send("ReplicaOpen", &overflow).await;
    tokio::time::timeout(Duration::from_secs(2), h.engine.shared.connection.closed())
        .await
        .unwrap();
    assert!(h.engine.shared.authority.check().is_err());
}

#[tokio::test]
async fn query_tracks_actual_abort_cleanup_and_graceful_close_operations() {
    for mode in [4, 5, 6, 7] {
        let mut h = Harness::new().await;
        let mut request = h.request(1, 1, mode);
        h.send("ReplicaOpen", &request).await;
        if mode != 5 {
            h.receive("ReplicaOpenReply").await;
            if mode == 4 {
                request.flags = 4
            }

            h.send("ReplicaClose", &request).await;
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let u = unit(&h.engine.shared, &request, false).unwrap();
                if let Some(u) = u {
                    let expected = if mode == 7 { 8 } else { 16 };
                    if u.state.lock().unwrap().operation_kind == expected
                        && (mode == 7 || h.calls.aborts.load(Ordering::SeqCst) > 0)
                    {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        h.send("RAPQuery", &request).await;
        let query: QueryReply = from_slice(&h.receive("RAPQueryReply").await.body).unwrap();
        assert_eq!(query.detail.operation, if mode == 7 { 8 } else { 16 });
        assert!(query.detail.operation_start > 0);
        h.calls.abort_release.store(true, Ordering::Release);
        h.calls.release.notify_waiters();
        h.receive(if mode == 5 {
            "ReplicaOpenReply"
        } else {
            "ReplicaCloseReply"
        })
        .await;
        h.engine.quiesce().await.unwrap();
    }
}

#[tokio::test]
async fn abort_panic_or_cleanup_timeout_never_acknowledges_disposal() {
    for mode in [11, 12] {
        let mut h = Harness::new().await;
        let mut request = h.request(1, 1, mode);
        h.send("ReplicaOpen", &request).await;
        h.receive("ReplicaOpenReply").await;
        request.flags = 4;
        h.send("ReplicaClose", &request).await;
        let retired =
            tokio::time::timeout(Duration::from_secs(1), h.engine.shared.connection.closed()).await;
        // Release and observe the controlled blocking worker even if the
        // assertions below fail; it must not outlive the test.
        h.calls.abort_release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while h.calls.abort_running.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        let cause = retired.expect("failing Abort must retire the connection");
        assert!(if mode == 12 {
            matches!(cause, Error::Timeout)
        } else {
            matches!(cause, Error::Protocol("service abort panicked"))
        });
        assert!(h.engine.shared.authority.check().is_err());
        let response = tokio::time::timeout(Duration::from_secs(1), read_message(&mut h.peer))
            .await
            .unwrap();
        assert!(
            response.is_err(),
            "failed Abort must not send successful ReplicaCloseReply"
        );
    }
}

#[tokio::test]
async fn named_partition_wire_decode_preserves_owned_model_identity() {
    let mut h = Harness::new().await;
    let mut request = h.request(9, 1, 0);
    request.unit.consistency.kind = 3;
    request.unit.consistency.name = "owned-named-partition".into();
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert_eq!(
        h.calls.partitions.lock().unwrap()[0].kind,
        PartitionKind::Named("owned-named-partition".into())
    );
    h.send("ReplicaClose", &request).await;
    h.receive("ReplicaCloseReply").await;
    h.engine.quiesce().await.unwrap();
}

#[tokio::test]
async fn retired_authority_cannot_republish_cached_duplicate_endpoint() {
    let mut h = Harness::new().await;
    let request = h.request(1, 1, 0);
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    // Model an operation already admitted but not yet polled by its task.
    h.engine.shared.authority.check().unwrap();
    let admitted = IpcRequest {
        message: Message {
            headers: vec![
                Header::actor(12).unwrap(),
                Header::action("ReplicaOpen").unwrap(),
                Header::message_id(MessageId {
                    guid: [7; 16],
                    index: 99,
                })
                .unwrap(),
            ],
            body: to_vec(&request).unwrap(),
        },
        reply: None,
    };
    h.engine.shared.authority.retire();
    assert!(matches!(
        dispatch(h.engine.shared.clone(), admitted).await,
        Err(Error::Closed)
    ));
    h.no_reply().await;
    assert_eq!(h.calls.opens.load(Ordering::SeqCst), 1);
    h.engine.quiesce().await.unwrap();
}
