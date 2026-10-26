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
            if self.mode == 1 {
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
            if self.mode == 2 {
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
    }
}
struct Harness {
    engine: Engine,
    peer: TcpStream,
    calls: Arc<Calls>,
    index: u32,
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
                body: to_vec(request).unwrap(),
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
        assert_eq!(msg.properties().unwrap().actor, 12);
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
    assert!(partition.report_health("a", "b", "c").is_err());
    assert!(partition.report_load(&[]).is_err());
    assert!(partition.report_fault(FaultType::Transient).is_err());
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    assert_eq!(h.calls.opens.load(Ordering::SeqCst), 1);
    let old_epoch = request.clone();
    request.unit.current.configuration += 1;
    h.send("ReplicaOpen", &request).await;
    h.receive("ReplicaOpenReply").await;
    h.send("ReplicaClose", &old_epoch).await;
    h.no_reply().await;
    let id = h.send("RAPQuery", &request).await;
    let response = h.receive("RAPQueryReply").await;
    assert_eq!(response.properties().unwrap().relates_to, id);
    let query: QueryReply = from_slice(&response.body).unwrap();
    assert_eq!(query.error, 0);
    assert_eq!(query.detail.instance_id, 100);
    assert_eq!(query.detail.operation, 1);
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
    h.send("ProxyUpdateServiceDescription", &request).await;
    let update: UpdateServiceReply =
        from_slice(&h.receive("ProxyUpdateServiceDescriptionReply").await.body).unwrap();
    assert_eq!(update.service.update_version, 2);
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
