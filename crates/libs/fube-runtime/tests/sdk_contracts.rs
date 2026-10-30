//! Public-only consumers of the replacement runtime contracts.
use fube_runtime::{
    ApiFuture, Cancellation, Epoch, Error, ErrorKind, FaultType, HealthInformation,
    HealthReportSendOption, IPrimaryReplicator, IReplicator, IStatefulServiceFactory,
    IStatefulServicePartition, IStatefulServiceReplica, IStatelessServiceFactory,
    IStatelessServiceInstance, IStatelessServicePartition, LoadMetric, MoveCost, OpenMode,
    RejectedRegistrationInput, ReplicaInformation, ReplicaRole, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, Result, Runtime, ServicePartitionAccessStatus,
    ServicePartitionInformation, SingletonPartitionInformation,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Partition;
impl IStatelessServicePartition for Partition {
    fn get_partition_info(&self) -> Result<ServicePartitionInformation> {
        Ok(ServicePartitionInformation::Singleton(
            SingletonPartitionInformation {
                id: uuid::Uuid::nil(),
            },
        ))
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        if metrics.len() != 1 || metrics[0].name != "work" {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "unexpected test metric",
            ));
        }
        Ok(())
    }
    fn report_move_cost(&self, _: MoveCost) -> Result<()> {
        Ok(())
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        if fault == FaultType::Invalid {
            Err(Error::new(ErrorKind::InvalidArgument, "invalid fault"))
        } else {
            Ok(())
        }
    }
    fn report_partition_health(&self, _: &HealthInformation) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "test route is unsupported",
        ))
    }
    fn report_instance_health(&self, _: &HealthInformation) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "test route is unsupported",
        ))
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        IStatelessServicePartition::report_partition_health(self, health)
    }
    fn report_instance_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_instance_health(health)
    }
}
impl IStatefulServicePartition for Partition {
    fn get_partition_information(&self) -> Result<ServicePartitionInformation> {
        self.get_partition_info()
    }
    fn get_read_status(&self) -> Result<ServicePartitionAccessStatus> {
        Ok(ServicePartitionAccessStatus::Granted)
    }
    fn get_write_status(&self) -> Result<ServicePartitionAccessStatus> {
        Ok(ServicePartitionAccessStatus::ReconfigurationPending)
    }
    fn report_load(&self, metrics: &[LoadMetric]) -> Result<()> {
        IStatelessServicePartition::report_load(self, metrics)
    }
    fn report_move_cost(&self, cost: MoveCost) -> Result<()> {
        IStatelessServicePartition::report_move_cost(self, cost)
    }
    fn report_fault(&self, fault: FaultType) -> Result<()> {
        IStatelessServicePartition::report_fault(self, fault)
    }
    fn report_partition_health(&self, health: &HealthInformation) -> Result<()> {
        IStatelessServicePartition::report_partition_health(self, health)
    }
    fn report_replica_health(&self, _: &HealthInformation) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "test route is unsupported",
        ))
    }
    fn report_partition_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        IStatefulServicePartition::report_partition_health(self, health)
    }
    fn report_replica_health2(
        &self,
        health: &HealthInformation,
        _: Option<&HealthReportSendOption>,
    ) -> Result<()> {
        self.report_replica_health(health)
    }
}
struct Factory(Arc<AtomicUsize>);
struct Instance;
struct Replica(Arc<AtomicUsize>);
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        ty: String,
        name: String,
        initialization: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        assert_eq!(
            (ty.as_str(), name.as_str(), initialization, partition),
            ("test", "fabric:/test", &[1, 2][..], uuid::Uuid::nil())
        );
        if id != 7 {
            return Err(Error::new(ErrorKind::InvalidArgument, "invalid instance"));
        }
        Ok(Box::new(Instance))
    }
}
impl IStatelessServiceInstance for Instance {
    fn open(
        &self,
        partition: Arc<dyn IStatelessServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, String> {
        Box::pin(async move {
            let _ = partition.get_partition_info()?;
            Ok("test://instance".into())
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
impl IStatefulServiceFactory for Factory {
    fn create_replica(
        &self,
        ty: String,
        name: String,
        initialization: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        assert_eq!(
            (ty.as_str(), name.as_str(), initialization, partition, id),
            ("test", "fabric:/test", &[1, 2][..], uuid::Uuid::nil(), 8)
        );
        Ok(Box::new(Replica(self.0.clone())))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            let _ = partition.get_partition_information()?;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Control(AtomicUsize::new(0))) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("test://replica".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct Control(AtomicUsize);
impl IReplicator for Control {
    fn open(&self, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("test://replicator".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn change_role(&self, epoch: Epoch, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if epoch != Epoch::ZERO || role != ReplicaRole::Primary {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "unexpected test role/epoch",
                ));
            }
            Ok(())
        })
    }
    fn update_epoch(&self, _: Epoch, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn get_current_progress(&self) -> Result<i64> {
        Ok(9)
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        Ok(3)
    }
    fn abort(&self) {}
}
impl IPrimaryReplicator for Control {
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    ) -> Result<()> {
        assert_eq!((current.write_quorum, previous.write_quorum), (1, 0));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn update_current_replica_set_configuration(&self, current: ReplicaSetConfig) -> Result<()> {
        assert_eq!(current.write_quorum, 1);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn wait_for_catch_up_quorum(
        &self,
        mode: ReplicaSetQuorumMode,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if mode == ReplicaSetQuorumMode::Invalid {
                Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "invalid quorum mode",
                ))
            } else {
                Ok(())
            }
        })
    }
    fn build_replica(&self, replica: ReplicaInformation, _: Cancellation) -> ApiFuture<'_, ()> {
        assert_eq!(
            (
                replica.id,
                replica.role,
                replica.status,
                replica.current_progress,
                replica.catch_up_capability
            ),
            (12, ReplicaRole::IdleSecondary, ReplicaStatus::Up, -1, -1)
        );
        Box::pin(async { Ok(()) })
    }
    fn remove_replica(&self, id: i64) -> Result<()> {
        assert_eq!(id, 12);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn canonical_objects_pin_real_sync_results_and_separate_async_roles() {
    let activated = Arc::new(AtomicUsize::new(0));
    let factory = Factory(activated.clone());
    let stateless: &dyn IStatelessServiceFactory = &factory;
    let result: Result<Box<dyn IStatelessServiceInstance>> = stateless.create_instance(
        "test".into(),
        "fabric:/test".into(),
        &[1, 2],
        uuid::Uuid::nil(),
        7,
    );
    let instance = result.unwrap();
    assert_eq!(
        instance
            .open(Arc::new(Partition), Cancellation::default())
            .await
            .unwrap(),
        "test://instance"
    );
    let failure: Result<Box<dyn IStatelessServiceInstance>> = stateless.create_instance(
        "test".into(),
        "fabric:/test".into(),
        &[1, 2],
        uuid::Uuid::nil(),
        -1,
    );
    assert_eq!(failure.err().unwrap().kind(), ErrorKind::InvalidArgument);
    let stateful: &dyn IStatefulServiceFactory = &factory;
    let result: Result<Box<dyn IStatefulServiceReplica>> = stateful.create_replica(
        "test".into(),
        "fabric:/test".into(),
        &[1, 2],
        uuid::Uuid::nil(),
        8,
    );
    let replica = result.unwrap();
    for (mode, kind) in [
        (OpenMode::Existing, ErrorKind::Unsupported),
        (OpenMode::Invalid, ErrorKind::InvalidArgument),
    ] {
        assert_eq!(
            replica
                .open(mode, Arc::new(Partition), Cancellation::default())
                .await
                .err()
                .unwrap()
                .kind(),
            kind
        );
    }
    assert_eq!(activated.load(Ordering::SeqCst), 0);
    let control = replica
        .open(OpenMode::New, Arc::new(Partition), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(activated.load(Ordering::SeqCst), 1);
    let common: &dyn IReplicator = &*control;
    common
        .change_role(Epoch::ZERO, ReplicaRole::Primary, Cancellation::default())
        .await
        .unwrap();
    assert_eq!(
        (
            common.get_catch_up_capability().unwrap(),
            common.get_current_progress().unwrap()
        ),
        (3, 9)
    );
    let configured: Result<()> = control.update_catch_up_replica_set_configuration(
        ReplicaSetConfig {
            replicas: vec![],
            write_quorum: 1,
        },
        ReplicaSetConfig {
            replicas: vec![],
            write_quorum: 0,
        },
    );
    configured.unwrap();
    let current: Result<()> = control.update_current_replica_set_configuration(ReplicaSetConfig {
        replicas: vec![],
        write_quorum: 1,
    });
    current.unwrap();
    let build = control.build_replica(
        ReplicaInformation {
            id: 12,
            role: ReplicaRole::IdleSecondary,
            status: ReplicaStatus::Up,
            replicator_address: "test://12".into(),
            current_progress: -1,
            catch_up_capability: -1,
            must_catch_up: false,
        },
        Cancellation::default(),
    );
    build.await.unwrap();
    control
        .wait_for_catch_up_quorum(ReplicaSetQuorumMode::Write, Cancellation::default())
        .await
        .unwrap();
    assert!(!control.on_data_loss(Cancellation::default()).await.unwrap());
    let removed: Result<()> = control.remove_replica(12);
    removed.unwrap();
    let partition: Arc<dyn IStatefulServicePartition> = Arc::new(Partition);
    assert_eq!(
        partition.get_read_status().unwrap(),
        ServicePartitionAccessStatus::Granted
    );
    assert_eq!(
        partition.get_write_status().unwrap(),
        ServicePartitionAccessStatus::ReconfigurationPending
    );
    let reported: Result<()> = partition.report_load(&[LoadMetric {
        name: "work".into(),
        value: 1,
    }]);
    reported.unwrap();
    let fault: Result<()> = partition.report_fault(FaultType::Invalid);
    assert_eq!(fault.unwrap_err().kind(), ErrorKind::InvalidArgument);
    let moved: Result<()> = partition.report_move_cost(MoveCost::Low);
    moved.unwrap();
    instance.close(Cancellation::default()).await.unwrap();
    control.close(Cancellation::default()).await.unwrap();
    replica.close(Cancellation::default()).await.unwrap();
}

/// Also used with a genuinely attached runtime by the host-core unit fixture.
/// Only public APIs are used here; the fixture supplies native IPC, not internals.
pub async fn recover_and_register(
    runtime: Runtime,
    name: &'static str,
    factory: Box<dyn IStatelessServiceFactory>,
) -> Runtime {
    let original = (&*factory as *const dyn IStatelessServiceFactory).cast::<()>();
    let rejected: Result<()> = runtime.register_stateless_service_factory(name, factory);
    let mut error = rejected.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidExecutionContext);
    let Some(RejectedRegistrationInput::Stateless(factory)) =
        error.take_rejected_registration_input()
    else {
        panic!("exact stateless input was not recoverable")
    };
    assert_eq!(
        original,
        (&*factory as *const dyn IStatelessServiceFactory).cast::<()>()
    );
    assert!(error.take_rejected_registration_input().is_none());
    let permit = runtime.execution_runtime().try_reserve().unwrap();
    permit
        .run_owned(move || {
            struct RejectedOnWorker {
                execution: fube_runtime::extensions::ExecutionRuntime,
                dropped: Arc<AtomicUsize>,
            }
            impl IStatelessServiceFactory for RejectedOnWorker {
                fn create_instance(
                    &self,
                    _: String,
                    _: String,
                    _: &[u8],
                    _: uuid::Uuid,
                    _: i64,
                ) -> Result<Box<dyn IStatelessServiceInstance>> {
                    panic!("invalid registration must never reach creation")
                }
            }
            impl Drop for RejectedOnWorker {
                fn drop(&mut self) {
                    assert!(self.execution.blocking_deadline().is_ok());
                    assert_eq!(std::thread::current().name(), Some("fube-native-call"));
                    self.dropped.fetch_add(1, Ordering::SeqCst);
                }
            }
            let dropped = Arc::new(AtomicUsize::new(0));
            let rejected: Result<()> = runtime.register_stateless_service_factory(
                "",
                Box::new(RejectedOnWorker {
                    execution: runtime.execution_runtime(),
                    dropped: dropped.clone(),
                }),
            );
            assert_eq!(rejected.unwrap_err().kind(), ErrorKind::InvalidArgument);
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            let result: Result<()> = runtime.register_stateless_service_factory(name, factory);
            result?;
            println!("SYNC_REG_COMPLETED");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
            Ok(runtime)
        })
        .await
        .unwrap()
}

#[test]
fn registration_is_sync_and_errors_preserve_structural_native_and_io_causes() {
    let _: fn(&Runtime, &str, Box<dyn IStatelessServiceFactory>) -> Result<()> =
        Runtime::register_stateless_service_factory;
    let _: fn(&Runtime, &str, Box<dyn IStatefulServiceFactory>) -> Result<()> =
        Runtime::register_stateful_service_factory;
    let native = Error::Remote {
        code: -987,
        message: "provider/native failure".into(),
    };
    let error = Error::with_cause(ErrorKind::Provider, "callback failed", native).uncertain();
    assert_eq!(error.kind(), ErrorKind::Provider);
    assert_eq!(error.native_code(), Some(-987));
    assert!(error.is_uncertain());
    let error = Error::from(std::io::Error::from_raw_os_error(13));
    assert_eq!(error.kind(), ErrorKind::Transport);
    let source = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.raw_os_error(), Some(13));
    assert_eq!(error.clone().kind(), ErrorKind::Transport);
}
