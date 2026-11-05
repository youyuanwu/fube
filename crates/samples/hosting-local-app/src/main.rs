//! Explicitly minimal native-free local Hosting package fixtures.
use fube_runtime::{
    ApiFuture, Cancellation, Epoch, Error, IPrimaryReplicator, IReplicator,
    IStatefulServiceFactory, IStatefulServicePartition, IStatefulServiceReplica,
    IStatelessServiceFactory, IStatelessServiceInstance, IStatelessServicePartition, OpenMode,
    ReplicaInformation, ReplicaRole, ReplicaSetConfig, ReplicaSetQuorumMode, Result, Runtime,
    ServicePartitionAccessStatus,
};
use std::{
    io::Write,
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Default)]
struct Mode {
    block_dispose: bool,
    close_error: bool,
    open_delay: u64,
}
struct Factory(Mode);
struct StatefulFactory {
    lifecycle: bool,
    mode: Mode,
}
struct StatefulReplica {
    resource: Instance,
    partition: Mutex<Option<Arc<dyn IStatefulServicePartition>>>,
}
struct LocalControl(Mutex<Option<TcpListener>>);
struct Instance {
    mode: Mode,
    resource: PathBuf,
}
fn marker(value: &str) {
    println!("{value}");
    std::io::stdout()
        .flush()
        .expect("flush lifecycle observation");
}
impl IStatefulServiceFactory for StatefulFactory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        _: uuid::Uuid,
        _: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        marker("LOCAL_CALLBACK create_replica");
        if !self.lifecycle {
            return Err(Error::Protocol("registration-only stateful fixture"));
        }
        let resource =
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"))
                .join("local-instance.resource");
        std::fs::write(&resource, b"actual replica-owned resource")
            .expect("acquire stateful fixture resource");
        marker("LOCAL_RESOURCE acquired");
        Ok(Box::new(StatefulReplica {
            resource: Instance {
                mode: self.mode,
                resource,
            },
            partition: Mutex::new(None),
        }))
    }
}
impl IStatefulServiceReplica for StatefulReplica {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            assert_eq!(mode, OpenMode::New);
            assert_ne!(
                partition.get_write_status()?,
                ServicePartitionAccessStatus::Granted
            );
            *self.partition.lock().unwrap() = Some(partition);
            marker("LOCAL_CALLBACK open");
            tokio::time::sleep(Duration::from_millis(self.resource.mode.open_delay)).await;
            let control: Box<dyn IPrimaryReplicator> = Box::new(LocalControl(Mutex::new(None)));
            Ok(control)
        })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async move {
            if !matches!(role, ReplicaRole::IdleSecondary | ReplicaRole::None) {
                return Err(Error::Protocol(
                    "fixture prohibits Primary/secondary promotion",
                ));
            }
            marker(&format!("LOCAL_CALLBACK role {role:?}"));
            if role == ReplicaRole::IdleSecondary {
                let partition = self
                    .partition
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("opened partition");
                assert_ne!(
                    partition.get_read_status()?,
                    ServicePartitionAccessStatus::Granted
                );
                assert_ne!(
                    partition.get_write_status()?,
                    ServicePartitionAccessStatus::Granted
                );
                marker("LOCAL_ACCESS denied");
            }
            Ok(String::new())
        })
    }
    fn close(&self, cancel: Cancellation) -> ApiFuture<'_, ()> {
        IStatelessServiceInstance::close(&self.resource, cancel)
    }
    fn abort(&self) {
        marker("LOCAL_CALLBACK abort");
    }
}
impl LocalControl {
    fn release(&self) {
        if let Some(listener) = self.0.lock().unwrap().take() {
            drop(listener);
            marker("LOCAL_CONTROL released");
        }
    }
}
impl IReplicator for LocalControl {
    fn open(&self, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("bind actual fixture control listener");
            let address = listener
                .local_addr()
                .expect("fixture control address")
                .to_string();
            *self.0.lock().unwrap() = Some(listener);
            marker("LOCAL_CONTROL acquired");
            Ok(address)
        })
    }
    fn change_role(&self, _: Epoch, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if !matches!(role, ReplicaRole::IdleSecondary | ReplicaRole::None) {
                return Err(Error::Protocol("fixture control prohibits promotion"));
            }
            marker(&format!("LOCAL_CONTROL role {role:?}"));
            Ok(())
        })
    }
    fn update_epoch(&self, _: Epoch, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no epoch/data capability")) })
    }
    fn get_current_progress(&self) -> Result<i64> {
        Err(Error::Protocol("fixture has no data progress"))
    }
    fn get_catch_up_capability(&self) -> Result<i64> {
        Err(Error::Protocol("fixture has no retained history"))
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            self.release();
            Ok(())
        })
    }
    fn abort(&self) {
        marker("LOCAL_CONTROL abort");
        self.release();
    }
}
impl IPrimaryReplicator for LocalControl {
    fn on_data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Err(Error::Protocol("fixture has no data-loss capability")) })
    }
    fn update_catch_up_replica_set_configuration(
        &self,
        _: ReplicaSetConfig,
        _: ReplicaSetConfig,
    ) -> Result<()> {
        Err(Error::Protocol("fixture has no configuration capability"))
    }
    fn update_current_replica_set_configuration(&self, _: ReplicaSetConfig) -> Result<()> {
        Err(Error::Protocol("fixture has no configuration capability"))
    }
    fn wait_for_catch_up_quorum(
        &self,
        _: ReplicaSetQuorumMode,
        _: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no quorum capability")) })
    }
    fn build_replica(&self, _: ReplicaInformation, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Err(Error::Protocol("fixture has no build capability")) })
    }
    fn remove_replica(&self, _: i64) -> Result<()> {
        Err(Error::Protocol("fixture has no peer-removal capability"))
    }
}
impl Drop for LocalControl {
    fn drop(&mut self) {
        assert!(
            self.0.lock().unwrap().is_none(),
            "control listener was not disposed"
        );
        marker("LOCAL_CONTROL disposed");
    }
}
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        instance: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        marker(&format!(
            "LOCAL_CALLBACK create partition={partition} instance={instance}"
        ));
        let resource =
            PathBuf::from(std::env::var_os("Fabric_Folder_App_Work").expect("owned work folder"))
                .join("local-instance.resource");
        std::fs::write(&resource, b"actual instance-owned resource")
            .expect("acquire fixture resource");
        marker("LOCAL_RESOURCE acquired");
        Ok(Box::new(Instance {
            mode: self.0,
            resource,
        }))
    }
}
impl IStatelessServiceInstance for Instance {
    fn open(
        &self,
        _: Arc<dyn IStatelessServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, String> {
        Box::pin(async {
            marker("LOCAL_CALLBACK open");
            tokio::time::sleep(Duration::from_millis(self.mode.open_delay)).await;
            Ok(String::new())
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            marker("LOCAL_CALLBACK close");
            if self.mode.close_error {
                Err(Error::Protocol("fixture graceful close failure"))
            } else {
                Ok(())
            }
        })
    }
    fn abort(&self) {
        marker("LOCAL_CALLBACK abort");
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        marker("LOCAL_DISPOSE begin");
        if self.mode.block_dispose {
            let release = self.resource.with_file_name("local-dispose.release");
            let end = std::time::Instant::now() + Duration::from_secs(5);
            while !release.exists() {
                assert!(
                    std::time::Instant::now() < end,
                    "bounded fixture disposal barrier timed out"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        std::fs::remove_file(&self.resource).expect("dispose actual fixture resource");
        marker("LOCAL_RESOURCE released");
        marker("LOCAL_DISPOSE complete");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // These explicit fault modes are for owned-lifecycle qualification only.
    let args: Vec<_> = std::env::args().skip(1).collect();
    let stateful_lifecycle = args.first().is_some_and(|s| s == "--stateful-lifecycle");
    let args = if stateful_lifecycle {
        &args[1..]
    } else {
        &args[..]
    };
    let delay = match args {
        [flag, value]
            if flag == "--delay-ms"
                || flag == "--crash-after-ms"
                || flag == "--shutdown-after-ms" =>
        {
            Some((
                flag.as_str(),
                value.parse::<u64>().expect("integer milliseconds"),
            ))
        }
        [] => None,
        [flag] if flag == "--no-register" || flag == "--exit-before-attach" => None,
        [flag] if flag == "--stateful-register" => None,
        [flag] if flag == "--block-dispose" || flag == "--close-error-block-dispose" => None,
        [flag, _] if flag == "--open-delay-ms" => None,
        _ => panic!("unsupported fixture arguments"),
    };
    if args.first().is_some_and(|s| s == "--exit-before-attach") {
        std::process::exit(17);
    }
    if let Some(("--delay-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    let runtime = Runtime::attach().await.expect("owned Hosting attach");
    let mode = Mode {
        block_dispose: args
            .first()
            .is_some_and(|s| s == "--block-dispose" || s == "--close-error-block-dispose"),
        close_error: args
            .first()
            .is_some_and(|s| s == "--close-error-block-dispose"),
        open_delay: if args.first().is_some_and(|s| s == "--open-delay-ms") {
            args[1].parse().expect("integer callback milliseconds")
        } else {
            0
        },
    };
    let stateful_registration = args.first().is_some_and(|s| s == "--stateful-register");
    let _runtime = if args.first().is_some_and(|s| s == "--no-register") {
        runtime
    } else {
        runtime
            .execution_runtime()
            .try_reserve()
            .expect("execution reservation")
            .run_owned(move || {
                if stateful_registration || stateful_lifecycle {
                    runtime.register_stateful_service_factory(
                        "LocalStatefulType",
                        Box::new(StatefulFactory {
                            lifecycle: stateful_lifecycle,
                            mode,
                        }),
                    )?;
                    marker("LOCAL_STATEFUL_REGISTERED");
                } else {
                    runtime.register_stateless_service_factory(
                        "LocalStatelessType",
                        Box::new(Factory(mode)),
                    )?;
                }
                Ok(runtime)
            })
            .await
            .expect("declared factory registration")
    };
    println!("LOCAL_REGISTERED");
    std::io::stdout()
        .flush()
        .expect("flush registration marker");
    if let Some(("--crash-after-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        std::process::exit(23);
    }
    if let Some(("--shutdown-after-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        _runtime
            .shutdown()
            .await
            .expect("orderly Runtime/session shutdown");
    }
    std::future::pending::<()>().await;
}
