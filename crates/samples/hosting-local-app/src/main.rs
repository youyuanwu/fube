//! Explicitly minimal native-free stateless package fixture.
use fube_runtime::{
    ApiFuture, Cancellation, Error, IStatelessServiceFactory, IStatelessServiceInstance,
    IStatelessServicePartition, Result, Runtime,
};
use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone, Copy, Default)]
struct Mode {
    block_dispose: bool,
    close_error: bool,
    open_delay: u64,
}
struct Factory(Mode);
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
    let delay = match args.as_slice() {
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
    let _runtime = if args.first().is_some_and(|s| s == "--no-register") {
        runtime
    } else {
        runtime
            .execution_runtime()
            .try_reserve()
            .expect("execution reservation")
            .run_owned(move || {
                runtime.register_stateless_service_factory(
                    "LocalStatelessType",
                    Box::new(Factory(mode)),
                )?;
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
