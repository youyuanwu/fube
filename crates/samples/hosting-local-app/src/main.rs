//! Explicitly minimal native-free stateless package fixture.
use fube_runtime::{
    ApiFuture, Cancellation, IStatelessServiceFactory, IStatelessServiceInstance,
    IStatelessServicePartition, Result, Runtime,
};
use std::{io::Write, sync::Arc, time::Duration};

struct Factory;
struct Instance;
impl IStatelessServiceFactory for Factory {
    fn create_instance(
        &self,
        _: String,
        _: String,
        _: &[u8],
        _: uuid::Uuid,
        _: i64,
    ) -> Result<Box<dyn IStatelessServiceInstance>> {
        println!("LOCAL_CALLBACK create");
        Ok(Box::new(Instance))
    }
}
impl IStatelessServiceInstance for Instance {
    fn open(
        &self,
        _: Arc<dyn IStatelessServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, String> {
        Box::pin(async {
            println!("LOCAL_CALLBACK open");
            Ok(String::new())
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async {
            println!("LOCAL_CALLBACK close");
            Ok(())
        })
    }
    fn abort(&self) {
        println!("LOCAL_CALLBACK abort");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // These explicit fault modes are for owned-lifecycle qualification only.
    let args: Vec<_> = std::env::args().skip(1).collect();
    let delay = match args.as_slice() {
        [flag, value] if flag == "--delay-ms" || flag == "--crash-after-ms" => Some((
            flag.as_str(),
            value.parse::<u64>().expect("integer milliseconds"),
        )),
        [] => None,
        [flag] if flag == "--no-register" || flag == "--exit-before-attach" => None,
        _ => panic!("unsupported fixture arguments"),
    };
    if args.first().is_some_and(|s| s == "--exit-before-attach") {
        std::process::exit(17);
    }
    if let Some(("--delay-ms", ms)) = delay {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    let runtime = Runtime::attach().await.expect("owned Hosting attach");
    let _runtime = if args.first().is_some_and(|s| s == "--no-register") {
        runtime
    } else {
        runtime
            .execution_runtime()
            .try_reserve()
            .expect("execution reservation")
            .run_owned(move || {
                runtime
                    .register_stateless_service_factory("LocalStatelessType", Box::new(Factory))?;
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
    std::future::pending::<()>().await;
}
