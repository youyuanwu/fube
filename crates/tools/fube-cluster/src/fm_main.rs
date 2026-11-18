use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    version,
    about = "Run a fresh same-host native-protocol Failover Manager"
)]
struct Arguments {
    /// Private static deployment manifest produced by the cluster supervisor.
    #[arg(long)]
    config: PathBuf,
}

fn main() {
    let arguments = Arguments::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("FM runtime");
    let result = runtime.block_on(fube_cluster::run_fm(&arguments.config));
    let code = match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{}", serde_json::json!({"event":"fm_error","error":error}));
            1
        }
    };
    std::process::exit(code);
}
