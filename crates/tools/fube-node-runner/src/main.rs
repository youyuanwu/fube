use clap::Parser;
use fube_node_runner::{Config, Result};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Run one volatile replica Node under an external FM")]
struct Arguments {
    #[arg(long)]
    config: PathBuf,
}

async fn stop() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).map_err(|error| error.to_string())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string()),
        result = term.recv() => result.ok_or_else(|| "SIGTERM stream closed".into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let result = match Config::read(&Arguments::parse().config) {
        Ok(config) => fube_node_runner::run(config, stop()).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(outcome) if outcome.clean => std::process::ExitCode::SUCCESS,
        Ok(_) => std::process::ExitCode::FAILURE,
        Err(error) => {
            eprintln!("{}", serde_json::json!({"event": "error", "error": error}));
            std::process::ExitCode::FAILURE
        }
    }
}
