use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};
use fube_client::{Error, ImageStoreClient, Result};

#[derive(Parser)]
#[command(version, about = "Native Service Fabric development CLI")]
struct Arguments {
    /// Native TCP gateway, not an HTTP management endpoint.
    #[arg(long)]
    endpoint: SocketAddr,

    /// Explicitly allow an unsecured development cluster.
    #[arg(long)]
    unsecured: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Application {
        #[command(subcommand)]
        command: ApplicationCommand,
    },
}

#[derive(Subcommand)]
enum ApplicationCommand {
    /// Copy a local package to fabric:ImageStore using native atomic-copy semantics.
    Upload {
        #[arg(long)]
        path: PathBuf,

        /// Whole-package timeout in seconds.
        #[arg(long, short = 't', default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: u64,
    },
}

async fn run(arguments: Arguments) -> Result<()> {
    if !arguments.unsecured {
        return Err(Error::InvalidInput(
            "native upload requires explicit --unsecured development mode",
        ));
    }
    let Command::Application {
        command: ApplicationCommand::Upload { path, timeout },
    } = arguments.command;
    let budget = Duration::from_secs(timeout);
    let deadline = tokio::time::Instant::now()
        .checked_add(budget)
        .ok_or(Error::InvalidInput("upload timeout is too large"))?;
    let destination = tokio::time::timeout_at(deadline, async {
        let resolved = tokio::fs::canonicalize(&path).await?;
        let destination = resolved
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::InvalidInput(
                "package directory must have a Unicode final name",
            ))?
            .to_owned();
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(Error::Timeout)?;
        let mut client = ImageStoreClient::connect_clear(arguments.endpoint, remaining).await?;
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(Error::Timeout)?;
        client
            .copy_application_package(&path, &destination, Some(remaining))
            .await?;
        client.close().await?;
        Ok::<_, Error>(destination)
    })
    .await
    .map_err(|_| Error::Timeout)??;
    println!("Uploaded application package to {destination}.");
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run(Arguments::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fube: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
