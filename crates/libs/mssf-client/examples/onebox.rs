use std::{net::SocketAddr, time::Duration};

use mssf_client::{Error, FabricClient, Result, DEFAULT_TIMEOUT};

const USAGE: &str =
    "usage: onebox --clear IP:port ping [--timeout-seconds N]\n       onebox --clear IP:port name-exists fabric:/name [--timeout-seconds N]";

fn arguments() -> Result<(SocketAddr, String, Option<String>, Duration)> {
    let mut args: Vec<_> = std::env::args().skip(1).collect();
    let mut timeout = DEFAULT_TIMEOUT;
    if let Some(index) = args.iter().position(|arg| arg == "--timeout-seconds") {
        let seconds = args
            .get(index + 1)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or(Error::InvalidInput(
                "timeout must be positive finite seconds",
            ))?;
        timeout = Duration::try_from_secs_f64(seconds)
            .map_err(|_| Error::InvalidInput("timeout is out of range"))?;
        mssf_transport::connection::deadline_after(timeout)?;
        args.drain(index..=index + 1);
    }
    if args.len() < 3 || args[0] != "--clear" {
        return Err(Error::InvalidInput(USAGE));
    }
    let endpoint = args[1]
        .parse()
        .map_err(|_| Error::InvalidInput("clear endpoint must be an IP:port socket address"))?;
    let name = match args[2].as_str() {
        "ping" if args.len() == 3 => None,
        "name-exists" if args.len() == 4 => Some(args[3].clone()),
        _ => return Err(Error::InvalidInput(USAGE)),
    };
    Ok((endpoint, args[2].clone(), name, timeout))
}

async fn run() -> Result<()> {
    let (endpoint, operation, name, timeout) = arguments()?;
    let mut client = FabricClient::connect_clear(endpoint, timeout).await?;
    if operation == "ping" {
        let gateway = client.ping().await?;
        println!(
            "gateway={} node_name={} node_id={:x} node_instance={}",
            gateway.address, gateway.node_name, gateway.node_id, gateway.node_instance
        );
    } else {
        let name = name.as_deref().unwrap();
        println!("name={name} exists={}", client.name_exists(name).await?);
    }
    client.close().await
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
