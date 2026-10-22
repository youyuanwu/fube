use std::{net::SocketAddr, time::Duration};

use mssf_client::{
    Error, FabricClient, HealthEventsFilter, NodeHealthPolicy, NodeHealthQueryDescription,
    NodeQueryDescription, Result, DEFAULT_TIMEOUT,
};

const USAGE: &str =
    "usage: onebox --clear IP:port ping|node-list [--timeout-seconds N]\n       onebox --clear IP:port name-exists fabric:/name [--timeout-seconds N]\n       onebox --clear IP:port node-health NODE [--events-filter U32] [--warning-as-error true|false] [--timeout-seconds N]";

#[derive(Debug)]
enum Operation {
    Ping,
    NodeList,
    NameExists(String),
    NodeHealth(NodeHealthQueryDescription),
}

fn arguments(mut args: Vec<String>) -> Result<(SocketAddr, Operation, Duration)> {
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
    let operation = match args[2].as_str() {
        "ping" if args.len() == 3 => Operation::Ping,
        "node-list" if args.len() == 3 => Operation::NodeList,
        "name-exists" if args.len() == 4 => Operation::NameExists(args[3].clone()),
        "node-health" if args.len() >= 4 => {
            let mut query = NodeHealthQueryDescription::new(&args[3]);
            let (options, remainder) = args[4..].as_chunks::<2>();
            for pair in options {
                match pair[0].as_str() {
                    "--events-filter" if query.events_filter.is_none() => {
                        query.events_filter = Some(HealthEventsFilter::from_bits(
                            pair[1].parse().map_err(|_| {
                                Error::InvalidInput("events filter must be a decimal UInt32 mask")
                            })?,
                        ));
                    }
                    "--warning-as-error" if query.health_policy.is_none() => {
                        query.health_policy = Some(NodeHealthPolicy {
                            consider_warning_as_error: pair[1].parse().map_err(|_| {
                                Error::InvalidInput("warning-as-error must be true or false")
                            })?,
                        });
                    }
                    _ => return Err(Error::InvalidInput(USAGE)),
                }
            }
            if !remainder.is_empty() {
                return Err(Error::InvalidInput(USAGE));
            }
            Operation::NodeHealth(query)
        }
        _ => return Err(Error::InvalidInput(USAGE)),
    };
    Ok((endpoint, operation, timeout))
}

async fn run() -> Result<()> {
    let (endpoint, operation, timeout) = arguments(std::env::args().skip(1).collect())?;
    let client = FabricClient::connect_clear(endpoint, timeout).await?;
    match operation {
        Operation::Ping => {
            let gateway = client.ping().await?;
            println!(
                "gateway={} node_name={} node_id={:x} node_instance={}",
                gateway.address, gateway.node_name, gateway.node_id, gateway.node_instance
            );
        }
        Operation::NodeList => {
            // Explicit single page: never imply this is the complete membership.
            let page = client
                .get_node_list(&NodeQueryDescription::default())
                .await?;
            for node in page.nodes {
                println!("{node:?}");
            }
            println!("continuation_token={:?}", page.continuation_token);
        }
        Operation::NodeHealth(query) => {
            println!("{:#?}", client.get_node_health(&query).await?);
        }
        Operation::NameExists(name) => {
            println!("name={name} exists={}", client.name_exists(&name).await?);
        }
    }
    client.close().await
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(suffix: &str) -> Result<(SocketAddr, Operation, Duration)> {
        arguments(
            format!("--clear 127.0.0.1:19000 {suffix}")
                .split_whitespace()
                .map(String::from)
                .collect(),
        )
    }

    #[test]
    fn explicit_health_options_preserve_native_defaults() {
        let (_, Operation::NodeHealth(query), _) = parse("node-health N0020").unwrap() else {
            panic!()
        };
        assert_eq!(query.node_name, "N0020");
        assert_eq!(query.health_policy, None);
        assert_eq!(query.events_filter, None);
        let (_, Operation::NodeHealth(query), timeout) = parse(
            "node-health N0020 --warning-as-error false --events-filter 0 --timeout-seconds 5",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(query.health_policy, Some(NodeHealthPolicy::default()));
        assert_eq!(query.events_filter, Some(HealthEventsFilter::DEFAULT));
        assert_eq!(timeout, Duration::from_secs(5));
    }

    #[test]
    fn rejects_unknown_duplicate_or_invalid_options() {
        for input in [
            "node-health",
            "node-health N0020 extra",
            "node-health N0020 --events-filter",
            "node-health N0020 --events-filter -1",
            "node-health N0020 --events-filter 4294967296",
            "node-health N0020 --warning-as-error yes",
            "node-health N0020 --cluster-health true",
            "node-health N0020 --events-filter 0 --events-filter 1",
            "node-health N0020 --warning-as-error true --warning-as-error false",
            "ping --timeout-seconds 1 --timeout-seconds 2",
        ] {
            assert!(parse(input).is_err(), "{input}");
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
