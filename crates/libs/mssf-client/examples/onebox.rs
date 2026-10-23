use std::{net::SocketAddr, num::NonZeroU64, time::Duration};

use mssf_client::{
    ApplicationDefinitionKindFilter, ApplicationListFilter, ApplicationQueryDescription, Error,
    FabricClient, HealthEventsFilter, NamingUri, NodeHealthPolicy, NodeHealthQueryDescription,
    NodeHealthQueryParameters, NodeQueryDescription, Result, ServiceListFilter,
    ServiceQueryDescription, ServiceQueryParameters, DEFAULT_TIMEOUT,
};

const USAGE: &str =
    "usage: onebox --clear IP:port ping|node-list [--timeout-seconds N]\n       onebox --clear IP:port name-exists|create-name|delete-name fabric:/name [--timeout-seconds N]\n       onebox --clear IP:port node-health NODE [--events-filter U32] [--warning-as-error true|false] [--timeout-seconds N]\n       onebox --clear IP:port application-list [--name URI|--type NAME|--definition-kind U32] [--exclude-parameters true|false] [--max-results N] [--continuation-token TOKEN] [--timeout-seconds N]\n       onebox --clear IP:port service-list APP_URI [--name URI|--type NAME] [--max-results N] [--continuation-token TOKEN] [--timeout-seconds N]";

#[derive(Debug)]
enum Operation {
    Ping,
    NodeList,
    NameExists(NamingUri),
    CreateName(NamingUri),
    DeleteName(NamingUri),
    NodeHealth(NodeHealthQueryDescription),
    ApplicationList(ApplicationQueryDescription),
    ServiceList(ServiceQueryDescription),
}

fn list_arguments(args: &[String], application: Option<&str>) -> Result<Operation> {
    let mut query = ApplicationQueryDescription::default();
    let mut selected = false;
    let mut exclude_set = false;
    let (pairs, remainder) = args.as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(Error::InvalidInput(USAGE));
    }
    for [key, value] in pairs {
        match key.as_str() {
            "--name" | "--type" | "--definition-kind" if !selected => {
                query.parameters.filter = match key.as_str() {
                    "--name" => ApplicationListFilter::Name(value.parse()?),
                    "--type" => ApplicationListFilter::TypeName(value.clone()),
                    _ if application.is_none() => ApplicationListFilter::DefinitionKind(
                        ApplicationDefinitionKindFilter::try_from_bits(value.parse().map_err(
                            |_| Error::InvalidInput("definition filter must be UInt32"),
                        )?)?,
                    ),
                    _ => return Err(Error::InvalidInput(USAGE)),
                };
                selected = true;
            }
            "--exclude-parameters" if application.is_none() && !exclude_set => {
                query.parameters.exclude_application_parameters = value
                    .parse()
                    .map_err(|_| Error::InvalidInput("exclude-parameters must be true or false"))?;
                exclude_set = true;
            }
            "--continuation-token" if query.paging.continuation_token.is_none() => {
                query.paging.continuation_token = Some(value.clone());
            }
            "--max-results" if query.paging.max_results.is_none() => {
                let count = value
                    .parse::<u64>()
                    .ok()
                    .and_then(NonZeroU64::new)
                    .filter(|v| v.get() <= i64::MAX as u64)
                    .ok_or(Error::InvalidInput("max-results must be 1..i64::MAX"))?;
                query.paging.max_results = Some(count);
            }
            _ => return Err(Error::InvalidInput(USAGE)),
        }
    }
    if let Some(application) = application {
        let mut service =
            ServiceQueryDescription::new(ServiceQueryParameters::new(application.parse()?));
        service.parameters.filter = match query.parameters.filter {
            ApplicationListFilter::All => ServiceListFilter::All,
            ApplicationListFilter::Name(value) => ServiceListFilter::Name(value),
            ApplicationListFilter::TypeName(value) => ServiceListFilter::TypeName(value),
            ApplicationListFilter::DefinitionKind(_) => return Err(Error::InvalidInput(USAGE)),
        };
        service.paging = query.paging;
        Ok(Operation::ServiceList(service))
    } else {
        Ok(Operation::ApplicationList(query))
    }
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
        "application-list" => list_arguments(&args[3..], None)?,
        "service-list" if args.len() >= 4 => list_arguments(&args[4..], Some(&args[3]))?,
        "name-exists" if args.len() == 4 => Operation::NameExists(args[3].parse()?),
        "create-name" if args.len() == 4 => Operation::CreateName(args[3].parse()?),
        "delete-name" if args.len() == 4 => Operation::DeleteName(args[3].parse()?),
        "node-health" if args.len() >= 4 => {
            let mut query =
                NodeHealthQueryDescription::new(NodeHealthQueryParameters::new(&args[3]));
            let (options, remainder) = args[4..].as_chunks::<2>();
            for pair in options {
                match pair[0].as_str() {
                    "--events-filter" if query.parameters.events_filter.is_none() => {
                        query.parameters.events_filter =
                            Some(HealthEventsFilter::from_bits_retain(
                                pair[1].parse().map_err(|_| {
                                    Error::InvalidInput(
                                        "events filter must be a decimal UInt32 mask",
                                    )
                                })?,
                            ));
                    }
                    "--warning-as-error" if query.parameters.health_policy.is_none() => {
                        query.parameters.health_policy = Some(NodeHealthPolicy {
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
            for node in page.items {
                println!("{node:?}");
            }
            println!("continuation_token={:?}", page.continuation_token);
        }
        Operation::NodeHealth(query) => {
            println!("{:#?}", client.get_node_health(&query).await?);
        }
        Operation::ApplicationList(query) => {
            println!(
                "{:#?}",
                client
                    .get_application_list_with_timeout(&query, timeout)
                    .await?
            );
        }
        Operation::ServiceList(query) => {
            println!(
                "{:#?}",
                client
                    .get_service_list_with_timeout(&query, timeout)
                    .await?
            );
        }
        Operation::NameExists(name) => {
            println!("name={name} exists={}", client.name_exists(&name).await?);
        }
        Operation::CreateName(name) => {
            // Sent writes can execute despite timeout/cancel/disconnect. Never replay.
            client.create_name_with_timeout(&name, timeout).await?;
            println!("created name={name}");
        }
        Operation::DeleteName(name) => {
            client.delete_name_with_timeout(&name, timeout).await?;
            println!("deleted name={name}");
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
        assert_eq!(query.parameters.node_name, "N0020");
        assert_eq!(query.parameters.health_policy, None);
        assert_eq!(query.parameters.events_filter, None);
        let (_, Operation::NodeHealth(query), timeout) = parse(
            "node-health N0020 --warning-as-error false --events-filter 0 --timeout-seconds 5",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(
            query.parameters.health_policy,
            Some(NodeHealthPolicy::default())
        );
        assert_eq!(
            query.parameters.events_filter,
            Some(HealthEventsFilter::DEFAULT)
        );
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
            "create-name",
            "delete-name",
            "create-name fabric:/owned extra",
            "delete-name fabric:/owned --recursive",
            "create-name fabric:/owned --timeout-seconds 0",
            "delete-name fabric:/owned --timeout-seconds NaN",
            "create-name fabric:/owned --timeout-seconds 1 --timeout-seconds 2",
            "name-exists fabric:/",
            "create-name fabric:/bad//name",
            "delete-name fabric:/bad/",
        ] {
            assert!(parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn name_writes_preserve_explicit_name_and_timeout() {
        let (endpoint, operation, timeout) =
            parse("create-name fabric:/owned --timeout-seconds 0.5").unwrap();
        assert_eq!(endpoint, "127.0.0.1:19000".parse().unwrap());
        assert!(
            matches!(operation, Operation::CreateName(name) if name.as_str() == "fabric:/owned")
        );
        assert_eq!(timeout, Duration::from_millis(500));
        let (_, operation, timeout) = parse("delete-name fabric:/owned").unwrap();
        assert!(
            matches!(operation, Operation::DeleteName(name) if name.as_str() == "fabric:/owned")
        );
        assert_eq!(timeout, DEFAULT_TIMEOUT);
    }

    #[test]
    fn list_options_preserve_explicit_filters_and_one_page_controls() {
        let (_, Operation::ApplicationList(query), _) = parse("application-list --type MyType --exclude-parameters true --max-results 1 --continuation-token opaque").unwrap() else { panic!() };
        assert_eq!(
            query.parameters.filter,
            ApplicationListFilter::TypeName("MyType".into())
        );
        assert!(query.parameters.exclude_application_parameters);
        assert_eq!(query.paging.max_results, NonZeroU64::new(1));
        assert_eq!(query.paging.continuation_token.as_deref(), Some("opaque"));
        let (_, Operation::ServiceList(query), _) =
            parse("service-list fabric:/App --name fabric:/App/Svc").unwrap()
        else {
            panic!()
        };
        assert_eq!(query.parameters.application_name.as_str(), "fabric:/App");
        assert_eq!(
            query.parameters.filter,
            ServiceListFilter::Name("fabric:/App/Svc".parse().unwrap())
        );
        for input in [
            "service-list",
            "application-list --max-results 0",
            "application-list --max-results 9223372036854775808",
            "application-list --name fabric:/App --type T",
            "application-list --definition-kind 8",
            "application-list --exclude-parameters yes",
            "application-list --max-results 1 --max-results 2",
            "application-list --type",
            "service-list fabric:/App --definition-kind 1",
            "service-list fabric:/App --exclude-parameters true",
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
