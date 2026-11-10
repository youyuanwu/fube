use std::{net::SocketAddr, num::NonZeroU64, time::Duration};

use clap::{Args, CommandFactory, Parser, Subcommand};
use fube_client::{
    ApplicationDefinitionKindFilter, ApplicationListFilter, ApplicationQueryDescription,
    ClientOptions, FabricClient, HealthEventsFilter, NamingUri, NodeHealthPolicy,
    NodeHealthQueryDescription, NodeHealthQueryParameters, NodeQueryDescription, Paging,
    ServiceListFilter, ServiceQueryDescription, ServiceQueryParameters, DEFAULT_TIMEOUT,
};

#[derive(Parser)]
#[command(about = "Query or update an unsecured native Service Fabric gateway")]
struct Arguments {
    #[arg(long, value_name = "IP:PORT")]
    clear: SocketAddr,
    #[command(flatten)]
    timeout: TimeoutArguments,
    #[command(subcommand)]
    command: Command,
}

impl Arguments {
    fn validate(self) -> Result<Self, clap::Error> {
        // A global clap option would silently override a value across subcommands.
        if self.timeout.timeout_seconds.is_some() && self.command.timeout().is_some() {
            return Err(Self::command().error(
                clap::error::ErrorKind::ArgumentConflict,
                "--timeout-seconds cannot be used more than once",
            ));
        }
        Ok(self)
    }

    fn timeout(&self) -> Duration {
        self.timeout
            .timeout_seconds
            .or(self.command.timeout())
            .unwrap_or(DEFAULT_TIMEOUT)
    }
}

#[derive(Args)]
struct TimeoutArguments {
    #[arg(long, value_parser = timeout)]
    timeout_seconds: Option<Duration>,
}

#[derive(Subcommand)]
enum Command {
    Ping(TimeoutArguments),
    NodeList(TimeoutArguments),
    NameExists {
        name: NamingUri,
        #[command(flatten)]
        timeout: TimeoutArguments,
    },
    CreateName {
        name: NamingUri,
        #[command(flatten)]
        timeout: TimeoutArguments,
    },
    DeleteName {
        name: NamingUri,
        #[command(flatten)]
        timeout: TimeoutArguments,
    },
    NodeHealth {
        node: String,
        #[arg(long)]
        events_filter: Option<u32>,
        #[arg(long, action = clap::ArgAction::Set)]
        warning_as_error: Option<bool>,
        #[command(flatten)]
        timeout: TimeoutArguments,
    },
    ApplicationList(ApplicationListArguments),
    ServiceList(ServiceListArguments),
}

impl Command {
    fn timeout(&self) -> Option<Duration> {
        match self {
            Self::Ping(timeout)
            | Self::NodeList(timeout)
            | Self::NameExists { timeout, .. }
            | Self::CreateName { timeout, .. }
            | Self::DeleteName { timeout, .. }
            | Self::NodeHealth { timeout, .. } => timeout.timeout_seconds,
            Self::ApplicationList(args) => args.timeout.timeout_seconds,
            Self::ServiceList(args) => args.timeout.timeout_seconds,
        }
    }
}

#[derive(Args)]
#[group(id = "application_filter", multiple = false)]
struct ApplicationFilter {
    #[arg(long)]
    name: Option<NamingUri>,
    #[arg(long = "type")]
    type_name: Option<String>,
    #[arg(long, value_parser = definition_kind)]
    definition_kind: Option<ApplicationDefinitionKindFilter>,
}

#[derive(Args)]
struct ApplicationListArguments {
    #[command(flatten)]
    filter: ApplicationFilter,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
    exclude_parameters: bool,
    #[command(flatten)]
    paging: PagingArguments,
    #[command(flatten)]
    timeout: TimeoutArguments,
}

#[derive(Args)]
#[group(id = "service_filter", multiple = false)]
struct ServiceFilter {
    #[arg(long)]
    name: Option<NamingUri>,
    #[arg(long = "type")]
    type_name: Option<String>,
}

#[derive(Args)]
struct ServiceListArguments {
    application: NamingUri,
    #[command(flatten)]
    filter: ServiceFilter,
    #[command(flatten)]
    paging: PagingArguments,
    #[command(flatten)]
    timeout: TimeoutArguments,
}

#[derive(Args)]
struct PagingArguments {
    #[arg(long)]
    continuation_token: Option<String>,
    #[arg(long, value_parser = max_results)]
    max_results: Option<NonZeroU64>,
}

impl From<PagingArguments> for Paging {
    fn from(value: PagingArguments) -> Self {
        Self {
            continuation_token: value.continuation_token,
            max_results: value.max_results,
        }
    }
}

fn timeout(value: &str) -> Result<Duration, String> {
    let seconds = value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or("timeout must be positive finite seconds")?;
    let timeout = Duration::try_from_secs_f64(seconds).map_err(|_| "timeout is out of range")?;
    fube_transport::connection::deadline_after(timeout).map_err(|error| error.to_string())?;
    Ok(timeout)
}

fn max_results(value: &str) -> Result<NonZeroU64, String> {
    value
        .parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .filter(|v| v.get() <= i64::MAX as u64)
        .ok_or_else(|| "max-results must be 1..i64::MAX".into())
}

fn definition_kind(value: &str) -> Result<ApplicationDefinitionKindFilter, String> {
    let bits = value
        .parse()
        .map_err(|_| "definition filter must be UInt32")?;
    ApplicationDefinitionKindFilter::try_from_bits(bits).map_err(|error| error.to_string())
}

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

impl From<Command> for Operation {
    fn from(command: Command) -> Self {
        match command {
            Command::Ping(_) => Self::Ping,
            Command::NodeList(_) => Self::NodeList,
            Command::NameExists { name, .. } => Self::NameExists(name),
            Command::CreateName { name, .. } => Self::CreateName(name),
            Command::DeleteName { name, .. } => Self::DeleteName(name),
            Command::NodeHealth {
                node,
                events_filter,
                warning_as_error,
                ..
            } => {
                let mut query =
                    NodeHealthQueryDescription::new(NodeHealthQueryParameters::new(node));
                query.parameters.events_filter =
                    events_filter.map(|bits| HealthEventsFilter::from_bits_retain(bits).into());
                query.parameters.health_policy =
                    warning_as_error.map(|consider_warning_as_error| NodeHealthPolicy {
                        consider_warning_as_error,
                    });
                Self::NodeHealth(query)
            }
            Command::ApplicationList(args) => {
                let mut query = ApplicationQueryDescription::default();
                query.parameters.filter = if let Some(name) = args.filter.name {
                    ApplicationListFilter::Name(name)
                } else if let Some(name) = args.filter.type_name {
                    ApplicationListFilter::TypeName(name)
                } else if let Some(kind) = args.filter.definition_kind {
                    ApplicationListFilter::DefinitionKind(kind)
                } else {
                    ApplicationListFilter::All
                };
                query.parameters.exclude_application_parameters = args.exclude_parameters;
                query.paging = args.paging.into();
                Self::ApplicationList(query)
            }
            Command::ServiceList(args) => {
                let mut query =
                    ServiceQueryDescription::new(ServiceQueryParameters::new(args.application));
                query.parameters.filter = if let Some(name) = args.filter.name {
                    ServiceListFilter::Name(name)
                } else if let Some(name) = args.filter.type_name {
                    ServiceListFilter::TypeName(name)
                } else {
                    ServiceListFilter::All
                };
                query.paging = args.paging.into();
                Self::ServiceList(query)
            }
        }
    }
}

async fn run(arguments: Arguments) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let timeout = arguments.timeout();
    let operation = Operation::from(arguments.command);
    let endpoint = FabricClient::endpoint_clear(arguments.clear)?.connect_timeout(timeout);
    let channel = endpoint.connect().await?;
    let client = FabricClient::new(
        channel,
        ClientOptions {
            default_timeout: timeout,
            ..ClientOptions::default()
        },
    )?;
    match operation {
        Operation::Ping => {
            // Explicit stub clones share one channel and typed admission budget.
            let cloned = client.clone();
            let (first, second) = tokio::try_join!(client.ping(None), cloned.ping(None))?;
            for gateway in [first, second] {
                println!(
                    "gateway={} node_name={} node_id={:x} node_instance={}",
                    gateway.address, gateway.node_name, gateway.node_id, gateway.node_instance
                );
            }
        }
        Operation::NodeList => {
            // Explicit single page: never imply this is the complete membership.
            let page = client
                .get_node_list(&NodeQueryDescription::default(), None)
                .await?;
            for node in page.items {
                println!("{node:?}");
            }
            println!("continuation_token={:?}", page.continuation_token);
        }
        Operation::NodeHealth(query) => {
            println!("{:#?}", client.get_node_health(&query, None).await?);
        }
        Operation::ApplicationList(query) => {
            println!(
                "{:#?}",
                client.get_application_list(&query, Some(timeout)).await?
            );
        }
        Operation::ServiceList(query) => {
            println!(
                "{:#?}",
                client.get_service_list(&query, Some(timeout)).await?
            );
        }
        Operation::NameExists(name) => {
            println!(
                "name={name} exists={}",
                client.name_exists(&name, None).await?
            );
        }
        Operation::CreateName(name) => {
            // Sent writes can execute despite timeout/cancel/disconnect. Never replay.
            client.create_name(&name, Some(timeout)).await?;
            println!("created name={name}");
        }
        Operation::DeleteName(name) => {
            client.delete_name(&name, Some(timeout)).await?;
            println!("deleted name={name}");
        }
    }
    client.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(suffix: &str) -> Result<(SocketAddr, Operation, Duration), clap::Error> {
        let input = format!("onebox --clear 127.0.0.1:19000 {suffix}");
        let args = Arguments::try_parse_from(input.split_whitespace())?.validate()?;
        let timeout = args.timeout();
        Ok((args.clear, args.command.into(), timeout))
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
            Some(HealthEventsFilter::DEFAULT.into())
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
            "--timeout-seconds 1 ping --timeout-seconds 2",
            "create-name",
            "delete-name",
            "create-name fabric:/owned extra",
            "delete-name fabric:/owned --recursive",
            "create-name fabric:/owned --timeout-seconds 0",
            "delete-name fabric:/owned --timeout-seconds NaN",
            "ping --timeout-seconds inf",
            "ping --timeout-seconds 1e100",
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
        let (_, operation, timeout) = parse("--timeout-seconds 0.25 ping").unwrap();
        assert!(matches!(operation, Operation::Ping));
        assert_eq!(timeout, Duration::from_millis(250));
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
    let arguments = Arguments::parse()
        .validate()
        .unwrap_or_else(|error| error.exit());
    if let Err(error) = run(arguments).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
