//! Thin native-profile command runner; scenario behavior lives in the library.
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    version,
    about = "Run native Hosting qualification scenarios",
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
struct Arguments {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(required = true)]
    native_directory: Option<PathBuf>,
    #[arg(required = true)]
    dependency_directory: Option<PathBuf>,
    #[arg(required = true)]
    app_executable: Option<PathBuf>,
    #[arg(required = true)]
    run_parent: Option<PathBuf>,
    #[arg(required = true, value_enum)]
    scenario: Option<Scenario>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Scenario {
    Bootstrap,
    Close,
    Abort,
    HostingLeases,
    HostingRegistration,
}

#[derive(Clone, Copy, ValueEnum)]
enum Terminal {
    Close,
    Abort,
}

impl From<Terminal> for fube_test_host::Scenario {
    fn from(value: Terminal) -> Self {
        match value {
            Terminal::Close => Self::Close,
            Terminal::Abort => Self::Abort,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Activate a manifest package and close or abort its service.
    Manifest {
        native_directory: PathBuf,
        dependency_directory: PathBuf,
        package_directory: PathBuf,
        run_parent: PathBuf,
        #[arg(value_enum)]
        scenario: Terminal,
        application_uri: String,
        #[arg(value_name = "NAME=VALUE", value_parser = parameter)]
        parameters: Vec<(String, String)>,
    },
}

fn parameter(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .ok_or_else(|| format!("override must be NAME=VALUE: {value}"))
}

fn main() {
    let args = Arguments::parse();
    if let Some(Command::Manifest {
        native_directory,
        dependency_directory,
        package_directory,
        run_parent,
        scenario,
        application_uri,
        parameters,
    }) = args.command
    {
        report(fube_test_host::run_manifest(
            fube_test_host::ManifestOptions {
                native_directory,
                dependency_directory,
                package_directory,
                run_parent,
                application_name: application_uri,
                parameters,
            },
            scenario.into(),
            |event| {
                println!(
                    "MSSF_LIFECYCLE {}",
                    serde_json::json!({"stage":format!("{:?}", event.stage),
                "endpoint":event.endpoint, "pid":event.pid})
                );
                Ok(())
            },
        ));
        return;
    }
    let options = fube_test_host::Options {
        native_directory: args
            .native_directory
            .expect("clap requires native directory"),
        dependency_directory: args
            .dependency_directory
            .expect("clap requires dependency directory"),
        executable: args.app_executable.expect("clap requires app executable"),
        run_parent: args.run_parent.expect("clap requires run parent"),
    };
    report(match args.scenario.expect("clap requires scenario") {
        Scenario::HostingLeases => fube_test_host::run_hosting_leases(options),
        Scenario::HostingRegistration => fube_test_host::run_hosting_registration(options),
        Scenario::Bootstrap => fube_test_host::run(options, fube_test_host::Scenario::Bootstrap),
        Scenario::Close => fube_test_host::run(options, fube_test_host::Scenario::Close),
        Scenario::Abort => fube_test_host::run(options, fube_test_host::Scenario::Abort),
    });
}

fn report(result: fube_test_host::Result<fube_test_host::RunReport>) {
    match result {
        Ok(report) => println!("MSSF_REPORT {}", serde_json::to_string(&report).unwrap()),
        Err(error) => {
            eprintln!("native scenario FAILED: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_direct_scenarios_and_manifest_overrides() {
        for scenario in [
            "bootstrap",
            "close",
            "abort",
            "hosting-leases",
            "hosting-registration",
        ] {
            let args = Arguments::try_parse_from([
                "fube-test-host",
                "/native",
                "/deps",
                "/app",
                "/runs",
                scenario,
            ])
            .unwrap();
            assert!(args.command.is_none());
            assert_eq!(args.app_executable, Some(PathBuf::from("/app")));
        }
        let args = Arguments::try_parse_from([
            "fube-test-host",
            "manifest",
            "/native",
            "/deps",
            "/package",
            "/runs",
            "abort",
            "fabric:/App",
            "Arguments=a=b",
            "Empty=",
        ])
        .unwrap();
        let Some(Command::Manifest {
            parameters,
            scenario: Terminal::Abort,
            ..
        }) = args.command
        else {
            panic!("expected manifest abort");
        };
        assert_eq!(
            parameters,
            [
                ("Arguments".into(), "a=b".into()),
                ("Empty".into(), "".into())
            ]
        );
    }

    #[test]
    fn rejects_missing_invalid_and_extra_arguments() {
        for args in [
            vec![],
            vec!["/native", "/deps", "/app", "/runs", "unknown"],
            vec!["/native", "/deps", "/app", "/runs", "close", "extra"],
            vec![
                "manifest",
                "/native",
                "/deps",
                "/package",
                "/runs",
                "bootstrap",
                "fabric:/App",
            ],
            vec![
                "manifest",
                "/native",
                "/deps",
                "/package",
                "/runs",
                "close",
                "fabric:/App",
                "invalid",
            ],
        ] {
            assert!(
                Arguments::try_parse_from(std::iter::once("fube-test-host").chain(args)).is_err()
            );
        }
    }
}
