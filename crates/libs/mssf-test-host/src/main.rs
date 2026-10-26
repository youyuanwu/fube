//! Thin native-profile command runner; scenario behavior lives in the library.
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|v| v == "manifest") {
        if args.len() < 8 {
            eprintln!("usage: mssf-test-host manifest NATIVE_DIRECTORY DEPENDENCY_DIRECTORY PACKAGE_DIRECTORY RUN_PARENT close|abort APPLICATION_URI [NAME=VALUE ...]");
            std::process::exit(2);
        }
        let parameters: Result<Vec<_>, _> = args[8..]
            .iter()
            .map(|arg| {
                arg.split_once('=')
                    .map(|(name, value)| (name.to_owned(), value.to_owned()))
                    .ok_or_else(|| format!("override must be NAME=VALUE: {arg}"))
            })
            .collect();
        let parameters = parameters.unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(2);
        });
        let scenario = match args[6].as_str() {
            "close" => mssf_test_host::Scenario::Close,
            "abort" => mssf_test_host::Scenario::Abort,
            _ => {
                eprintln!("manifest scenario must be close or abort");
                std::process::exit(2);
            }
        };
        report(mssf_test_host::run_manifest(
            mssf_test_host::ManifestOptions {
                native_directory: PathBuf::from(&args[2]),
                dependency_directory: PathBuf::from(&args[3]),
                package_directory: PathBuf::from(&args[4]),
                run_parent: PathBuf::from(&args[5]),
                application_name: args[7].clone(),
                parameters,
            },
            scenario,
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
    if args.len() != 6 {
        eprintln!(
            "usage: mssf-test-host NATIVE_DIRECTORY DEPENDENCY_DIRECTORY APP_EXECUTABLE RUN_PARENT bootstrap|close|abort"
        );
        std::process::exit(2);
    }
    let options = mssf_test_host::Options {
        native_directory: PathBuf::from(&args[1]),
        dependency_directory: PathBuf::from(&args[2]),
        executable: PathBuf::from(&args[3]),
        run_parent: PathBuf::from(&args[4]),
    };
    let scenario = match args[5].as_str() {
        "bootstrap" => mssf_test_host::Scenario::Bootstrap,
        "close" => mssf_test_host::Scenario::Close,
        "abort" => mssf_test_host::Scenario::Abort,
        _ => {
            eprintln!("unsupported scenario");
            std::process::exit(2);
        }
    };
    report(mssf_test_host::run(options, scenario));
}

fn report(result: mssf_test_host::Result<mssf_test_host::RunReport>) {
    match result {
        Ok(report) => println!("MSSF_REPORT {}", serde_json::to_string(&report).unwrap()),
        Err(error) => {
            eprintln!("native scenario FAILED: {error}");
            std::process::exit(1);
        }
    }
}
