//! Thin native-profile command runner; scenario behavior lives in the library.
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
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
    match mssf_test_host::run(options, scenario) {
        Ok(report) => println!("MSSF_REPORT {}", serde_json::to_string(&report).unwrap()),
        Err(error) => {
            eprintln!("native scenario FAILED: {error}");
            std::process::exit(1);
        }
    }
}
