//! Caller-owned probes for this fixture only; not general host admission.
use mssf_test_host::{run_manifest, HostError, LifecycleStage, ManifestOptions, Result, Scenario};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    time::Duration,
};

fn require(value: bool, message: &str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(HostError(message.into()))
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    require(
        args.len() == 6 || args.len() == 7,
        "usage: manifest_acceptance NATIVE DEPS PACKAGE RUN_PARENT close|abort [opaque]",
    )?;
    let scenario = match args[5].as_str() {
        "close" => Scenario::Close,
        "abort" => Scenario::Abort,
        _ => return Err(HostError("expected close or abort".into())),
    };
    let opaque = args.get(6).map(|s| s == "opaque").unwrap_or(false);
    require(args.len() != 7 || opaque, "unknown acceptance mode")?;
    let mut parameters = Vec::new();
    let (service, service_type) = if scenario == Scenario::Abort {
        parameters.extend([
            ("Type".into(), "OverriddenEchoType".into()),
            ("Service".into(), "OverriddenEcho".into()),
            ("Instances".into(), "1".into()),
        ]);
        ("OverriddenEcho", "OverriddenEchoType")
    } else {
        ("DefaultEcho", "ManifestEchoType")
    };
    if scenario == Scenario::Abort || opaque {
        parameters.push(("Arguments".into(), format!(
            "--manifest-fixture --type {service_type} --service fabric:/ManifestQualification/{service} --terminal {} --endpoint {}",
            args[5], if opaque { "opaque" } else { "socket" })));
    }
    let mut observed = Vec::new();
    let report = run_manifest(
        ManifestOptions {
            native_directory: PathBuf::from(&args[1]),
            dependency_directory: PathBuf::from(&args[2]),
            package_directory: PathBuf::from(&args[3]),
            run_parent: PathBuf::from(&args[4]),
            application_name: "fabric:/ManifestQualification".into(),
            parameters,
        },
        scenario,
        |event| {
            // The API also checks liveness on both sides of every observation.
            require(
                PathBuf::from(format!("/proc/{}", event.pid)).is_dir(),
                "child must remain alive during probes",
            )?;
            if opaque {
                require(
                    event.endpoint == "opaque:manifest-fixture",
                    "opaque endpoint changed",
                )?;
            } else {
                let endpoint: SocketAddr = event
                    .endpoint
                    .parse()
                    .map_err(|e| HostError(format!("{e}")))?;
                require(
                    endpoint.ip().is_loopback() && endpoint.port() != 0,
                    "fixture returned invalid socket",
                )?;
                if event.stage == LifecycleStage::Opened {
                    let mut stream = TcpStream::connect_timeout(&endpoint, Duration::from_secs(1))?;
                    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                    stream.write_all(b"ping\n")?;
                    let mut response = [0; 22];
                    stream.read_exact(&mut response)?;
                    require(
                        &response == b"native-stateless-pong\n",
                        "independent fixture ping failed",
                    )?;
                } else {
                    match TcpStream::connect_timeout(&endpoint, Duration::from_secs(1)) {
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => (),
                        _ => {
                            return Err(HostError(
                                "endpoint refusal not proven before cleanup".into(),
                            ))
                        }
                    }
                }
            }
            println!(
                "MSSF_PROBE {}",
                serde_json::json!({"stage":format!("{:?}", event.stage),
            "endpoint":event.endpoint, "process_still_alive":true, "opaque":opaque})
            );
            observed.push(event.stage);
            Ok(())
        },
    )?;
    require(
        observed
            == [
                LifecycleStage::Opened,
                if scenario == Scenario::Abort {
                    LifecycleStage::Aborted
                } else {
                    LifecycleStage::Closed
                },
            ],
        "wrong observer ordering",
    )?;
    require(
        report.service_name == format!("fabric:/ManifestQualification/{service}")
            && report.service_type == service_type,
        "manifest identity report mismatch",
    )?;
    require(
        report.callbacks.is_empty(),
        "manifest acceptance must not use MSSF_APP markers",
    )?;
    require(
        report.lifecycle_completed
            && !report.runtime_unregistered
            && report.cleanup["forced"] == true
            && report.cleanup["reaped"] == true,
        "semantic completion must be separate from forced cleanup",
    )?;
    println!("MSSF_REPORT {}", serde_json::to_string(&report).unwrap());
    Ok(())
}
