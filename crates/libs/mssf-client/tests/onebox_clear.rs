use std::{net::SocketAddr, path::PathBuf};

use mssf_client::{FabricClient, DEFAULT_TIMEOUT};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    profile: String,
    gateway: String,
    runtime_version: String,
    existing_name: String,
    absent_name: String,
    expected_node_name: String,
    expected_node_id: String,
    expected_node_instance: u64,
    report_path: PathBuf,
}

impl Config {
    fn validate(&self) -> Result<(SocketAddr, u128), &'static str> {
        if self.profile != "clear" {
            return Err("profile must be explicitly clear");
        }
        let gateway = self
            .gateway
            .parse::<SocketAddr>()
            .map_err(|_| "gateway must be IP:port")?;
        if gateway.port() == 0 {
            return Err("gateway port must be nonzero");
        }
        if self.runtime_version.trim().is_empty() || self.expected_node_name.trim().is_empty() {
            return Err("independently observed runtime and node name must be nonempty");
        }
        let valid_name = |name: &str| {
            name.len() <= 4096
                && name.strip_prefix("fabric:/").is_some_and(|suffix| {
                    suffix.split('/').all(|part| {
                        !part.is_empty()
                            && part.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
                            })
                    })
                })
        };
        if !valid_name(&self.existing_name)
            || !valid_name(&self.absent_name)
            || self.existing_name == self.absent_name
        {
            return Err("distinct supported existing and absent fixture names are required");
        }
        let node_id = u128::from_str_radix(&self.expected_node_id, 16)
            .map_err(|_| "expected_node_id must be native lowercase hexadecimal")?;
        if format!("{node_id:x}") != self.expected_node_id {
            return Err("expected_node_id must be canonical native lowercase hexadecimal");
        }
        if !self.report_path.is_absolute() || self.report_path.file_name().is_none() {
            return Err("report_path must be an absolute JSON record file path");
        }
        Ok((gateway, node_id))
    }
}

/// This is deliberately not part of ordinary offline tests. Missing config or
/// fixtures is an error, never a successful skip.
#[tokio::test]
#[ignore = "requires an independently prepared clear onebox and MSSF_TEST_CONFIG"]
async fn onebox_ping_and_name_exists() {
    let path = PathBuf::from(
        std::env::var_os("MSSF_TEST_CONFIG")
            .expect("MSSF_TEST_CONFIG must point to the private JSON fixture configuration"),
    );
    assert!(
        path.is_absolute(),
        "MSSF_TEST_CONFIG must be absolute; Cargo runs tests from the package directory"
    );
    let bytes = std::fs::read(path).expect("read MSSF_TEST_CONFIG");
    let config: Config = serde_json::from_slice(&bytes).expect("parse exact onebox configuration");
    let (endpoint, node_id) = config
        .validate()
        .expect("validate independent onebox fixtures");
    let mut client = FabricClient::connect_clear(endpoint, DEFAULT_TIMEOUT)
        .await
        .expect("connect explicitly to native clear gateway");
    let gateway = client.ping().await.expect("native correlated ping");
    assert!(
        !gateway.address.is_empty(),
        "native gateway address is missing"
    );
    assert_eq!(gateway.node_name, config.expected_node_name);
    assert_eq!(gateway.node_id, node_id);
    assert_eq!(gateway.node_instance, config.expected_node_instance);
    assert!(client
        .name_exists(&config.existing_name)
        .await
        .expect("existing native name"));
    assert!(!client
        .name_exists(&config.absent_name)
        .await
        .expect("absent native name"));
    client.close().await.expect("close native connection");
    assert!(client.is_closed());

    let record = serde_json::json!({
        "runtime_version": config.runtime_version,
        "profile": config.profile,
        "endpoint": endpoint.to_string(),
        "gateway": {
            "address": gateway.address,
            "node_name": gateway.node_name,
            "node_id": format!("{:x}", gateway.node_id),
            "node_instance": gateway.node_instance
        },
        "executed_cases": [
            {"case": "native_ping", "passed": true},
            {"case": "native_name_exists_existing", "result": true, "passed": true},
            {"case": "native_name_exists_absent", "result": false, "passed": true},
            {"case": "explicit_close", "passed": true}
        ]
    });
    std::fs::write(
        &config.report_path,
        serde_json::to_vec_pretty(&record).expect("serialize sanitized compatibility record"),
    )
    .expect("write sanitized compatibility record after all native cases passed");
}

#[cfg(test)]
fn sample_config() -> serde_json::Value {
    serde_json::json!({
        "profile": "clear",
        "gateway": "127.0.0.1:19000",
        "runtime_version": "independently-observed-version",
        "existing_name": "fabric:/poc/existing",
        "absent_name": "fabric:/poc/absent",
        "expected_node_name": "N0010",
        "expected_node_id": "abcdef0123456789",
        "expected_node_instance": 123456789,
        "report_path": std::env::temp_dir().join("mssf-poc-evidence.json")
    })
}

#[test]
fn private_config_requires_exact_fields_and_independent_fixtures() {
    let good = sample_config();
    assert!(serde_json::from_value::<Config>(good.clone())
        .unwrap()
        .validate()
        .is_ok());
    let mut unknown = good.clone();
    unknown["secret"] = "not-allowed".into();
    assert!(serde_json::from_value::<Config>(unknown).is_err());
    for field in good.as_object().unwrap().keys() {
        let mut missing = good.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<Config>(missing).is_err(),
            "{field}"
        );
    }
    for (field, value) in [
        ("profile", "tls"),
        ("gateway", "localhost:19000"),
        ("gateway", "127.0.0.1:0"),
        ("runtime_version", " "),
        ("existing_name", "fabric:/"),
        ("absent_name", "fabric:/a?b"),
        ("absent_name", "fabric:/poc/existing"),
        ("expected_node_name", ""),
        ("expected_node_id", "ABCDEF"),
        ("expected_node_id", "0xabc"),
        ("expected_node_id", "00abc"),
        ("report_path", ""),
        ("report_path", "relative-report.json"),
    ] {
        let mut bad = good.clone();
        bad[field] = value.into();
        assert!(
            serde_json::from_value::<Config>(bad)
                .unwrap()
                .validate()
                .is_err(),
            "{field}={value:?}"
        );
    }
}
