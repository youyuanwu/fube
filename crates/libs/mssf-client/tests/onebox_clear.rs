use std::{
    future::{poll_fn, Future},
    net::SocketAddr,
    path::PathBuf,
    task::Poll,
    time::Duration,
};

use mssf_client::{FabricClient, GatewayDescription, DEFAULT_TIMEOUT};
use serde::Deserialize;
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
    time::{sleep, timeout},
};

const CONCURRENT_OPERATIONS: usize = 32;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

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

fn assert_independent_gateway(gateway: &GatewayDescription, config: &Config, node_id: u128) {
    assert!(
        !gateway.address.is_empty(),
        "native gateway address is missing"
    );
    assert_eq!(gateway.node_name, config.expected_node_name);
    assert_eq!(gateway.node_id, node_id);
    assert_eq!(gateway.node_instance, config.expected_node_instance);
}

async fn assert_idle_proxy_disconnect(
    endpoint: SocketAddr,
    baseline: &GatewayDescription,
    config: &Config,
    node_id: u128,
) {
    let listener = timeout(SHUTDOWN_TIMEOUT, TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("bound proxy listener setup")
        .expect("bind test-owned ephemeral proxy");
    let proxy_endpoint = listener.local_addr().expect("proxy listener address");
    let (disconnect, disconnected) = oneshot::channel();
    let mut proxy_tasks = JoinSet::new();
    proxy_tasks.spawn(async move {
        timeout(DEFAULT_TIMEOUT * 3, async move {
            let (mut downstream, _) = listener.accept().await.expect("accept proxy client");
            drop(listener);
            let mut upstream = TcpStream::connect(endpoint)
                .await
                .expect("connect proxy to configured native gateway");
            tokio::select! {
                signal = disconnected => {
                    signal.expect("explicit idle proxy disconnect signal");
                }
                result = copy_bidirectional(&mut downstream, &mut upstream) => {
                    panic!("proxy forwarding ended before explicit disconnect: {result:?}");
                }
            }
            drop(downstream);
            drop(upstream);
        })
        .await
        .expect("bound entire proxy forwarding lifetime");
    });

    let client = timeout(
        DEFAULT_TIMEOUT + SHUTDOWN_TIMEOUT,
        FabricClient::connect_clear(proxy_endpoint, DEFAULT_TIMEOUT),
    )
    .await
    .expect("bound native connection through proxy")
    .expect("connect through test-owned proxy");
    let gateway = timeout(DEFAULT_TIMEOUT + SHUTDOWN_TIMEOUT, client.ping())
        .await
        .expect("bound real native ping through proxy")
        .expect("real native ping through proxy");
    assert_independent_gateway(&gateway, config, node_id);
    assert_eq!(&gateway, baseline, "full native gateway identity via proxy");
    assert!(
        !client.is_closed(),
        "proxy client must be live before cutoff"
    );
    assert!(client.terminal_error().is_none());

    disconnect.send(()).expect("signal idle proxy cutoff");
    timeout(SHUTDOWN_TIMEOUT, proxy_tasks.join_next())
        .await
        .expect("bound proxy shutdown")
        .expect("proxy task exists")
        .expect("proxy task completed without error");
    assert!(proxy_tasks.is_empty());

    // Only inspect state after cutoff: another operation must not drive EOF.
    timeout(SHUTDOWN_TIMEOUT, async {
        while !client.is_closed() || client.terminal_error().is_none() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("observe idle closure and terminal error without sending an operation");
    assert!(client.is_closed());
    assert!(client.terminal_error().is_some());
    timeout(SHUTDOWN_TIMEOUT, client.close())
        .await
        .expect("bound disconnected client driver join")
        .expect("close and join disconnected proxy client");
}

/// This is deliberately not part of ordinary offline tests. Missing config or
/// fixtures is an error, never a successful skip.
/// Mixed operations are initially polled together on the current-thread runtime,
/// proving overlapping lifetimes before yielding to the connection driver.
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
    let client = FabricClient::connect_clear(endpoint, DEFAULT_TIMEOUT)
        .await
        .expect("connect explicitly to native clear gateway");
    let gateway = client.ping().await.expect("native correlated ping");
    assert_independent_gateway(&gateway, &config, node_id);
    assert!(client
        .name_exists(&config.existing_name)
        .await
        .expect("existing native name"));
    assert!(!client
        .name_exists(&config.absent_name)
        .await
        .expect("absent native name"));
    let mut pending_operations = Vec::with_capacity(CONCURRENT_OPERATIONS);
    for index in 0..CONCURRENT_OPERATIONS {
        let handle = client.clone();
        let existing_name = config.existing_name.clone();
        let absent_name = config.absent_name.clone();
        pending_operations.push(Box::pin(async move {
            match index % 4 {
                0 | 1 => Some(handle.ping().await.expect("concurrent native ping")),
                2 => {
                    assert!(handle
                        .name_exists(&existing_name)
                        .await
                        .expect("concurrent existing native name"));
                    None
                }
                _ => {
                    assert!(!handle
                        .name_exists(&absent_name)
                        .await
                        .expect("concurrent absent native name"));
                    None
                }
            }
        }));
    }
    let initially_pending_count = poll_fn(|context| {
        let mut pending_count = 0;
        for operation in &mut pending_operations {
            assert!(
                operation.as_mut().poll(context).is_pending(),
                "every native operation must start without completing before the driver runs"
            );
            pending_count += 1;
        }
        Poll::Ready(pending_count)
    })
    .await;
    assert_eq!(initially_pending_count, CONCURRENT_OPERATIONS);
    assert!(initially_pending_count >= 16);
    let mut operations = JoinSet::new();
    for operation in pending_operations {
        operations.spawn(operation);
    }
    let mut concurrent_completed = 0;
    timeout(DEFAULT_TIMEOUT + SHUTDOWN_TIMEOUT, async {
        while let Some(result) = operations.join_next().await {
            if let Some(actual) = result.expect("concurrent native operation task") {
                assert_independent_gateway(&actual, &config, node_id);
                assert_eq!(
                    actual, gateway,
                    "complete concurrent native gateway identity"
                );
            }
            concurrent_completed += 1;
        }
    })
    .await
    .expect("bound all overlapping native operations and task joins");
    assert_eq!(concurrent_completed, CONCURRENT_OPERATIONS);
    assert!(operations.is_empty());

    drop(client.clone());
    assert!(
        !client.is_closed(),
        "dropping clones must preserve base client"
    );
    assert!(client.terminal_error().is_none());
    let after_clone_drop = client
        .ping()
        .await
        .expect("original native connection survives all operation clones being dropped");
    assert_independent_gateway(&after_clone_drop, &config, node_id);
    assert_eq!(after_clone_drop, gateway, "gateway after dropping clones");
    timeout(SHUTDOWN_TIMEOUT, client.close())
        .await
        .expect("bound native client driver join")
        .expect("close native connection");
    assert!(client.is_closed());

    assert_idle_proxy_disconnect(endpoint, &gateway, &config, node_id).await;

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
            {
                "case": "native_concurrent_mixed",
                "operation_count": concurrent_completed,
                "initially_pending_count": initially_pending_count,
                "ping_count": CONCURRENT_OPERATIONS / 2,
                "existing_name_count": CONCURRENT_OPERATIONS / 4,
                "absent_name_count": CONCURRENT_OPERATIONS / 4,
                "passed": true
            },
            {"case": "clone_drop_preserves_original_connection", "passed": true},
            {"case": "explicit_close", "passed": true},
            {
                "case": "test_owned_proxy_idle_disconnect",
                "native_ping_passed": true,
                "closed_without_operation": true,
                "terminal_error_observed": true,
                "passed": true
            }
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
