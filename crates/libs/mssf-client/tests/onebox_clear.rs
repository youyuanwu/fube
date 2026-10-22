use std::{
    future::{poll_fn, Future},
    net::SocketAddr,
    path::PathBuf,
    task::Poll,
    time::Duration,
};

use mssf_client::{
    FabricClient, GatewayDescription, HealthState, Node, NodeDeactivationIntent,
    NodeDeactivationStatus, NodeListPage, NodeQueryDescription, NodeStatus, NodeStatusFilter,
    DEFAULT_TIMEOUT,
};
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
const OBSERVATION_MAX_AGE_MS: u64 = 120_000;

mod support;
use support::ObservedNode;

#[derive(Debug, Clone, Deserialize)]
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
    observed_at_unix_ms: u64,
    expected_nodes: Vec<ObservedNode>,
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
        if self.observed_at_unix_ms == 0
            || self.expected_nodes.len() != 5
            || self
                .expected_nodes
                .iter()
                .any(|node| node.node_status != "Up")
        {
            return Err("fresh independent healthy five-node observations are required");
        }
        let names: std::collections::HashSet<_> =
            self.expected_nodes.iter().map(|node| &node.name).collect();
        let ids: std::collections::HashSet<_> =
            self.expected_nodes.iter().map(|node| &node.id.id).collect();
        if names.len() != 5 || ids.len() != 5 {
            return Err("independent node identities must be unique");
        }
        Ok((gateway, node_id))
    }
}

fn assert_fresh(config: &Config) {
    let now = support::unix_millis();
    assert!(
        now.checked_sub(config.observed_at_unix_ms)
            .is_some_and(|age| age <= OBSERVATION_MAX_AGE_MS),
        "independent observations expired or are future-dated; rerun explicit setup"
    );
}

fn assert_native_time(ticks: i64, observed: &str) {
    if observed == "0001-01-01T00:00:00.000Z" {
        assert_eq!(ticks, 0, "native default timestamp");
        return;
    }
    let date =
        time::OffsetDateTime::parse(observed, &time::format_description::well_known::Rfc3339)
            .expect("independent management timestamp");
    let expected = date.unix_timestamp_nanos() / 100 + 116_444_736_000_000_000i128;
    assert!(
        (i128::from(ticks) - expected).abs() < 10_000,
        "native ticks must match management timestamp to its millisecond precision"
    );
}

fn assert_independent_node(node: &Node, expected: &ObservedNode) {
    assert_eq!(node.name, expected.name);
    assert_eq!(node.id.to_string(), expected.id.id);
    assert_eq!(node.ip_address_or_fqdn, expected.ip_address_or_fqdn);
    assert_eq!(node.node_type, expected.node_type);
    assert_eq!(node.code_version, expected.code_version);
    assert_eq!(node.config_version, expected.config_version);
    assert_eq!(expected.node_status, "Up", "healthy qualification profile");
    assert_eq!(node.status, NodeStatus::Up);
    assert_eq!(expected.health_state, "Ok", "stable health prerequisite");
    assert_eq!(node.health_state, HealthState::Ok);
    assert_eq!(node.is_seed_node, expected.is_seed_node);
    assert_eq!(node.upgrade_domain, expected.upgrade_domain);
    assert_eq!(node.fault_domain, expected.fault_domain);
    assert_eq!(
        node.instance_id,
        expected.instance_id.parse::<u64>().unwrap()
    );
    assert_eq!(node.is_stopped, expected.is_stopped);
    for (native, observed) in [
        (node.up_time_seconds, &expected.node_up_time_in_seconds),
        (node.down_time_seconds, &expected.node_down_time_in_seconds),
    ] {
        let observed = observed.parse::<i64>().expect("management seconds");
        assert!(native >= 0 && observed >= 0);
        assert!((i128::from(native) - i128::from(observed)).abs() <= 120);
    }
    assert_native_time(node.up_at.0, &expected.node_up_at);
    assert_native_time(node.down_at.0, &expected.node_down_at);
    let deactivation = &expected.node_deactivation_info;
    assert_eq!(deactivation.node_deactivation_intent, "Invalid");
    assert_eq!(deactivation.node_deactivation_status, "None");
    assert!(deactivation.node_deactivation_task.is_empty());
    assert!(deactivation.pending_safety_checks.is_empty());
    assert_eq!(
        node.deactivation.effective_intent,
        NodeDeactivationIntent::Invalid
    );
    assert_eq!(node.deactivation.status, NodeDeactivationStatus::None);
    assert!(node.deactivation.tasks.is_empty());
    assert!(node.deactivation.pending_safety_checks.is_empty());
    // These binary fields aren't exposed by management JSON. Expectations come
    // from docker/onebox/ClusterManifest.template.xml's per-node port layout.
    let index = ["N0010", "N0020", "N0030", "N0040", "N0050"]
        .iter()
        .position(|name| *name == node.name)
        .expect("known disposable topology node");
    assert_eq!(node.cluster_connection_port, 19002 + index as u32 * 10);
    assert_eq!(node.http_gateway_port, 19080 + index as u16 * 2);
}

fn assert_membership(page: &NodeListPage, config: &Config) {
    assert!(
        page.continuation_token.is_none(),
        "small healthy membership fits one unspecified-size page"
    );
    let names: std::collections::BTreeSet<_> = page.nodes.iter().map(|n| &n.name).collect();
    let expected: std::collections::BTreeSet<_> =
        config.expected_nodes.iter().map(|n| &n.name).collect();
    assert_eq!(names, expected);
    assert_eq!(page.nodes.len(), expected.len(), "no duplicate members");
    for node in &page.nodes {
        let expected = config
            .expected_nodes
            .iter()
            .find(|n| n.name == node.name)
            .unwrap();
        assert_independent_node(node, expected);
    }
}

fn native_timing_evidence(nodes: &[Node]) -> serde_json::Value {
    nodes
        .iter()
        .map(|node| {
            serde_json::json!({
                "name": node.name, "id": node.id.to_string(),
                "up_time_seconds": node.up_time_seconds,
                "down_time_seconds": node.down_time_seconds,
                "up_at_native_ticks": node.up_at.0,
                "down_at_native_ticks": node.down_at.0,
            })
        })
        .collect()
}

async fn assert_node_queries(client: &FabricClient, config: &Config) -> serde_json::Value {
    assert_fresh(config);
    let native_observation_start_unix_ms = support::unix_millis();
    let baseline = client
        .get_node_list(&NodeQueryDescription::default())
        .await
        .expect("native GetNodeList");
    let native_observation_end_unix_ms = support::unix_millis();
    assert_membership(&baseline, config);
    for filter in [
        NodeStatusFilter::DEFAULT,
        NodeStatusFilter::ALL,
        NodeStatusFilter::UP,
    ] {
        assert_membership(
            &client
                .get_node_list(&NodeQueryDescription {
                    status_filter: filter,
                    ..Default::default()
                })
                .await
                .expect("native status selection"),
            config,
        );
    }
    let empty = client
        .get_node_list(&NodeQueryDescription {
            status_filter: NodeStatusFilter::DOWN,
            ..Default::default()
        })
        .await
        .expect("native nonmatching status selection");
    assert!(empty.nodes.is_empty() && empty.continuation_token.is_none());
    assert_membership(
        &client
            .get_node_list(&NodeQueryDescription {
                node_name: Some(String::new()),
                continuation_token: Some(String::new()),
                ..Default::default()
            })
            .await
            .expect("native empty omission semantics"),
        config,
    );
    for expected in &config.expected_nodes {
        let page = client
            .get_node_list(&NodeQueryDescription {
                node_name: Some(expected.name.clone()),
                status_filter: NodeStatusFilter::UP,
                max_results: std::num::NonZeroU64::new(1),
                ..Default::default()
            })
            .await
            .expect("native exact-name hit and full terminal page");
        assert_eq!(page.nodes.len(), 1);
        assert!(
            page.continuation_token.is_none(),
            "full terminal page needs no token"
        );
        assert_independent_node(&page.nodes[0], expected);
    }
    for absent in ["native-client-no-such-node", "n0010", "N00"] {
        let page = client
            .get_node_list(&NodeQueryDescription {
                node_name: Some(absent.into()),
                ..Default::default()
            })
            .await
            .expect("native exact-name miss");
        assert!(page.nodes.is_empty() && page.continuation_token.is_none());
    }
    let mut chains = Vec::new();
    for page_size in [1, 2] {
        let mut token = None;
        let mut all = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut page_lengths = Vec::new();
        for _ in 0..16 {
            let page = client
                .get_node_list(&NodeQueryDescription {
                    continuation_token: token.take(),
                    max_results: std::num::NonZeroU64::new(page_size),
                    ..Default::default()
                })
                .await
                .expect("native server continuation chain");
            assert!(!page.nodes.is_empty());
            assert!(page.nodes.len() <= page_size as usize);
            page_lengths.push(page.nodes.len());
            all.extend(page.nodes);
            token = page.continuation_token;
            if let Some(token) = &token {
                assert!(seen.insert(token.clone()), "continuation must progress");
            } else {
                break;
            }
        }
        assert!(token.is_none(), "bounded paging must reach terminal");
        assert!(
            !seen.is_empty(),
            "must exercise a real server-issued continuation"
        );
        assert!(
            all.windows(2).all(|pair| pair[0].id < pair[1].id),
            "strict native NodeId ordering without duplicates"
        );
        assert_membership(
            &NodeListPage {
                nodes: all,
                continuation_token: None,
            },
            config,
        );
        chains.push(serde_json::json!({"max_results": page_size, "page_lengths": page_lengths, "server_token_count": seen.len()}));
    }
    let final_id = config
        .expected_nodes
        .iter()
        .map(|node| u128::from_str_radix(&node.id.id, 16).expect("independent node ID"))
        .max()
        .unwrap();
    let exhausted = client
        .get_node_list(&NodeQueryDescription {
            continuation_token: Some(format!("{final_id:x}")),
            max_results: std::num::NonZeroU64::new(1),
            ..Default::default()
        })
        .await
        .expect("valid boundary continuation strictly after final independent ID");
    assert!(exhausted.nodes.is_empty() && exhausted.continuation_token.is_none());
    let invalid = client
        .get_node_list(&NodeQueryDescription {
            continuation_token: Some("this-is-not-a-node-id".into()),
            ..Default::default()
        })
        .await
        .expect_err("malformed continuation must be a native error, not empty success");
    let mssf_client::Error::Remote { code, message } = invalid else {
        panic!("expected native error, got {invalid:?}");
    };
    // On this runtime all invalid-token subqueries can fail, and the parallel
    // handler returns the last failure (QueryMessageHandler.cpp:212-225). Real
    // native AND independent HTTP probes observed both E_INVALIDARG and
    // FMFailoverUnitNotFound. Do not normalize either or retry the operation.
    assert!(
        [-2147024809, -2147017462].contains(&code),
        "unexpected native malformed-token error {code}: {message}"
    );
    assert!(
        !client.is_closed(),
        "complete native fault must not poison the stream"
    );
    assert_membership(
        &client
            .get_node_list(&NodeQueryDescription::default())
            .await
            .expect("reuse after native error"),
        config,
    );
    // Test-only low-level native probe records the effective success envelope,
    // not packet fixtures. Production operations above all use FabricClient.
    let deadline = mssf_transport::connection::deadline_after(DEFAULT_TIMEOUT).unwrap();
    let probe = mssf_transport::connection::Connection::connect_clear(
        config.gateway.parse().unwrap(),
        deadline,
    )
    .await
    .unwrap();
    let mut body = mssf_transport::serialization::Encoder::new();
    body.object(|encoder| {
        encoder.object(|encoder| encoder.object_array::<()>(&[], |_, _| Ok(())))?;
        encoder.string("GetNodeList")
    })
    .unwrap();
    let reply = probe
        .request(
            "QueryRequest",
            5,
            vec![
                mssf_transport::message::Header::activity(uuid::Uuid::new_v4().to_bytes_le())
                    .unwrap(),
                mssf_transport::message::Header::version(1, 1).unwrap(),
            ],
            body.finish(),
            deadline,
        )
        .await
        .expect("native success envelope observation");
    let properties = reply.properties().unwrap();
    let headers = serde_json::json!({
        "actor_present": reply.headers.iter().any(|h| h.id == mssf_transport::message::ACTOR),
        "effective_actor": properties.actor,
        "action_present": reply.headers.iter().any(|h| h.id == mssf_transport::message::ACTION),
        "effective_action": properties.action,
        "protocol_version": reply.protocol_version().unwrap(),
    });
    probe.close().await.unwrap();
    serde_json::json!({
        "native_node_count": baseline.nodes.len(), "independent_nodes": config.expected_nodes,
        "observation_unix_ms": config.observed_at_unix_ms,
        "native_observation_start_unix_ms": native_observation_start_unix_ms,
        "native_observation_end_unix_ms": native_observation_end_unix_ms,
        "native_timings": native_timing_evidence(&baseline.nodes),
        "typed_stable_fields_match": true, "timing_tolerance_seconds": 120,
        "timestamp_precision_tolerance_ticks": 10000,
        "binary_ports_match_onebox_manifest": true,
        "exact_name_hit_miss_case_prefix": true, "status_default_all_up_down": true,
        "empty_name_token_omitted": true, "paging_chains": chains,
        "valid_continuation_after_final_id_is_empty": true,
        "invalid_token_native_code": code, "connection_survives_native_error": true,
        "invalid_token_native_diagnostic": message,
        "success_headers": headers,
        "limits": "healthy five logical nodes, one host; empty deactivation only; no snapshot/partial-failure qualification"
    })
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
async fn onebox_ping_name_exists_and_node_list() {
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
    assert_fresh(&config);
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
    let node_evidence = assert_node_queries(&client, &config).await;
    let mut pending_operations = Vec::with_capacity(CONCURRENT_OPERATIONS);
    for index in 0..CONCURRENT_OPERATIONS {
        let handle = client.clone();
        let existing_name = config.existing_name.clone();
        let absent_name = config.absent_name.clone();
        let expected_config = config.clone();
        pending_operations.push(Box::pin(async move {
            match index % 5 {
                0 | 1 => Some(handle.ping().await.expect("concurrent native ping")),
                2 => {
                    assert!(handle
                        .name_exists(&existing_name)
                        .await
                        .expect("concurrent existing native name"));
                    None
                }
                3 => {
                    assert!(!handle
                        .name_exists(&absent_name)
                        .await
                        .expect("concurrent absent native name"));
                    None
                }
                _ => {
                    assert_membership(
                        &handle
                            .get_node_list(&NodeQueryDescription::default())
                            .await
                            .expect("concurrent native node list"),
                        &expected_config,
                    );
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
        "node_list": node_evidence,
        "executed_cases": [
            {"case": "native_ping", "passed": true},
            {"case": "native_name_exists_existing", "result": true, "passed": true},
            {"case": "native_name_exists_absent", "result": false, "passed": true},
            {
                "case": "native_concurrent_mixed",
                "operation_count": concurrent_completed,
                "initially_pending_count": initially_pending_count,
                "ping_count": (0..CONCURRENT_OPERATIONS).filter(|i| i % 5 <= 1).count(),
                "existing_name_count": (0..CONCURRENT_OPERATIONS).filter(|i| i % 5 == 2).count(),
                "absent_name_count": (0..CONCURRENT_OPERATIONS).filter(|i| i % 5 == 3).count(),
                "node_list_count": (0..CONCURRENT_OPERATIONS).filter(|i| i % 5 == 4).count(),
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

#[test]
fn observation_freshness_is_finite_and_future_dates_are_rejected() {
    let mut config: Config = serde_json::from_value(sample_config()).unwrap();
    assert_fresh(&config);
    config.observed_at_unix_ms = support::unix_millis() - OBSERVATION_MAX_AGE_MS - 1;
    assert!(std::panic::catch_unwind(|| assert_fresh(&config)).is_err());
    config.observed_at_unix_ms = support::unix_millis() + OBSERVATION_MAX_AGE_MS;
    assert!(std::panic::catch_unwind(|| assert_fresh(&config)).is_err());
}

#[test]
fn native_timestamp_oracle_preserves_epoch_and_millisecond_precision() {
    assert_native_time(0, "0001-01-01T00:00:00.000Z");
    assert_native_time(116_444_736_000_000_000, "1970-01-01T00:00:00.000Z");
    assert_native_time(134_371_187_016_289_999, "2026-10-22T04:58:21.628Z");
    assert!(std::panic::catch_unwind(|| {
        assert_native_time(134_371_187_016_300_000, "2026-10-22T04:58:21.628Z");
    })
    .is_err());
}

#[test]
fn evidence_retains_native_timing_values_without_precision_loss() {
    use mssf_client::{NodeDeactivation, NodeId, NodeTimestamp};
    let node = Node {
        name: "N0010".into(),
        id: NodeId { high: 1, low: 2 },
        ip_address_or_fqdn: "127.0.0.1".into(),
        node_type: "type".into(),
        code_version: "version".into(),
        config_version: "1".into(),
        status: NodeStatus::Up,
        up_time_seconds: 123,
        health_state: HealthState::Ok,
        is_seed_node: true,
        upgrade_domain: "UD0".into(),
        fault_domain: "fd:/n".into(),
        instance_id: 42,
        cluster_connection_port: 19002,
        deactivation: NodeDeactivation {
            effective_intent: NodeDeactivationIntent::Invalid,
            status: NodeDeactivationStatus::None,
            tasks: vec![],
            pending_safety_checks: vec![],
        },
        is_stopped: false,
        down_time_seconds: 456,
        up_at: NodeTimestamp(134_371_187_016_284_610),
        down_at: NodeTimestamp(0),
        http_gateway_port: 19080,
    };
    let evidence = native_timing_evidence(&[node]);
    assert_eq!(evidence[0]["name"], "N0010");
    assert_eq!(evidence[0]["id"], "10000000000000002");
    assert_eq!(evidence[0]["up_time_seconds"].as_i64(), Some(123));
    assert_eq!(evidence[0]["down_time_seconds"].as_i64(), Some(456));
    assert_eq!(
        evidence[0]["up_at_native_ticks"].as_i64(),
        Some(134_371_187_016_284_610)
    );
    assert_eq!(evidence[0]["down_at_native_ticks"].as_i64(), Some(0));
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
        "observed_at_unix_ms": support::unix_millis(),
        "expected_nodes": (0..5).map(|index| serde_json::json!({
            "Name": format!("N00{}0", index + 1),
            "IpAddressOrFQDN": "127.0.0.1", "Type": "node-type",
            "CodeVersion": "version", "ConfigVersion": "1", "NodeStatus": "Up",
            "NodeUpTimeInSeconds": "1", "HealthState": "Ok", "IsSeedNode": true,
            "UpgradeDomain": "UD0", "FaultDomain": "fd:/n",
            "Id": {"Id": format!("{:x}", index + 1)}, "InstanceId": "1",
            "NodeDeactivationInfo": {"NodeDeactivationIntent": "Invalid",
                "NodeDeactivationStatus": "None", "NodeDeactivationTask": [],
                "PendingSafetyChecks": []},
            "IsStopped": false, "NodeDownTimeInSeconds": "0",
            "NodeUpAt": "2026-10-22T00:00:00.000Z", "NodeDownAt": "0001-01-01T00:00:00.000Z"
        })).collect::<Vec<_>>(),
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
