use fube_client::{
    FurpcClient, FurpcClientError, GatewayDescription, HealthState, NamingUri, Node,
    NodeDeactivationIntent, NodeDeactivationStatus, NodeListPage, NodeQueryDescription,
    NodeQueryParameters, NodeStatus, NodeStatusFilter, Paging, DEFAULT_TIMEOUT,
};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
mod support;
use support::ObservedNode;
#[path = "support/rest.rs"]
mod rest;
#[path = "support/baseline_setup.rs"]
mod setup;
const OBSERVATION_MAX_AGE_MS: u64 = 120_000;

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
        "independent observations expired or are future-dated; rerun the onebox baseline test"
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
    let names: std::collections::BTreeSet<_> = page.items.iter().map(|n| &n.name).collect();
    let expected: std::collections::BTreeSet<_> =
        config.expected_nodes.iter().map(|n| &n.name).collect();
    assert_eq!(names, expected);
    assert_eq!(page.items.len(), expected.len(), "no duplicate members");
    for node in &page.items {
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

async fn assert_node_queries(
    client: &mut FurpcClient,
    config: &Config,
    generation: u64,
) -> serde_json::Value {
    assert_fresh(config);
    let native_observation_start_unix_ms = support::unix_millis();
    let baseline = client
        .get_node_list(
            furpc::Request::new((NodeQueryDescription::default()).clone()),
            furpc_deadline(),
        )
        .await
        .map(|response| furpc_body(response, generation))
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
                .get_node_list(
                    furpc::Request::new(
                        (NodeQueryDescription::new(NodeQueryParameters {
                            status_filter: filter,
                            ..Default::default()
                        }))
                        .clone(),
                    ),
                    furpc_deadline(),
                )
                .await
                .map(|response| furpc_body(response, generation))
                .expect("native status selection"),
            config,
        );
    }
    let empty = client
        .get_node_list(
            furpc::Request::new(
                (NodeQueryDescription::new(NodeQueryParameters {
                    status_filter: NodeStatusFilter::DOWN,
                    ..Default::default()
                }))
                .clone(),
            ),
            furpc_deadline(),
        )
        .await
        .map(|response| furpc_body(response, generation))
        .expect("native nonmatching status selection");
    assert!(empty.items.is_empty() && empty.continuation_token.is_none());
    assert_membership(
        &client
            .get_node_list(
                furpc::Request::new(
                    (NodeQueryDescription {
                        parameters: NodeQueryParameters {
                            node_name: Some(String::new()),
                            ..Default::default()
                        },
                        paging: Paging {
                            continuation_token: Some(String::new()),
                            ..Default::default()
                        },
                    })
                    .clone(),
                ),
                furpc_deadline(),
            )
            .await
            .map(|response| furpc_body(response, generation))
            .expect("native empty omission semantics"),
        config,
    );
    for expected in &config.expected_nodes {
        let page = client
            .get_node_list(
                furpc::Request::new(
                    (NodeQueryDescription {
                        parameters: NodeQueryParameters {
                            node_name: Some(expected.name.clone()),
                            status_filter: NodeStatusFilter::UP,
                        },
                        paging: Paging {
                            max_results: std::num::NonZeroU64::new(1),
                            ..Default::default()
                        },
                    })
                    .clone(),
                ),
                furpc_deadline(),
            )
            .await
            .map(|response| furpc_body(response, generation))
            .expect("native exact-name hit and full terminal page");
        assert_eq!(page.items.len(), 1);
        assert!(
            page.continuation_token.is_none(),
            "full terminal page needs no token"
        );
        assert_independent_node(&page.items[0], expected);
    }
    for absent in ["native-client-no-such-node", "n0010", "N00"] {
        let page = client
            .get_node_list(
                furpc::Request::new(
                    (NodeQueryDescription::new(NodeQueryParameters {
                        node_name: Some(absent.into()),
                        ..Default::default()
                    }))
                    .clone(),
                ),
                furpc_deadline(),
            )
            .await
            .map(|response| furpc_body(response, generation))
            .expect("native exact-name miss");
        assert!(page.items.is_empty() && page.continuation_token.is_none());
    }
    let mut chains = Vec::new();
    for page_size in [1, 2] {
        let mut token = None;
        let mut all = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut page_lengths = Vec::new();
        for _ in 0..16 {
            let page = client
                .get_node_list(
                    furpc::Request::new(
                        (NodeQueryDescription {
                            paging: Paging {
                                continuation_token: token.take(),
                                max_results: std::num::NonZeroU64::new(page_size),
                            },
                            ..Default::default()
                        })
                        .clone(),
                    ),
                    furpc_deadline(),
                )
                .await
                .map(|response| furpc_body(response, generation))
                .expect("native server continuation chain");
            assert!(!page.items.is_empty());
            assert!(page.items.len() <= page_size as usize);
            page_lengths.push(page.items.len());
            all.extend(page.items);
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
                items: all,
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
        .get_node_list(
            furpc::Request::new(
                (NodeQueryDescription {
                    paging: Paging {
                        continuation_token: Some(format!("{final_id:x}")),
                        max_results: std::num::NonZeroU64::new(1),
                    },
                    ..Default::default()
                })
                .clone(),
            ),
            furpc_deadline(),
        )
        .await
        .map(|response| furpc_body(response, generation))
        .expect("valid boundary continuation strictly after final independent ID");
    assert!(exhausted.items.is_empty() && exhausted.continuation_token.is_none());
    let invalid = client
        .get_node_list(
            furpc::Request::new(
                (NodeQueryDescription {
                    paging: Paging {
                        continuation_token: Some("this-is-not-a-node-id".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .clone(),
            ),
            furpc_deadline(),
        )
        .await
        .map(|response| furpc_body(response, generation))
        .expect_err("malformed continuation must be a native error, not empty success");
    let (code, message) = match invalid {
        FurpcClientError::Domain(failure) => (failure.code, failure.message),
        FurpcClientError::Native(furpc::Error::RemoteFault(fault)) => {
            (fault.code(), String::from_utf16(fault.message()).unwrap())
        }
        other => panic!("expected native error, got {other:?}"),
    };
    // On this runtime all invalid-token subqueries can fail, and the parallel
    // handler returns the last failure (QueryMessageHandler.cpp:212-225). Real
    // native AND independent HTTP probes observed both E_INVALIDARG and
    // FMFailoverUnitNotFound. Do not normalize either or retry the operation.
    assert!(
        [-2147024809, -2147017462].contains(&code),
        "unexpected native malformed-token error {code}: {message}"
    );
    assert_membership(
        &client
            .get_node_list(
                furpc::Request::new((NodeQueryDescription::default()).clone()),
                furpc_deadline(),
            )
            .await
            .map(|response| furpc_body(response, generation))
            .expect("reuse after native error"),
        config,
    );
    serde_json::json!({
        "native_node_count": baseline.items.len(), "independent_nodes": config.expected_nodes,
        "observation_unix_ms": config.observed_at_unix_ms,
        "native_observation_start_unix_ms": native_observation_start_unix_ms,
        "native_observation_end_unix_ms": native_observation_end_unix_ms,
        "native_timings": native_timing_evidence(&baseline.items),
        "typed_fields_match_http": true, "timing_tolerance_seconds": 120,
        "timestamp_precision_tolerance_ticks": 10000,
        "binary_ports_match_onebox_manifest": true,
        "exact_name_hit_miss_case_prefix": true, "status_default_all_up_down": true,
        "empty_name_token_omitted": true, "paging_chains": chains,
        "valid_continuation_after_final_id_is_empty": true,
        "invalid_token_native_code": code, "connection_survives_native_error": true,
        "invalid_token_native_diagnostic": message,
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

fn furpc_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + DEFAULT_TIMEOUT
}

fn furpc_body<T>(response: furpc::Response<T>, generation: u64) -> T {
    assert_eq!(
        response
            .source()
            .expect("native furpc response source")
            .generation(),
        generation,
        "operation must reuse the original native generation, not reconnect"
    );
    response.into_body()
}

fn node_identities(page: &NodeListPage) -> serde_json::Value {
    let mut nodes: Vec<_> = page.items.iter().collect();
    nodes.sort_by_key(|node| node.id);
    nodes
        .into_iter()
        .map(|node| serde_json::json!({
            "name": node.name, "node_id": node.id.to_string(), "node_instance": node.instance_id,
        }))
        .collect()
}

async fn assert_furpc_management(
    client: &mut FurpcClient,
    gateway: &GatewayDescription,
    config: &Config,
    generation: u64,
) -> serde_json::Value {
    assert_fresh(config);
    let native_observation_start_unix_ms = support::unix_millis();
    let existing: NamingUri = config.existing_name.parse().unwrap();
    let absent: NamingUri = config.absent_name.parse().unwrap();
    let existing_result = furpc_body(
        client
            .name_exists(furpc::Request::new(existing.clone()), furpc_deadline())
            .await
            .expect("furpc existing name"),
        generation,
    );
    let absent_result = furpc_body(
        client
            .name_exists(furpc::Request::new(absent), furpc_deadline())
            .await
            .expect("furpc absent name"),
        generation,
    );
    assert!(existing_result && !absent_result);
    let baseline = furpc_body(
        client
            .get_node_list(
                furpc::Request::new(NodeQueryDescription::default()),
                furpc_deadline(),
            )
            .await
            .expect("furpc native GetNodeList"),
        generation,
    );
    assert_membership(&baseline, config);
    for (name, status, expected_count) in [
        (
            Some(config.expected_node_name.clone()),
            NodeStatusFilter::UP,
            1,
        ),
        (Some("furpc-no-such-node".into()), NodeStatusFilter::ALL, 0),
        (None, NodeStatusFilter::DOWN, 0),
    ] {
        let query = NodeQueryDescription::new(NodeQueryParameters {
            node_name: name,
            status_filter: status,
        });
        let page = furpc_body(
            client
                .get_node_list(furpc::Request::new(query), furpc_deadline())
                .await
                .expect("furpc native node selector"),
            generation,
        );
        assert_eq!(page.items.len(), expected_count);
        assert!(page.continuation_token.is_none());
        for node in &page.items {
            assert_independent_node(
                node,
                config
                    .expected_nodes
                    .iter()
                    .find(|observed| observed.name == node.name)
                    .unwrap(),
            );
        }
    }
    let mut token = None;
    let mut seen = std::collections::HashSet::new();
    let mut all = Vec::new();
    let mut page_lengths = Vec::new();
    for _ in 0..16 {
        let query = NodeQueryDescription {
            paging: Paging {
                continuation_token: token.take(),
                max_results: std::num::NonZeroU64::new(2),
            },
            ..Default::default()
        };
        let page = furpc_body(
            client
                .get_node_list(furpc::Request::new(query), furpc_deadline())
                .await
                .expect("furpc server-issued node continuation"),
            generation,
        );
        assert!(!page.items.is_empty() && page.items.len() <= 2);
        page_lengths.push(page.items.len());
        all.extend(page.items);
        token = page.continuation_token;
        if let Some(token) = &token {
            assert!(seen.insert(token.clone()), "continuation must advance");
        } else {
            break;
        }
    }
    assert!(
        token.is_none() && !seen.is_empty(),
        "follow a real native continuation to completion"
    );
    assert!(all.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert_membership(
        &NodeListPage {
            items: all,
            continuation_token: None,
        },
        config,
    );
    let invalid = NodeQueryDescription {
        paging: Paging {
            continuation_token: Some("this-is-not-a-node-id".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let error = client
        .get_node_list(furpc::Request::new(invalid), furpc_deadline())
        .await
        .expect_err("furpc malformed continuation must remain a native error");
    let (native_error_code, native_error_origin) = match error {
        FurpcClientError::Domain(failure) => (failure.code, "domain"),
        FurpcClientError::Native(furpc::Error::RemoteFault(fault)) => {
            (fault.code(), "native_fault")
        }
        other => panic!("unexpected furpc native error category: {other:?}"),
    };
    assert!([-2147024809, -2147017462].contains(&native_error_code));
    let reused = furpc_body(
        client
            .get_node_list(
                furpc::Request::new(NodeQueryDescription::default()),
                furpc_deadline(),
            )
            .await
            .expect("furpc same-generation reuse after native error"),
        generation,
    );
    assert_membership(&reused, config);
    let mut ping = client.clone();
    let mut naming = client.clone();
    let mut nodes = client.clone();
    let deadline = furpc_deadline();
    let (ping, naming, nodes) = tokio::try_join!(
        ping.ping(furpc::Request::new(()), deadline),
        naming.name_exists(furpc::Request::new(existing), deadline),
        nodes.get_node_list(
            furpc::Request::new(NodeQueryDescription::default()),
            deadline
        ),
    )
    .expect("mixed concurrent furpc calls over clones");
    assert_eq!(&furpc_body(ping, generation), gateway);
    assert!(furpc_body(naming, generation));
    assert_membership(&furpc_body(nodes, generation), config);
    serde_json::json!({
        "existing_name": existing_result, "absent_name": absent_result,
        "native_observation_start_unix_ms": native_observation_start_unix_ms,
        "native_observation_end_unix_ms": support::unix_millis(),
        "native_nodes": node_identities(&baseline),
        "stable_fields_match_http": true,
        "exact_name_and_status_filters": true,
        "page_lengths": page_lengths, "server_token_count": seen.len(),
        "native_error_code": native_error_code, "native_error_origin": native_error_origin,
        "same_generation_after_error": true, "mixed_concurrent_operations": 3,
        "mixed_results_match": true, "generation": generation,
    })
}
#[tokio::test]
#[ignore = "prepares owned fixtures on a disposable clear onebox; requires MSSF_TEST_CONFIG and MSSF_TEST_REPORT"]
async fn onebox_ping_name_exists_and_node_list() {
    setup::prepare()
        .await
        .expect("prepare independent baseline");
    let path = PathBuf::from(std::env::var_os("MSSF_TEST_CONFIG").expect("MSSF_TEST_CONFIG"));
    assert!(path.is_absolute());
    let config: Config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let (endpoint, node_id) = config.validate().unwrap();
    assert_fresh(&config);
    let channel = FurpcClient::endpoint_clear(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FurpcClient::new(channel);
    let mut second_client = client.clone();
    let mut third_client = client.clone();
    let deadline = furpc_deadline();
    let (first, second, third) = tokio::try_join!(
        client.ping(furpc::Request::new(()), deadline),
        second_client.ping(furpc::Request::new(()), deadline),
        third_client.ping(furpc::Request::new(()), deadline),
    )
    .unwrap();
    let generation = first.source().unwrap().generation();
    let gateway = furpc_body(first, generation);
    for actual in [
        &gateway,
        &furpc_body(second, generation),
        &furpc_body(third, generation),
    ] {
        assert_independent_gateway(actual, &config, node_id);
    }
    let node_evidence = assert_node_queries(&mut client, &config, generation).await;
    let management = assert_furpc_management(&mut client, &gateway, &config, generation).await;
    let gateway = serde_json::json!({
        "address": gateway.address, "node_name": gateway.node_name,
        "node_id": format!("{:x}", gateway.node_id), "node_instance": gateway.node_instance,
    });
    let record = serde_json::json!({
        "profile": config.profile, "runtime_version": config.runtime_version,
        "endpoint": endpoint.to_string(), "gateway": gateway,
        "node_list": node_evidence,
        "furpc_ping": {"operation_count": 3, "gateway": gateway},
        "furpc_management": management,
    });
    std::fs::write(
        &config.report_path,
        serde_json::to_vec_pretty(&record).unwrap(),
    )
    .unwrap();
}
