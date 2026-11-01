//! Live native topology queries against the existing independently owned workload.
use fube_client::*;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    num::NonZeroU64,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::oracle::{Http, Journal, Result};

fn signed(value: &Value) -> i64 {
    match value {
        Value::String(value) => value.parse().expect("independent signed integer string"),
        value => value.as_i64().expect("independent signed integer"),
    }
}

fn compare_partition(partition: &Partition, observed: &Value) {
    assert_eq!(format!("{:?}", partition.kind), observed["ServiceKind"]);
    assert_eq!(
        format!("{:?}", partition.status),
        observed["PartitionStatus"]
    );
    assert_eq!(
        format!("{:?}", partition.health_state),
        observed["HealthState"]
    );
    let information = &partition.partition_information;
    let observed_information = &observed["PartitionInformation"];
    assert_eq!(
        information.id.to_string(),
        observed_information["Id"]
            .as_str()
            .expect("independent partition id")
    );
    assert_eq!(
        format!("{:?}", information.kind),
        observed_information["ServicePartitionKind"]
    );
    match information.kind {
        PartitionKind::Singleton => {}
        PartitionKind::Named => assert_eq!(information.name, observed_information["Name"]),
        PartitionKind::Int64Range => {
            assert_eq!(information.low_key, signed(&observed_information["LowKey"]));
            assert_eq!(
                information.high_key,
                signed(&observed_information["HighKey"])
            );
        }
    }
    match partition.kind {
        ServiceKind::Stateless => {
            assert_eq!(
                i64::from(partition.instance_count),
                signed(&observed["InstanceCount"])
            );
        }
        ServiceKind::Stateful => {
            assert_eq!(
                i64::from(partition.target_replica_set_size),
                signed(&observed["TargetReplicaSetSize"])
            );
            assert_eq!(
                i64::from(partition.min_replica_set_size),
                signed(&observed["MinReplicaSetSize"])
            );
            assert_eq!(
                partition.last_quorum_loss_duration_in_seconds,
                signed(&observed["LastQuorumLossDurationInSeconds"])
            );
            // REST masks its PrimaryEpoch projection; the native raw epoch is
            // intentionally not compared to that different representation.
        }
        ServiceKind::Invalid => panic!("invalid kind in validated partition result"),
    }
}

fn replica_id(replica: &Replica) -> i64 {
    match replica.kind {
        ServiceKind::Stateful => replica.replica_id,
        ServiceKind::Stateless => replica.instance_id,
        ServiceKind::Invalid => panic!("invalid kind in validated replica result"),
    }
}

fn observed_replica_id(replica: &Value) -> i64 {
    signed(
        &replica[if replica["ServiceKind"] == "Stateful" {
            "ReplicaId"
        } else {
            "InstanceId"
        }],
    )
}

fn compare_replica(replica: &Replica, observed: &Value) {
    assert_eq!(format!("{:?}", replica.kind), observed["ServiceKind"]);
    assert_eq!(replica_id(replica), observed_replica_id(observed));
    assert_eq!(format!("{:?}", replica.status), observed["ReplicaStatus"]);
    assert_eq!(
        format!("{:?}", replica.health_state),
        observed["HealthState"]
    );
    assert_eq!(replica.address, observed["Address"]);
    assert_eq!(replica.node_name, observed["NodeName"]);
    assert_eq!(
        replica.last_in_build_duration_in_seconds,
        signed(&observed["LastInBuildDurationInSeconds"])
    );
    if replica.kind == ServiceKind::Stateful {
        assert_eq!(format!("{:?}", replica.role), observed["ReplicaRole"]);
    }
}

async fn native_replicas(
    client: &FabricClient,
    id: uuid::Uuid,
    expected_count: usize,
) -> Result<(Vec<Replica>, bool)> {
    let mut query = ReplicaQueryDescription::new(ReplicaQueryParameters::new(id));
    let mut seen_tokens = BTreeSet::new();
    let mut native = Vec::new();
    loop {
        let page = client
            .get_replica_list(&query, Some(Duration::from_secs(30)))
            .await?;
        assert!(!page.items.is_empty());
        native.extend(page.items);
        assert!(native.len() <= expected_count);
        let Some(token) = page.continuation_token else {
            break;
        };
        assert!(
            seen_tokens.insert(token.clone()),
            "repeated native replica token"
        );
        assert!(seen_tokens.len() < expected_count);
        query.paging.continuation_token = Some(token);
    }
    assert_eq!(native.len(), expected_count);
    Ok((native, !seen_tokens.is_empty()))
}

fn movement_replicas(native: &[Replica]) -> Value {
    json!(native
        .iter()
        .map(|replica| json!({
            "replica_id": replica_id(replica), "node": replica.node_name,
            "kind": format!("{:?}", replica.kind), "status": format!("{:?}", replica.status),
            "role": format!("{:?}", replica.role),
        }))
        .collect::<Vec<_>>())
}

fn movement_matches(native: &[Replica], observed: &[Value]) -> bool {
    native.len() == observed.len()
        && native.iter().map(replica_id).collect::<BTreeSet<_>>().len() == native.len()
        && native.iter().all(|replica| {
            observed.iter().any(|value| {
                observed_replica_id(value) == replica_id(replica)
                    && value["ServiceKind"] == "Stateful"
                    && replica.kind == ServiceKind::Stateful
                    && value["NodeName"] == replica.node_name
                    && value["ReplicaStatus"] == format!("{:?}", replica.status)
                    && value["ReplicaRole"] == format!("{:?}", replica.role)
            })
        })
}

pub async fn move_primary(
    client: &FabricClient,
    http: &Http,
    journal: &Journal,
    topology: &Value,
) -> Result<Value> {
    let budget = Duration::from_secs(120);
    let deadline = tokio::time::Instant::now() + budget;
    let started = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let movement = async {
        let application = &journal.applications[0];
        let service = format!("{application}/Stateful");
        let captured = topology["independent_partition_replicas"]
            .as_array()
            .ok_or("missing captured owned topology")?
            .iter()
            .find(|value| value["service"] == service)
            .ok_or("missing captured owned stateful partition")?;
        if captured["partition"]["ServiceKind"] != "Stateful" {
            return Err("movement requires the captured owned stateful partition".into());
        }
        let partition = captured["partition"]["PartitionInformation"]["Id"]
            .as_str()
            .ok_or("missing captured partition identity")?
            .parse::<uuid::Uuid>()?;
        let captured_replicas = captured["replicas"]
            .as_array()
            .ok_or("missing captured owned replicas")?;
        let (before, _) = native_replicas(client, partition, captured_replicas.len()).await?;
        let before_http = http.replicas(partition).await?;
        if !movement_matches(&before, captured_replicas)
            || !movement_matches(&before, &before_http)
            || before
                .iter()
                .filter(|replica| replica.role == ReplicaRole::Primary)
                .count()
                != 1
            || before.iter().any(|replica| {
                replica.status != ReplicaStatus::Ready
                    || !matches!(
                        replica.role,
                        ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                    )
            })
        {
            return Err("owned placement changed before the single movement request".into());
        }
        for replica in &before {
            compare_replica(
                replica,
                before_http
                    .iter()
                    .find(|value| observed_replica_id(value) == replica_id(replica))
                    .ok_or("missing independent pre-movement replica")?,
            );
        }
        let old = before
            .iter()
            .find(|replica| {
                replica.role == ReplicaRole::Primary && replica.status == ReplicaStatus::Ready
            })
            .ok_or("missing ready owned primary")?;
        let target = before
            .iter()
            .find(|replica| {
                replica.role == ReplicaRole::ActiveSecondary
                    && replica.status == ReplicaStatus::Ready
                    && replica.node_name != old.node_name
            })
            .ok_or("missing existing ready owned secondary")?;
        let nodes = http.nodes().await?;
        let target_node = nodes["Items"]
            .as_array()
            .ok_or("missing independent nodes before movement")?
            .iter()
            .find(|node| node["Name"] == target.node_name && node["NodeStatus"] == "Up")
            .ok_or("target secondary node is not independently Up")?;
        // Only observation is repeated. An ambiguous mutation is a fixture failure.
        client
            .move_primary(
                partition,
                &target.node_name,
                Some(
                    deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(Duration::from_secs(60)),
                ),
            )
            .await?;
        let mut polls = 0;
        loop {
            polls += 1;
            let (after, _) = native_replicas(client, partition, before.len()).await?;
            let after_http = http.replicas(partition).await?;
            let primaries: Vec<_> = after
                .iter()
                .filter(|replica| replica.role == ReplicaRole::Primary)
                .collect();
            if primaries.len() == 1
                && primaries[0].node_name == target.node_name
                && primaries[0].status == ReplicaStatus::Ready
                && after.iter().any(|replica| {
                    replica.node_name == old.node_name
                        && replica.role == ReplicaRole::ActiveSecondary
                        && replica.status == ReplicaStatus::Ready
                })
                && after.iter().map(replica_id).collect::<BTreeSet<_>>()
                    == before.iter().map(replica_id).collect()
                && after.iter().all(|replica| {
                    replica.status == ReplicaStatus::Ready
                        && matches!(
                            replica.role,
                            ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                        )
                        && before.iter().any(|previous| {
                            replica_id(previous) == replica_id(replica)
                                && previous.node_name == replica.node_name
                        })
                })
                && movement_matches(&after, &after_http)
            {
                for replica in &after {
                    compare_replica(
                        replica,
                        after_http
                            .iter()
                            .find(|value| observed_replica_id(value) == replica_id(replica))
                            .ok_or("missing independent post-movement replica")?,
                    );
                }
                return Ok(json!({
                    "owner": journal.owner, "application": application, "service": service,
                    "partition": partition, "old_primary": old.node_name, "target": target.node_name,
                    "target_node_observation": target_node,
                    "request": {"operation": "MovePrimary", "constraints": "enforced",
                        "count": 1, "native_succeeded": true},
                    "overall_budget_ms": budget.as_millis(), "poll_count": polls,
                    "native_start_unix_ms": started,
                    "native_end_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                    "verified_observations": {
                        "before": {"native_replicas": movement_replicas(&before), "http_replicas": before_http},
                        "after": {"native_replicas": movement_replicas(&after), "http_replicas": after_http},
                    },
                }));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    tokio::time::timeout_at(deadline, movement)
        .await
        .map_err(|_| "owned MovePrimary observation exceeded its overall budget")?
}

pub async fn observe(client: &FabricClient, http: &Http, journal: &Journal) -> Result<Value> {
    let mut partition_count = 0;
    let mut replica_count = 0;
    let mut partition_paging = false;
    let mut replica_paging = false;
    let mut observations = Vec::new();
    for application in &journal.applications {
        for (service, expected_count) in [("Stateless", 1), ("Stateful", 2)] {
            let name = format!("{application}/{service}");
            let observed = http.partitions(&name).await?;
            assert_eq!(observed.len(), expected_count);
            let mut query =
                PartitionQueryDescription::new(PartitionQueryParameters::new(name.parse()?));
            let first = client.get_partition_list(&query, None).await?;
            let mut partitions = first.items;
            let mut continuation = first.continuation_token;
            let mut seen_tokens = BTreeSet::new();
            while let Some(token) = continuation {
                assert!(
                    seen_tokens.insert(token.clone()),
                    "repeated native partition token"
                );
                assert!(seen_tokens.len() <= expected_count);
                query.paging.continuation_token = Some(token);
                let page = client
                    .get_partition_list(&query, Some(Duration::from_secs(30)))
                    .await?;
                partitions.extend(page.items);
                continuation = page.continuation_token;
                partition_paging = true;
            }
            assert_eq!(partitions.len(), expected_count);
            assert_eq!(
                partitions
                    .iter()
                    .map(|p| p.partition_information.id)
                    .collect::<BTreeSet<_>>(),
                observed
                    .iter()
                    .map(|p| {
                        p["PartitionInformation"]["Id"]
                            .as_str()
                            .expect("independent partition id")
                            .parse::<uuid::Uuid>()
                            .expect("independent UUID")
                    })
                    .collect()
            );
            for partition in &partitions {
                let observed = observed
                    .iter()
                    .find(|observed| {
                        observed["PartitionInformation"]["Id"]
                            == partition.partition_information.id.to_string()
                    })
                    .ok_or("missing independent partition")?;
                compare_partition(partition, observed);
                query.parameters.partition_id = Some(partition.partition_information.id);
                query.paging = Paging::default();
                let selected = client.get_partition_list(&query, None).await?;
                assert_eq!(selected.items.len(), 1);
                assert_eq!(
                    selected.items[0].partition_information.id,
                    partition.partition_information.id
                );
            }
            partition_count += partitions.len();
            for observation in &observed {
                let id = observation["PartitionInformation"]["Id"]
                    .as_str()
                    .ok_or("missing independent partition identity")?
                    .parse()?;
                let replicas = http.replicas(id).await?;
                let expected_replicas = replicas.len();
                assert!(
                    expected_replicas > 0,
                    "independent replica/instance list is empty"
                );
                let (native, paged) = native_replicas(client, id, expected_replicas).await?;
                replica_paging |= paged;
                assert_eq!(
                    native.iter().map(replica_id).collect::<BTreeSet<_>>(),
                    replicas.iter().map(observed_replica_id).collect()
                );
                let mut query = ReplicaQueryDescription::new(ReplicaQueryParameters::new(id));
                for replica in &native {
                    compare_replica(
                        replica,
                        replicas
                            .iter()
                            .find(|observed| observed_replica_id(observed) == replica_id(replica))
                            .ok_or("missing independent replica")?,
                    );
                    query.parameters.replica_or_instance_id = Some(replica_id(replica));
                    query.paging = Paging::default();
                    let selected = client.get_replica_list(&query, None).await?;
                    assert_eq!(selected.items.len(), 1);
                    assert_eq!(replica_id(&selected.items[0]), replica_id(replica));
                }
                query.parameters.replica_or_instance_id = None;
                query.parameters.status_filter = ReplicaStatusFilter::READY;
                let ready = client.get_replica_list(&query, None).await?;
                assert_eq!(
                    ready.items.iter().map(replica_id).collect::<BTreeSet<_>>(),
                    native
                        .iter()
                        .filter(|r| r.status == ReplicaStatus::Ready)
                        .map(replica_id)
                        .collect()
                );
                replica_count += native.len();
                observations.push(json!({
                    "service": name, "partition": observation, "replicas": replicas,
                }));
            }
        }
    }
    let nodes = http.nodes().await?;
    let nodes = nodes["Items"]
        .as_array()
        .ok_or("missing independent nodes")?;
    let mut deployment_observations = Vec::new();
    let mut deployment_paging = false;
    for node in nodes {
        let name = node["Name"]
            .as_str()
            .ok_or("missing independent node identity")?;
        let observed = http.deployed_applications(name).await?;
        let mut query =
            DeployedApplicationQueryDescription::new(DeployedApplicationQueryParameters::new(name));
        query.parameters.include_health_state = true;
        query.paging.max_results = NonZeroU64::new(1);
        let mut seen_tokens = BTreeSet::new();
        let mut deployments = Vec::new();
        loop {
            let page = client.get_deployed_application_list(&query, None).await?;
            assert!(page.items.len() <= 1);
            deployments.extend(page.items);
            let Some(token) = page.continuation_token else {
                break;
            };
            assert!(
                seen_tokens.insert(token.clone()),
                "repeated native deployment token"
            );
            assert!(seen_tokens.len() <= observed.len());
            query.paging.continuation_token = Some(token);
            deployment_paging = true;
        }
        assert_eq!(deployments.len(), observed.len());
        let native_names: BTreeSet<_> = deployments.iter().map(|app| app.name.as_str()).collect();
        let observed_names: BTreeSet<_> = observed
            .iter()
            .map(|app| {
                app["Name"]
                    .as_str()
                    .expect("independent deployed application name")
            })
            .collect();
        assert_eq!(native_names, observed_names);
        for app in &deployments {
            let observation = observed
                .iter()
                .find(|observation| observation["Name"] == app.name)
                .ok_or("missing independent deployed application")?;
            assert_eq!(app.type_name, observation["TypeName"]);
            assert_eq!(format!("{:?}", app.status), observation["Status"]);
            assert_eq!(
                format!("{:?}", app.health_state),
                observation["HealthState"]
            );
            assert_eq!(app.work_directory, observation["WorkDirectory"]);
            assert_eq!(app.log_directory, observation["LogDirectory"]);
            assert_eq!(app.temp_directory, observation["TempDirectory"]);
            query.parameters.application_name = Some(app.name.parse()?);
            query.parameters.include_health_state = false;
            query.paging = Paging::default();
            let selected = client
                .get_deployed_application_list(&query, Some(Duration::from_secs(30)))
                .await?;
            assert_eq!(selected.items.len(), 1);
            assert_eq!(selected.items[0].name, app.name);
            assert_eq!(selected.items[0].health_state, HealthState::Unknown);
        }
        deployment_observations.push(json!({"node": name, "applications": observed}));
    }
    assert!(
        deployment_paging,
        "owned deployments must force a native continuation"
    );
    let missing_partition =
        ReplicaQueryDescription::new(ReplicaQueryParameters::new(uuid::Uuid::nil()));
    let missing_replica_code = match client.get_replica_list(&missing_partition, None).await {
        Err(
            ClientError::Domain(DomainFailure { code, .. })
            | ClientError::Transport(Error::Remote { code, .. }),
        ) => code,
        other => {
            return Err(format!("missing partition must return a native error: {other:?}").into())
        }
    };
    assert!(!client.is_closed());
    client.ping(None).await?;
    let absent_node = format!("missing-{}", journal.owner);
    assert!(!nodes.iter().any(|node| node["Name"] == absent_node));
    let missing_node = DeployedApplicationQueryDescription::new(
        DeployedApplicationQueryParameters::new(absent_node),
    );
    let missing_node_code = match client
        .get_deployed_application_list(&missing_node, None)
        .await
    {
        Err(
            ClientError::Domain(DomainFailure { code, .. })
            | ClientError::Transport(Error::Remote { code, .. }),
        ) => code,
        other => return Err(format!("missing node must return a native error: {other:?}").into()),
    };
    assert!(!client.is_closed());
    client.ping(None).await?;
    Ok(json!({
        "partition_count": partition_count,
        "replica_instance_count": replica_count,
        "partition_native_continuation_followed": partition_paging,
        "replica_native_continuation_followed": replica_paging,
        "deployment_native_continuation_followed": deployment_paging,
        "exact_partition_replica_application_selectors": true,
        "ready_replica_status_filter": true,
        "deployed_health_requested_and_omitted": true,
        "raw_native_epoch_not_compared_to_rest_projection": true,
        "native_missing_partition_code": missing_replica_code,
        "native_missing_deployment_node_code": missing_node_code,
        "shared_connection_reuse_after_native_errors": true,
        "independent_partition_replicas": observations,
        "independent_deployments": deployment_observations,
    }))
}
