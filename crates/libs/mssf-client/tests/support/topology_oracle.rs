//! Live native topology queries against the existing independently owned workload.
use mssf_client::*;
use serde_json::{json, Value};
use std::{collections::BTreeSet, num::NonZeroU64, time::Duration};

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
            let first = client.get_partition_list(&query).await?;
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
                    .get_partition_list_with_timeout(&query, Duration::from_secs(30))
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
                let selected = client.get_partition_list(&query).await?;
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
                let mut query = ReplicaQueryDescription::new(ReplicaQueryParameters::new(id));
                let mut seen_tokens = BTreeSet::new();
                let mut native = Vec::new();
                loop {
                    let page = client
                        .get_replica_list_with_timeout(&query, Duration::from_secs(30))
                        .await?;
                    assert!(!page.items.is_empty());
                    native.extend(page.items);
                    let Some(token) = page.continuation_token else {
                        break;
                    };
                    assert!(
                        seen_tokens.insert(token.clone()),
                        "repeated native replica token"
                    );
                    assert!(seen_tokens.len() < expected_replicas);
                    query.paging.continuation_token = Some(token);
                    replica_paging = true;
                }
                assert_eq!(native.len(), expected_replicas);
                assert_eq!(
                    native.iter().map(replica_id).collect::<BTreeSet<_>>(),
                    replicas.iter().map(observed_replica_id).collect()
                );
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
                    let selected = client.get_replica_list(&query).await?;
                    assert_eq!(selected.items.len(), 1);
                    assert_eq!(replica_id(&selected.items[0]), replica_id(replica));
                }
                query.parameters.replica_or_instance_id = None;
                query.parameters.status_filter = ReplicaStatusFilter::READY;
                let ready = client.get_replica_list(&query).await?;
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
            let page = client.get_deployed_application_list(&query).await?;
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
                .get_deployed_application_list_with_timeout(&query, Duration::from_secs(30))
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
    let missing_replica_code = match client.get_replica_list(&missing_partition).await {
        Err(Error::Remote { code, .. }) => code,
        other => {
            return Err(format!("missing partition must return a native error: {other:?}").into())
        }
    };
    assert!(!client.is_closed());
    client.ping().await?;
    let absent_node = format!("missing-{}", journal.owner);
    assert!(!nodes.iter().any(|node| node["Name"] == absent_node));
    let missing_node = DeployedApplicationQueryDescription::new(
        DeployedApplicationQueryParameters::new(absent_node),
    );
    let missing_node_code = match client.get_deployed_application_list(&missing_node).await {
        Err(Error::Remote { code, .. }) => code,
        other => return Err(format!("missing node must return a native error: {other:?}").into()),
    };
    assert!(!client.is_closed());
    client.ping().await?;
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
