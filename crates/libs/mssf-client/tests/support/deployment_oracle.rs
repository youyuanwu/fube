//! Independent REST observations for the native node-local deployment lists.

use mssf_client::*;
use serde_json::{json, Value};
use std::{collections::BTreeSet, time::Duration};

use super::oracle::{Http, Journal, Result};

fn signed(value: &Value) -> i64 {
    match value {
        Value::String(value) => value.parse().expect("independent signed integer string"),
        value => value.as_i64().expect("independent signed integer"),
    }
}

fn compare_time(value: NodeTimestamp, observed: &Value) {
    let text = observed.as_str().expect("independent timestamp");
    if text == "0001-01-01T00:00:00.000Z" {
        assert_eq!(value.0, 0);
        return;
    }
    let date = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .expect("independent timestamp format");
    let expected = date.unix_timestamp_nanos() / 100 + 116_444_736_000_000_000i128;
    assert!((i128::from(value.0) - expected).abs() < 10_000);
}

fn compare_entry_point(entry: &CodePackageEntryPoint, observed: &Value) {
    assert_eq!(entry.entry_point_location, observed["EntryPointLocation"]);
    assert_eq!(entry.process_id, signed(&observed["ProcessId"]));
    assert_eq!(entry.run_as_user_name, observed["RunAsUserName"]);
    compare_time(entry.next_activation_time, &observed["NextActivationTime"]);
    assert_eq!(format!("{:?}", entry.status), observed["Status"]);
    assert_eq!(entry.instance_id, signed(&observed["InstanceId"]));
    let stats = &entry.statistics;
    let observed = &observed["CodePackageEntryPointStatistics"];
    for (value, name) in [
        (stats.last_exit_code, "LastExitCode"),
        (stats.activation_failure_count, "ActivationFailureCount"),
        (
            stats.continuous_activation_failure_count,
            "ContinuousActivationFailureCount",
        ),
        (stats.exit_failure_count, "ExitFailureCount"),
        (
            stats.continuous_exit_failure_count,
            "ContinuousExitFailureCount",
        ),
        (stats.activation_count, "ActivationCount"),
        (stats.exit_count, "ExitCount"),
    ] {
        assert_eq!(i64::from(value), signed(&observed[name]), "{name}");
    }
    // Native JSON swaps these two labels; binary field names/order are authoritative.
    compare_time(stats.last_activation_time, &observed["LastExitTime"]);
    compare_time(stats.last_exit_time, &observed["LastActivationTime"]);
    compare_time(
        stats.last_successful_activation_time,
        &observed["LastSuccessfulActivationTime"],
    );
    compare_time(
        stats.last_successful_exit_time,
        &observed["LastSuccessfulExitTime"],
    );
}

fn compare_code_package(package: &DeployedCodePackage, observed: &Value) {
    assert_eq!(package.name, observed["Name"]);
    assert_eq!(package.version, observed["Version"]);
    assert_eq!(
        package.service_manifest_name,
        observed["ServiceManifestName"]
    );
    assert_eq!(format!("{:?}", package.status), observed["Status"]);
    assert_eq!(
        i64::from(package.run_frequency_interval),
        signed(&observed["RunFrequencyInterval"])
    );
    assert_eq!(
        package.has_setup_entry_point,
        observed["HasSetupEntryPoint"]
    );
    assert_eq!(
        package.service_package_activation_id,
        observed["ServicePackageActivationId"]
    );
    assert_eq!(format!("{:?}", package.host_type), observed["HostType"]);
    assert_eq!(
        format!("{:?}", package.host_isolation_mode),
        observed["HostIsolationMode"]
    );
    compare_entry_point(&package.setup_entry_point, &observed["SetupEntryPoint"]);
    compare_entry_point(&package.main_entry_point, &observed["MainEntryPoint"]);
}

fn replica_id(replica: &DeployedReplica) -> i64 {
    match replica.kind {
        ServiceKind::Stateful => replica.replica_id,
        ServiceKind::Stateless => replica.instance_id,
        ServiceKind::Invalid => panic!("invalid validated deployed replica"),
    }
}

fn observed_replica_key(replica: &Value) -> (uuid::Uuid, i64) {
    (
        replica["PartitionId"]
            .as_str()
            .expect("independent partition ID")
            .parse()
            .unwrap(),
        signed(
            &replica[if replica["ServiceKind"] == "Stateful" {
                "ReplicaId"
            } else {
                "InstanceId"
            }],
        ),
    )
}

fn compare_replica(replica: &DeployedReplica, observed: &Value) {
    assert_eq!(format!("{:?}", replica.kind), observed["ServiceKind"]);
    assert_eq!(replica.service_name, observed["ServiceName"]);
    assert_eq!(replica.service_type_name, observed["ServiceTypeName"]);
    assert_eq!(
        replica.service_manifest_version,
        observed["ServiceManifestVersion"]
    );
    assert_eq!(replica.code_package_name, observed["CodePackageName"]);
    assert_eq!(
        (replica.partition_id, replica_id(replica)),
        observed_replica_key(observed)
    );
    assert_eq!(
        format!("{:?}", replica.replica_status),
        observed["ReplicaStatus"]
    );
    assert_eq!(replica.address, observed["Address"]);
    assert_eq!(
        replica.service_manifest_name,
        observed["ServiceManifestName"]
    );
    match &replica.service_package_activation_id {
        None => assert!(observed["ServicePackageActivationId"].is_null()),
        Some(value) => assert_eq!(*value, observed["ServicePackageActivationId"]),
    }
    assert_eq!(replica.host_process_id, signed(&observed["HostProcessId"]));
    if replica.kind == ServiceKind::Stateful {
        assert_eq!(
            format!("{:?}", replica.replica_role),
            observed["ReplicaRole"]
        );
        let info = &replica.reconfiguration_information;
        let observed = &observed["ReconfigurationInformation"];
        assert_eq!(
            format!("{:?}", info.previous_configuration_role),
            observed["PreviousConfigurationRole"]
        );
        assert_eq!(
            format!("{:?}", info.reconfiguration_phase),
            observed["ReconfigurationPhase"]
        );
        assert_eq!(
            format!("{:?}", info.reconfiguration_type),
            observed["ReconfigurationType"]
        );
        compare_time(
            info.reconfiguration_start_time_utc,
            &observed["ReconfigurationStartTimeUtc"],
        );
    }
}

pub async fn observe(client: &FabricClient, http: &Http, journal: &Journal) -> Result<Value> {
    let nodes = http.nodes().await?;
    let nodes = nodes["Items"]
        .as_array()
        .ok_or("missing independent nodes")?;
    let mut service_package_count = 0;
    let mut code_package_count = 0;
    let mut replica_count = 0;
    let mut kinds = BTreeSet::new();
    let mut observations = Vec::new();
    for node in nodes {
        let node_name = node["Name"]
            .as_str()
            .ok_or("missing independent node name")?;
        for application in &journal.applications {
            let observed = http
                .deployed_service_packages(node_name, application)
                .await?;
            assert!(
                !observed.is_empty(),
                "owned service-package oracle must be nonempty"
            );
            let mut query = DeployedServicePackageQueryDescription::new(
                DeployedServicePackageQueryParameters::new(node_name, application.parse()?),
            );
            let packages = client.get_deployed_service_package_list(&query).await?;
            assert_eq!(packages.len(), observed.len());
            let native_ids: BTreeSet<_> = packages
                .iter()
                .map(|package| {
                    (
                        package.service_manifest_name.as_str(),
                        package.service_package_activation_id.as_str(),
                    )
                })
                .collect();
            let observed_ids: BTreeSet<_> = observed
                .iter()
                .map(|package| {
                    (
                        package["Name"].as_str().expect("independent manifest name"),
                        package["ServicePackageActivationId"]
                            .as_str()
                            .expect("independent activation ID"),
                    )
                })
                .collect();
            assert_eq!(native_ids, observed_ids);
            for package in &packages {
                let observation = observed
                    .iter()
                    .find(|value| {
                        value["Name"] == package.service_manifest_name
                            && value["ServicePackageActivationId"]
                                == package.service_package_activation_id
                    })
                    .ok_or("missing independent service package")?;
                assert_eq!(package.service_manifest_version, observation["Version"]);
                assert_eq!(format!("{:?}", package.status), observation["Status"]);
                query.parameters.service_manifest_name =
                    Some(package.service_manifest_name.clone());
                let selected = client
                    .get_deployed_service_package_list_with_timeout(&query, Duration::from_secs(30))
                    .await?;
                assert_eq!(selected.len(), 1);
                assert_eq!(selected[0], *package);
            }
            query.parameters.service_manifest_name = Some("AbsentManifest".into());
            assert!(client
                .get_deployed_service_package_list(&query)
                .await?
                .is_empty());
            let observed_code = http.deployed_code_packages(node_name, application).await?;
            assert!(
                !observed_code.is_empty(),
                "owned code-package oracle must be nonempty"
            );
            let mut code_query = DeployedCodePackageQueryDescription::new(
                DeployedCodePackageQueryParameters::new(node_name, application.parse()?),
            );
            let mut replica_query = DeployedReplicaQueryDescription::new(
                DeployedReplicaQueryParameters::new(node_name, application.parse()?),
            );
            let (code, replicas, _) = tokio::try_join!(
                client.get_deployed_code_package_list(&code_query),
                client.get_deployed_replica_list(&replica_query),
                client.ping(),
            )?;
            assert_eq!(code.len(), observed_code.len());
            let native_ids: BTreeSet<_> = code
                .iter()
                .map(|package| {
                    (
                        package.name.as_str(),
                        package.service_manifest_name.as_str(),
                        package.service_package_activation_id.as_str(),
                    )
                })
                .collect();
            let observed_ids: BTreeSet<_> = observed_code
                .iter()
                .map(|package| {
                    (
                        package["Name"]
                            .as_str()
                            .expect("independent code-package name"),
                        package["ServiceManifestName"]
                            .as_str()
                            .expect("independent manifest name"),
                        package["ServicePackageActivationId"]
                            .as_str()
                            .expect("independent activation ID"),
                    )
                })
                .collect();
            assert_eq!(native_ids.len(), code.len());
            assert_eq!(native_ids, observed_ids);
            for package in &code {
                let observation = observed_code
                    .iter()
                    .find(|value| {
                        value["Name"] == package.name
                            && value["ServiceManifestName"] == package.service_manifest_name
                            && value["ServicePackageActivationId"]
                                == package.service_package_activation_id
                    })
                    .ok_or("missing independent code package")?;
                compare_code_package(package, observation);
                code_query.parameters.service_manifest_name =
                    Some(package.service_manifest_name.clone());
                code_query.parameters.code_package_name = Some(package.name.clone());
                let selected = client
                    .get_deployed_code_package_list_with_timeout(
                        &code_query,
                        Duration::from_secs(30),
                    )
                    .await?;
                assert_eq!(selected.len(), 1);
                assert_eq!(selected[0], *package);
            }
            code_query.parameters.code_package_name = Some("AbsentCodePackage".into());
            assert!(client
                .get_deployed_code_package_list(&code_query)
                .await?
                .is_empty());
            let observed_replicas = http.deployed_replicas(node_name, application).await?;
            assert!(
                !observed_replicas.is_empty(),
                "owned replica oracle must be nonempty"
            );
            assert_eq!(replicas.len(), observed_replicas.len());
            let native_ids: BTreeSet<_> = replicas
                .iter()
                .map(|replica| (replica.partition_id, replica_id(replica)))
                .collect();
            assert_eq!(native_ids.len(), replicas.len());
            assert_eq!(
                native_ids,
                observed_replicas.iter().map(observed_replica_key).collect()
            );
            for replica in &replicas {
                let observation = observed_replicas
                    .iter()
                    .find(|value| {
                        observed_replica_key(value) == (replica.partition_id, replica_id(replica))
                    })
                    .ok_or("missing independent deployed replica")?;
                compare_replica(replica, observation);
                kinds.insert(format!("{:?}", replica.kind));
                replica_query.parameters.service_manifest_name =
                    Some(replica.service_manifest_name.clone());
                replica_query.parameters.partition_id = Some(replica.partition_id);
                let selected = client
                    .get_deployed_replica_list_with_timeout(&replica_query, Duration::from_secs(30))
                    .await?;
                assert_eq!(selected.len(), 1);
                assert_eq!(selected[0], *replica);
            }
            replica_query.parameters.partition_id = Some(uuid::Uuid::new_v4());
            assert!(client
                .get_deployed_replica_list(&replica_query)
                .await?
                .is_empty());
            service_package_count += packages.len();
            code_package_count += code.len();
            replica_count += replicas.len();
            observations.push(json!({
                "node": node_name, "application": application,
                "service_packages": observed, "native_service_packages": packages,
                "code_packages": observed_code, "native_code_packages": code,
                "replicas": observed_replicas, "native_replicas": replicas,
            }));
        }
    }
    assert!(service_package_count > 0);
    assert!(code_package_count > 0);
    assert!(replica_count > 0);
    assert_eq!(
        kinds,
        BTreeSet::from(["Stateful".to_owned(), "Stateless".to_owned()])
    );
    let mut missing_node_codes = Vec::new();
    let missing_node = format!("AbsentNode-{}", journal.owner);
    let application = journal.applications[0].parse()?;
    for result in [
        client
            .get_deployed_service_package_list(&DeployedServicePackageQueryDescription::new(
                DeployedServicePackageQueryParameters::new(&missing_node, application),
            ))
            .await
            .map(|_| ()),
        client
            .get_deployed_code_package_list(&DeployedCodePackageQueryDescription::new(
                DeployedCodePackageQueryParameters::new(
                    &missing_node,
                    journal.applications[0].parse()?,
                ),
            ))
            .await
            .map(|_| ()),
        client
            .get_deployed_replica_list(&DeployedReplicaQueryDescription::new(
                DeployedReplicaQueryParameters::new(
                    &missing_node,
                    journal.applications[0].parse()?,
                ),
            ))
            .await
            .map(|_| ()),
    ] {
        match result {
            Err(Error::Remote { code, .. }) => missing_node_codes.push(code),
            other => {
                return Err(
                    format!("missing deployment node must return native error: {other:?}").into(),
                )
            }
        }
        assert!(!client.is_closed());
        client.ping().await?;
    }
    Ok(json!({
        "service_package_count": service_package_count,
        "code_package_count": code_package_count, "replica_count": replica_count,
        "service_manifest_selectors": true,
        "code_package_and_partition_selectors": true,
        "stateful_and_stateless": true, "mixed_shared_native_connection": true,
        "native_missing_node_codes": missing_node_codes,
        "reuse_after_native_errors": true,
        "timestamp_precision_tolerance_ticks": 10000,
        "statistics_rest_activation_exit_labels_swapped": true,
        "limits": "shared activation and ExeHost without setup only; native node/internal-service fields and inactive replica fields have offline coverage, not independent REST projections",
        "observations": observations,
    }))
}

#[test]
fn deployment_timestamp_oracle_preserves_native_default_epoch_and_precision() {
    compare_time(NodeTimestamp(0), &json!("0001-01-01T00:00:00.000Z"));
    compare_time(
        NodeTimestamp(116_444_736_000_000_000),
        &json!("1970-01-01T00:00:00.000Z"),
    );
    compare_time(
        NodeTimestamp(134_371_187_016_289_999),
        &json!("2026-10-22T04:58:21.628Z"),
    );
    assert!(std::panic::catch_unwind(|| {
        compare_time(
            NodeTimestamp(116_444_736_000_010_000),
            &json!("1970-01-01T00:00:00.000Z"),
        );
    })
    .is_err());
}
