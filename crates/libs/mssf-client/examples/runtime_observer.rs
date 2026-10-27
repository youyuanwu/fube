//! Independent native resolution and application RPC oracle, not runtime state.
use mssf_client::{
    DeployedCodePackageQueryDescription, DeployedCodePackageQueryParameters,
    DeployedReplicaQueryDescription, DeployedReplicaQueryParameters, FabricClient, NamingUri,
    PartitionKey, PartitionQueryDescription, PartitionQueryParameters, ReplicaQueryDescription,
    ReplicaQueryParameters, ServiceEndpointRole, ServiceKind,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
type ObserverResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn http_status(address: &str) -> ObserverResult<(u16, serde_json::Value)> {
    let (authority, path) = address
        .strip_prefix("http://")
        .and_then(|v| v.split_once('/'))
        .ok_or("not an HTTP endpoint")?;
    let mut stream =
        tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(authority)).await??;
    stream
        .write_all(
            format!("GET /{path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        (&mut stream).take(16_385).read_to_end(&mut bytes),
    )
    .await??;
    if bytes.len() > 16_384 {
        return Err("HTTP response bound".into());
    }
    let reply = String::from_utf8(bytes)?;
    let status = reply
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("HTTP/1.1 "))
        .and_then(|line| line.split_whitespace().next())
        .ok_or("HTTP status")?
        .parse()?;
    Ok((
        status,
        serde_json::from_str(reply.split_once("\r\n\r\n").ok_or("HTTP framing")?.1)?,
    ))
}
async fn http(address: &str) -> ObserverResult<serde_json::Value> {
    let (status, value) = http_status(address).await?;
    if status != 200 {
        return Err(format!("endpoint not ready: HTTP {status}").into());
    }
    Ok(value)
}

async fn stateful_snapshot(
    client: &FabricClient,
    name: &NamingUri,
) -> ObserverResult<serde_json::Value> {
    let mut description =
        PartitionQueryDescription::new(PartitionQueryParameters::new(name.clone()));
    let mut partitions = Vec::new();
    for _ in 0..8 {
        let page = client
            .get_partition_list(&description, Some(Duration::from_secs(10)))
            .await?;
        partitions.extend(page.items);
        if partitions.len() > 32 {
            return Err("partition observation bound".into());
        }
        description.paging.continuation_token = page.continuation_token;
        if description.paging.continuation_token.is_none() {
            break;
        }
    }
    if description.paging.continuation_token.is_some() || partitions.len() != 1 {
        return Err("singleton partition observation incomplete".into());
    }
    let partition = partitions.remove(0);
    if partition.kind != ServiceKind::Stateful {
        return Err("service is not natively stateful".into());
    }
    let id = partition.partition_information.id;
    let mut description = ReplicaQueryDescription::new(ReplicaQueryParameters::new(id));
    let mut replicas = Vec::new();
    for _ in 0..8 {
        let page = client
            .get_replica_list(&description, Some(Duration::from_secs(10)))
            .await?;
        replicas.extend(page.items);
        if replicas.len() > 64 {
            return Err("replica observation bound".into());
        }
        description.paging.continuation_token = page.continuation_token;
        if description.paging.continuation_token.is_none() {
            break;
        }
    }
    if description.paging.continuation_token.is_some() {
        return Err("replica observation incomplete".into());
    }
    let service = name.to_string();
    let application: NamingUri = service
        .rsplit_once('/')
        .ok_or("service application")?
        .0
        .parse()?;
    let mut observations = Vec::new();
    for replica in replicas {
        let mut item = serde_json::json!({"replica":replica.replica_id,"role":format!("{:?}",replica.role),
            "status":format!("{:?}",replica.status),"node":replica.node_name,"address":replica.address});
        if replica.status == mssf_client::ReplicaStatus::Ready && !replica.address.is_empty() {
            let mut query =
                DeployedReplicaQueryParameters::new(&replica.node_name, application.clone());
            query.partition_id = Some(id);
            query.service_manifest_name = Some("RustRuntimeService".into());
            let deployed = client
                .get_deployed_replica_list(
                    &DeployedReplicaQueryDescription::new(query),
                    Some(Duration::from_secs(10)),
                )
                .await?;
            let deployed = deployed
                .into_iter()
                .find(|r| r.replica_id == replica.replica_id && r.kind == ServiceKind::Stateful)
                .ok_or("native deployed replica missing")?;
            let base = replica
                .address
                .strip_suffix("/service")
                .ok_or("stateful service-address profile")?;
            let rpc = http(&format!("{base}/diagnostics")).await?;
            if rpc["service"] != service
                || rpc["partition"] != id.to_string()
                || rpc["replica"].as_i64() != Some(replica.replica_id)
                || rpc["node"] != replica.node_name
                || rpc["pid"].as_i64() != Some(deployed.host_process_id)
                || deployed.service_type_name != "RustStatefulRuntimeType"
            {
                return Err("independent replica/deployment/HTTP identity mismatch".into());
            }
            item["deployed"] = serde_json::to_value(&deployed)?;
            item["service_http_status"] = serde_json::json!(http_status(&replica.address).await?.0);
            let control = rpc["replication_address"]
                .as_str()
                .ok_or("missing separate control address")?;
            if control == replica.address {
                return Err("client and replication address conflated".into());
            }
            item["control"] = http(control).await?;
            item["rpc"] = rpc;
        }
        observations.push(item);
    }
    let resolved = client
        .resolve_service_partition(
            name,
            &PartitionKey::Singleton,
            None,
            Some(Duration::from_secs(10)),
        )
        .await?;
    if resolved.kind != ServiceKind::Stateful || resolved.partition_information.id != id {
        return Err("native discovery partition/kind mismatch".into());
    }
    let primaries: Vec<_> = resolved
        .endpoints
        .into_iter()
        .filter(|e| e.role == ServiceEndpointRole::StatefulPrimary)
        .collect();
    if primaries.len() != 1 {
        return Err("native discovery does not have exactly one primary".into());
    }
    let address = &primaries[0].address;
    let rpc = http(address).await?;
    let native = observations
        .iter()
        .find(|r| r["role"] == "Primary" && r["address"] == *address)
        .ok_or("native placement does not corroborate discovered primary")?;
    if rpc["replica"] != native["replica"] || rpc["node"] != native["node"] {
        return Err("primary HTTP identity mismatch".into());
    }
    let packages = client
        .get_deployed_code_package_list(
            &DeployedCodePackageQueryDescription::new(DeployedCodePackageQueryParameters::new(
                native["node"].as_str().ok_or("node")?,
                application,
            )),
            None,
        )
        .await?;
    let package = packages
        .iter()
        .find(|p| {
            p.name == "Code"
                && p.service_manifest_name == "RustRuntimeService"
                && Some(p.main_entry_point.process_id) == rpc["pid"].as_i64()
        })
        .ok_or("native primary code package PID mismatch")?;
    let primary = serde_json::json!({"endpoint":address,"rpc":rpc,"native_pid":package.main_entry_point.process_id,
        "native_entry_point_instance":package.main_entry_point.instance_id,
        "native_activation_count":package.main_entry_point.statistics.activation_count,
        "native_entry_point":package.main_entry_point.entry_point_location,"native_code_version":package.version,
        "resolver":"mssf-client"});
    Ok(
        serde_json::json!({"observer":"mssf-client","service":service,"partition":id.to_string(),
        "native_partition":{"target":partition.target_replica_set_size,"minimum":partition.min_replica_set_size,
            "epoch_kind":"FMPrimaryEpoch",
            "status":format!("{:?}",partition.status),"epoch":{"data_loss":partition.current_configuration_epoch.data_loss_version,
                "configuration":partition.current_configuration_epoch.configuration_version}},
        "replicas":observations,"primary":primary,"best_effort_non_atomic":true}),
    )
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err(
            "usage: runtime_observer GATEWAY_IP:PORT fabric:/APP/SERVICE [NAMED_PARTITION|int:I64|-] [REPORT_QUERY]"
                .into(),
        );
    }
    let client = FabricClient::connect_clear(args[0].parse()?, Duration::from_secs(10)).await?;
    let name: NamingUri = args[1].parse()?;
    if args.len() == 3 && args[2] == "--stateful" {
        let snapshot = stateful_snapshot(&client, &name).await?;
        println!("{snapshot}");
        client.close().await?;
        return Ok(());
    }
    let key = match args.get(2) {
        None => PartitionKey::Singleton,
        Some(value) if value == "-" => PartitionKey::Singleton,
        Some(value) if value.starts_with("int:") => PartitionKey::Int64(value[4..].parse()?),
        Some(value) => PartitionKey::Named(value.clone()),
    };
    let query = args.get(3).map_or("", String::as_str);
    if query.len() > 1024
        || query
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && !b"-_=&".contains(&b))
    {
        return Err("unsupported bounded sample report query".into());
    }
    let resolved = client
        .resolve_service_partition(&name, &key, None, Some(Duration::from_secs(10)))
        .await?;
    if resolved.endpoints.is_empty() {
        return Err("no resolved endpoints".into());
    }
    for endpoint in resolved.endpoints {
        let (authority, path) = endpoint
            .address
            .strip_prefix("http://")
            .and_then(|v| v.split_once('/'))
            .ok_or("sample endpoint is not HTTP")?;
        let mut stream =
            tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(authority)).await??;
        stream
            .write_all(
                format!(
                    "GET /{path}{}{} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n",
                    if query.is_empty() { "" } else { "?" },
                    query
                )
                .as_bytes(),
            )
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(35),
            (&mut stream).take(16_385).read_to_end(&mut response),
        )
        .await??;
        if response.len() > 16_384 {
            return Err("RPC response limit".into());
        }
        let response = String::from_utf8(response)?;
        if !response.starts_with("HTTP/1.1 200 ") {
            return Err(format!("RPC did not succeed: {response}").into());
        }
        let (_, body) = response.split_once("\r\n\r\n").ok_or("missing RPC body")?;
        let rpc: serde_json::Value = serde_json::from_str(body)?;
        let application = args[1]
            .rsplit_once('/')
            .ok_or("service must belong to app")?
            .0;
        let packages = client
            .get_deployed_code_package_list(
                &DeployedCodePackageQueryDescription::new(DeployedCodePackageQueryParameters::new(
                    rpc["node"].as_str().ok_or("missing node")?,
                    application.parse()?,
                )),
                None,
            )
            .await?;
        let package = packages
            .iter()
            .find(|p| {
                p.service_manifest_name == "RustRuntimeService"
                    && p.name == "Code"
                    && p.main_entry_point.process_id == rpc["pid"].as_i64().unwrap_or(-1)
            })
            .ok_or("native code package does not confirm serving PID")?;
        println!(
            "{}",
            serde_json::json!({
                "endpoint":endpoint.address,"rpc":rpc,"native_pid":package.main_entry_point.process_id,
                "native_entry_point_instance":package.main_entry_point.instance_id,
                "native_activation_count":package.main_entry_point.statistics.activation_count,
                "native_entry_point":package.main_entry_point.entry_point_location,
                "native_code_version":package.version,
                "resolver":"mssf-client",
            })
        );
    }
    client.close().await?;
    Ok(())
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1)
    }
}
