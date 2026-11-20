use super::loss_proxy::LossProxy;
use super::*;

pub fn endpoints(result: &ResolvedServicePartition, replicas: &[Value]) -> Result<()> {
    let mut native: Vec<_> = result
        .endpoints
        .iter()
        .map(|e| (format!("{:?}", e.role), e.address.clone()))
        .collect();
    let mut independent = Vec::new();
    for replica in replicas {
        if replica["ReplicaStatus"] != "Ready" {
            continue;
        }
        let role = if replica["ServiceKind"] == "Stateless" {
            "Stateless"
        } else if replica["ReplicaRole"] == "Primary" {
            "StatefulPrimary"
        } else if replica["ReplicaRole"] == "ActiveSecondary" {
            "StatefulSecondary"
        } else {
            continue;
        };
        independent.push((
            role.to_owned(),
            replica["Address"]
                .as_str()
                .ok_or("missing independent address")?
                .to_owned(),
        ));
    }
    native.sort();
    independent.sort();
    if native != independent {
        return Err(format!("endpoint/role multisets not yet converged: native={native:?}, independent={independent:?}").into());
    }
    Ok(())
}

pub async fn resolve_checked(
    http: &Http,
    client: &mut FurpcClient,
    generation: u64,
    name: &NamingUri,
    key: &PartitionKey,
    previous: Option<&ResolvedServicePartition>,
) -> Result<ResolvedServicePartition> {
    // Independent HTTP and native gateway cache observations are not an atomic
    // cluster snapshot. Require eventual exact agreement, never accept a subset.
    let mut previous = previous.cloned();
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let result = resolve(client, generation, name, key, previous.as_ref()).await?;
            let replicas = http.replicas(result.partition_information.id).await?;
            if endpoints(&result, &replicas).is_ok() {
                break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result);
            }
            previous = Some(result);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await?
}

pub async fn resolve(
    client: &mut FurpcClient,
    generation: u64,
    name: &NamingUri,
    key: &PartitionKey,
    previous: Option<&ResolvedServicePartition>,
) -> Result<ResolvedServicePartition> {
    let response = client
        .resolve_service_partition(
            furpc::Request::new(ResolutionRequest::new(name, key, previous)?),
            tokio::time::Instant::now() + Duration::from_secs(15),
        )
        .await?;
    Ok(furpc_support::body(response, generation))
}

pub async fn resolution_case(
    http: &Http,
    client: &mut FurpcClient,
    generation: u64,
    name: &NamingUri,
    key: &PartitionKey,
) -> Result<Value> {
    let start = furpc_support::now_ms();
    let result = resolve_checked(http, client, generation, name, key, None).await?;
    let refreshed = resolve_checked(http, client, generation, name, key, Some(&result)).await?;
    let order = result.version.compare(&refreshed.version)?;
    assert!(order.is_le());
    assert_eq!(
        result.partition_information,
        refreshed.partition_information
    );
    let partitions = http.partitions(name.as_str()).await?;
    let observed = partitions
        .iter()
        .find(|value| {
            value["PartitionInformation"]["Id"]
                .as_str()
                .is_some_and(|id| {
                    id.eq_ignore_ascii_case(&result.partition_information.id.to_string())
                })
        })
        .ok_or("resolved partition absent from independent inventory")?;
    let replicas = http.replicas(result.partition_information.id).await?;
    endpoints(&result, &replicas)?;
    endpoints(&refreshed, &replicas)?;
    fn evidence(result: &ResolvedServicePartition) -> Value {
        let info = &result.partition_information;
        json!({
            "name": result.service_name.as_str(), "kind": format!("{:?}", result.kind),
            "partition": {"id": info.id, "kind": format!("{:?}", info.kind),
                "name": info.name, "low_key": info.low_key, "high_key": info.high_key},
            "endpoints": result.endpoints.iter().map(|value| json!({
                "address": value.address, "role": format!("{:?}", value.role),
            })).collect::<Vec<_>>(),
        })
    }
    let key = match key {
        PartitionKey::Singleton => json!({"kind": "Singleton"}),
        PartitionKey::Int64(value) => json!({"kind": "Int64", "value": value}),
        PartitionKey::Named(value) => json!({"kind": "Named", "value": value}),
    };
    Ok(json!({
        "key": key, "native": evidence(&result), "refreshed": evidence(&refreshed),
        "expected_partition": observed, "expected_replicas": replicas,
        "refresh_order": format!("{order:?}"),
        "native_start_ms": start, "native_end_ms": furpc_support::now_ms(),
    }))
}

pub async fn named(
    http: &Http,
    client: &mut FurpcClient,
    generation: u64,
    app: &NamingUri,
) -> Result<Value> {
    let name: NamingUri = format!("{app}/Named").parse()?;
    let management = rest::configuration(Duration::from_secs(60))?;
    management.client.post(format!("{}/Applications/{}/$/GetServices/$/Create?api-version=6.0",
        management.base_path,&app.as_str()["fabric:/".len()..]))
        .json(&json!({"ServiceKind":"Stateless","ServiceName":name.as_str(),"ServiceTypeName":"ListStateless",
            "PartitionDescription":{"PartitionScheme":"Named","Count":2,"Names":["blue","green"]},"InstanceCount":1}))
        .send().await?.error_for_status()?;
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let partitions = http.partitions(name.as_str()).await?;
            let mut ready = partitions.len() == 2;
            for partition in &partitions {
                let id = partition["PartitionInformation"]["Id"]
                    .as_str()
                    .ok_or("missing named partition id")?
                    .parse()?;
                let replicas = http.replicas(id).await?;
                ready &=
                    replicas.len() == 1 && replicas.iter().all(|r| r["ReplicaStatus"] == "Ready");
            }
            if ready {
                break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(partitions);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;
    let mut evidence = Vec::new();
    for key in ["blue", "green"] {
        evidence.push(
            resolution_case(
                http,
                client,
                generation,
                &name,
                &PartitionKey::Named(key.into()),
            )
            .await?,
        );
    }
    Ok(json!({"name":name.as_str(),"partitions":evidence}))
}

pub async fn endpoint_change(
    http: &Http,
    client: &mut FurpcClient,
    generation: u64,
    notifications: &mut ServiceNotificationClient,
    name: &NamingUri,
) -> Result<Value> {
    let baseline = resolve(client, generation, name, &PartitionKey::Singleton, None).await?;
    let management = rest::configuration(Duration::from_secs(60))?;
    management
        .client
        .post(format!(
            "{}/Services/{}/$/Update?api-version=6.0",
            management.base_path,
            name.as_str()["fabric:/".len()..].replace('/', "~")
        ))
        .json(&json!({"ServiceKind":"Stateless","Flags":"1","InstanceCount":1}))
        .send()
        .await?
        .error_for_status()?;
    let updated = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let update = super::update(notifications, name, false).await?;
            if update.endpoints.len() == 1 && baseline.version.compare(&update.version)?.is_lt() {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(update);
            }
        }
    })
    .await??;
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let replicas = http.replicas(updated.partition_id).await?;
            if replicas.len() == 1 && replicas[0]["ReplicaStatus"] == "Ready" {
                break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;
    let resolved = resolve_checked(
        http,
        client,
        generation,
        name,
        &PartitionKey::Singleton,
        Some(&baseline),
    )
    .await?;
    endpoints(&resolved, &http.replicas(updated.partition_id).await?)?;
    assert_eq!(resolved.endpoints.len(), 1);
    Ok(
        json!({"from":baseline.endpoints.len(),"to":resolved.endpoints.len(),"partition":updated.partition_id}),
    )
}

pub async fn role_change(
    http: &Http,
    client: &mut FurpcClient,
    generation: u64,
    notifications: &mut ServiceNotificationClient,
    name: &NamingUri,
) -> Result<Value> {
    let baseline = resolve(client, generation, name, &PartitionKey::Int64(-10), None).await?;
    let replicas = http.replicas(baseline.partition_information.id).await?;
    let old = replicas
        .iter()
        .find(|r| r["ReplicaRole"] == "Primary")
        .ok_or("missing owned primary")?;
    let node = old["NodeName"].as_str().ok_or("missing primary node")?;
    let id = old["ReplicaId"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| old["ReplicaId"].to_string());
    let management = rest::configuration(Duration::from_secs(60))?;
    // Exact replica from the independently observed, newly owned application.
    // No node/container restart or shared-allocation mutation is performed.
    management.client.post(format!("{}/Nodes/{node}/$/GetPartitions/{}/$/GetReplicas/{id}/$/Delete?api-version=6.0&ForceRemove=true",
        management.base_path,baseline.partition_information.id)).send().await?.error_for_status()?;
    let observed = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let replicas = http.replicas(baseline.partition_information.id).await?;
            let primary = replicas
                .iter()
                .find(|r| r["ReplicaRole"] == "Primary" && r["ReplicaStatus"] == "Ready");
            if let Some(primary) = primary {
                if (primary["NodeName"] != old["NodeName"]
                    || primary["ReplicaId"] != old["ReplicaId"])
                    && replicas
                        .iter()
                        .filter(|r| r["ReplicaStatus"] == "Ready")
                        .count()
                        >= 3
                {
                    break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(primary.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;
    let resolved = resolve_checked(
        http,
        client,
        generation,
        name,
        &PartitionKey::Int64(-10),
        Some(&baseline),
    )
    .await?;
    assert!(baseline.version.compare(&resolved.version)?.is_lt());
    endpoints(
        &resolved,
        &http.replicas(resolved.partition_information.id).await?,
    )?;
    let event = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = super::update(notifications, name, false).await?;
            if event.partition_id == baseline.partition_information.id
                && baseline.version.compare(&event.version)?.is_lt()
            {
                break Ok::<_, Box<dyn std::error::Error + Send + Sync>>(event);
            }
        }
    })
    .await??;
    Ok(
        json!({"partition":event.partition_id,"old_primary_node":old["NodeName"],"new_primary_node":observed["NodeName"]}),
    )
}

pub async fn retry(proxy: &LossProxy, client: &mut ServiceNotificationClient) -> Result<Value> {
    let mut versions = Vec::new();
    let mut delivered = 0usize;
    tokio::time::timeout(Duration::from_secs(90),async {
        loop {
            let evidence=proxy.evidence();
            if evidence.successful_acks_dropped==1 && evidence.native_page_retries>0
                && evidence.subsequent_target_acks_forwarded>0 {break;}
            tokio::select! {
                _=proxy.changed()=>{},
                update=client.recv()=>{
                    let update=update.ok_or("proxy notification stream ended")??;
                    if versions.contains(&update.version) {return Err::<(),Box<dyn std::error::Error+Send+Sync>>("native ACK retry redelivered a version".into());}
                    versions.push(update.version);delivered+=1;
                }
            }
        }
        // Drain admitted events to check the retry did not enqueue another copy.
        while let Ok(Some(event))=tokio::time::timeout(Duration::from_millis(50),client.recv()).await {
            let event=event?;
            if versions.contains(&event.version) {return Err("native retry duplicate was queued".into());}
            versions.push(event.version);delivered+=1;
        }
        Ok::<_,Box<dyn std::error::Error+Send+Sync>>(())
    }).await??;
    let evidence = proxy.evidence();
    let dropped = evidence
        .dropped_attempt
        .as_ref()
        .ok_or("native dropped-attempt witness missing")?;
    let retry = evidence
        .retry_attempt
        .as_ref()
        .ok_or("native retry-attempt witness missing")?;
    let ack = evidence
        .forwarded_success_ack
        .as_ref()
        .ok_or("native forwarded-ACK witness missing")?;
    if dropped.request_id == retry.request_id
        || retry.request_id != ack.request_id
        || dropped.page_id != retry.page_id
        || retry.page_id != ack.page_id
    {
        return Err("native retry did not use a fresh target request ID and its own ACK".into());
    }
    let mut report = serde_json::to_value(evidence)?;
    report["unique_delivered_versions"] = json!(delivered);
    report["duplicate_versions"] = json!(0);
    Ok(report)
}
