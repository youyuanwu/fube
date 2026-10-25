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
    client: &FabricClient,
    name: &NamingUri,
    key: &PartitionKey,
    previous: Option<&ResolvedServicePartition>,
) -> Result<ResolvedServicePartition> {
    // Independent HTTP and native gateway cache observations are not an atomic
    // cluster snapshot. Require eventual exact agreement, never accept a subset.
    let mut previous = previous.cloned();
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let result = client
                .resolve_service_partition(
                    name,
                    key,
                    previous.as_ref(),
                    Some(Duration::from_secs(15)),
                )
                .await?;
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

pub async fn named(http: &Http, client: &FabricClient, app: &NamingUri) -> Result<Value> {
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
        let result =
            resolve_checked(http, client, &name, &PartitionKey::Named(key.into()), None).await?;
        assert_eq!(result.partition_information.name, key);
        let partitions = http.partitions(name.as_str()).await?;
        let observed = partitions
            .iter()
            .find(|p| p["PartitionInformation"]["Name"] == key)
            .ok_or("missing independent named key")?;
        assert_eq!(
            observed["PartitionInformation"]["Id"]
                .as_str()
                .unwrap()
                .parse::<uuid::Uuid>()?,
            result.partition_information.id
        );
        endpoints(
            &result,
            &http.replicas(result.partition_information.id).await?,
        )?;
        evidence.push(json!({"key":key,"partition":result.partition_information.id}));
    }
    Ok(json!({"name":name.as_str(),"partitions":evidence}))
}

pub async fn endpoint_change(
    http: &Http,
    client: &FabricClient,
    notifications: &mut ServiceNotificationClient,
    name: &NamingUri,
) -> Result<Value> {
    let baseline = client
        .resolve_service_partition(name, &PartitionKey::Singleton, None, None)
        .await?;
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
    client: &FabricClient,
    notifications: &mut ServiceNotificationClient,
    name: &NamingUri,
) -> Result<Value> {
    let baseline = client
        .resolve_service_partition(name, &PartitionKey::Int64(-10), None, None)
        .await?;
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
            let (dropped,retries,forwarded)=proxy.evidence();
            if dropped==1 && retries>0 && forwarded>0 {break;}
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
    let (dropped, retries, forwarded) = proxy.evidence();
    Ok(
        json!({"successful_acks_dropped":dropped,"native_page_retries":retries,"subsequent_target_acks_forwarded":forwarded,"unique_delivered_versions":delivered,"duplicate_versions":0}),
    )
}
