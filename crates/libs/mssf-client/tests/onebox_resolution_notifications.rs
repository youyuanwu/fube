//! Real native clear-TCP qualification on an explicitly owned disposable onebox.
use mssf_client::*;
use serde_json::{json, Value};
use std::time::Duration;

#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/notification_live_scenarios.rs"]
mod live;
#[path = "support/notification_live_proxy.rs"]
mod loss_proxy;
#[path = "support/list_fixture.rs"]
mod oracle;
#[path = "support/rest.rs"]
mod rest;
#[path = "support/notification_setup.rs"]
mod setup;
use oracle::{Http, Journal, Result};
use setup::Config;

async fn update(
    client: &mut ServiceNotificationClient,
    name: &NamingUri,
    empty: bool,
) -> Result<ServiceNotification> {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = client.recv().await.ok_or("notification stream ended")??;
            let application = name
                .as_str()
                .rsplit_once('/')
                .ok_or("missing application scope")?
                .0;
            if !event
                .service_name
                .as_str()
                .starts_with(&format!("{application}/"))
            {
                return Err("notification escaped the registered application ancestry".into());
            }
            if &event.service_name == name && event.endpoints.is_empty() == empty {
                return Ok(event);
            }
        }
    })
    .await?
}

async fn exact_update(
    client: &mut ServiceNotificationClient,
    name: &NamingUri,
    empty: bool,
) -> Result<ServiceNotification> {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = client.recv().await.ok_or("exact stream ended")??;
            if &event.service_name != name {
                return Err("exact filter delivered another service".into());
            }
            if event.endpoints.is_empty() == empty {
                return Ok(event);
            }
        }
    })
    .await?
}

async fn scenario(config: Config, mut journal: Journal) -> Result<Value> {
    let http = Http::new()?;
    let nodes = http.nodes().await?;
    let observed = nodes["Items"]
        .as_array()
        .ok_or("missing independent nodes")?;
    if observed.len() != 5
        || observed
            .iter()
            .any(|n| n["NodeStatus"] != "Up" || n["CodeVersion"] != config.runtime)
    {
        return Err("owned onebox runtime/readiness mismatch".into());
    }

    let ordinary = FabricClient::connect_clear(config.endpoint, Duration::from_secs(60))
        .await
        .map_err(|e| format!("ordinary establishment: {e}"))?;
    let mut notifications =
        ServiceNotificationClient::connect_clear(config.endpoint, Duration::from_secs(60))
            .await
            .map_err(|e| format!("first notification establishment: {e}"))?;
    let mut independent =
        ServiceNotificationClient::connect_clear(config.endpoint, Duration::from_secs(60))
            .await
            .map_err(|e| format!("second notification establishment: {e}"))?;
    if !observed
        .iter()
        .any(|n| n["Name"] == notifications.gateway().node_name)
        || !observed
            .iter()
            .any(|n| n["Name"] == independent.gateway().node_name)
    {
        return Err("returned session gateway is absent from independent node inventory".into());
    }
    let app: NamingUri = journal.applications[0].parse()?;
    let stateless: NamingUri = format!("{app}/Stateless").parse()?;
    let stateful: NamingUri = format!("{app}/Stateful").parse()?;
    let proxy = loss_proxy::LossProxy::start(config.endpoint).await?;
    let mut retry_client =
        ServiceNotificationClient::connect_clear(proxy.address, Duration::from_secs(60)).await?;
    let prefix = notifications
        .register_filter(
            &ServiceNotificationFilter {
                name: app.clone(),
                name_prefix: true,
                primary_only: false,
            },
            None,
        )
        .await?;
    let primary = notifications
        .register_filter(
            &ServiceNotificationFilter {
                name: stateful.clone(),
                name_prefix: false,
                primary_only: true,
            },
            None,
        )
        .await?;
    let exact = independent
        .register_filter(
            &ServiceNotificationFilter {
                name: stateless.clone(),
                name_prefix: false,
                primary_only: false,
            },
            None,
        )
        .await?;
    let retry_filter = retry_client
        .register_filter(
            &ServiceNotificationFilter {
                name: stateless.clone(),
                name_prefix: false,
                primary_only: false,
            },
            None,
        )
        .await?;
    http.prepare(&mut journal, &config.journal).await?;
    http.wait_ready(&journal).await?;
    let mut results = Vec::new();
    for app in &journal.applications {
        for (suffix, keys) in [
            ("Stateless", vec![PartitionKey::Singleton]),
            (
                "Stateful",
                vec![PartitionKey::Int64(-10), PartitionKey::Int64(9)],
            ),
        ] {
            let name: NamingUri = format!("{app}/{suffix}").parse()?;
            let partitions = http.partitions(name.as_str()).await?;
            for key in keys {
                let result = live::resolve_checked(&http, &ordinary, &name, &key, None).await?;
                let partition = partitions
                    .iter()
                    .find(|p| {
                        p["PartitionInformation"]["Id"].as_str().is_some_and(|id| {
                            id.eq_ignore_ascii_case(&result.partition_information.id.to_string())
                        })
                    })
                    .ok_or("resolved identity absent from independent partition inventory")?;
                let replicas = http.replicas(result.partition_information.id).await?;
                if result.endpoints.is_empty() {
                    return Err("expected nonempty independent endpoint result".into());
                }
                live::endpoints(&result, &replicas)?;
                let refreshed = ordinary
                    .resolve_service_partition(&name, &key, Some(&result), None)
                    .await?;
                assert!(result.version.compare(&refreshed.version)?.is_le());
                results.push(json!({"name":name.as_str(),"partition":partition["PartitionInformation"],"endpoints":result.endpoints.iter().map(|e|json!({"address":e.address,"role":format!("{:?}",e.role)})).collect::<Vec<_>>()}));
            }
        }
    }
    let named = live::named(&http, &ordinary, &app).await?;
    let first = update(&mut notifications, &stateless, false).await?;
    let exact_first = exact_update(&mut independent, &stateless, false).await?;
    assert_eq!(first.partition_id, exact_first.partition_id);
    let retry = live::retry(&proxy, &mut retry_client).await?;
    let endpoint_change =
        live::endpoint_change(&http, &ordinary, &mut notifications, &stateless).await?;
    let role_change = live::role_change(&http, &ordinary, &mut notifications, &stateful).await?;
    let missing: NamingUri = format!("{app}/DoesNotExist").parse()?;
    assert!(matches!(
        ordinary
            .resolve_service_partition(&missing, &PartitionKey::Singleton, None, None)
            .await,
        Err(Error::Remote { .. })
    ));
    ordinary.ping(None).await?;
    // Only a service under the exact owned application journal is mutated.
    let management = rest::configuration(Duration::from_secs(60))?;
    management
        .client
        .post(format!(
            "{}/Services/{}/$/Delete?api-version=6.0",
            management.base_path,
            stateless.as_str()["fabric:/".len()..].replace('/', "~")
        ))
        .json(&json!({"ForceRemove":true}))
        .send()
        .await?
        .error_for_status()?;
    let removed = update(&mut notifications, &stateless, true).await?;
    let exact_removed = exact_update(&mut independent, &stateless, true).await?;
    assert_eq!(removed.partition_id, first.partition_id);
    assert_eq!(exact_removed.partition_id, first.partition_id);
    assert!(first.version.compare(&removed.version)?.is_lt());
    ordinary.close().await?;
    // Closing the ordinary connection has not retired either subscription.
    primary.unregister(None).await?;
    prefix.unregister(None).await?;
    notifications.close(None).await?;
    // Closing the other notification owner does not affect this one.
    exact.unregister(None).await?;
    let gateway = independent.gateway().clone();
    independent.close(None).await?;
    retry_filter.unregister(None).await?;
    retry_client.close(None).await?;
    proxy.stop().await;
    Ok(json!({
        "runtime_version":config.runtime,"profile":"clear","allocation":config.allocation,
        "gateway":{"address":gateway.address,"node_id":gateway.node_id.to_string(),"node_instance":gateway.node_instance,"node_name":gateway.node_name},
        "resolved":results,"named":named,"ack_retry":retry,"endpoint_change":endpoint_change,"role_change":role_change,
        "passed":["native session establishment","three independent notification clients","exact/prefix/primary-only registration","singleton, named and signed-int64 resolution","previous-result refresh","independent replica endpoint/role multiset comparison","native endpoint count change and empty invalidations","owned primary replacement and versioned updates","forced successful ACK loss, gateway retry, no duplicate delivery","native error and healthy reuse","explicit unregister","independent close"],
        "unqualified":["TLS/authentication (out of scope)","automatic recovery (out of scope)","isolated primary-only content-change delivery on a service with distinct published primary addresses"]
    }))
}

#[tokio::test]
#[ignore = "requires explicit MSSF_NOTIFICATION_CONFIG for a new task-owned disposable onebox"]
async fn onebox_resolution_and_notifications() -> Result<()> {
    let config = setup::config()?;
    let archive = std::env::var("MSSF_TEST_ARCHIVE_SHA256")?;
    let journal = Journal::new(config.owner.clone(), archive.clone())?;
    journal.reserve(&config.journal)?;
    let expected = journal.clone();
    let cleanup_config = config.clone();
    let mut report = fixture::with_cleanup(scenario(config.clone(), journal), async move {
        let current = Journal::load(&cleanup_config.journal, &cleanup_config.owner)?;
        if current.applications != expected.applications
            || current.type_name != expected.type_name
            || current.upload != expected.upload
            || current.archive_sha256 != expected.archive_sha256
        {
            return Err("refusing replaced notification workload journal".into());
        }
        oracle::cleanup(
            &cleanup_config.journal,
            &cleanup_config.owner,
            &Http::new()?,
        )
        .await
    })
    .await?;
    report["owned_cleanup_confirmed"] = json!(true);
    report["archive_sha256"] = json!(archive);
    std::fs::write(&config.report, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
