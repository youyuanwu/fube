//! Real native clear-TCP qualification on an explicitly owned disposable onebox.
use fube_client::*;
use serde_json::{json, Value};
use std::time::Duration;

#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/furpc.rs"]
mod furpc_support;
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

    let resolution_start_ms = furpc_support::now_ms();
    let channel = FurpcClient::endpoint_clear(config.endpoint)?
        .connect_timeout(Duration::from_secs(60))
        .connect()
        .await
        .map_err(|e| format!("ordinary establishment: {e}"))?;
    let mut ordinary = FurpcClient::new(channel);
    let response = ordinary
        .ping(furpc::Request::new(()), furpc_support::deadline())
        .await?;
    let generation = response
        .source()
        .ok_or("missing native Ping source")?
        .generation();
    let channel = ServiceNotificationClient::endpoint_clear(config.endpoint)?
        .connect_timeout(Duration::from_secs(60))
        .connect()
        .await?;
    let mut notifications = ServiceNotificationClient::start(
        channel,
        NotificationClientOptions {
            startup_timeout: Duration::from_secs(45),
            default_timeout: Duration::from_secs(60),
        },
    )
    .await
    .map_err(|e| format!("first notification startup: {e}"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let channel = ServiceNotificationClient::endpoint_clear(config.endpoint)?
        .connect_until(deadline)
        .await
        .map_err(|e| format!("second notification establishment: {e}"))?;
    let mut independent = tokio::time::timeout_at(
        deadline,
        ServiceNotificationClient::start(
            channel,
            NotificationClientOptions {
                startup_timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                default_timeout: Duration::from_secs(60),
            },
        ),
    )
    .await
    .map_err(|_| Error::Timeout)?
    .map_err(|e| format!("second notification startup: {e}"))?;
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let channel = ServiceNotificationClient::endpoint_clear(proxy.address)?
        .connect_until(deadline)
        .await?;
    let mut retry_client = tokio::time::timeout_at(
        deadline,
        ServiceNotificationClient::start(
            channel,
            NotificationClientOptions {
                startup_timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                default_timeout: Duration::from_secs(60),
            },
        ),
    )
    .await
    .map_err(|_| Error::Timeout)??;
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
            for key in keys {
                results.push(
                    live::resolution_case(&http, &mut ordinary, generation, &name, &key).await?,
                );
            }
        }
    }
    let named = live::named(&http, &mut ordinary, generation, &app).await?;
    let first = update(&mut notifications, &stateless, false).await?;
    let exact_first = exact_update(&mut independent, &stateless, false).await?;
    assert_eq!(first.partition_id, exact_first.partition_id);
    let retry = live::retry(&proxy, &mut retry_client).await?;
    let endpoint_change = live::endpoint_change(
        &http,
        &mut ordinary,
        generation,
        &mut notifications,
        &stateless,
    )
    .await?;
    let role_change = live::role_change(
        &http,
        &mut ordinary,
        generation,
        &mut notifications,
        &stateful,
    )
    .await?;
    let missing: NamingUri = format!("{app}/DoesNotExist").parse()?;
    let (missing_code, missing_origin) = furpc_support::failure(
        ordinary
            .resolve_service_partition(
                furpc::Request::new(ResolutionRequest::new(
                    &missing,
                    &PartitionKey::Singleton,
                    None,
                )?),
                furpc_support::deadline(),
            )
            .await
            .unwrap_err(),
    );
    let missing_http_absent = !http
        .services(app.as_str())
        .await?
        .iter()
        .any(|service| service["Name"] == missing.as_str());
    assert!(missing_http_absent);
    {
        let mut ping = ordinary.clone();
        let mut nodes = ordinary.clone();
        let (resolved, ping, nodes) = tokio::try_join!(
            ordinary.resolve_service_partition(
                furpc::Request::new(ResolutionRequest::new(
                    &stateless,
                    &PartitionKey::Singleton,
                    None
                )?),
                furpc_support::deadline(),
            ),
            ping.ping(furpc::Request::new(()), furpc_support::deadline()),
            nodes.get_node_list(
                furpc::Request::new(NodeQueryDescription::default()),
                furpc_support::deadline()
            ),
        )?;
        let result = furpc_support::body(resolved, generation);
        live::endpoints(
            &result,
            &http.replicas(result.partition_information.id).await?,
        )?;
        furpc_support::body(ping, generation);
        assert_eq!(furpc_support::body(nodes, generation).items.len(), 5);
    }
    let resolution_end_ms = furpc_support::now_ms();
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
    drop(ordinary);
    // Dropping the last ordinary owner has not retired either subscription.
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
        "owner":config.owner, "owned_applications":journal.applications, "owned_type":journal.type_name,
        "gateway":{"address":gateway.address,"node_id":gateway.node_id.to_string(),"node_instance":gateway.node_instance,"node_name":gateway.node_name},
        "resolved":results,"named":named,"ack_retry":retry,"endpoint_change":endpoint_change,"role_change":role_change,
        "furpc_resolution": {
            "generation":generation, "same_generation_after_error":true,
            "mixed_concurrent_operations":3, "missing_service":missing,
            "native_missing_service_code":missing_code, "native_error_origin":missing_origin,
            "missing_http_absent":missing_http_absent,
            "native_start_ms":resolution_start_ms, "native_end_ms":resolution_end_ms,
        },
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
