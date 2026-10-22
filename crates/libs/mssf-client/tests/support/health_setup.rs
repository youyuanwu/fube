//! Independent HTTP-only preparation and owned-report restoration helpers.
use std::time::Duration;

use reqwest::{redirect::Policy, Client};
use serde_json::json;

use super::oracle::*;

fn management() -> Result<Client> {
    Ok(Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?)
}

async fn observe(
    client: &Client,
    node: &str,
    policy: Option<bool>,
    filter: Option<u32>,
) -> Result<ObservedHealth> {
    let url = format!("{MANAGEMENT}/Nodes/{node}/$/GetHealth?api-version=6.3");
    let mut request = if let Some(value) = policy {
        client
            .post(url)
            .json(&json!({"ConsiderWarningAsError": value}))
    } else {
        client.get(url)
    };
    if let Some(value) = filter {
        request = request.query(&[("EventsHealthStateFilter", value)]);
    }
    Ok(request.send().await?.error_for_status()?.json().await?)
}

async fn submit(
    client: &Client,
    config: &Config,
    state: &str,
    sequence: i64,
    ttl_ms: i64,
) -> Result<()> {
    client
        .post(format!(
            "{MANAGEMENT}/Nodes/{}/$/ReportHealth?api-version=6.3&Immediate=true",
            config.target
        ))
        .json(&json!({
            "SourceId": config.source, "Property": config.property,
            "HealthState": state, "TimeToLiveInMilliSeconds": ttl_ms.to_string(),
            "Description": config.description, "SequenceNumber": sequence.to_string(),
            "RemoveWhenExpired": true,
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn restore(client: &Client, config: &Config) -> Result<()> {
    config.validate()?;
    // Supersede only this task's report. Short finite TTL removes even the Ok report.
    submit(client, config, "Ok", config.sequence + 1, 1_000).await?;
    for _ in 0..60 {
        let health = observe(client, &config.target, Some(false), None).await?;
        let owned = health
            .health_events
            .iter()
            .any(|e| e.source_id == config.source && e.property == config.property);
        if health.aggregated_health_state == "Ok" && !owned {
            let nodes: serde_json::Value = client
                .get(format!("{MANAGEMENT}/Nodes?api-version=6.3"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let items = nodes["Items"]
                .as_array()
                .ok_or("missing management node list during restoration")?;
            if items
                .iter()
                .any(|node| node["Name"] == config.target && node["HealthState"] == "Ok")
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err("owned report did not expire/remove after restoration".into())
}

pub async fn prepare() -> Result<()> {
    let path = output("MSSF_HEALTH_CONFIG")?;
    let report_path = output("MSSF_HEALTH_REPORT")?;
    if path == report_path {
        return Err("health config and report must differ".into());
    }
    remove_stale(&report_path)?;
    let owner = std::env::var("MSSF_HEALTH_OWNER")?;
    if owner.is_empty()
        || owner.len() > 80
        || !owner
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err("MSSF_HEALTH_OWNER must be a unique task-owned ASCII identifier".into());
    }
    let client = management()?;
    if path.exists() {
        let previous: Config = serde_json::from_slice(&std::fs::read(&path)?)?;
        if previous.owner != owner {
            return Err("refusing to replace another owner's health profile".into());
        }
        restore(&client, &previous).await?;
        remove_stale(&path)?;
    }
    let nodes: serde_json::Value = client
        .get(format!("{MANAGEMENT}/Nodes?api-version=6.3"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let items = nodes["Items"]
        .as_array()
        .ok_or("missing management node list")?;
    if items.len() != 5
        || items
            .iter()
            .any(|n| n["NodeStatus"] != "Up" || n["HealthState"] != "Ok")
    {
        return Err("requires independently healthy five-logical-node baseline".into());
    }
    let runtime = items[0]["CodeVersion"]
        .as_str()
        .ok_or("missing runtime")?
        .to_string();
    let property = format!("owned-{}", uuid::Uuid::new_v4());
    let mut config = Config {
        profile: "clear".into(),
        gateway: "127.0.0.1:19000".into(),
        runtime_version: runtime,
        archive_sha256: std::env::var("MSSF_TEST_ARCHIVE_SHA256")?,
        source: format!("NativeHealth-{owner}"),
        owner,
        missing: format!("missing-{property}"),
        property,
        target: "N0020".into(),
        healthy: "N0010".into(),
        sequence: now_ms().try_into()?,
        ttl_ms: 1_800_000,
        description: "Disposable native node health qualification warning".into(),
        report_path,
        observed_at_unix_ms: 0,
        missing_http_code: String::new(),
        snapshots: vec![],
    };
    config.validate()?;
    // Persist ownership BEFORE the first injection: restoration works even if preparation fails.
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    let healthy = observe(&client, &config.healthy, None, None).await?;
    if healthy.aggregated_health_state != "Ok" || !healthy.unhealthy_evaluations.is_empty() {
        return Err("healthy node prerequisite failed".into());
    }
    submit(&client, &config, "Warning", config.sequence, config.ttl_ms).await?;
    let mut converged = false;
    for _ in 0..60 {
        let false_health = observe(&client, &config.target, Some(false), None).await?;
        let true_health = observe(&client, &config.target, Some(true), None).await?;
        let ready = |health: &ObservedHealth, expected: &str, policy: bool| {
            health.aggregated_health_state == expected
                && health.health_events.iter().any(|e| {
                    e.source_id == config.source
                        && e.property == config.property
                        && e.sequence_number == config.sequence.to_string()
                        && e.health_state == "Warning"
                        && !e.is_expired
                })
                && health.unhealthy_evaluations.iter().any(|e| {
                    e.health_evaluation.kind == "Event"
                        && e.health_evaluation.unhealthy_event.source_id == config.source
                        && e.health_evaluation.unhealthy_event.property == config.property
                        && e.health_evaluation.consider_warning_as_error == policy
                        && e.health_evaluation.aggregated_health_state == expected
                        && !e.health_evaluation.description.is_empty()
                })
        };
        if ready(&false_health, "Warning", false) && ready(&true_health, "Error", true) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !converged {
        return Err("independent health ingestion did not converge within 60 bounded polls".into());
    }
    for (label, healthy, policy, filter) in cases() {
        let node_name = if healthy {
            config.healthy.clone()
        } else {
            config.target.clone()
        };
        let start_ms = now_ms();
        let health = observe(&client, &node_name, policy, filter).await?;
        config.snapshots.push(Snapshot {
            label: label.into(),
            node_name,
            policy,
            filter,
            start_ms,
            end_ms: now_ms(),
            health,
        });
    }
    let missing = client
        .get(format!(
            "{MANAGEMENT}/Nodes/{}/$/GetHealth?api-version=6.3",
            config.missing
        ))
        .send()
        .await?;
    if missing.status() != reqwest::StatusCode::NOT_FOUND {
        return Err("unknown-node management status was not 404".into());
    }
    let failure: serde_json::Value = missing.json().await?;
    config.missing_http_code = failure["Error"]["Code"]
        .as_str()
        .ok_or("missing independent error code")?
        .into();
    config.observed_at_unix_ms = now_ms();
    config.validate_ready()?;
    std::fs::write(path, serde_json::to_vec_pretty(&config)?)?;
    Ok(())
}

pub async fn restore_owned() -> Result<()> {
    let path = output("MSSF_HEALTH_CONFIG")?;
    if !path.exists() {
        return Ok(());
    } // Setup may fail before any injection.
    let config: Config = serde_json::from_slice(&std::fs::read(&path)?)?;
    if config.owner != std::env::var("MSSF_HEALTH_OWNER")? {
        return Err("health restoration owner mismatch".into());
    }
    restore(&management()?, &config).await
}

#[test]
fn health_time_oracles_use_exact_ticks_and_reject_overflow() {
    assert_eq!(ttl_ticks("P10675199DT2H48M5.4775807S").unwrap(), i64::MAX);
    assert_eq!(ttl_ticks("PT30M").unwrap(), 18_000_000_000);
    assert_eq!(ttl_ticks("1800000").unwrap(), 18_000_000_000);
    assert_eq!(ttl_ticks("PT0.0000001S").unwrap(), 1);
    assert!(ttl_ticks("PT0.00000001S").is_err());
    assert!(ttl_ticks("P999999999999999999999D").is_err());
    assert_eq!(timestamp_ticks("0001-01-01T00:00:00.000Z").unwrap(), 0);
    assert_eq!(
        timestamp_ticks("1970-01-01T00:00:00.000Z").unwrap(),
        116_444_736_000_000_000
    );
    assert!(fresh(1, 120_001));
    assert!(!fresh(1, 120_002));
    assert!(!fresh(10, 9));
}
