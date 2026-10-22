//! Test-only HTTP preparation; actual client operations use native TCP.

use std::{error::Error, path::PathBuf, time::Duration};

use reqwest::{redirect::Policy, Client, Response, StatusCode};
use serde::Deserialize;

use super::support;
use support::ObservedNode;

const MANAGEMENT: &str = "http://127.0.0.1:19080";
const EXISTING: &str = "fabric:/native-client-ci-existing";
const ABSENT: &str = "fabric:/native-client-ci-absent";

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Nodes {
    items: Vec<ObservedNode>,
    continuation_token: String,
}

fn absolute_output(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || path.file_name().is_none() || path.is_dir() {
        return Err("fixture output must be an absolute file path".into());
    }
    if !path.parent().is_some_and(|parent| parent.is_dir()) {
        return Err("fixture output directory must already exist".into());
    }
    Ok(path)
}

fn output_from_env(name: &str) -> Result<PathBuf> {
    let value = std::env::var(name).map_err(|error| format!("{name}: {error}"))?;
    absolute_output(&value)
}

fn successful(response: Response) -> Result<Response> {
    let response = response.error_for_status()?;
    if !response.status().is_success() {
        return Err(format!("unexpected management HTTP status: {}", response.status()).into());
    }
    Ok(response)
}

pub async fn prepare() -> Result<()> {
    let config_path = output_from_env("MSSF_TEST_CONFIG")?;
    let report_path = output_from_env("MSSF_TEST_REPORT")?;
    if config_path == report_path {
        return Err("configuration and report paths must differ".into());
    }
    if report_path.exists() {
        std::fs::remove_file(&report_path)?;
    }
    // A failed rerun must not leave an older successful observation usable.
    if config_path.exists() {
        std::fs::remove_file(&config_path)?;
    }
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?;

    let created = client
        .post(format!("{MANAGEMENT}/Names/$/Create?api-version=6.0"))
        .json(&serde_json::json!({"Name": EXISTING}))
        .send()
        .await?;
    // Re-running independent observation before each archive execution leg is
    // required for freshness. Accept only success or an already-existing name;
    // the following GET independently confirms the naming prerequisite.
    if created.status() != StatusCode::CONFLICT {
        successful(created)?;
    }
    successful(
        client
            .get(format!(
                "{MANAGEMENT}/Names/native-client-ci-existing?api-version=6.0"
            ))
            .send()
            .await?,
    )?;
    let absent = client
        .get(format!(
            "{MANAGEMENT}/Names/native-client-ci-absent?api-version=6.0"
        ))
        .send()
        .await?;
    if absent.status() != StatusCode::NOT_FOUND {
        return Err(format!(
            "absent naming fixture returned {}, expected 404",
            absent.status()
        )
        .into());
    }
    let observed_at_unix_ms = support::unix_millis();
    let mut items = Vec::new();
    let mut token = String::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..128 {
        let mut request = client.get(format!("{MANAGEMENT}/Nodes?api-version=6.3"));
        if !token.is_empty() {
            request = request.query(&[("ContinuationToken", &token)]);
        }
        let page: Nodes = successful(request.send().await?)?.json().await?;
        items.extend(page.items);
        if items.len() > 4096 {
            return Err("management membership exceeds test observation bound".into());
        }
        token = page.continuation_token;
        if token.is_empty() {
            break;
        }
        if !seen.insert(token.clone()) {
            return Err("management continuation repeated".into());
        }
    }
    if !token.is_empty() {
        return Err("management paging exceeded test bound".into());
    }
    if items.len() != 5
        || items
            .iter()
            .any(|node| node.node_status != "Up" || node.health_state != "Ok")
    {
        return Err("qualification requires the healthy five-logical-node onebox profile".into());
    }
    let node = items
        .iter()
        .find(|node| node.name == "N0010")
        .ok_or("management response is missing gateway node N0010")?;
    let config = serde_json::json!({
        "profile": "clear",
        "gateway": "127.0.0.1:19000",
        "runtime_version": node.code_version,
        "existing_name": EXISTING,
        "absent_name": ABSENT,
        "expected_node_name": node.name,
        "expected_node_id": node.id.id,
        "expected_node_instance": node.instance_id.parse::<u64>()?,
        "observed_at_unix_ms": observed_at_unix_ms,
        "expected_nodes": items,
        "report_path": report_path,
    });
    std::fs::write(config_path, serde_json::to_vec_pretty(&config)?)?;
    Ok(())
}

#[test]
fn fixture_outputs_require_explicit_absolute_paths() {
    assert!(absolute_output("relative-config.json").is_err());
    let directory = std::env::temp_dir();
    assert!(absolute_output(directory.to_str().unwrap()).is_err());
    let output = directory.join("mssf-native-client-config.json");
    assert_eq!(absolute_output(output.to_str().unwrap()).unwrap(), output);
    let missing_parent = directory
        .join(uuid::Uuid::new_v4().to_string())
        .join("config.json");
    assert!(absolute_output(missing_parent.to_str().unwrap()).is_err());
}
