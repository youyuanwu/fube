//! Self-contained health scenario with independent management setup and native assertions.
use std::{
    future::{poll_fn, Future},
    path::PathBuf,
    task::Poll,
    time::Duration,
};

use mssf_client::{
    Error, FabricClient, HealthEvaluation, HealthEvent, HealthEventsFilter, HealthState,
    NodeHealth, NodeHealthPolicy, NodeHealthQueryDescription, NodeQueryDescription,
};
use serde_json::{json, Value};
use tokio::task::JoinSet;

#[path = "support/health_oracle.rs"]
mod oracle;
use oracle::*;

#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/rest.rs"]
mod rest;
#[path = "support/health_setup.rs"]
mod setup;

fn state(value: &str) -> HealthState {
    match value {
        "Invalid" => HealthState::Invalid,
        "Ok" => HealthState::Ok,
        "Warning" => HealthState::Warning,
        "Error" => HealthState::Error,
        "Unknown" => HealthState::Unknown,
        _ => panic!("unsupported independently observed health state: {value}"),
    }
}

fn assert_time(native: i64, expected: &str, end_ms: u64, dynamic: bool) {
    let expected = timestamp_ticks(expected).unwrap();
    let native = i128::from(native);
    if (native - expected).abs() < 10_000 {
        return;
    }
    // HM's LastModified accessor can advance between the management and native
    // queries without changing report identity/state. Permit only forward motion
    // inside this actual observation window, never a blanket duration tolerance.
    assert!(
        dynamic && expected > 0 && native >= expected && native <= unix_ms_ticks(end_ms) + 10_000,
        "native timestamp {native}, management {expected}, end {end_ms}, dynamic {dynamic}"
    );
}

fn assert_event(native: &HealthEvent, expected: &ObservedEvent, end_ms: u64) {
    assert_eq!(native.source_id, expected.source_id);
    assert_eq!(native.property, expected.property);
    assert_eq!(
        native.time_to_live_ticks,
        ttl_ticks(&expected.time_to_live_in_milli_seconds).unwrap()
    );
    assert_eq!(native.health_state, state(&expected.health_state));
    assert_eq!(native.description, expected.description);
    assert_eq!(
        native.sequence_number,
        expected.sequence_number.parse::<i64>().unwrap()
    );
    assert_eq!(native.is_expired, expected.is_expired);
    assert_eq!(native.remove_when_expired, expected.remove_when_expired);
    assert_time(
        native.source_utc_timestamp.0,
        &expected.source_utc_timestamp,
        end_ms,
        false,
    );
    assert_time(
        native.last_modified_utc_timestamp.0,
        &expected.last_modified_utc_timestamp,
        end_ms,
        true,
    );
    assert_time(
        native.last_ok_transition_at.0,
        &expected.last_ok_transition_at,
        end_ms,
        false,
    );
    assert_time(
        native.last_warning_transition_at.0,
        &expected.last_warning_transition_at,
        end_ms,
        false,
    );
    assert_time(
        native.last_error_transition_at.0,
        &expected.last_error_transition_at,
        end_ms,
        false,
    );
}

fn assert_health(native: &NodeHealth, snapshot: &Snapshot, end_ms: u64) {
    let expected = &snapshot.health;
    assert_eq!(native.node_name, expected.name, "{}", snapshot.label);
    assert_eq!(
        native.aggregated_health_state,
        state(&expected.aggregated_health_state),
        "{}",
        snapshot.label
    );
    assert_eq!(
        native.health_events.len(),
        expected.health_events.len(),
        "{}",
        snapshot.label
    );
    let mut seen = std::collections::HashSet::new();
    for event in &native.health_events {
        assert!(
            seen.insert((&event.source_id, &event.property)),
            "duplicate native event"
        );
        let expected = expected
            .health_events
            .iter()
            .find(|e| e.source_id == event.source_id && e.property == event.property)
            .expect("independently observed event identity");
        assert_event(event, expected, end_ms);
    }
    assert_eq!(
        native.unhealthy_evaluations.len(),
        expected.unhealthy_evaluations.len()
    );
    for (native, expected) in native
        .unhealthy_evaluations
        .iter()
        .zip(&expected.unhealthy_evaluations)
    {
        let HealthEvaluation::Event(native) = native else {
            panic!("unsupported live evaluation")
        };
        let expected = &expected.health_evaluation;
        assert_eq!(expected.kind, "Event");
        assert_eq!(native.description, expected.description);
        assert_eq!(
            native.aggregated_health_state,
            state(&expected.aggregated_health_state)
        );
        assert_eq!(
            native.consider_warning_as_error,
            expected.consider_warning_as_error
        );
        assert_event(&native.unhealthy_event, &expected.unhealthy_event, end_ms);
    }
}

fn event_evidence(event: &HealthEvent) -> Value {
    json!({
        "source_id": event.source_id, "property": event.property,
        "time_to_live_ticks": event.time_to_live_ticks.to_string(),
        "health_state": event.health_state.as_raw(), "description": event.description,
        "sequence_number": event.sequence_number.to_string(),
        "source_utc_timestamp": event.source_utc_timestamp.0.to_string(),
        "last_modified_utc_timestamp": event.last_modified_utc_timestamp.0.to_string(),
        "is_expired": event.is_expired, "remove_when_expired": event.remove_when_expired,
        "last_ok_transition_at": event.last_ok_transition_at.0.to_string(),
        "last_warning_transition_at": event.last_warning_transition_at.0.to_string(),
        "last_error_transition_at": event.last_error_transition_at.0.to_string(),
    })
}

fn health_evidence(health: &NodeHealth) -> Value {
    json!({
        "node_name": health.node_name,
        "aggregated_health_state": health.aggregated_health_state.as_raw(),
        "health_events": health.health_events.iter().map(event_evidence).collect::<Vec<_>>(),
        "unhealthy_evaluations": health.unhealthy_evaluations.iter().map(|e| match e {
            HealthEvaluation::Event(e) => json!({
                "kind": "Event", "description": e.description,
                "aggregated_health_state": e.aggregated_health_state.as_raw(),
                "consider_warning_as_error": e.consider_warning_as_error,
                "unhealthy_event": event_evidence(&e.unhealthy_event),
            }),
            _ => panic!("unsupported live evaluation"),
        }).collect::<Vec<_>>(),
    })
}

fn assert_independent_fixture(config: &Config) {
    // These predicates are from the independently planted report/native contract,
    // not inferred from any Rust query result.
    for s in &config.snapshots {
        if s.label == "healthy" {
            assert_eq!(s.health.aggregated_health_state, "Ok");
            assert!(s.health.unhealthy_evaluations.is_empty());
            continue;
        }
        if let Some(policy) = s.policy {
            assert_eq!(
                s.health.aggregated_health_state,
                if policy { "Error" } else { "Warning" }
            );
        }
        assert!(!s.health.unhealthy_evaluations.is_empty());
        assert!(s.health.unhealthy_evaluations.iter().any(|e| {
            let e = &e.health_evaluation;
            e.kind == "Event"
                && e.unhealthy_event.source_id == config.source
                && e.unhealthy_event.property == config.property
                && e.unhealthy_event.sequence_number == config.sequence.to_string()
                && e.unhealthy_event.health_state == "Warning"
                && !e.description.is_empty()
        }));
        let has_owned = s
            .health
            .health_events
            .iter()
            .any(|e| e.source_id == config.source && e.property == config.property);
        let should_have_owned = match s.filter {
            None | Some(0 | 65535) => true,
            Some(1) => false,
            Some(mask) => mask & if s.policy == Some(true) { 8 } else { 4 } != 0,
        };
        assert_eq!(
            has_owned, should_have_owned,
            "independent membership {}",
            s.label
        );
        if s.filter == Some(1) {
            assert!(s.health.health_events.is_empty());
        }
    }
}

#[tokio::test]
#[ignore = "prepares and restores an owned health report on a disposable onebox; requires MSSF_HEALTH_* and archive hash"]
async fn onebox_native_node_health() -> Result<()> {
    let (path, report) = fixture::with_cleanup(
        async {
            setup::prepare().await?;
            assert_native_node_health().await
        },
        setup::restore_owned(),
    )
    .await?;
    std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

async fn assert_native_node_health() -> Result<(PathBuf, Value)> {
    let config_path = output("MSSF_HEALTH_CONFIG")?;
    let config: Config = serde_json::from_slice(&std::fs::read(&config_path)?)?;
    config.validate_ready()?;
    remove_stale(&config.report_path)?;
    assert_eq!(
        std::env::var("MSSF_TEST_ARCHIVE_SHA256")?,
        config.archive_sha256
    );
    assert_independent_fixture(&config);
    let client =
        FabricClient::connect_clear(config.gateway.parse()?, Duration::from_secs(30)).await?;
    let mut evidence = vec![];
    for snapshot in &config.snapshots {
        assert!(
            fresh(snapshot.start_ms, now_ms()),
            "rerun preparation, observations expired"
        );
        let mut query = NodeHealthQueryDescription::new(snapshot.node_name.clone());
        query.health_policy = snapshot
            .policy
            .map(|consider_warning_as_error| NodeHealthPolicy {
                consider_warning_as_error,
            });
        query.events_filter = snapshot.filter.map(HealthEventsFilter::from_bits);
        let start_ms = now_ms();
        let health = client.get_node_health(&query).await?;
        let end_ms = now_ms();
        assert_health(&health, snapshot, end_ms);
        evidence.push(
            json!({"case": snapshot.label, "native_start_ms": start_ms, "native_end_ms": end_ms,
            "expected": snapshot, "native": health_evidence(&health)}),
        );
    }
    let error = client
        .get_node_health(&NodeHealthQueryDescription::new(&config.missing))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Remote {
                code: MISSING_CODE,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(!client.is_closed());
    let healthy_query = NodeHealthQueryDescription::new(&config.healthy);
    let reused = client
        .get_node_health_with_timeout(&healthy_query, Duration::from_secs(5))
        .await?;
    assert_health(&reused, &config.snapshots[0], now_ms());
    // Poll once then cancel, without claiming that a fast loopback necessarily
    // transmitted the request. Model tests establish deterministic late-reply safety.
    {
        let pending = client.get_node_health(&healthy_query);
        tokio::pin!(pending);
        poll_fn(|cx| {
            let _ = pending.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
    }
    let mut operations = JoinSet::new();
    for index in 0..32 {
        let client = client.clone();
        let node = config.healthy.clone();
        operations.spawn(async move {
            if index % 3 == 0 {
                assert!(!client.ping().await.unwrap().node_name.is_empty());
            } else if index % 3 == 1 {
                let health = client
                    .get_node_health(&NodeHealthQueryDescription::new(node))
                    .await
                    .unwrap();
                assert_eq!(health.aggregated_health_state, HealthState::Ok);
            } else {
                assert_eq!(
                    client
                        .get_node_list(&NodeQueryDescription::default())
                        .await
                        .unwrap()
                        .nodes
                        .len(),
                    5
                );
            }
        });
    }
    while let Some(result) = operations.join_next().await {
        result?;
    }
    let final_health = client.get_node_health(&healthy_query).await?;
    assert_health(&final_health, &config.snapshots[0], now_ms());
    assert!(!client.is_closed());
    client.close().await?;
    let report = json!({
        "profile": config.profile, "runtime_version": config.runtime_version,
        "native_source_pin": "3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d",
        "archive_sha256": config.archive_sha256, "owner": config.owner,
        "observation_max_age_ms": MAX_AGE_MS, "timestamp_precision_ticks": 10_000,
        "dynamic_tolerance": "LastModified may advance from independently observed value only up to native query end +1ms; source and transition times match <1ms",
        "cases": evidence, "unknown_node_code": MISSING_CODE,
        "unknown_node_management_code": config.missing_http_code,
        "healthy_reuse_after_error": health_evidence(&reused),
        "concurrent_operations": 32, "cancelled_after_first_poll": true,
        "deterministic_late_cancellation": "offline model evidence, not guaranteed live wire emission",
        "final_healthy_reuse": health_evidence(&final_health),
        "completed_at_unix_ms": now_ms(),
    });
    Ok((config.report_path, report))
}

#[test]
fn health_snapshot_cases_cover_policy_and_raw_filter_boundaries() {
    let cases = cases();
    assert_eq!(cases.len(), 17);
    assert!(cases.iter().any(|c| c.2.is_none() && c.3.is_none()));
    assert!(cases.iter().any(|c| c.2 == Some(true) && c.3 == Some(8)));
    assert!(cases.iter().any(|c| c.3 == Some(0x8000_0008)));
    assert!(cases.iter().any(|c| c.3 == Some(0x8000_0000)));
}

#[test]
fn health_timestamp_comparison_only_allows_documented_forward_modified_window() {
    let rendered = "2026-10-22T00:00:00.000Z";
    let ticks: i64 = timestamp_ticks(rendered).unwrap().try_into().unwrap();
    let end_ms = 1_792_627_200_100;
    assert_time(ticks + 9_999, rendered, end_ms, false);
    assert_time(ticks + 100_000, rendered, end_ms, true);
    for (native, dynamic) in [
        (ticks + 10_000, false),
        (ticks - 10_000, true),
        (ticks + 10_000_000, true),
    ] {
        assert!(
            std::panic::catch_unwind(|| assert_time(native, rendered, end_ms, dynamic)).is_err()
        );
    }
    assert!(std::panic::catch_unwind(|| {
        assert_time(ticks, "0001-01-01T00:00:00.000Z", end_ms, true);
    })
    .is_err());
}
