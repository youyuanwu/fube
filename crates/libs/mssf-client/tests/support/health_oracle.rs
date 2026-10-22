//! Test-only management observations. Never used by the production client.
#![allow(dead_code)] // Shared by independent preparation and native assertions.

use std::{error::Error, path::PathBuf};

use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub const MANAGEMENT: &str = "http://127.0.0.1:19080";
pub const MAX_AGE_MS: u64 = 120_000;
pub const MISSING_CODE: i64 = -2147017705; // Pinned FABRIC_E_HEALTH_ENTITY_NOT_FOUND.

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

pub fn output(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(std::env::var(name)?);
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.is_dir()
        || !path.parent().is_some_and(|parent| parent.is_dir())
    {
        return Err(format!("{name} must be an absolute file with an existing parent").into());
    }
    Ok(path)
}

pub fn remove_stale(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub profile: String,
    pub gateway: String,
    pub runtime_version: String,
    pub archive_sha256: String,
    pub owner: String,
    pub source: String,
    pub property: String,
    pub target: String,
    pub healthy: String,
    pub missing: String,
    pub sequence: i64,
    pub ttl_ms: i64,
    pub description: String,
    pub report_path: PathBuf,
    pub observed_at_unix_ms: u64,
    pub missing_http_code: String,
    pub snapshots: Vec<Snapshot>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.profile != "clear"
            || self.gateway != "127.0.0.1:19000"
            || self.runtime_version.trim().is_empty()
            || self.archive_sha256.len() != 64
            || !self.archive_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || self.owner.is_empty()
            || self.source != format!("NativeHealth-{}", self.owner)
            || !self.property.starts_with("owned-")
            || self.target != "N0020"
            || self.healthy != "N0010"
            || self.missing != format!("missing-{}", self.property)
            || self.sequence <= 0
            || self.sequence == i64::MAX
            || self.ttl_ms != 1_800_000
            || self.description != "Disposable native node health qualification warning"
            || !self.report_path.is_absolute()
        {
            return Err("invalid private health fixture identity/profile".into());
        }
        Ok(())
    }

    pub fn validate_ready(&self) -> Result<()> {
        self.validate()?;
        if !fresh(self.observed_at_unix_ms, now_ms())
            || self.missing_http_code != "FABRIC_E_HEALTH_ENTITY_NOT_FOUND"
            || self.snapshots.len() != cases().len()
        {
            return Err(
                "health observations incomplete/stale; rerun the onebox health test".into(),
            );
        }
        for (snapshot, (label, healthy, policy, filter)) in self.snapshots.iter().zip(cases()) {
            if snapshot.label != label
                || &snapshot.node_name != if healthy { &self.healthy } else { &self.target }
                || snapshot.policy != policy
                || snapshot.filter != filter
                || snapshot.start_ms > snapshot.end_ms
                || !fresh(snapshot.start_ms, now_ms())
                || snapshot.end_ms > self.observed_at_unix_ms
            {
                return Err(
                    "health snapshot coordinates/windows do not match required cases".into(),
                );
            }
        }
        Ok(())
    }
}

pub fn fresh(observed: u64, now: u64) -> bool {
    observed > 0
        && now
            .checked_sub(observed)
            .is_some_and(|age| age <= MAX_AGE_MS)
}

pub type Case = (&'static str, bool, Option<bool>, Option<u32>);
pub fn cases() -> Vec<Case> {
    vec![
        ("healthy", true, None, None),
        ("inherited", false, None, None),
        ("false-omitted", false, Some(false), None),
        ("true-omitted", false, Some(true), None),
        ("default", false, Some(false), Some(0)),
        ("all", false, Some(false), Some(65535)),
        ("none", false, Some(false), Some(1)),
        ("ok", false, Some(false), Some(2)),
        ("warning", false, Some(false), Some(4)),
        ("error-false", false, Some(false), Some(8)),
        ("combined", false, Some(false), Some(12)),
        ("error-true", false, Some(true), Some(8)),
        ("warning-true", false, Some(true), Some(4)),
        ("none-true", false, Some(true), Some(1)),
        ("none-or-error", false, Some(true), Some(9)),
        ("raw-error", false, Some(true), Some(0x8000_0008)),
        ("raw-only", false, Some(true), Some(0x8000_0000)),
    ]
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub label: String,
    pub node_name: String,
    pub policy: Option<bool>,
    pub filter: Option<u32>,
    pub start_ms: u64,
    pub end_ms: u64,
    pub health: ObservedHealth,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedHealth {
    pub name: String,
    pub health_events: Vec<ObservedEvent>,
    pub aggregated_health_state: String,
    pub unhealthy_evaluations: Vec<ObservedEvaluationWrapper>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedEvent {
    pub source_id: String,
    pub property: String,
    pub time_to_live_in_milli_seconds: String,
    pub health_state: String,
    pub description: String,
    pub sequence_number: String,
    pub source_utc_timestamp: String,
    pub last_modified_utc_timestamp: String,
    pub is_expired: bool,
    pub remove_when_expired: bool,
    pub last_ok_transition_at: String,
    pub last_warning_transition_at: String,
    pub last_error_transition_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedEvaluationWrapper {
    pub health_evaluation: ObservedEvaluation,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObservedEvaluation {
    pub kind: String,
    pub description: String,
    pub aggregated_health_state: String,
    pub unhealthy_event: ObservedEvent,
    pub consider_warning_as_error: bool,
}

/// Exact ISO TimeSpan arithmetic, including the native i64::MAX sentinel.
/// Management API 6.3 emits ISO values despite its legacy property name.
pub fn ttl_ticks(text: &str) -> Result<i64> {
    if !text.starts_with('P') {
        return Ok(text
            .parse::<i64>()?
            .checked_mul(10_000)
            .ok_or("TTL overflow")?);
    }
    let mut value = 0i128;
    let mut number = String::new();
    for c in text[1..].chars() {
        match c {
            'T' if number.is_empty() => {}
            '0'..='9' | '.' => number.push(c),
            'D' | 'H' | 'M' | 'S' => {
                if number.is_empty() {
                    return Err("missing ISO duration value".into());
                }
                let multiplier: i128 = match c {
                    'D' => 864_000_000_000,
                    'H' => 36_000_000_000,
                    'M' => 600_000_000,
                    _ => 10_000_000,
                };
                let (whole, fraction) = number.split_once('.').unwrap_or((&number, ""));
                if fraction.len() > 7 || (c != 'S' && !fraction.is_empty()) {
                    return Err("unsupported ISO duration precision".into());
                }
                value = value
                    .checked_add(
                        whole
                            .parse::<i128>()?
                            .checked_mul(multiplier)
                            .ok_or("TTL overflow")?,
                    )
                    .ok_or("TTL overflow")?;
                if !fraction.is_empty() {
                    value = value
                        .checked_add(
                            fraction.parse::<i128>()? * 10i128.pow(7 - fraction.len() as u32),
                        )
                        .ok_or("TTL overflow")?;
                }
                number.clear();
            }
            _ => return Err("unsupported ISO duration".into()),
        }
    }
    if !number.is_empty() {
        return Err("unterminated ISO duration".into());
    }
    Ok(value.try_into()?)
}

pub fn timestamp_ticks(text: &str) -> Result<i128> {
    if text == "0001-01-01T00:00:00.000Z" {
        return Ok(0);
    }
    let date = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)?;
    Ok(date.unix_timestamp_nanos() / 100 + 116_444_736_000_000_000i128)
}

pub fn unix_ms_ticks(ms: u64) -> i128 {
    i128::from(ms) * 10_000 + 116_444_736_000_000_000i128
}
