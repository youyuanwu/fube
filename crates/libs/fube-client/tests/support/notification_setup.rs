//! Explicit owned-allocation preparation for native notification qualification.
use std::{
    env::VarError,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::oracle::{self, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub profile: String,
    pub allocation: String,
    pub allocation_owner: String,
    pub endpoint: std::net::SocketAddr,
    pub runtime: String,
    pub owner: String,
    pub journal: PathBuf,
    pub report: PathBuf,
}

fn allocation_state(root: &Path, allocation: &str) -> Result<Value> {
    if allocation.is_empty()
        || !allocation
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err("invalid task-owned allocation name".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(
        root.join("build/oneboxes")
            .join(allocation)
            .join("state.json"),
    )?)?)
}

impl Config {
    fn validate(&self, root: &Path, path: &Path, owner: &str) -> Result<()> {
        if !path.is_absolute()
            || path.file_name().is_none()
            || path.is_dir()
            || !path.parent().is_some_and(Path::is_dir)
        {
            return Err("explicit absolute notification profile required".into());
        }
        if self.owner != owner {
            return Err("explicit exact workload owner mismatch".into());
        }
        if self.profile != "clear"
            || !self.endpoint.ip().is_loopback()
            || self.endpoint.port() != 19000
        {
            return Err("test requires explicitly selected loopback clear profile".into());
        }
        if self.journal == self.report
            || self.journal == path
            || self.report == path
            || self.report.exists()
            || self.journal.exists()
            || [&self.journal, &self.report]
                .iter()
                .any(|p| !p.is_absolute() || !p.parent().is_some_and(Path::is_dir))
        {
            return Err("requires fresh distinct report/journal paths".into());
        }
        let state = allocation_state(root, &self.allocation)?;
        if self.allocation_owner.is_empty()
            || self.runtime.is_empty()
            || state["name"] != self.allocation
            || state["project"] != format!("sf-onebox-{}", self.allocation)
            || state["owner"] != self.allocation_owner
            || state["runtime_version"] != self.runtime
            || state["phase"] != "ready"
        {
            return Err("allocation identity mismatch; refusing live mutations".into());
        }
        Ok(())
    }
}

fn load(root: &Path, path: &Path, owner: &str) -> Result<Config> {
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    config.validate(root, path, owner)?;
    Ok(config)
}

fn prepare(
    root: &Path,
    path: &Path,
    allocation: &str,
    owner: &str,
    journal: PathBuf,
    report: PathBuf,
) -> Result<Config> {
    let state = allocation_state(root, allocation)?;
    let config = Config {
        profile: "clear".into(),
        allocation: allocation.into(),
        allocation_owner: state["owner"]
            .as_str()
            .ok_or("allocation record has no owner")?
            .into(),
        endpoint: "127.0.0.1:19000".parse()?,
        runtime: state["runtime_version"]
            .as_str()
            .ok_or("allocation record has no runtime")?
            .into(),
        owner: owner.into(),
        journal,
        report,
    };
    config.validate(root, path, owner)?;
    let bytes = serde_json::to_vec_pretty(&config)?;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?
        .write_all(&bytes)?;
    Ok(config)
}

pub(super) fn config() -> Result<Config> {
    let path = oracle::output("MSSF_NOTIFICATION_CONFIG")?;
    let owner = oracle::owner()?;
    // Archive consumers execute from the remapped package cwd, not the producer's root.
    let root = std::env::current_dir()?.join("../../..");
    match std::env::var("MSSF_NOTIFICATION_ALLOCATION") {
        Ok(allocation) => prepare(
            &root,
            &path,
            &allocation,
            &owner,
            oracle::output("MSSF_NOTIFICATION_JOURNAL")?,
            oracle::output("MSSF_NOTIFICATION_REPORT")?,
        ),
        Err(VarError::NotPresent) => load(&root, &path, &owner),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ALLOCATION: &str = "ci-owned";
    const OWNER: &str = "workload-owner";

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("notification-setup-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(root.join("build/oneboxes").join(ALLOCATION)).unwrap();
            std::fs::create_dir(root.join("evidence")).unwrap();
            let fixture = Self(root);
            fixture.state(json!({
                "name": ALLOCATION,
                "project": format!("sf-onebox-{ALLOCATION}"),
                "owner": "controller-owner",
                "runtime_version": "11.8.121.4",
                "phase": "ready",
            }));
            fixture
        }
        fn state(&self, state: Value) {
            std::fs::write(
                self.0
                    .join("build/oneboxes")
                    .join(ALLOCATION)
                    .join("state.json"),
                serde_json::to_vec(&state).unwrap(),
            )
            .unwrap();
        }
        fn path(&self, name: &str) -> PathBuf {
            self.0.join("evidence").join(name)
        }
        fn prepare(&self) -> Result<Config> {
            prepare(
                &self.0,
                &self.path("config.json"),
                ALLOCATION,
                OWNER,
                self.path("journal.json"),
                self.path("report.json"),
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn rust_preparation_writes_the_owned_profile_consumed_by_manual_loading() {
        let fixture = Fixture::new();
        let config = fixture.prepare().unwrap();
        assert_eq!(config.owner, OWNER);
        assert_eq!(config.allocation, ALLOCATION);
        assert_eq!(config.allocation_owner, "controller-owner");
        assert_eq!(config.runtime, "11.8.121.4");
        assert_eq!(config.endpoint, "127.0.0.1:19000".parse().unwrap());
        assert_eq!(config.journal, fixture.path("journal.json"));
        assert_eq!(config.report, fixture.path("report.json"));
        let bytes = std::fs::read(fixture.path("config.json")).unwrap();
        let loaded = load(&fixture.0, &fixture.path("config.json"), OWNER).unwrap();
        assert_eq!(loaded.allocation_owner, config.allocation_owner);
        assert_eq!(std::fs::read(fixture.path("config.json")).unwrap(), bytes);
        assert!(!config.journal.exists());
        assert!(!config.report.exists());
    }

    #[test]
    fn preparation_never_overwrites_an_existing_profile_or_evidence() {
        for name in ["config.json", "journal.json", "report.json"] {
            let fixture = Fixture::new();
            let path = fixture.path(name);
            std::fs::write(&path, b"existing").unwrap();
            assert!(fixture.prepare().is_err());
            assert_eq!(std::fs::read(path).unwrap(), b"existing");
            if name != "config.json" {
                assert!(!fixture.path("config.json").exists());
            }
        }
    }

    #[test]
    fn manual_profiles_still_reject_foreign_owner_and_stale_allocation_identity() {
        for (field, value) in [
            ("name", "another-allocation"),
            ("project", "another-project"),
            ("owner", "another-owner"),
            ("runtime_version", "another-runtime"),
            ("phase", "starting"),
        ] {
            let fixture = Fixture::new();
            fixture.prepare().unwrap();
            assert!(load(&fixture.0, &fixture.path("config.json"), "another-owner").is_err());
            let state_path = fixture
                .0
                .join("build/oneboxes")
                .join(ALLOCATION)
                .join("state.json");
            let mut state: Value =
                serde_json::from_slice(&std::fs::read(state_path).unwrap()).unwrap();
            state[field] = json!(value);
            fixture.state(state);
            assert!(load(&fixture.0, &fixture.path("config.json"), OWNER).is_err());
        }
    }

    #[test]
    fn preparation_requires_ready_complete_owned_state() {
        for (field, value) in [
            ("name", json!("wrong")),
            ("project", json!("wrong")),
            ("phase", json!("starting")),
            ("owner", json!("")),
            ("owner", Value::Null),
            ("runtime_version", json!("")),
            ("runtime_version", Value::Null),
        ] {
            let fixture = Fixture::new();
            let path = fixture
                .0
                .join("build/oneboxes")
                .join(ALLOCATION)
                .join("state.json");
            let mut state: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            state[field] = value;
            fixture.state(state);
            assert!(fixture.prepare().is_err());
            assert!(!fixture.path("config.json").exists());
        }
    }

    #[test]
    fn invalid_allocation_names_and_missing_state_cannot_prepare_profiles() {
        let fixture = Fixture::new();
        for allocation in ["", "../ci-owned", "UPPER", "missing"] {
            assert!(prepare(
                &fixture.0,
                &fixture.path("config.json"),
                allocation,
                OWNER,
                fixture.path("journal.json"),
                fixture.path("report.json"),
            )
            .is_err());
            assert!(!fixture.path("config.json").exists());
        }
    }

    #[test]
    fn preparation_requires_absolute_distinct_outputs_in_existing_directories() {
        let fixture = Fixture::new();
        for (profile, journal, report) in [
            (
                PathBuf::from("config.json"),
                fixture.path("journal.json"),
                fixture.path("report.json"),
            ),
            (
                fixture.path("config.json"),
                PathBuf::from("journal.json"),
                fixture.path("report.json"),
            ),
            (
                fixture.path("config.json"),
                fixture.path("journal.json"),
                PathBuf::from("report.json"),
            ),
            (
                fixture.path("config.json"),
                fixture.path("same.json"),
                fixture.path("same.json"),
            ),
            (
                fixture.path("config.json"),
                fixture.path("config.json"),
                fixture.path("report.json"),
            ),
            (
                fixture.path("config.json"),
                fixture.path("journal.json"),
                fixture.path("config.json"),
            ),
            (
                fixture.path("config.json"),
                fixture.path("missing/journal.json"),
                fixture.path("report.json"),
            ),
        ] {
            assert!(prepare(&fixture.0, &profile, ALLOCATION, OWNER, journal, report).is_err());
            assert!(!fixture.path("config.json").exists());
        }
    }

    #[test]
    fn manual_profiles_still_require_the_explicit_loopback_clear_profile() {
        let fixture = Fixture::new();
        let config = fixture.prepare().unwrap();
        for (profile, endpoint) in [
            ("tls", "127.0.0.1:19000"),
            ("clear", "192.0.2.1:19000"),
            ("clear", "127.0.0.1:19080"),
        ] {
            let mut config = config.clone();
            config.profile = profile.into();
            config.endpoint = endpoint.parse().unwrap();
            std::fs::write(
                fixture.path("config.json"),
                serde_json::to_vec(&config).unwrap(),
            )
            .unwrap();
            assert!(load(&fixture.0, &fixture.path("config.json"), OWNER).is_err());
        }
    }
}
