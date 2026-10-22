//! Test-only name ownership journal and independent HTTP observation/cleanup.
use std::{
    error::Error,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::{redirect::Policy, Client, StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub const DUPLICATE: i64 = -2147017792;
pub const MISSING: i64 = -2147017791;
const MANAGEMENT: &str = "http://127.0.0.1:19080";

pub fn output(variable: &str) -> Result<PathBuf> {
    let path = PathBuf::from(std::env::var(variable)?);
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.is_dir()
        || !path.parent().is_some_and(Path::is_dir)
    {
        return Err(
            format!("{variable} requires an absolute file in an existing directory").into(),
        );
    }
    Ok(path)
}

pub fn owner() -> Result<String> {
    let owner = std::env::var("MSSF_NAME_OWNER")?;
    validate_owner(&owner)?;
    Ok(owner)
}

fn validate_owner(owner: &str) -> Result<()> {
    if owner.is_empty()
        || owner.len() > 64
        || !owner
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err("name owner must be a unique task-owned ASCII identifier".into());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Stage {
    Reserved,
    Absent,
    Pending,
    Settled,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    owner: String,
    nonce: String,
    pub name: String,
    pub archive_sha256: String,
    pub stage: Stage,
}

impl Journal {
    pub fn new(owner: String, archive_sha256: String) -> Result<Self> {
        let nonce = Uuid::new_v4().simple().to_string();
        let journal = Self {
            name: format!("fabric:/native-names-{owner}-{nonce}"),
            owner,
            nonce,
            archive_sha256,
            stage: Stage::Reserved,
        };
        journal.validate(&journal.owner)?;
        Ok(journal)
    }

    fn validate(&self, owner: &str) -> Result<()> {
        validate_owner(owner)?;
        let nonce = Uuid::parse_str(&self.nonce)?;
        if self.owner != owner
            || nonce.get_version_num() != 4
            || self.nonce != nonce.simple().to_string()
            || self.name != format!("fabric:/native-names-{owner}-{}", self.nonce)
            || self.archive_sha256.len() != 64
            || !self.archive_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("refusing unrelated/invalid name ownership journal".into());
        }
        Ok(())
    }

    pub fn load(path: &Path, owner: &str) -> Result<Self> {
        let journal: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        journal.validate(owner)?;
        Ok(journal)
    }

    pub fn reserve(&self, path: &Path) -> Result<()> {
        // Never overwrite a prior candidate, including a same-owner uncertain write.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn record(&mut self, path: &Path, stage: Stage) -> Result<()> {
        let previous = Self::load(path, &self.owner)?;
        if previous.name != self.name
            || !matches!(
                (previous.stage, stage),
                (Stage::Reserved, Stage::Absent)
                    | (Stage::Absent | Stage::Settled, Stage::Pending)
                    | (Stage::Pending, Stage::Settled)
            )
        {
            return Err("invalid name ownership transition".into());
        }
        // Replace atomically: a failed save must not erase pre-write ownership.
        let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            self.stage = stage;
            file.write_all(&serde_json::to_vec_pretty(self)?)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            // Windows cannot open directories as files; retain POSIX rename durability.
            #[cfg(unix)]
            std::fs::File::open(path.parent().ok_or("journal has no parent")?)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }
}

pub trait NameManagement {
    async fn exists(&self, name: &str) -> Result<bool>;
    async fn delete(&self, name: &str) -> Result<()>;
}

pub struct Http(Client, String);

impl Http {
    pub fn new() -> Result<Self> {
        Ok(Self(
            Client::builder()
                .no_proxy()
                .redirect(Policy::none())
                .timeout(Duration::from_secs(10))
                .build()?,
            MANAGEMENT.into(),
        ))
    }

    pub async fn runtime(&self) -> Result<String> {
        let nodes: serde_json::Value = self
            .0
            .get(format!("{}/Nodes?api-version=6.3", self.1))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let items = nodes["Items"]
            .as_array()
            .ok_or("missing independent nodes")?;
        if items.len() != 5 || items.iter().any(|node| node["NodeStatus"] != "Up") {
            return Err("requires five independently observed Up nodes".into());
        }
        Ok(items[0]["CodeVersion"]
            .as_str()
            .ok_or("missing independent runtime version")?
            .into())
    }

    fn name_url(&self, name: &str) -> String {
        // Journal validation guarantees one literal ASCII segment (no URL escaping).
        format!("{}/Names/{}", self.1, &name["fabric:/".len()..])
    }
}

impl NameManagement for Http {
    async fn exists(&self, name: &str) -> Result<bool> {
        let response = self
            .0
            .get(format!("{}?api-version=6.0", self.name_url(name)))
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            // NamesHandler::OnNameExistsComplete returns bare 404 for absence,
            // not an Error.Code JSON envelope (unlike health query failures).
            StatusCode::NOT_FOUND => Ok(false),
            status => Err(format!("unexpected independent name status: {status}").into()),
        }
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let response = self
            .0
            .delete(format!("{}?api-version=6.0", self.name_url(name)))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(format!("owned name cleanup returned {}", response.status()).into());
        }
        Ok(())
    }
}

pub async fn confirm_absent(
    journal: &mut Journal,
    path: &Path,
    management: &impl NameManagement,
) -> Result<()> {
    if management.exists(&journal.name).await? {
        return Err("unique name collision: refusing to adopt or delete existing name".into());
    }
    journal.record(path, Stage::Absent)
}

pub async fn cleanup(path: &Path, owner: &str, management: &impl NameManagement) -> Result<()> {
    if !path.try_exists()? {
        return Ok(());
    }
    let journal = Journal::load(path, owner)?;
    if journal.stage != Stage::Reserved {
        if management.exists(&journal.name).await? {
            management.delete(&journal.name).await?;
        }
        if management.exists(&journal.name).await? {
            return Err("owned name remains after cleanup; ownership retained".into());
        }
        if journal.stage == Stage::Pending {
            return Err(
                "write outcome uncertain: absence is not a terminal boundary; retain journal until exact allocation destruction"
                    .into(),
            );
        }
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("name-fixture-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn path(&self) -> PathBuf {
            self.0.join("journal.json")
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    #[derive(Default)]
    struct Model {
        present: Cell<bool>,
        deletes: Cell<usize>,
        fail_observation: Cell<bool>,
        fail_delete: Cell<bool>,
    }
    impl NameManagement for Model {
        async fn exists(&self, _: &str) -> Result<bool> {
            if self.fail_observation.get() {
                return Err("observation failed".into());
            }
            Ok(self.present.get())
        }
        async fn delete(&self, _: &str) -> Result<()> {
            self.deletes.set(self.deletes.get() + 1);
            if self.fail_delete.get() {
                return Err("deletion failed".into());
            }
            self.present.set(false);
            Ok(())
        }
    }
    fn reserved(path: &Path) -> Journal {
        let journal = Journal::new("test-owner".into(), "a".repeat(64)).unwrap();
        journal.reserve(path).unwrap();
        journal
    }

    #[test]
    fn journal_transitions_replace_the_existing_file_and_survive_reload() {
        let dir = Directory::new();
        let path = dir.path();
        let journal = reserved(&path);
        let name = journal.name.clone();
        drop(journal);
        for stage in [
            Stage::Absent,
            Stage::Pending,
            Stage::Settled,
            Stage::Pending,
            Stage::Settled,
        ] {
            let mut journal = Journal::load(&path, "test-owner").unwrap();
            journal.record(&path, stage).unwrap();
            drop(journal);
            let saved = Journal::load(&path, "test-owner").unwrap();
            assert_eq!(saved.stage, stage);
            assert_eq!(saved.name, name);
            assert_eq!(saved.archive_sha256, "a".repeat(64));
            assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1);
        }
    }

    #[tokio::test]
    async fn independent_http_uses_empty_existence_statuses_and_exact_delete_route() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (method, status) in [
                ("GET", "404 Not Found"),
                ("GET", "200 OK"),
                ("DELETE", "200 OK"),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with(&format!(
                    "{method} /Names/native-names-owned?api-version=6.0 HTTP/1.1\r\n"
                )));
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let http = Http(Http::new().unwrap().0, format!("http://{endpoint}"));
        assert!(!http.exists("fabric:/native-names-owned").await.unwrap());
        assert!(http.exists("fabric:/native-names-owned").await.unwrap());
        http.delete("fabric:/native-names-owned").await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn partial_preparation_and_collision_never_adopt_or_delete() {
        let dir = Directory::new();
        let path = dir.path();
        let mut journal = reserved(&path);
        let model = Model::default();
        model.present.set(true);
        assert!(confirm_absent(&mut journal, &path, &model).await.is_err());
        cleanup(&path, "test-owner", &model).await.unwrap();
        assert!(model.present.get());
        assert_eq!(model.deletes.get(), 0);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn preparation_failure_after_absence_and_settled_mutation_cleanup() {
        for mutated in [false, true] {
            let dir = Directory::new();
            let path = dir.path();
            let mut journal = reserved(&path);
            let model = Model::default();
            confirm_absent(&mut journal, &path, &model).await.unwrap();
            if mutated {
                journal.record(&path, Stage::Pending).unwrap();
                model.present.set(true);
                journal.record(&path, Stage::Settled).unwrap();
            }
            cleanup(&path, "test-owner", &model).await.unwrap();
            assert!(!model.present.get());
            assert_eq!(model.deletes.get(), usize::from(mutated));
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn absence_then_late_create_retains_uncertain_ownership() {
        let dir = Directory::new();
        let path = dir.path();
        let mut journal = reserved(&path);
        let model = Model::default();
        confirm_absent(&mut journal, &path, &model).await.unwrap();
        journal.record(&path, Stage::Pending).unwrap();
        assert!(cleanup(&path, "test-owner", &model).await.is_err());
        assert!(path.exists()); // Current absence is not proof of nonexecution.
        model.present.set(true); // The abandoned native create commits later.
        assert!(cleanup(&path, "test-owner", &model).await.is_err());
        assert!(!model.present.get()); // Best effort only; journal still retained.
        assert_eq!(
            Journal::load(&path, "test-owner").unwrap().stage,
            Stage::Pending
        );
    }

    #[tokio::test]
    async fn failed_observation_or_delete_preserves_exact_owner() {
        for observation in [false, true] {
            let dir = Directory::new();
            let path = dir.path();
            let mut journal = reserved(&path);
            let model = Model::default();
            confirm_absent(&mut journal, &path, &model).await.unwrap();
            model.present.set(true);
            model.fail_observation.set(observation);
            model.fail_delete.set(!observation);
            assert!(cleanup(&path, "test-owner", &model).await.is_err());
            assert_eq!(
                Journal::load(&path, "test-owner").unwrap().name,
                journal.name
            );
            assert!(model.present.get());
        }
    }

    #[tokio::test]
    async fn existing_or_foreign_journal_is_never_overwritten_or_cleaned() {
        let dir = Directory::new();
        let path = dir.path();
        let journal = reserved(&path);
        assert!(journal.reserve(&path).is_err());
        let model = Model::default();
        model.present.set(true);
        assert!(cleanup(&path, "other-owner", &model).await.is_err());
        assert!(path.exists());
        assert!(model.present.get());
        assert_eq!(model.deletes.get(), 0);
    }

    #[tokio::test]
    async fn uncertain_native_error_reports_after_cleanup_and_retains_journal() {
        let dir = Directory::new();
        let path = dir.path();
        let mut journal = reserved(&path);
        let model = Model::default();
        confirm_absent(&mut journal, &path, &model).await.unwrap();
        journal.record(&path, Stage::Pending).unwrap();
        model.present.set(true);
        let outcome = super::super::fixture::with_cleanup::<()>(
            async { Err("native write disconnected".into()) },
            cleanup(&path, "test-owner", &model),
        )
        .await;
        let error = outcome.unwrap_err().to_string();
        assert!(error.contains("native write disconnected"));
        assert!(error.contains("fixture cleanup failed: write outcome uncertain"));
        assert_eq!(model.deletes.get(), 1);
        assert!(!model.present.get());
        assert_eq!(
            Journal::load(&path, "test-owner").unwrap().stage,
            Stage::Pending
        );
    }

    #[tokio::test]
    async fn assertion_panic_after_mutation_cleans_owned_name_before_reporting() {
        let dir = Directory::new();
        let path = dir.path();
        let scenario_path = path.clone();
        let mut journal = reserved(&path);
        let model = Model::default();
        confirm_absent(&mut journal, &path, &model).await.unwrap();
        journal.record(&path, Stage::Pending).unwrap();
        model.present.set(true);
        journal.record(&path, Stage::Settled).unwrap();
        // The shared wrapper resumes the original panic only after real journal cleanup.
        let error = tokio::task::LocalSet::new()
            .run_until(async move {
                tokio::task::spawn_local(async move {
                    super::super::fixture::with_cleanup::<()>(
                        async { panic!("native lifecycle assertion") },
                        async {
                            cleanup(&scenario_path, "test-owner", &model).await?;
                            assert!(!model.present.get());
                            Ok(())
                        },
                    )
                    .await
                })
                .await
                .unwrap_err()
            })
            .await;
        assert!(error.is_panic());
        assert!(!path.exists());
    }
}
