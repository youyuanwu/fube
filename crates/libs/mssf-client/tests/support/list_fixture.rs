//! Test-only multi-resource ownership and HTTP workload preparation/oracle.
use std::{
    error::Error,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use openapi_service_fabric::{
    apis::{
        application_api, application_type_api, configuration::Configuration, image_store_api,
        Error as ApiError,
    },
    models::{
        ApplicationDescription, ApplicationParameter, UnprovisionApplicationTypeDescriptionInfo,
    },
};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
use super::rest;

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
    let owner = std::env::var("MSSF_LIST_OWNER")?;
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
        return Err("invalid exact workload owner".into());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stage {
    Reserved,
    Absent,
    Pending,
    Settled,
    Cleaned,
}

/// Slots are upload root, exact type/version, first app, second app.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    pub owner: String,
    nonce: String,
    pub archive_sha256: String,
    pub type_name: String,
    pub upload: String,
    pub applications: [String; 2],
    pub stages: [Stage; 4],
}

impl Journal {
    pub fn new(owner: String, archive_sha256: String) -> Result<Self> {
        let nonce = Uuid::new_v4().simple().to_string();
        let stem = format!("native-lists-{owner}-{nonce}");
        let value = Self {
            owner,
            nonce,
            archive_sha256,
            type_name: format!("ListType-{stem}"),
            upload: format!("ListUpload-{stem}"),
            applications: [format!("fabric:/{stem}-a"), format!("fabric:/{stem}-b")],
            stages: [Stage::Reserved; 4],
        };
        value.validate(&value.owner)?;
        Ok(value)
    }

    fn validate(&self, owner: &str) -> Result<()> {
        validate_owner(owner)?;
        let nonce = Uuid::parse_str(&self.nonce)?;
        let stem = format!("native-lists-{owner}-{}", self.nonce);
        if self.owner != owner
            || nonce.get_version_num() != 4
            || self.nonce != nonce.simple().to_string()
            || self.type_name != format!("ListType-{stem}")
            || self.upload != format!("ListUpload-{stem}")
            || self.applications != [format!("fabric:/{stem}-a"), format!("fabric:/{stem}-b")]
            || self.archive_sha256.len() != 64
            || !self.archive_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("refusing foreign or invalid list workload journal".into());
        }
        Ok(())
    }

    pub fn load(path: &Path, owner: &str) -> Result<Self> {
        let value: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        value.validate(owner)?;
        Ok(value)
    }

    pub fn reserve(&self, path: &Path) -> Result<()> {
        self.validate(&self.owner)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn record(&mut self, path: &Path, index: usize, stage: Stage) -> Result<()> {
        let previous = Self::load(path, &self.owner)?;
        if previous != *self
            || index >= 4
            || !matches!(
                (self.stages[index], stage),
                (Stage::Reserved, Stage::Absent)
                    | (Stage::Absent, Stage::Pending)
                    | (Stage::Pending, Stage::Settled)
                    | (Stage::Settled, Stage::Pending)
                    | (Stage::Settled, Stage::Cleaned)
            )
        {
            return Err("invalid workload ownership transition".into());
        }
        let mut next = self.clone();
        next.stages[index] = stage;
        let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(&next)?)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(path.parent().ok_or("journal has no parent")?)?.sync_all()?;
        *self = next;
        Ok(())
    }
}

pub struct Http {
    config: Configuration,
}

fn optional_resource<T: Serialize, E: std::fmt::Debug + Send + Sync + 'static>(
    result: std::result::Result<T, ApiError<E>>,
) -> Result<Option<Value>> {
    match result {
        Ok(value) => Ok(Some(serde_json::to_value(value)?)),
        Err(ApiError::ResponseError(response)) if response.status == StatusCode::NOT_FOUND => {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

impl Http {
    pub fn new() -> Result<Self> {
        Ok(Self {
            config: rest::configuration(Duration::from_secs(120))?,
        })
    }

    pub async fn nodes(&self) -> Result<Value> {
        rest::nodes(&self.config, None).await
    }

    async fn get(&self, path: &str) -> Result<Value> {
        Ok(self
            .config
            .client
            .get(format!("{}{path}", self.config.base_path))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    // SDK unit responses discard status, accepting even redirects. Retain the
    // strict success boundary before settling an owned mutation.
    async fn mutation(&self, method: Method, path: &str, value: Option<Value>) -> Result<()> {
        let mut request = self
            .config
            .client
            .request(method, format!("{}{path}", self.config.base_path));
        if let Some(value) = value {
            request = request.json(&value);
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let details: Value = response.json().await?;
            let code = details["Error"]["Code"]
                .as_str()
                .ok_or("missing HTTP native error code")?;
            let message = details["Error"]["Message"]
                .as_str()
                .ok_or("missing HTTP native error message")?;
            return Err(format!(
                "owned workload mutation returned {status}, {code}: {}",
                message.chars().take(512).collect::<String>()
            )
            .into());
        }
        Ok(())
    }

    pub async fn exists(&self, journal: &Journal, index: usize) -> Result<bool> {
        let value = match index {
            0 => optional_resource(
                image_store_api::get_image_store_content(
                    &self.config,
                    "6.0",
                    &journal.upload,
                    None,
                )
                .await,
            )?,
            1 => optional_resource(
                application_type_api::get_application_type_info_list_by_name(
                    &self.config,
                    "6.0",
                    &journal.type_name,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await,
            )?,
            2 | 3 => {
                // SDK 0.1.1 tries to deserialize the documented empty 204.
                let response = self
                    .config
                    .client
                    .get(format!(
                        "{}/Applications/{}?api-version=6.0",
                        self.config.base_path,
                        app_id(&journal.applications[index - 2])
                    ))
                    .send()
                    .await?;
                if matches!(
                    response.status(),
                    StatusCode::NOT_FOUND | StatusCode::NO_CONTENT
                ) {
                    return Ok(false);
                }
                Some(response.error_for_status()?.json::<Value>().await?)
            }
            _ => return Err("invalid workload resource index".into()),
        };
        let Some(value) = value else {
            return Ok(false);
        };
        match index {
            0 => Ok(!value["StoreFiles"]
                .as_array()
                .ok_or("missing image-store files")?
                .is_empty()
                || !value["StoreFolders"]
                    .as_array()
                    .ok_or("missing image-store folders")?
                    .is_empty()),
            1 => Ok(!value["Items"]
                .as_array()
                .ok_or("missing application types")?
                .is_empty()),
            _ => {
                if value["Name"]
                    .as_str()
                    .ok_or("missing exact application identity")?
                    != journal.applications[index - 2]
                {
                    return Err("independent application response identity mismatch".into());
                }
                Ok(true)
            }
        }
    }

    pub async fn confirm_absent(&self, journal: &mut Journal, path: &Path) -> Result<()> {
        for index in 0..4 {
            if self.exists(journal, index).await? {
                return Err("owned workload collision; nothing is adopted".into());
            }
        }
        // Do not claim any identity until every initial absence was observed.
        for index in 0..4 {
            journal.record(path, index, Stage::Absent)?;
        }
        Ok(())
    }

    pub async fn prepare(&self, journal: &mut Journal, path: &Path) -> Result<()> {
        self.confirm_absent(journal, path).await?;
        journal.record(path, 0, Stage::Pending)?;
        // The Swagger/SDK upload operation has no body parameter.
        for (relative, contents) in manifests(&journal.type_name) {
            self.config
                .client
                .put(format!(
                    "{}/ImageStore/{}/{}?api-version=6.0",
                    self.config.base_path, journal.upload, relative
                ))
                .header("Content-Type", "application/octet-stream")
                .body(contents)
                .send()
                .await?
                .error_for_status()?;
        }
        journal.record(path, 0, Stage::Settled)?;
        journal.record(path, 1, Stage::Pending)?;
        self.mutation(
            Method::POST,
            "/ApplicationTypes/$/Provision?api-version=6.0",
            // API 6.0 uses the legacy untagged shape. The SDK's newer
            // polymorphic provision model also loses the required build path.
            Some(json!({"ApplicationTypeBuildPath": journal.upload, "Async": false})),
        )
        .await?;
        journal.record(path, 1, Stage::Settled)?;
        for index in 0..2 {
            journal.record(path, index + 2, Stage::Pending)?;
            let mut description = ApplicationDescription::new(
                journal.applications[index].clone(),
                journal.type_name.clone(),
                "1.0".into(),
            );
            description.parameter_list = Some(vec![ApplicationParameter::new(
                "Evidence".into(),
                "owned-list-value".into(),
            )]);
            self.mutation(
                Method::POST,
                "/Applications/$/Create?api-version=6.0",
                Some(serde_json::to_value(description)?),
            )
            .await?;
            journal.record(path, index + 2, Stage::Settled)?;
        }
        Ok(())
    }

    pub async fn applications(&self, journal: &Journal) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        for name in &journal.applications {
            result.push(serde_json::to_value(
                application_api::get_application_info(
                    &self.config,
                    "6.3",
                    app_id(name),
                    None,
                    None,
                )
                .await?,
            )?);
        }
        Ok(result)
    }

    pub async fn services(&self, name: &str) -> Result<Vec<Value>> {
        // SDK 0.1.1 has incorrect ServiceKind tags and loses HasPersistedState.
        let value = self
            .get(&format!(
                "/Applications/{}/$/GetServices?api-version=6.3",
                app_id(name)
            ))
            .await?;
        if value
            .get("ContinuationToken")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
        {
            return Err("unexpected oracle service pagination".into());
        }
        Ok(value["Items"]
            .as_array()
            .ok_or("missing independent services")?
            .clone())
    }

    pub async fn partitions(&self, service_name: &str) -> Result<Vec<Value>> {
        let service_id = app_id(service_name).replace('/', "~");
        let value = self
            .get(&format!(
                "/Services/{service_id}/$/GetPartitions?api-version=6.3"
            ))
            .await?;
        unpaged_items(value, "partitions")
    }

    pub async fn replicas(&self, partition_id: uuid::Uuid) -> Result<Vec<Value>> {
        let value = self
            .get(&format!(
                "/Partitions/{partition_id}/$/GetReplicas?api-version=6.3"
            ))
            .await?;
        unpaged_items(value, "replicas")
    }

    pub async fn deployed_applications(&self, node_name: &str) -> Result<Vec<Value>> {
        let value = self
            .get(&format!(
                "/Nodes/{node_name}/$/GetApplications?api-version=6.3&IncludeHealthState=true"
            ))
            .await?;
        unpaged_items(value, "deployed applications")
    }

    pub async fn wait_ready(&self, journal: &Journal) -> Result<()> {
        for _ in 0..120 {
            let apps = self.applications(journal).await?;
            let mut ready = apps
                .iter()
                .all(|a| a["Status"] == "Ready" && a["HealthState"] == "Ok");
            for name in &journal.applications {
                let services = self.services(name).await?;
                ready &= services.len() == 2
                    && services
                        .iter()
                        .all(|s| s["ServiceStatus"] == "Active" && s["HealthState"] == "Ok");
                if ready {
                    for service in &services {
                        let name = service["Name"]
                            .as_str()
                            .ok_or("missing independent service identity")?;
                        let partitions = self.partitions(name).await?;
                        let expected = if service["ServiceKind"] == "Stateful" {
                            2
                        } else {
                            1
                        };
                        ready &= partitions.len() == expected
                            && partitions.iter().all(|partition| {
                                partition["PartitionStatus"] == "Ready"
                                    && partition["HealthState"] == "Ok"
                            });
                        for partition in partitions {
                            let id = partition["PartitionInformation"]["Id"]
                                .as_str()
                                .ok_or("missing independent partition identity")?
                                .parse()?;
                            let replicas = self.replicas(id).await?;
                            let minimum = if service["ServiceKind"] == "Stateless" {
                                5
                            } else {
                                3
                            };
                            ready &= replicas.len() >= minimum
                                && replicas.iter().all(|replica| {
                                    replica["ReplicaStatus"] == "Ready"
                                        && replica["HealthState"] == "Ok"
                                });
                        }
                    }
                }
            }
            if ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err("owned paired workload did not converge to Ready/Active/Ok".into())
    }

    async fn delete_settled(&self, journal: &mut Journal, path: &Path, index: usize) -> Result<()> {
        if matches!(
            journal.stages[index],
            Stage::Reserved | Stage::Absent | Stage::Cleaned
        ) {
            return Ok(());
        }
        if journal.stages[index] == Stage::Pending {
            return Err(
                "ambiguous workload mutation retained; destroy exact-owned disposable allocation"
                    .into(),
            );
        }
        // Journal the destructive mutation as well; uncertain cleanup must not
        // be reported as success even if a single absence is seen.
        journal.record(path, index, Stage::Pending)?;
        let (method, route, value) = match index {
            0 => (
                Method::DELETE,
                format!("/ImageStore/{}?api-version=6.0", journal.upload),
                None,
            ),
            1 => (
                Method::POST,
                format!(
                    "/ApplicationTypes/{}/$/Unprovision?api-version=6.0",
                    journal.type_name
                ),
                Some(serde_json::to_value(
                    UnprovisionApplicationTypeDescriptionInfo {
                        r#async: Some(false),
                        ..UnprovisionApplicationTypeDescriptionInfo::new("1.0".into())
                    },
                )?),
            ),
            2 | 3 => (
                Method::POST,
                format!(
                    "/Applications/{}/$/Delete?api-version=6.0",
                    app_id(&journal.applications[index - 2])
                ),
                None,
            ),
            _ => return Err("invalid cleanup slot".into()),
        };
        self.mutation(method, &route, value).await?;
        journal.record(path, index, Stage::Settled)?;
        for _ in 0..120 {
            if !self.exists(journal, index).await? {
                journal.record(path, index, Stage::Cleaned)?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err("owned cleanup did not reach absence".into())
    }
}

pub fn app_id(name: &str) -> &str {
    &name["fabric:/".len()..]
}

fn unpaged_items(value: Value, description: &str) -> Result<Vec<Value>> {
    match value.get("ContinuationToken") {
        None | Some(Value::Null) => {}
        Some(Value::String(token)) if token.is_empty() => {}
        Some(Value::String(_)) => {
            return Err(format!("unexpected oracle pagination for {description}").into());
        }
        Some(_) => return Err(format!("malformed oracle continuation for {description}").into()),
    }
    Ok(value["Items"]
        .as_array()
        .ok_or_else(|| format!("missing independent {description}"))?
        .clone())
}

pub async fn cleanup(path: &Path, owner: &str, http: &Http) -> Result<()> {
    if !path.try_exists()? {
        return Ok(());
    }
    let mut journal = Journal::load(path, owner)?;
    // Dependencies must disappear before type and upload deletion.
    for index in [3, 2, 1, 0] {
        http.delete_settled(&mut journal, path, index).await?;
    }
    Ok(())
}

fn manifests(type_name: &str) -> [(&'static str, String); 6] {
    [
        ("ApplicationManifest.xml", format!(r#"<?xml version="1.0" encoding="utf-8"?>
<ApplicationManifest ApplicationTypeName="{type_name}" ApplicationTypeVersion="1.0" xmlns="http://schemas.microsoft.com/2011/01/fabric">
  <Parameters><Parameter Name="Evidence" DefaultValue="default-list-value" /></Parameters>
  <ServiceManifestImport><ServiceManifestRef ServiceManifestName="ListServices" ServiceManifestVersion="1.0" /></ServiceManifestImport>
  <DefaultServices>
    <Service Name="Stateless"><StatelessService ServiceTypeName="ListStateless" InstanceCount="-1"><SingletonPartition /></StatelessService></Service>
    <Service Name="Stateful"><StatefulService ServiceTypeName="ListStateful" TargetReplicaSetSize="3" MinReplicaSetSize="1"><UniformInt64Partition PartitionCount="2" LowKey="-10" HighKey="9" /></StatefulService></Service>
  </DefaultServices>
</ApplicationManifest>"#)),
        ("ListServices/ServiceManifest.xml", r#"<?xml version="1.0" encoding="utf-8"?>
<ServiceManifest Name="ListServices" Version="1.0" xmlns="http://schemas.microsoft.com/2011/01/fabric">
  <ServiceTypes>
    <StatelessServiceType ServiceTypeName="ListStateless" UseImplicitHost="true" />
    <StatefulServiceType ServiceTypeName="ListStateful" HasPersistedState="false" UseImplicitHost="true" />
  </ServiceTypes>
  <CodePackage Name="Guest" Version="1.0"><EntryPoint><ExeHost IsExternalExecutable="true"><Program>sleep</Program><Arguments>86400</Arguments><WorkingFolder>Work</WorkingFolder></ExeHost></EntryPoint></CodePackage>
</ServiceManifest>"#.into()),
        // ImageBuilder requires the code-package directory even for an external
        // executable. This inert file creates it without shipping a host binary.
        ("ListServices/Guest/placeholder.txt", "Owned native-list qualification package.\n".into()),
        // NativeImageStore tests directory existence using _.dir; REST folder
        // upload must commit each marker after its contents, bottom-up.
        ("ListServices/Guest/_.dir", String::new()),
        ("ListServices/_.dir", String::new()),
        ("_.dir", String::new()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn scripted_http(
        requests: Vec<(String, StatusCode, Value)>,
    ) -> (Http, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (expected, status, value) in requests {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(stream.read_u8().await.unwrap());
                    assert!(headers.len() < 16384);
                }
                let headers = String::from_utf8(headers).unwrap();
                assert_eq!(
                    headers.lines().next().unwrap(),
                    format!("{expected} HTTP/1.1")
                );
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                if expected.starts_with("DELETE ") || expected.contains("/$/Delete?api-version=6.0")
                {
                    assert!(body.is_empty(), "bodyless cleanup must not send JSON null");
                }
                if expected.contains("/$/Unprovision?api-version=6.0") {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&body).unwrap(),
                        json!({"ApplicationTypeVersion": "1.0", "Async": false})
                    );
                }
                let bytes = if status == StatusCode::NO_CONTENT {
                    Vec::new()
                } else {
                    serde_json::to_vec(&value).unwrap()
                };
                let response = format!("HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
                    status.as_u16(), status.canonical_reason().unwrap(), bytes.len());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&bytes).await.unwrap();
            }
        });
        (
            Http {
                config: {
                    let mut config = rest::configuration(Duration::from_secs(5)).unwrap();
                    config.base_path = base;
                    config
                },
            },
            server,
        )
    }

    #[tokio::test]
    async fn independent_topology_routes_preserve_polymorphic_fields() {
        let id = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let partition = json!({
            "ServiceKind": "Stateful",
            "PartitionInformation": {
                "Id": id, "ServicePartitionKind": "Int64Range",
                "LowKey": "-10", "HighKey": "-1"
            }
        });
        let replica = json!({
            "ServiceKind": "Stateful", "ReplicaId": "9223372036854775807",
            "ReplicaRole": "Primary", "NodeName": "N0010"
        });
        let deployed = json!({
            "Name": "fabric:/Owned", "Status": "Active", "HealthState": "Ok"
        });
        let (http, server) = scripted_http(vec![
            (
                "GET /Services/Owned~Stateful/$/GetPartitions?api-version=6.3".into(),
                StatusCode::OK,
                json!({"Items": [partition.clone()]}),
            ),
            (
                format!("GET /Partitions/{id}/$/GetReplicas?api-version=6.3"),
                StatusCode::OK,
                json!({"Items": [replica.clone()], "ContinuationToken": ""}),
            ),
            (
                "GET /Nodes/N0010/$/GetApplications?api-version=6.3&IncludeHealthState=true".into(),
                StatusCode::OK,
                json!({"Items": [deployed.clone()]}),
            ),
        ])
        .await;
        assert_eq!(
            http.partitions("fabric:/Owned/Stateful").await.unwrap(),
            vec![partition]
        );
        assert_eq!(http.replicas(id).await.unwrap(), vec![replica]);
        assert_eq!(
            http.deployed_applications("N0010").await.unwrap(),
            vec![deployed]
        );
        server.await.unwrap();
    }

    #[test]
    fn topology_oracle_requires_complete_independent_pages() {
        for value in [
            json!({"Items": [], "ContinuationToken": "next"}),
            json!({"Items": [], "ContinuationToken": 1}),
            json!({"Items": [], "ContinuationToken": false}),
            json!({}),
            json!({"Items": null}),
        ] {
            assert!(unpaged_items(value, "test records").is_err());
        }
    }

    fn request(j: &Journal, index: usize, method: &str, delete: bool) -> String {
        let path = match index {
            0 => format!("/ImageStore/{}", j.upload),
            1 => format!(
                "/ApplicationTypes/{}{}",
                j.type_name,
                if delete { "/$/Unprovision" } else { "" }
            ),
            2 | 3 => format!(
                "/Applications/{}{}",
                app_id(&j.applications[index - 2]),
                if delete { "/$/Delete" } else { "" }
            ),
            _ => unreachable!(),
        };
        format!("{method} {path}?api-version=6.0")
    }

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("list-fixture-{}", Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn ownership_is_collision_safe_and_journal_replacement_survives_reload() {
        let dir = Directory::new();
        let path = dir.0.join("journal.json");
        let mut j = Journal::new("offline-owner".into(), "a".repeat(64)).unwrap();
        j.reserve(&path).unwrap();
        assert!(j.reserve(&path).is_err());
        assert!(Journal::load(&path, "another-owner").is_err());
        assert!(j.record(&path, 0, Stage::Settled).is_err());
        for stage in [
            Stage::Absent,
            Stage::Pending,
            Stage::Settled,
            Stage::Cleaned,
        ] {
            j.record(&path, 0, stage).unwrap();
            assert_eq!(Journal::load(&path, "offline-owner").unwrap(), j);
        }
        let mut foreign = j.clone();
        foreign.applications[0] = "fabric:/foreign".into();
        assert!(foreign.record(&path, 1, Stage::Absent).is_err());
        assert!(Journal::new("bad/owner".into(), "a".repeat(64)).is_err());
    }

    #[tokio::test]
    async fn pending_mutation_cannot_be_declared_clean_from_observed_absence() {
        let dir = Directory::new();
        let path = dir.0.join("journal.json");
        let mut j = Journal::new("offline-owner".into(), "a".repeat(64)).unwrap();
        j.reserve(&path).unwrap();
        j.record(&path, 3, Stage::Absent).unwrap();
        j.record(&path, 3, Stage::Pending).unwrap();
        // No management request can occur: Pending is rejected first.
        let error = cleanup(&path, "offline-owner", &Http::new().unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ambiguous workload mutation"));
        assert_eq!(
            Journal::load(&path, "offline-owner").unwrap().stages[3],
            Stage::Pending
        );
    }

    #[tokio::test]
    async fn initial_absence_profiles_and_collisions_never_adopt_existing_resources() {
        for collision in [false, true] {
            let dir = Directory::new();
            let path = dir.0.join("journal.json");
            let mut j = Journal::new("offline-owner".into(), "b".repeat(64)).unwrap();
            j.reserve(&path).unwrap();
            let mut script = vec![
                (
                    request(&j, 0, "GET", false),
                    StatusCode::NOT_FOUND,
                    Value::Null,
                ),
                (
                    request(&j, 1, "GET", false),
                    StatusCode::OK,
                    json!({"Items": []}),
                ),
            ];
            script.push((
                request(&j, 2, "GET", false),
                if collision {
                    StatusCode::OK
                } else {
                    StatusCode::NO_CONTENT
                },
                json!({"Name": j.applications[0]}),
            ));
            if !collision {
                script.push((
                    request(&j, 3, "GET", false),
                    StatusCode::NO_CONTENT,
                    Value::Null,
                ));
            }
            let (http, server) = scripted_http(script).await;
            assert_eq!(http.confirm_absent(&mut j, &path).await.is_ok(), !collision);
            assert_eq!(
                j.stages,
                [if collision {
                    Stage::Reserved
                } else {
                    Stage::Absent
                }; 4]
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cleanup_targets_exact_owned_resources_in_dependency_order() {
        let dir = Directory::new();
        let path = dir.0.join("journal.json");
        let mut j = Journal::new("offline-owner".into(), "c".repeat(64)).unwrap();
        j.reserve(&path).unwrap();
        for i in 0..4 {
            for stage in [Stage::Absent, Stage::Pending, Stage::Settled] {
                j.record(&path, i, stage).unwrap();
            }
        }
        let mut script = Vec::new();
        for i in [3, 2, 1, 0] {
            script.push((
                request(&j, i, if i == 0 { "DELETE" } else { "POST" }, true),
                StatusCode::OK,
                Value::Null,
            ));
            script.push((
                request(&j, i, "GET", false),
                if i >= 2 {
                    StatusCode::NO_CONTENT
                } else if i == 1 {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                },
                json!({"Items": []}),
            ));
        }
        let (http, server) = scripted_http(script).await;
        cleanup(&path, "offline-owner", &http).await.unwrap();
        assert_eq!(
            Journal::load(&path, "offline-owner").unwrap().stages,
            [Stage::Cleaned; 4]
        );
        // Repeated cleanup is idempotent without making management requests.
        cleanup(&path, "offline-owner", &http).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn partial_preparation_cleans_only_settled_upload_and_preserves_uncertainty() {
        for status in [
            StatusCode::OK,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::FOUND,
        ] {
            let failed_delete = status != StatusCode::OK;
            let dir = Directory::new();
            let path = dir.0.join("journal.json");
            let mut j = Journal::new("offline-owner".into(), "d".repeat(64)).unwrap();
            j.reserve(&path).unwrap();
            for stage in [Stage::Absent, Stage::Pending, Stage::Settled] {
                j.record(&path, 0, stage).unwrap();
            }
            let mut script = vec![(
                request(&j, 0, "DELETE", true),
                status,
                json!({"Error": {"Code": "OwnedTestError", "Message": "uncertain delete"}}),
            )];
            if !failed_delete {
                script.push((
                    request(&j, 0, "GET", false),
                    StatusCode::NOT_FOUND,
                    Value::Null,
                ));
            }
            let (http, server) = scripted_http(script).await;
            assert_eq!(
                cleanup(&path, "offline-owner", &http).await.is_ok(),
                !failed_delete
            );
            let loaded = Journal::load(&path, "offline-owner").unwrap();
            assert_eq!(
                loaded.stages[0],
                if failed_delete {
                    Stage::Pending
                } else {
                    Stage::Cleaned
                }
            );
            assert_eq!(loaded.stages[1..], [Stage::Reserved; 3]);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn sdk_errors_and_malformed_observations_never_authorize_ownership() {
        for (index, status, value) in [
            (0, StatusCode::OK, json!({"StoreFiles": "not-an-array"})),
            (
                0,
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"Error": {"Code": "OwnedTestError", "Message": "failed observation"}}),
            ),
            (1, StatusCode::OK, json!({"Items": "not-an-array"})),
        ] {
            let dir = Directory::new();
            let path = dir.0.join("journal.json");
            let mut journal = Journal::new("offline-owner".into(), "e".repeat(64)).unwrap();
            journal.reserve(&path).unwrap();
            let mut script = Vec::new();
            if index == 1 {
                script.push((
                    request(&journal, 0, "GET", false),
                    StatusCode::NOT_FOUND,
                    Value::Null,
                ));
            }
            script.push((request(&journal, index, "GET", false), status, value));
            let (http, server) = scripted_http(script).await;
            assert!(http.confirm_absent(&mut journal, &path).await.is_err());
            assert_eq!(journal.stages, [Stage::Reserved; 4]);
            assert_eq!(Journal::load(&path, "offline-owner").unwrap(), journal);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn generated_application_reads_preserve_parameters_and_errors() {
        let journal = Journal::new("offline-owner".into(), "f".repeat(64)).unwrap();
        let mut script = Vec::new();
        for name in &journal.applications {
            script.push((
                format!("GET /Applications/{}?api-version=6.3", app_id(name)),
                StatusCode::OK,
                json!({"Name": name, "Status": "Ready", "HealthState": "Ok",
                    "Parameters": [{"Key": "Evidence", "Value": "owned-list-value"}]}),
            ));
        }
        let (http, server) = scripted_http(script).await;
        let applications = http.applications(&journal).await.unwrap();
        for (app, name) in applications.iter().zip(&journal.applications) {
            assert_eq!(app["Name"], *name);
            assert_eq!(app["Status"], "Ready");
            assert_eq!(app["Parameters"][0]["Value"], "owned-list-value");
        }
        server.await.unwrap();

        let (http, server) = scripted_http(vec![(
            format!(
                "GET /Applications/{}?api-version=6.3",
                app_id(&journal.applications[0])
            ),
            StatusCode::NO_CONTENT,
            Value::Null,
        )])
        .await;
        // A readiness read must fail on absence, not retry through a raw fallback.
        assert!(http.applications(&journal).await.is_err());
        server.await.unwrap();
    }

    #[test]
    fn package_commits_directory_markers_after_contents() {
        let files = manifests("ExactOwnedType");
        assert!(files[0].1.contains("ExactOwnedType"));
        assert!(files[1].1.contains("UseImplicitHost=\"true\""));
        assert_eq!(files[3].0, "ListServices/Guest/_.dir");
        assert_eq!(files[4].0, "ListServices/_.dir");
        assert_eq!(files[5].0, "_.dir");
        assert!(files[3..].iter().all(|(_, content)| content.is_empty()));
    }
}
