use std::{collections::BTreeMap, net::SocketAddr};

use mssf_transport::{serialization::serde::Guid, Error, Result};

use crate::wire::*;

/// Node-provided activated executable identity. No nonactivated fallback.
#[derive(Debug, Clone)]
pub struct ActivationContext {
    pub host_id: String,
    pub application_name: String,
    pub code_package_name: String,
    pub code_sequence: i64,
    pub package_sequence: i64,
    pub work_directory: String,
    pub log_directory: String,
    pub temp_directory: String,
    pub endpoints: BTreeMap<String, u16>,
    pub node_ip: String,
    pub(crate) address: SocketAddr,
    pub(crate) runtime: RuntimeContext,
}

impl ActivationContext {
    pub fn from_environment() -> Result<Self> {
        Self::parse(&std::env::vars().collect())
    }

    fn parse(env: &BTreeMap<String, String>) -> Result<Self> {
        let get = |key: &str| -> Result<String> {
            env.get(key)
                .filter(|v| !v.is_empty() && v.len() <= 32_768)
                .cloned()
                .ok_or(Error::InvalidInput(
                    "missing/invalid SF activated-host environment",
                ))
        };
        let flag = |key: &str| -> Result<bool> {
            match env.get(key).map(|v| v.to_ascii_lowercase()).as_deref() {
                None | Some("false") => Ok(false),
                Some("true") => Ok(true),
                _ => Err(Error::InvalidInput("invalid SF boolean")),
            }
        };
        if flag("Fabric_RuntimeConnectionUseSsl")?
            || flag("Fabric_IsContainerHost")?
            || flag("Fabric_IsCodePackageActivatorHost")?
        {
            return Err(Error::InvalidInput("unsupported SF host/security profile"));
        }
        if !get("Fabric_ApplicationHostType")?.eq_ignore_ascii_case("Activated_SingleCodePackage") {
            return Err(Error::InvalidInput(
                "requires SF-activated single-code-package ExeHost",
            ));
        }
        let host_id = get("Fabric_ApplicationHostId")?;
        uuid::Uuid::parse_str(&host_id)
            .map_err(|_| Error::InvalidInput("invalid native host GUID"))?;
        let address: SocketAddr = get("Fabric_RuntimeConnectionAddress")?
            .parse()
            .map_err(|_| Error::InvalidInput("invalid native IPC address"))?;
        if !address.ip().is_loopback() {
            return Err(Error::InvalidInput("native IPC must be loopback"));
        }
        let app_id = get("Fabric_ApplicationId")?;
        let (type_name, number) = app_id
            .rsplit_once("_App")
            .ok_or(Error::InvalidInput("invalid application identifier"))?;
        let application = ApplicationId {
            type_name: type_name.to_owned(),
            number: number
                .parse()
                .map_err(|_| Error::InvalidInput("invalid application number"))?,
        };
        if application.type_name.is_empty() {
            return Err(Error::InvalidInput("empty application type"));
        }
        let activation = match env.get("Fabric_ServicePackageActivationGuid") {
            None => Activation {
                guid: Guid([0; 16]),
                mode: 0,
            },
            Some(value) => {
                let guid = uuid::Uuid::parse_str(value)
                    .map_err(|_| Error::InvalidInput("invalid package activation GUID"))?;
                if guid.is_nil() {
                    return Err(Error::InvalidInput("empty exclusive activation"));
                }
                Activation {
                    guid: Guid(guid.to_bytes_le()),
                    mode: 1,
                }
            }
        };
        let public_id = env
            .get("Fabric_ServicePackageActivationId")
            .cloned()
            .unwrap_or_default();
        if activation.mode == 1 || !public_id.is_empty() {
            uuid::Uuid::parse_str(&public_id)
                .map_err(|_| Error::InvalidInput("invalid public activation ID"))?;
        }
        let package = PackageInstance {
            package: PackageId {
                application,
                name: get("Fabric_ServicePackageName")?,
            },
            activation,
            public_id,
        };
        let code_package_name = get("Fabric_CodePackageName")?;
        let code_sequence = get("Fabric_CodePackageInstanceSeqNum")?
            .parse()
            .map_err(|_| Error::InvalidInput("invalid code incarnation"))?;
        let package_sequence = get("Fabric_ServicePackageInstanceSeqNum")?
            .parse()
            .map_err(|_| Error::InvalidInput("invalid package incarnation"))?;
        let parts: Vec<_> = get("Fabric_ServicePackageVersionInstance")?
            .split(':')
            .map(str::to_owned)
            .collect();
        if parts.len() != 3 {
            return Err(Error::InvalidInput("invalid package version"));
        }
        let rollout = |value: &str| -> Result<Rollout> {
            let (major, minor) = value
                .split_once('.')
                .ok_or(Error::InvalidInput("invalid rollout"))?;
            Ok(Rollout {
                major: major
                    .parse()
                    .map_err(|_| Error::InvalidInput("invalid rollout major"))?,
                minor: minor
                    .parse()
                    .map_err(|_| Error::InvalidInput("invalid rollout minor"))?,
            })
        };
        let version = VersionInstance {
            version: PackageVersion {
                application: ApplicationVersion {
                    value: rollout(&parts[0])?,
                },
                value: rollout(&parts[1])?,
            },
            instance: parts[2]
                .parse()
                .map_err(|_| Error::InvalidInput("invalid version instance"))?,
        };
        let application_name = get("Fabric_ApplicationName")?;
        let runtime = RuntimeContext {
            id: uuid::Uuid::new_v4().to_string(),
            host: HostContext {
                id: host_id.clone(),
                kind: 1,
                pid: std::process::id(),
                container: false,
                activator: false,
            },
            code: CodeContext {
                id: CodeId {
                    package,
                    name: code_package_name.clone(),
                },
                code_sequence,
                package_sequence,
                version,
                application_name: application_name.clone(),
            },
        };
        let mut endpoints = BTreeMap::new();
        for (key, value) in env {
            if let Some(name) = key.strip_prefix("Fabric_Endpoint_") {
                if !name.starts_with("IPOrFQDN_") {
                    endpoints.insert(
                        name.to_owned(),
                        value
                            .parse()
                            .map_err(|_| Error::InvalidInput("invalid endpoint port"))?,
                    );
                }
            }
        }
        Ok(Self {
            host_id,
            application_name,
            code_package_name,
            code_sequence,
            package_sequence,
            work_directory: get("Fabric_Folder_App_Work")?,
            log_directory: get("Fabric_Folder_App_Log")?,
            temp_directory: get("Fabric_Folder_App_Temp")?,
            endpoints,
            node_ip: get("Fabric_NodeIPOrFQDN")?,
            address,
            runtime,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn env() -> BTreeMap<String, String> {
        [
            (
                "Fabric_ApplicationHostId",
                "443de6bf-b823-40d2-915d-0826843aeddb",
            ),
            ("Fabric_ApplicationHostType", "Activated_SingleCodePackage"),
            ("Fabric_RuntimeConnectionAddress", "127.0.0.1:4000"),
            ("Fabric_ApplicationId", "RustApp_App0"),
            ("Fabric_ServicePackageName", "RustService"),
            ("Fabric_CodePackageName", "Code"),
            ("Fabric_CodePackageInstanceSeqNum", "123"),
            ("Fabric_ServicePackageInstanceSeqNum", "456"),
            ("Fabric_ServicePackageVersionInstance", "1.0:2.0:3"),
            ("Fabric_ApplicationName", "fabric:/RustApp"),
            ("Fabric_Folder_App_Work", "/work"),
            ("Fabric_Folder_App_Log", "/log"),
            ("Fabric_Folder_App_Temp", "/temp"),
            ("Fabric_NodeIPOrFQDN", "172.30.91.11"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }
    #[test]
    fn requires_real_complete_activated_profile() {
        assert!(ActivationContext::parse(&BTreeMap::new()).is_err());
        let source = env();
        let parsed = ActivationContext::parse(&source).unwrap();
        assert_eq!(parsed.runtime.host.pid, std::process::id());
        assert_eq!(parsed.runtime.code.version.instance, 3);
        for key in source.keys() {
            let mut broken = source.clone();
            broken.remove(key);
            assert!(ActivationContext::parse(&broken).is_err(), "{key}");
        }
    }
    #[test]
    fn rejects_security_and_identity_substitution() {
        for (k, v) in [
            ("Fabric_RuntimeConnectionUseSsl", "true"),
            ("Fabric_IsContainerHost", "true"),
            ("Fabric_IsCodePackageActivatorHost", "true"),
            ("Fabric_ApplicationHostType", "NonActivated"),
            ("Fabric_RuntimeConnectionAddress", "172.30.91.11:4000"),
            (
                "Fabric_ServicePackageActivationGuid",
                "00000000-0000-0000-0000-000000000000",
            ),
        ] {
            let mut e = env();
            e.insert(k.into(), v.into());
            assert!(ActivationContext::parse(&e).is_err(), "{k}");
        }
    }
}
