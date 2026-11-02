use crate::{
    manifest::{self, escape, Package},
    HostError, Result,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Exclusive run-root reservation. Keep this value even when `materialize` fails;
/// only its root is owned, never the source package or caller's parent.
#[derive(Debug)]
pub struct Deployment {
    pub root: PathBuf,
    pub deployment: PathBuf,
    pub work: PathBuf,
    pub code: PathBuf,
    pub application: PathBuf,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub working_directory: PathBuf,
    pub service_name: String,
}

impl Deployment {
    pub fn reserve(parent: &Path, package: &Package) -> Result<Self> {
        let parent = parent.canonicalize()?;
        let root = parent.join(format!("local-host-{}", uuid::Uuid::new_v4()));
        let deployment = root.join("deployment");
        let application = deployment.join(package.application_id());
        let code = application.join(format!(
            "{}.{}.{}",
            package.service_manifest, package.code_name, package.code_version
        ));
        let value = Self {
            executable: code.join(&package.program),
            arguments: package.arguments.clone(),
            working_directory: if package.working_folder == "Work" {
                application.join("work")
            } else {
                code.clone()
            },
            service_name: format!("{}/{}", package.application_name, package.service_name),
            work: root.join("node-work"),
            root,
            deployment,
            code,
            application,
        };
        // Last fallible operation: once the root exists the caller gets its owner.
        fs::create_dir(&value.root)?;
        Ok(value)
    }

    pub fn materialize(&self, package: &Package) -> Result<()> {
        manifest::validate_tree(&package.source_code)?;
        for directory in [
            self.code.clone(),
            self.work.clone(),
            self.application.join("work"),
            self.application.join("log"),
            self.application.join("temp"),
        ] {
            fs::create_dir_all(directory)?;
        }
        fn copy(source: &Path, destination: &Path) -> Result<()> {
            for entry in fs::read_dir(source)? {
                let entry = entry?;
                let target = destination.join(entry.file_name());
                let meta = fs::symlink_metadata(entry.path())?;
                if meta.is_dir() {
                    fs::create_dir(&target)?;
                    copy(&entry.path(), &target)?;
                } else if meta.is_file() {
                    fs::copy(entry.path(), target)?;
                } else {
                    return Err(HostError(
                        "code package changed: symlink/nonregular file".into(),
                    ));
                }
            }
            Ok(())
        }
        copy(&package.source_code, &self.code)?;
        let namespace = manifest::NS;
        let types = package.types_xml();
        let code = package.code_xml();
        fs::write(
            self.application.join("App.1.0.xml"),
            format!(
                r#"<ApplicationPackage xmlns="{namespace}" ApplicationId="{}" ApplicationTypeName="{}" NameUri="{}" RolloutVersion="1.0"><DigestedEnvironment RolloutVersion="1.0"><Principals /><Policies /><Diagnostics /></DigestedEnvironment><DigestedCertificates RolloutVersion="1.0" /></ApplicationPackage>"#,
                escape(&package.application_id()),
                escape(&package.application_type),
                escape(&package.application_name)
            ),
        )?;
        let resources = package.endpoints_xml(true);
        fs::write(
            self.application
                .join(format!("{}.Package.Current.xml", package.service_manifest)),
            format!(
                r#"<ServicePackage xmlns="{namespace}" Name="{}" ManifestVersion="{}" RolloutVersion="1.0"><DigestedServiceTypes RolloutVersion="1.0">{types}</DigestedServiceTypes><DigestedCodePackage RolloutVersion="1.0">{code}</DigestedCodePackage><DigestedResources RolloutVersion="1.0"><DigestedEndpoints>{resources}</DigestedEndpoints></DigestedResources><Diagnostics /></ServicePackage>"#,
                escape(&package.service_manifest),
                escape(&package.manifest_version)
            ),
        )?;
        let endpoints = package.endpoints_xml(false);
        fs::write(
            self.application.join(format!(
                "{}.Manifest.{}.xml",
                package.service_manifest, package.manifest_version
            )),
            format!(
                r#"<ServiceManifest xmlns="{namespace}" Name="{}" Version="{}">{types}{code}<Resources><Endpoints>{endpoints}</Endpoints></Resources></ServiceManifest>"#,
                escape(&package.service_manifest),
                escape(&package.manifest_version)
            ),
        )?;
        fs::write(
            self.application
                .join(format!("{}.Endpoints.txt", package.service_manifest)),
            package.endpoint_file(),
        )?;
        fs::write(self.application.join("Application.Sids.txt"), "")?;
        Ok(())
    }

    /// Convenience for synchronous fixture callers. Production owners should
    /// reserve then materialize so a failed copy retains an inspectable owner.
    pub fn from_manifest(parent: &Path, package: &Package) -> Result<Self> {
        let value = Self::reserve(parent, package)?;
        if let Err(error) = value.materialize(package) {
            return match value.release() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(HostError(format!(
                    "{error}; retained root {}: {cleanup}",
                    value.root.display()
                ))),
            };
        }
        Ok(value)
    }

    /// Caller must first prove process and authority retirement. This layer has
    /// no knowledge of IPC, usage or process state.
    pub fn release(&self) -> Result<()> {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
