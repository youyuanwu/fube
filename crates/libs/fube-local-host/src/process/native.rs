use crate::{HostError, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// Explicit loader inputs for the same owned-process launcher. This does not
/// load native libraries in the host or select a native SDK version.
#[derive(Debug, Clone)]
pub struct NativeRuntime {
    directory: PathBuf,
    dependencies: PathBuf,
}

impl NativeRuntime {
    pub fn new(directory: &Path, dependencies: &Path) -> Result<Self> {
        let directory = directory.canonicalize()?;
        let dependencies = dependencies.canonicalize()?;
        if !directory.is_dir() || !dependencies.is_dir() {
            return Err(HostError(
                "native/dependency inputs must be directories".into(),
            ));
        }
        Ok(Self {
            directory,
            dependencies,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn dependencies(&self) -> &Path {
        &self.dependencies
    }

    pub fn prepare_layout(root: &Path) -> Result<PathBuf> {
        let objects = root.join("native-objects");
        fs::create_dir_all(&objects)?;
        fs::create_dir_all(root.join("native-data"))?;
        let config = root.join("FabricConfig.cfg");
        let contents = format!(
            "[Common]\nObjectFolder={}\n[Trace/Console]\nLevel=4\n[Trace/Etw]\nLevel=0\n[Trace/File]\nLevel=0\n",
            objects.display()
        );
        if config.exists() {
            if fs::read_to_string(&config)? != contents {
                return Err(HostError("native run configuration differs".into()));
            }
        } else {
            use std::io::Write;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&config)?
                .write_all(contents.as_bytes())?;
        }
        Ok(config)
    }

    /// Configure only explicitly selected native inputs in a caller-owned run
    /// layout; all host/code identity and process ownership remain with caller.
    pub fn configure(&self, command: &mut Command, root: &Path, application: &Path) -> Result<()> {
        let config = Self::prepare_layout(root)?;
        command
            .env(
                "LD_LIBRARY_PATH",
                std::env::join_paths([&self.directory, &self.dependencies])
                    .map_err(|error| HostError(error.to_string()))?,
            )
            .env("FabricConfigFileName", config)
            .env("FabricDataRoot", root.join("native-data"))
            .env("FabricLogRoot", application.join("log"))
            .env("FabricCodePath", &self.directory);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_inputs_and_conflicting_configuration_do_not_replace_caller_files() {
        let root = std::env::temp_dir().join(format!("native-inputs-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let input = root.join("input");
        fs::write(&input, "caller input").unwrap();
        assert!(NativeRuntime::new(&input, &root).is_err());
        let native = NativeRuntime::new(&root, &root).unwrap();
        let config = root.join("FabricConfig.cfg");
        fs::write(&config, "caller configuration").unwrap();
        let mut command = Command::new("/bin/true");
        command.env("Fabric_ApplicationHostId", "original");
        assert!(native.configure(&mut command, &root, &root).is_err());
        assert_eq!(fs::read_to_string(config).unwrap(), "caller configuration");
        assert_eq!(fs::read_to_string(input).unwrap(), "caller input");
        assert!(!command.get_envs().any(|(key, _)| key == "LD_LIBRARY_PATH"));
        fs::remove_dir_all(root).unwrap();
    }
}
