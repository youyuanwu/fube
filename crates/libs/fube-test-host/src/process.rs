use crate::{deployment::Deployment, HostError, Result};
use std::{path::Path, process::Command, time::Instant};

pub(crate) fn absolute_directories(
    native: &Path,
    dependencies: &Path,
) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
    let native = native.canonicalize()?;
    let dependencies = dependencies.canonicalize()?;
    if !native.is_dir() || !dependencies.is_dir() {
        return Err(HostError(
            "native/dependency inputs must be directories".into(),
        ));
    }
    Ok((native, dependencies))
}

pub struct OwnedApp(fube_local_host::process::OwnedApp);
impl OwnedApp {
    pub fn launch(
        bundle: &Deployment,
        native: &Path,
        dependencies: &Path,
        host: &str,
        address: &str,
    ) -> Result<Self> {
        let (native, dependencies) = absolute_directories(native, dependencies)?;
        let identity = &bundle.identity;
        let package = &identity.code.package.package;
        let mut command = Command::new(&bundle.executable);
        command
            .args(&bundle.arguments)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &bundle.root)
            .env(
                "LD_LIBRARY_PATH",
                std::env::join_paths([&native, &dependencies])?,
            )
            .env("FabricConfigFileName", &bundle.config)
            .env("FabricDataRoot", bundle.root.join("native-data"))
            .env("FabricLogRoot", bundle.application.join("log"))
            .env("FabricCodePath", &native)
            .env("Fabric_ApplicationHostId", host)
            .env("Fabric_ApplicationHostType", "Activated_SingleCodePackage")
            .env("Fabric_IsContainerHost", "false")
            .env("Fabric_IsCodePackageActivatorHost", "false")
            .env("Fabric_RuntimeConnectionAddress", address)
            .env("Fabric_RuntimeConnectionUseSsl", "false")
            .env(
                "Fabric_ApplicationId",
                format!(
                    "{}_App{}",
                    package.application.kind, package.application.number
                ),
            )
            .env("Fabric_ApplicationName", &identity.application_name)
            .env("Fabric_ServicePackageName", &package.name)
            .env("Fabric_CodePackageName", &identity.code.name)
            .env("Fabric_CodePackageInstanceSeqNum", "1")
            .env("Fabric_ServicePackageInstanceSeqNum", "1")
            .env("Fabric_ServicePackageVersionInstance", "1.0:1.0:1")
            .env("Fabric_ServicePackageActivationId", "")
            .current_dir(&bundle.working_directory);
        let mut owned = None;
        fube_local_host::process::OwnedApp::start(&mut owned, command)?;
        Ok(Self(owned.expect("spawned child")))
    }
    pub fn pid(&self) -> u32 {
        self.0.pid()
    }
    pub fn logs(&self) -> Vec<String> {
        self.0.logs()
    }
    pub fn callbacks(&self) -> Vec<serde_json::Value> {
        self.0.callbacks()
    }
    pub fn check_alive(&mut self) -> Result<()> {
        self.0.check_alive().map_err(Into::into)
    }
    pub fn command(&mut self, command: &[u8]) -> Result<()> {
        self.0.command(command).map_err(Into::into)
    }
    pub fn wait_exit(&mut self, deadline: Instant) -> Result<()> {
        self.0.wait_exit(deadline).map_err(Into::into)
    }
    pub fn cleanup(&mut self) -> Result<serde_json::Value> {
        self.0.cleanup().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn relative_loader_paths_are_absolute_in_the_changed_child_directory() {
        let relative = std::path::PathBuf::from(format!(".launch-input-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(relative.join("native")).unwrap();
        std::fs::create_dir(relative.join("deps")).unwrap();
        let native = relative.join("native").canonicalize().unwrap();
        let deps = relative.join("deps").canonicalize().unwrap();
        let bundle = Deployment::create(&std::env::temp_dir(), Path::new("/usr/bin/env")).unwrap();
        let mut owned = OwnedApp::launch(
            &bundle,
            &relative.join("native"),
            &relative.join("deps"),
            "owned",
            "127.0.0.1:0",
        )
        .unwrap();
        owned
            .wait_exit(Instant::now() + Duration::from_secs(3))
            .unwrap();
        assert_eq!(owned.cleanup().unwrap()["forced"], false);
        let logs = owned.logs();
        assert!(logs.contains(&format!("FabricCodePath={}", native.display())));
        assert!(logs.contains(&format!(
            "LD_LIBRARY_PATH={}:{}",
            native.display(),
            deps.display()
        )));
        std::fs::remove_dir_all(bundle.root).unwrap();
        std::fs::remove_dir_all(relative).unwrap();
    }
    #[test]
    fn expired_protocol_admission_and_caller_failure_still_reap_owned_app() {
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let mut slot = None;
        fube_local_host::process::OwnedApp::start(&mut slot, command).unwrap();
        let mut owned = OwnedApp(slot.unwrap());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        let mut session =
            crate::ipc::Session::new(socket, listener.local_addr().unwrap().to_string()).unwrap();
        let bundle =
            Deployment::create(&std::env::temp_dir(), &std::env::current_exe().unwrap()).unwrap();
        let mut state = crate::Registration::default();
        let error = crate::pump(
            &mut state,
            &mut session,
            &mut owned,
            "owned",
            &bundle,
            None,
            Instant::now() - Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(error.0.contains("native operation deadline"));
        let outcome = crate::observe(
            &mut |_| Err(HostError("caller failed".into())),
            crate::LifecycleStage::Opened,
            "opaque",
            owned.pid(),
            Instant::now() + Duration::from_secs(1),
        );
        let result = crate::complete(outcome, owned.cleanup()).unwrap_err();
        assert_eq!(result.0, "caller failed");
        assert!(!Path::new(&format!("/proc/{}", owned.pid())).exists());
        drop(session);
        std::fs::remove_dir_all(bundle.root).unwrap();
    }
}
