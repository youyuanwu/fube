//! Bounded trusted local activation, NOT production deployed RA/RAP readiness.
//! The caller retains this owner while observing registration or retrying cleanup.
//! A dropped/expired observer does not cancel the underlying process. Explicit
//! cleanup errors preserve every resource here; owner abandonment/abrupt death
//! and escaped descendants have no durable recovery guarantee.
use crate::{ipc, wire, *};
use fube_local_host::{
    deployment::Deployment,
    manifest::Package,
    process::{OwnedApp, ProcessStatus},
};
use fube_transport::serialization::serde::{Guid, ObjectArray, Pointer};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalError {
    InvalidOptions,
    AlreadyStarted,
    NotStarted,
    Mechanics(String),
    Hosting(Error),
    ObservationTimeout,
    RegistrationTimeout,
    ProcessExited(String),
    SessionLost,
    Busy,
}
impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LocalError {}
impl From<Error> for LocalError {
    fn from(error: Error) -> Self {
        Self::Hosting(error)
    }
}
impl From<fube_local_host::HostError> for LocalError {
    fn from(error: fube_local_host::HostError) -> Self {
        Self::Mechanics(error.0)
    }
}
type LocalResult<T> = std::result::Result<T, LocalError>;

#[derive(Debug, Clone)]
pub struct Options {
    pub run_parent: PathBuf,
    pub registration_timeout: Duration,
    pub cleanup_budget: Duration,
    pub lease_duration: Duration,
    pub residency: ResidencyConfig,
    pub required: ipc::RequiredCapabilities,
}
impl Options {
    pub fn new(run_parent: PathBuf) -> Self {
        Self {
            run_parent,
            registration_timeout: Duration::from_secs(30),
            cleanup_budget: Duration::from_secs(10),
            lease_duration: ipc::LOCAL_LEASE_DURATION,
            residency: ResidencyConfig::default(),
            required: ipc::RequiredCapabilities::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LaunchIdentity {
    pub host_id: String,
    pub code: wire::CodeContext,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupStatus {
    pub authority_revoked: bool,
    pub logical_retired: bool,
    pub declarations_removed: bool,
    pub admission_released: bool,
    pub package_retired: bool,
    pub application_retired: bool,
    pub directory_released: bool,
}

/// One application/service-package/code-package/ExeHost per owner. H2 handles
/// are nonclone and remain in its ledger when dropped. No command queue is
/// invented: mutation is serialized by `&mut self`, H3 queues remain bounded.
#[must_use = "retain the activation owner until explicit cleanup completes"]
pub struct Activation {
    options: Options,
    package: Option<Package>,
    deployment: Option<Deployment>,
    process: Option<OwnedApp>,
    server: Option<ipc::Server>,
    registration: Option<ipc::RegistrationOwner>,
    lease: Option<ipc::HostLease>,
    declared: usize,
    identity: Option<LaunchIdentity>,
    premise: Option<ResidencyAdmission>,
    residency: Residency,
    started: Option<Instant>,
    registered: bool,
    failure: Option<LocalError>,
    stopping: bool,
    explicit: bool,
    cleanup: CleanupStatus,
    retired_operations: Vec<ipc::OperationResult>,
    server_stopped: bool,
    retained_callback: Option<DeactivationCallback>,
}
impl Activation {
    /// Side-effect-free. Unsupported downstream capabilities reject here, not
    /// after writing a deployment or launching a process.
    pub fn new(options: Options) -> LocalResult<Self> {
        if options.registration_timeout.is_zero()
            || options.run_parent.to_str().is_none()
            || options.registration_timeout > Duration::from_secs(30)
            || options.cleanup_budget.is_zero()
            || options.cleanup_budget > Duration::from_secs(10)
            // fube-runtime's owned renewal task polls every second.
            || options.lease_duration < Duration::from_secs(2)
            || options.lease_duration > ipc::LOCAL_LEASE_DURATION
        {
            return Err(LocalError::InvalidOptions);
        }
        if options.required.health_reporting
            || options.required.application_reports
            || options.required.fm_status_completion
            || options.required.deployed_lifecycle
        {
            return Err(Error::UnsupportedCapability.into());
        }
        let residency = Residency::new(
            ResidencyLimits::default(),
            options.residency,
            &ManualClock(0),
        )?;
        Ok(Self {
            options,
            package: None,
            deployment: None,
            process: None,
            server: None,
            registration: None,
            lease: None,
            declared: 0,
            identity: None,
            premise: None,
            residency,
            started: None,
            registered: false,
            failure: None,
            stopping: false,
            explicit: false,
            cleanup: CleanupStatus::default(),
            retired_operations: vec![],
            server_stopped: false,
            retained_callback: None,
        })
    }

    /// Validation precedes all side effects. Once a root/process/admission exists,
    /// an error leaves it accessible through this already-owned value.
    pub fn start(
        &mut self,
        directory: &Path,
        application_name: &str,
        parameters: &[(String, String)],
    ) -> LocalResult<()> {
        if self.package.is_some() || self.stopping {
            return Err(LocalError::AlreadyStarted);
        }
        let package = Package::load(directory, application_name, parameters)?;
        self.package = Some(package);
        self.started = Some(Instant::now());
        let result = self.start_inner();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn start_inner(&mut self) -> LocalResult<()> {
        let package = self.package.as_ref().expect("validated package");
        self.deployment = Some(Deployment::reserve(&self.options.run_parent, package)?);
        let deployment = self.deployment.as_ref().expect("reserved root");
        deployment.materialize(package)?;
        fn fresh_sequence() -> i64 {
            // Random launch incarnation, not an RA incarnation or retry sequence.
            ((uuid::Uuid::new_v4().as_u128() as u64) & i64::MAX as u64).max(1) as i64
        }
        let package_sequence = fresh_sequence();
        let code = wire::CodeContext {
            id: wire::CodeId {
                package: wire::PackageInstance {
                    package: wire::PackageId {
                        application: wire::ApplicationId {
                            type_name: package.application_type.clone(),
                            number: 1,
                        },
                        name: package.service_manifest.clone(),
                    },
                    activation: wire::Activation {
                        guid: Guid([0; 16]),
                        mode: 0,
                    },
                    public_id: String::new(),
                },
                name: package.code_name.clone(),
            },
            code_sequence: fresh_sequence(),
            package_sequence,
            version: wire::VersionInstance {
                version: wire::PackageVersion {
                    application: wire::ApplicationVersion {
                        value: wire::Rollout { major: 1, minor: 0 },
                    },
                    value: wire::Rollout { major: 1, minor: 0 },
                },
                instance: package_sequence as u64,
            },
            application_name: package.application_name.clone(),
        };
        let host_id = uuid::Uuid::new_v4().to_string();
        let projected = wire::RuntimeContext {
            id: String::new(),
            host: wire::HostContext {
                id: host_id.clone(),
                kind: 1,
                pid: 1,
                container: false,
                activator: false,
            },
            code: code.clone(),
        }
        .project();
        let premise = ResidencyAdmission {
            package: projected.package,
            version: projected.version,
            package_instance_id: package_sequence,
            service_name: deployment.service_name.clone(),
            application_name: package.application_name.clone(),
        };
        self.residency.admit_package(premise.clone())?;
        self.premise = Some(premise);
        self.identity = Some(LaunchIdentity {
            host_id: host_id.clone(),
            code: code.clone(),
        });
        let node = wire::StartReply {
            node_id: "1".into(),
            node_pid: std::process::id(),
            lease_handle: 0,
            lease_duration: 0,
            error: 0,
            node_instance: 1,
            node_name: "local-activation".into(),
            node_type: "local".into(),
            client_address: "127.0.0.1:19000".into(),
            deployment: deployment.deployment.to_string_lossy().into(),
            ip: "127.0.0.1".into(),
            work: deployment.work.to_string_lossy().into(),
            initial_ttl: 0,
            application_directories: ObjectArray(vec![]),
            node_directories: ObjectArray(vec![]),
            application_log: Pointer(None),
            system_log: Pointer(None),
        };
        self.server = Some(ipc::Server::bind_suspended(
            "127.0.0.1:0".parse().expect("literal"),
            ipc::Config {
                node,
                core_limits: Limits::default(),
                max_admissions: 1,
                scratch_bytes: 5 * 1024 * 1024,
                decode_bytes: 256 * 1024,
                projection_bytes: 256 * 1024,
                reply_bytes: 64 * 1024,
                lease_duration: self.options.lease_duration,
            },
        )?);
        let server = self.server.as_ref().expect("owned server");
        self.registration = Some(server.registration_owner(self.options.required)?);
        let mut command = Command::new(&deployment.executable);
        command
            .args(&deployment.arguments)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &deployment.root)
            .env("Fabric_ApplicationHostId", &host_id)
            .env("Fabric_ApplicationHostType", "Activated_SingleCodePackage")
            .env("Fabric_IsContainerHost", "false")
            .env("Fabric_IsCodePackageActivatorHost", "false")
            .env(
                "Fabric_RuntimeConnectionAddress",
                server.address().to_string(),
            )
            .env("Fabric_RuntimeConnectionUseSsl", "false")
            .env("Fabric_ApplicationId", package.application_id())
            .env("Fabric_ApplicationName", &package.application_name)
            .env("Fabric_ServicePackageName", &package.service_manifest)
            .env("Fabric_CodePackageName", &package.code_name)
            .env(
                "Fabric_CodePackageInstanceSeqNum",
                code.code_sequence.to_string(),
            )
            .env(
                "Fabric_ServicePackageInstanceSeqNum",
                code.package_sequence.to_string(),
            )
            .env(
                "Fabric_ServicePackageVersionInstance",
                format!("1.0:1.0:{}", code.version.instance),
            )
            .env("Fabric_ServicePackageActivationId", "")
            .env(
                "Fabric_Folder_App_Work",
                deployment.application.join("work"),
            )
            .env("Fabric_Folder_App_Log", deployment.application.join("log"))
            .env(
                "Fabric_Folder_App_Temp",
                deployment.application.join("temp"),
            )
            .env("Fabric_NodeIPOrFQDN", "127.0.0.1")
            .current_dir(&deployment.working_directory);
        OwnedApp::start(&mut self.process, command)?;
        let pid = self.process.as_ref().expect("spawned child").pid();
        let prepared = server.admit_registration(
            self.registration.as_ref().expect("local consumer"),
            ipc::Admission {
                host: wire::HostContext {
                    id: host_id,
                    kind: 1,
                    pid,
                    container: false,
                    activator: false,
                },
                code,
                deadlines: ipc::RequestDeadlines::Propagated,
            },
            vec![package.service_type.clone()],
            self.identity.as_ref().expect("identity").host_id.as_str(),
        )?;
        self.declared = prepared.admitted;
        self.lease = Some(prepared.lease);
        if let Some((_, error)) = prepared.failure {
            return Err(error.into());
        }
        server.start_admitted()?;
        Ok(())
    }

    pub fn identity(&self) -> Option<&LaunchIdentity> {
        self.identity.as_ref()
    }
    pub fn root(&self) -> Option<&Path> {
        self.deployment.as_ref().map(|d| d.root.as_path())
    }
    pub fn process_status(&self) -> Option<ProcessStatus> {
        self.process.as_ref().map(OwnedApp::status)
    }
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup
    }
    pub fn residency(&self) -> &Residency {
        &self.residency
    }
    pub fn logs(&self) -> Vec<String> {
        self.process
            .as_ref()
            .map(OwnedApp::logs)
            .unwrap_or_default()
    }
    pub fn session_status(&self) -> LocalResult<Option<ipc::SessionStatus>> {
        match (&self.server, &self.lease) {
            (Some(server), Some(lease)) => Ok(Some(server.status(lease)?)),
            _ => Ok(None),
        }
    }
    pub fn poll_registration(&mut self) -> LocalResult<bool> {
        if self.started.is_none() {
            return Err(LocalError::NotStarted);
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.stopping {
            return Err(Error::ObjectClosed.into());
        }
        let result = self.poll_inner();
        if let Err(error) = &result {
            if let Some(lease) = &self.lease {
                lease.revoke();
            }
            self.failure = Some(error.clone());
        }
        result
    }
    fn poll_inner(&mut self) -> LocalResult<bool> {
        let started = self.started.ok_or(LocalError::NotStarted)?;
        if !self.registered && started.elapsed() >= self.options.registration_timeout {
            return Err(LocalError::RegistrationTimeout);
        }
        if let Some(process) = &mut self.process {
            if let Some(status) = process
                .observed_exit()
                .map_err(|e| LocalError::Mechanics(e.to_string()))?
            {
                return Err(LocalError::ProcessExited(status.to_string()));
            }
        }
        let status = self.session_status()?.ok_or(LocalError::NotStarted)?;
        if status.loss.is_some() || status.cleanup.as_ref().is_some_and(|r| r.is_err()) {
            return Err(LocalError::SessionLost);
        }
        let premise = self.premise.as_ref().expect("admitted");
        let package = self.package.as_ref().expect("validated");
        let result = self
            .registration
            .as_ref()
            .expect("consumer")
            .lookup(&LookupRequest {
                service_type: ServiceTypeId {
                    package: premise.package.package.clone(),
                    name: package.service_type.clone(),
                },
                version: premise.version.clone(),
                activation: crate::Activation::SHARED,
                service_name: premise.service_name.clone(),
                application_name: premise.application_name.clone(),
                should_activate: false,
            })?;
        if status.registered_host
            && status.connected
            && status.runtimes.len() == 1
            && result.result.is_ok()
        {
            let registration = result.candidate.ok_or(Error::InvalidState)?;
            let identity = self.identity.as_ref().expect("identity");
            if registration.host_id != identity.host_id
                || registration.process_id != self.process.as_ref().expect("child").pid()
                || registration.package_instance_sequence != identity.code.package_sequence
                || registration.version != premise.version
                || registration.code_package != package.code_name
                || registration.runtime_id != status.runtimes[0].runtime_id
            {
                return Err(Error::InvalidState.into());
            }
            self.registered = true;
            return Ok(true);
        }
        if let Err(error) = result.result {
            if error != Error::TypeNotRegistered {
                return Err(error.into());
            }
        }
        Ok(false)
    }
    /// Observer deadline only. Timeout retains the process, session and owner;
    /// it is not cancellation, shutdown or a failed activation receipt.
    pub fn wait_registration(&mut self, budget: Duration) -> LocalResult<()> {
        if budget.is_zero() || budget > Duration::from_secs(30) {
            return Err(LocalError::InvalidOptions);
        }
        let deadline = Instant::now() + budget;
        loop {
            if self.poll_registration()? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LocalError::ObservationTimeout);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    pub fn acquire_usage(&mut self) -> UsageIncrement<Acquisition> {
        if !self.registered || self.stopping {
            return UsageIncrement::Rejected(Error::InvalidState);
        }
        let Some(premise) = self.premise.as_ref() else {
            return UsageIncrement::Rejected(Error::InvalidState);
        };
        let target = TypeInstance {
            service_type: ServiceTypeId {
                package: premise.package.package.clone(),
                name: self.package.as_ref().expect("package").service_type.clone(),
            },
            activation: crate::Activation::SHARED,
            public_activation: String::new(),
        };
        self.residency.increment_usage(&target)
    }
    pub fn release_usage(&mut self, owner: Acquisition) -> UsageRelease<Acquisition> {
        self.residency.decrement_usage(owner)
    }
    /// Synchronously transfer bounded local outputs; NOT downstream RA/FM/HM ACK.
    pub fn take_outputs(&mut self) -> LocalResult<Option<ipc::Outputs>> {
        let Some(owner) = &self.registration else {
            return Ok(None);
        };
        let mut outputs = owner.take_outputs()?;
        outputs.operations.append(&mut self.retired_operations);
        Ok(Some(outputs))
    }
    /// Drive H2's current sequence-fenced idle decisions. Package and application
    /// grace remain distinct; true means every cleanup boundary completed.
    pub fn drive_deactivation(&mut self, clock: &impl Clock) -> LocalResult<bool> {
        if self.started.is_none() {
            return Err(LocalError::NotStarted);
        }
        self.residency.advance(clock)?;
        if let Some(callback) = self.retained_callback.take() {
            self.execute_deactivation(callback)?;
        }
        while let Some(callback) = self.residency.dequeue_due()? {
            self.execute_deactivation(callback)?;
        }
        if self.residency.final_obligations().next().is_some() {
            self.stopping = true;
        }
        if self.stopping {
            self.cleanup_inner()
        } else {
            Ok(false)
        }
    }
    fn execute_deactivation(&mut self, callback: DeactivationCallback) -> LocalResult<()> {
        if let CallbackExecution::Retained { callback, error } =
            self.residency.execute_callback(callback)
        {
            self.retained_callback = Some(callback);
            return Err(error.into());
        }
        Ok(())
    }

    /// Explicit periodic unused scan (application-first), separate from due
    /// callback execution and from admission. No background clock is implied.
    pub fn scan_unused(&mut self, clock: &impl Clock) -> LocalResult<bool> {
        if self.started.is_none() {
            return Err(LocalError::NotStarted);
        }
        self.residency.advance(clock)?;
        self.residency.scan_unused()?;
        self.drive_deactivation(clock)
    }
    /// Explicit termination request, independent of idle grace and service Close.
    /// Live H2 acquisitions do not prevent physical termination, but retain
    /// logical ownership and directory until released and cleanup is retried.
    pub fn terminate(&mut self) -> LocalResult<bool> {
        self.stopping = true;
        self.explicit = true;
        self.cleanup_inner()
    }
    fn cleanup_inner(&mut self) -> LocalResult<bool> {
        if self.cleanup.directory_released {
            return Ok(true);
        }
        if let Some(lease) = &self.lease {
            lease.revoke();
        }
        self.cleanup.authority_revoked = true;
        if !self.server_stopped {
            if let Some(server) = &mut self.server {
                server.stop()?;
            }
            self.server_stopped = true;
            if let Some(owner) = &self.registration {
                // At most the server's 16 admitted operation results, once only.
                self.retired_operations = owner.take_operations()?;
            }
        }
        if let Some(process) = &mut self.process {
            process.cleanup_with_budget(self.options.cleanup_budget)?;
        }
        if let (Some(server), Some(lease)) = (&self.server, &self.lease) {
            let status = server.status(lease)?;
            if status.connected
                || status.registered_host
                || !status.runtimes.is_empty()
                || status.cleanup.as_ref().is_some_and(|r| r.is_err())
            {
                return Err(LocalError::SessionLost);
            }
            self.cleanup.logical_retired = true;
        } else {
            self.cleanup.logical_retired = true;
        }
        if self.declared > 0 {
            let identity = self.identity.as_ref().expect("declared identity");
            let id = wire::TypeId {
                package: identity.code.id.package.clone(),
                name: self.package.as_ref().expect("package").service_type.clone(),
            }
            .project();
            self.registration
                .as_ref()
                .expect("owner")
                .remove_declaration(&id, &identity.host_id)?;
            self.declared = 0;
        }
        self.cleanup.declarations_removed = true;
        if let (Some(server), Some(lease)) = (&self.server, &self.lease) {
            server.release(lease.clone())?;
            self.lease = None;
        }
        self.cleanup.admission_released = true;
        if let Some(premise) = &self.premise {
            if !self.cleanup.package_retired {
                if self.residency.package_accounting(&premise.package)?.usage != 0 {
                    return Err(LocalError::Busy);
                }
                let id = self.residency.final_obligations().next().map(|w| w.id);
                let token = if let Some(id) = id {
                    self.residency.local_deactivation(id)?
                } else if self.explicit {
                    self.residency.begin_local_termination(premise)?
                } else {
                    return Ok(false);
                };
                self.residency
                    .finish_local_deactivation(&token, std::slice::from_ref(premise))?;
                self.cleanup.package_retired = true;
            }
            if self.residency.resources().applications > 0 {
                let id = self.residency.final_obligations().next().map(|w| w.id);
                let token = if let Some(id) = id {
                    self.residency.local_deactivation(id)?
                } else if self.explicit {
                    self.residency.begin_empty_local_application_termination(
                        &premise.package.package.application,
                    )?
                } else {
                    return Ok(false);
                };
                self.residency.finish_local_deactivation(&token, &[])?;
            }
        }
        self.cleanup.package_retired = true;
        self.cleanup.application_retired = true;
        if let Some(deployment) = &self.deployment {
            deployment.release()?;
        }
        self.cleanup.directory_released = true;
        Ok(true)
    }
}
impl Drop for Activation {
    fn drop(&mut self) {
        // Best effort only. Callers must inspect explicit termination errors;
        // dropping this owner is abandonment, not an observed successful cleanup.
        let _ = self.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_physical_cleanup_preserves_process_root_and_exact_final_for_retry() {
        let parent =
            std::env::temp_dir().join(format!("hosting-cleanup-fault-{}", uuid::Uuid::new_v4()));
        let input = parent.join("input");
        std::fs::create_dir_all(input.join("LocalService/Code")).unwrap();
        std::fs::write(
            input.join("ApplicationManifest.xml"),
            include_str!("../../../samples/hosting-local-app/pkg/ApplicationManifest.xml"),
        )
        .unwrap();
        std::fs::write(
            input.join("LocalService/ServiceManifest.xml"),
            include_str!("../../../samples/hosting-local-app/pkg/LocalService/ServiceManifest.xml"),
        )
        .unwrap();
        std::fs::copy(
            "/bin/sleep",
            input.join("LocalService/Code/hosting-local-app"),
        )
        .unwrap();
        let mut options = Options::new(parent.clone());
        options.residency.shared_grace = GracePeriod::Finite(0);
        let mut owner = Activation::new(options).unwrap();
        owner
            .start(
                &input,
                "fabric:/Local",
                &[("Arguments".into(), "30".into())],
            )
            .unwrap();
        let premise = owner.premise.as_ref().unwrap().clone();
        owner
            .residency
            .schedule_package_timer(&premise.package, 0)
            .unwrap();
        // Test-only deterministic failure at the physical cleanup boundary.
        owner.options.cleanup_budget = Duration::ZERO;
        assert!(owner.drive_deactivation(&ManualClock(0)).is_err());
        let id = owner.residency.final_obligations().next().unwrap().id;
        assert!(!owner.process_status().unwrap().reaped);
        assert!(owner.root().unwrap().exists());
        assert!(
            owner
                .residency
                .package_accounting(&premise.package)
                .unwrap()
                .closing
        );
        owner.options.cleanup_budget = Duration::from_secs(10);
        assert!(owner.terminate().unwrap());
        assert!(owner.residency.local_deactivation(id).is_err());
        assert!(!owner.root().unwrap().exists());
        drop(owner);
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn terminal_empty_owner_cannot_accept_a_later_unowned_launch() {
        let mut owner = Activation::new(Options::new(std::env::temp_dir())).unwrap();
        assert!(owner.terminate().unwrap());
        assert_eq!(
            owner.start(Path::new("/nonexistent"), "fabric:/Local", &[]),
            Err(LocalError::AlreadyStarted)
        );
    }
}
