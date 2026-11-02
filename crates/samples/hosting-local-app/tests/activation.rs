//! Real native-free package/process/IPC execution through production Hosting.
use fube_hosting::{
    local::{Activation, LocalError, Options},
    *,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

struct Fixture {
    parent: PathBuf,
    package: PathBuf,
}
fn acquire(owner: &mut Activation) -> Acquisition {
    match owner.acquire_usage() {
        UsageIncrement::Acquired(a) => a,
        other => panic!("unexpected acquisition {other:?}"),
    }
}
impl Fixture {
    fn new() -> Self {
        let parent = std::env::temp_dir().join(format!("hosting-owned-{}", uuid::Uuid::new_v4()));
        let package = parent.join("input");
        fs::create_dir_all(package.join("LocalService/Code")).unwrap();
        fs::write(
            package.join("ApplicationManifest.xml"),
            include_str!("../pkg/ApplicationManifest.xml"),
        )
        .unwrap();
        fs::write(
            package.join("LocalService/ServiceManifest.xml"),
            include_str!("../pkg/LocalService/ServiceManifest.xml"),
        )
        .unwrap();
        fs::copy(
            env!("CARGO_BIN_EXE_hosting-local-app"),
            package.join("LocalService/Code/hosting-local-app"),
        )
        .unwrap();
        Self { parent, package }
    }
    fn options(&self) -> Options {
        let mut options = Options::new(self.parent.clone());
        options.residency.shared_grace = GracePeriod::Finite(10);
        options
    }
    fn owner(&self, args: &str) -> Activation {
        let mut owner = Activation::new(self.options()).unwrap();
        owner
            .start(
                &self.package,
                "fabric:/Local",
                &[("Arguments".into(), args.into())],
            )
            .unwrap();
        owner
    }
    fn assert_released(&self, owner: &Activation, pid: u32, root: &Path) {
        assert!(owner.cleanup_status().directory_released);
        let status = owner.process_status().unwrap();
        assert!(
            status.termination_requested
                && status.exit_observed
                && status.group_drained
                && status.reaped
        );
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "direct child must be reaped"
        );
        assert!(!root.exists(), "owned root must be released");
        assert!(self.package.is_dir(), "caller input survives");
        assert_eq!(owner.residency().resources().logical_bytes, 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.parent).unwrap();
    }
}

#[test]
fn owned_runtime_registers_without_native_libraries_or_callback_shortcuts_and_fresh_relaunch() {
    let fixture = Fixture::new();
    let mut previous = None;
    for _ in 0..2 {
        let mut owner = fixture.owner("");
        owner
            .wait_registration(Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("{e}: {:?}", owner.logs()));
        let identity = owner.identity().unwrap().clone();
        let pid = owner.process_status().unwrap().pid;
        let status = owner.session_status().unwrap().unwrap();
        assert!(status.connected && status.registered_host);
        assert_eq!(status.runtimes.len(), 1);
        assert_eq!(status.runtimes[0].process_id, pid);
        assert_eq!(
            status.runtimes[0].package_instance_sequence,
            identity.code.package_sequence
        );
        let maps = fs::read_to_string(format!("/proc/{pid}/maps")).unwrap();
        assert!(!maps.contains("libFabricRuntime") && !maps.contains("libFabricCommon"));
        let environment = fs::read(format!("/proc/{pid}/environ")).unwrap();
        for item in environment.split(|b| *b == 0) {
            assert!(
                !item.starts_with(b"LD_LIBRARY_PATH=")
                    && !item.starts_with(b"FabricConfigFileName=")
            );
        }
        assert!(!owner
            .logs()
            .iter()
            .any(|line| line.contains("LOCAL_CALLBACK")));
        if let Some((host, code, package)) = previous {
            assert_ne!(identity.host_id, host);
            assert_ne!(identity.code.code_sequence, code);
            assert_ne!(identity.code.package_sequence, package);
        }
        previous = Some((
            identity.host_id,
            identity.code.code_sequence,
            identity.code.package_sequence,
        ));
        let root = owner.root().unwrap().to_path_buf();
        assert!(owner.terminate().unwrap());
        let outputs = owner.take_outputs().unwrap().unwrap();
        assert!(!outputs.events.is_empty());
        assert!(owner.terminate().unwrap(), "cleanup is idempotent");
        fixture.assert_released(&owner, pid, &root);
    }
}

#[test]
fn usage_residency_fences_idle_then_package_and_application_grace_are_distinct() {
    let fixture = Fixture::new();
    let mut owner = fixture.owner("");
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    let acquisition = acquire(&mut owner);
    assert!(!owner.drive_deactivation(&ManualClock(100)).unwrap());
    assert!(!owner.process_status().unwrap().termination_requested);
    assert!(matches!(
        owner.release_usage(acquisition),
        UsageRelease::Released
    ));
    assert!(!owner.drive_deactivation(&ManualClock(109)).unwrap());
    assert!(
        !owner.drive_deactivation(&ManualClock(110)).unwrap(),
        "package stops, application grace retained"
    );
    assert!(owner.cleanup_status().package_retired);
    assert!(!owner.cleanup_status().application_retired);
    assert!(root.exists());
    assert!(owner.drive_deactivation(&ManualClock(120)).unwrap());
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn observer_timeout_does_not_cancel_accepted_launch() {
    let fixture = Fixture::new();
    let mut owner = fixture.owner("--delay-ms 150");
    assert_eq!(
        owner.wait_registration(Duration::from_millis(5)),
        Err(LocalError::ObservationTimeout)
    );
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert!(Path::new(&format!("/proc/{pid}")).exists());
    assert!(!owner.cleanup_status().authority_revoked);
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    owner.terminate().unwrap();
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn registration_deadline_retains_real_child_until_explicit_cleanup() {
    let fixture = Fixture::new();
    let mut options = fixture.options();
    options.registration_timeout = Duration::from_millis(200);
    let mut owner = Activation::new(options).unwrap();
    owner
        .start(
            &fixture.package,
            "fabric:/Local",
            &[("Arguments".into(), "--no-register".into())],
        )
        .unwrap();
    assert_eq!(
        owner.wait_registration(Duration::from_secs(3)),
        Err(LocalError::RegistrationTimeout)
    );
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert!(root.exists());
    owner.terminate().unwrap();
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn crash_before_and_after_registration_are_not_successful_cleanup() {
    let fixture = Fixture::new();
    for args in ["--exit-before-attach", "--crash-after-ms 100"] {
        let mut owner = fixture.owner(args);
        let mut previous_acquisition = None;
        if args.starts_with("--crash-after") {
            owner.wait_registration(Duration::from_secs(5)).unwrap();
            previous_acquisition = Some(acquire(&mut owner));
            std::thread::sleep(Duration::from_millis(250));
        }
        assert!(owner.wait_registration(Duration::from_secs(3)).is_err());
        let acquisitions = owner.residency().resources().acquisitions;
        assert!(matches!(
            owner.acquire_usage(),
            UsageIncrement::Rejected(Error::InvalidState)
        ));
        assert_eq!(owner.residency().resources().acquisitions, acquisitions);
        let pid = owner.process_status().unwrap().pid;
        let root = owner.root().unwrap().to_path_buf();
        assert!(
            !owner.process_status().unwrap().reaped,
            "exit observation reserves leader"
        );
        assert!(root.exists());
        if let Some(acquisition) = previous_acquisition {
            assert_eq!(owner.terminate(), Err(LocalError::Busy));
            assert!(matches!(
                owner.release_usage(acquisition),
                UsageRelease::Released
            ));
        }
        owner.terminate().unwrap();
        fixture.assert_released(&owner, pid, &root);
    }
}

#[test]
fn explicit_termination_retains_live_acquisition_and_directory_then_retry_completes() {
    let fixture = Fixture::new();
    let mut owner = fixture.owner("");
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let acquisition = acquire(&mut owner);
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert_eq!(owner.terminate(), Err(LocalError::Busy));
    assert!(owner.process_status().unwrap().reaped);
    assert!(root.exists());
    assert_eq!(owner.residency().resources().acquisitions, 1);
    assert!(matches!(
        owner.release_usage(acquisition),
        UsageRelease::Released
    ));
    assert!(owner.terminate().unwrap());
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn unsupported_package_and_required_capabilities_fail_before_side_effects() {
    let fixture = Fixture::new();
    let mut owner = Activation::new(fixture.options()).unwrap();
    let mixed = Path::new(env!("CARGO_MANIFEST_DIR")).join("../rust-runtime-app/pkg");
    assert!(owner.start(&mixed, "fabric:/Mixed", &[]).is_err());
    assert!(owner.root().is_none() && owner.process_status().is_none());
    assert_eq!(fs::read_dir(&fixture.parent).unwrap().count(), 1);
    let mut options = fixture.options();
    options.required.deployed_lifecycle = true;
    assert!(matches!(
        Activation::new(options),
        Err(LocalError::Hosting(Error::UnsupportedCapability))
    ));
    assert_eq!(fs::read_dir(&fixture.parent).unwrap().count(), 1);
}

#[test]
fn launch_failure_keeps_materialized_root_owned_until_cleanup() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    let program = fixture.package.join("LocalService/Code/hosting-local-app");
    fs::write(&program, b"not an executable format").unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).unwrap();
    let mut owner = Activation::new(fixture.options()).unwrap();
    assert!(owner.start(&fixture.package, "fabric:/Local", &[]).is_err());
    let root = owner.root().unwrap().to_path_buf();
    assert!(root.exists());
    assert!(owner.process_status().is_none());
    assert!(owner.terminate().unwrap());
    assert!(!root.exists());
    assert!(fixture.package.exists());
    assert_eq!(owner.residency().resources().logical_bytes, 0);
}

#[test]
fn unused_scan_owns_application_first_deactivation_without_service_callbacks() {
    let fixture = Fixture::new();
    let mut options = fixture.options();
    options.residency.unused_scan_interval = GracePeriod::Finite(20);
    let mut owner = Activation::new(options).unwrap();
    owner.start(&fixture.package, "fabric:/Local", &[]).unwrap();
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert!(!owner.scan_unused(&ManualClock(20)).unwrap());
    assert!(owner.drive_deactivation(&ManualClock(30)).unwrap());
    assert!(!owner
        .logs()
        .iter()
        .any(|line| line.contains("LOCAL_CALLBACK")));
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn directory_release_failure_retains_owned_path_for_explicit_retry() {
    let fixture = Fixture::new();
    let mut owner = fixture.owner("");
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    // Controlled filesystem fault; trusted callers must not mutate deployment
    // trees concurrently in real use. Restore the exact reserved root for retry.
    let held = fixture.parent.join("held-owned-root");
    fs::rename(&root, &held).unwrap();
    fs::write(&root, "injected directory-release failure").unwrap();
    assert!(owner.terminate().is_err());
    assert!(!owner.cleanup_status().directory_released);
    assert!(owner.process_status().unwrap().reaped);
    fs::remove_file(&root).unwrap();
    fs::rename(&held, &root).unwrap();
    assert!(owner.terminate().unwrap());
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn live_owned_session_renews_finite_authority_across_multiple_lease_intervals() {
    let fixture = Fixture::new();
    let mut options = fixture.options();
    options.lease_duration = Duration::from_secs(2);
    let mut owner = Activation::new(options).unwrap();
    owner.start(&fixture.package, "fabric:/Local", &[]).unwrap();
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let acquisition = acquire(&mut owner);
    std::thread::sleep(Duration::from_millis(4500));
    assert!(
        owner.poll_registration().unwrap(),
        "same owned session must remain live"
    );
    let status = owner.session_status().unwrap().unwrap();
    assert!(status.connected && status.loss.is_none());
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert!(matches!(
        owner.release_usage(acquisition),
        UsageRelease::Released
    ));
    owner.terminate().unwrap();
    fixture.assert_released(&owner, pid, &root);
}

#[test]
fn terminal_session_loss_rejects_new_usage_but_keeps_existing_owner_releasable() {
    let fixture = Fixture::new();
    let mut owner = fixture.owner("--shutdown-after-ms 100");
    owner.wait_registration(Duration::from_secs(5)).unwrap();
    let acquisition = acquire(&mut owner);
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(owner.poll_registration(), Err(LocalError::SessionLost));
    assert!(
        !owner.process_status().unwrap().exit_observed,
        "orderly detached child remains alive"
    );
    let acquisitions = owner.residency().resources().acquisitions;
    assert!(matches!(
        owner.acquire_usage(),
        UsageIncrement::Rejected(Error::InvalidState)
    ));
    assert_eq!(owner.residency().resources().acquisitions, acquisitions);
    let pid = owner.process_status().unwrap().pid;
    let root = owner.root().unwrap().to_path_buf();
    assert_eq!(owner.terminate(), Err(LocalError::Busy));
    assert!(matches!(
        owner.release_usage(acquisition),
        UsageRelease::Released
    ));
    assert!(owner.terminate().unwrap());
    fixture.assert_released(&owner, pid, &root);
}
