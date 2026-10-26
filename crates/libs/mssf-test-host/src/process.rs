#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::process::CommandExt};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{deployment::Deployment, HostError, Result};

const LOG_LIMIT: usize = 1024 * 1024;

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

pub struct OwnedApp {
    child: Child,
    logs: Arc<Mutex<VecDeque<String>>>,
    readers: Vec<JoinHandle<()>>,
    reaped: bool,
    group_cleaned: bool,
    stop_logs: Arc<AtomicBool>,
}

fn capture(
    input: impl Read + Send + 'static,
    logs: Arc<Mutex<VecDeque<String>>>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut input = input;
        let mut chunk = [0; 4096];
        let mut tail = Vec::new();
        loop {
            let count = match input.read(&mut chunk) {
                Ok(count) => count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(_) => break,
            };
            if count == 0 {
                break;
            }
            for byte in &chunk[..count] {
                if *byte == b'\n' || tail.len() == 16 * 1024 {
                    let line = String::from_utf8_lossy(&tail).into_owned();
                    let mut logs = logs.lock().unwrap();
                    while logs.len() >= 4096
                        || logs.iter().map(String::len).sum::<usize>() + line.len() > LOG_LIMIT
                    {
                        logs.pop_front();
                    }
                    logs.push_back(line);
                    tail.clear();
                }
                if *byte != b'\n' {
                    tail.push(*byte);
                }
            }
        }
        if !tail.is_empty() {
            let mut logs = logs.lock().unwrap();
            let line = String::from_utf8_lossy(&tail).into_owned();
            while logs.len() >= 4096
                || logs.iter().map(String::len).sum::<usize>() + line.len() > LOG_LIMIT
            {
                logs.pop_front();
            }
            logs.push_back(line);
        }
    })
}

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
            .current_dir(&bundle.working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            command.process_group(0);
            // Parent-death containment and no potentially enormous native core.
            unsafe {
                command.pre_exec(|| {
                    let parent = libc::getppid();
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if parent == 1 || libc::getppid() != parent {
                        return Err(std::io::Error::other("owner exited during app launch"));
                    }
                    let limit = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::setrlimit(libc::RLIMIT_CORE, &limit) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let logs = Arc::new(Mutex::new(VecDeque::new()));
        let stop_logs = Arc::new(AtomicBool::new(false));
        let mut owned = Self {
            child,
            logs: logs.clone(),
            readers: Vec::new(),
            reaped: false,
            group_cleaned: false,
            stop_logs: stop_logs.clone(),
        };
        #[cfg(unix)]
        for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                let error = std::io::Error::last_os_error();
                if let Err(cleanup) = owned.cleanup() {
                    return Err(HostError(format!(
                        "{error}; additionally cleanup failed: {cleanup}"
                    )));
                }
                return Err(error.into());
            }
        }
        owned.readers = vec![
            capture(stdout, logs.clone(), stop_logs.clone()),
            capture(stderr, logs.clone(), stop_logs.clone()),
        ];
        Ok(owned)
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().iter().cloned().collect()
    }

    pub fn callbacks(&self) -> Vec<serde_json::Value> {
        self.logs()
            .iter()
            .filter_map(|line| {
                line.find("MSSF_APP ")
                    .and_then(|start| serde_json::from_str(&line[start + 9..]).ok())
            })
            .collect()
    }

    pub fn check_alive(&mut self) -> Result<()> {
        if let Some(status) = self.observed_exit()? {
            return Err(HostError(format!("native child exited: {status}")));
        }
        Ok(())
    }

    pub fn command(&mut self, command: &[u8]) -> Result<()> {
        self.child
            .stdin
            .as_mut()
            .ok_or_else(|| HostError("app stdin closed".into()))?
            .write_all(command)?;
        Ok(())
    }

    pub fn wait_exit(&mut self, deadline: Instant) -> Result<()> {
        loop {
            if let Some(status) = self.observed_exit()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(HostError(format!("native app exit failed: {status}")))
                };
            }
            if Instant::now() >= deadline {
                return Err(HostError("graceful app exit deadline exceeded".into()));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Keep an exited Linux leader waitable until its group is drained. Reaping
    // early releases the PID/PGID reservation and makes later group kills unsafe.
    #[cfg(target_os = "linux")]
    fn observed_exit(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        use std::os::unix::process::ExitStatusExt;
        if self.reaped {
            return self.child.try_wait();
        }
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::waitid(
                libc::P_PID,
                self.pid(),
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if unsafe { info.si_pid() } == 0 {
            return Ok(None);
        }
        let status = unsafe { info.si_status() };
        let raw = match info.si_code {
            libc::CLD_EXITED => status << 8,
            libc::CLD_KILLED => status,
            libc::CLD_DUMPED => status | 0x80,
            _ => return Err(std::io::Error::other("unexpected owned child wait state")),
        };
        Ok(Some(std::process::ExitStatus::from_raw(raw)))
    }

    #[cfg(not(target_os = "linux"))]
    fn observed_exit(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        self.reaped |= status.is_some();
        Ok(status)
    }

    #[cfg(target_os = "linux")]
    fn group_running(&self, deadline: Instant) -> Result<bool> {
        for entry in std::fs::read_dir("/proc")? {
            if Instant::now() >= deadline {
                return Err(HostError("owned-group cleanup deadline exceeded".into()));
            }
            let entry = entry?;
            if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
                continue;
            }
            let stat = match std::fs::read(entry.path().join("stat")) {
                Ok(stat) => stat,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                    ) =>
                {
                    continue
                }
                Err(error) => return Err(error.into()),
            };
            // comm can contain spaces/parentheses. Fields after its final ')':
            // state, ppid, pgrp. Zombies are terminated, but only our direct
            // child is waitable/reapable by this supervisor.
            let end = stat
                .iter()
                .rposition(|byte| *byte == b')')
                .ok_or_else(|| HostError("invalid proc process stat".into()))?;
            let fields: Vec<_> = std::str::from_utf8(&stat[end + 1..])
                .map_err(|error| HostError(format!("invalid proc stat fields: {error}")))?
                .split_ascii_whitespace()
                .take(3)
                .collect();
            require_stat(&fields)?;
            if fields[2].parse::<u32>().ok() == Some(self.pid()) && !["Z", "X"].contains(&fields[0])
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn cleanup(&mut self) -> Result<serde_json::Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        #[cfg(target_os = "linux")]
        let forced = if self.group_cleaned {
            false
        } else {
            // Check ownership before any numeric PGID signal. WNOWAIT keeps the
            // leader's identity reserved even if it exited during a callback.
            let _ = self.observed_exit()?;
            let mut running = self.group_running(deadline)?;
            // Always signal while ownership is reserved, including when the
            // leader is a zombie: a helper may fork/exit during the proc scan.
            if unsafe { libc::kill(-(self.pid() as i32), libc::SIGKILL) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error.into());
                }
            }
            while self.group_running(deadline)? {
                running = true;
                if unsafe { libc::kill(-(self.pid() as i32), libc::SIGKILL) } != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error.into());
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
            self.group_cleaned = true;
            running
        };
        #[cfg(not(target_os = "linux"))]
        let forced = {
            let running = !self.reaped && self.child.try_wait()?.is_none();
            if running {
                #[cfg(unix)]
                if unsafe { libc::kill(-(self.pid() as i32), libc::SIGKILL) } != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                #[cfg(not(unix))]
                self.child.kill()?;
            }
            self.group_cleaned = true;
            running
        };
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.reaped = true;
                self.stop_logs.store(true, Ordering::Release);
                for reader in self.readers.drain(..) {
                    let _ = reader.join();
                }
                return Ok(serde_json::json!({"pid":self.pid(), "forced":forced,
                    "reaped":true, "status":status.to_string(),
                    "process_group_drained":cfg!(target_os = "linux") && self.group_cleaned}));
            }
            if Instant::now() >= deadline {
                return Err(HostError("owned-child reap deadline exceeded".into()));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for OwnedApp {
    fn drop(&mut self) {
        self.stop_logs.store(true, Ordering::Release);
        if !self.reaped || !self.group_cleaned {
            let _ = self.cleanup();
        }
    }
}

#[cfg(target_os = "linux")]
fn require_stat(fields: &[&str]) -> Result<()> {
    if fields.len() == 3 && fields[2].parse::<u32>().is_ok() {
        Ok(())
    } else {
        Err(HostError("invalid proc process group fields".into()))
    }
}
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn forced_cleanup_kills_and_reaps_only_owned_child() {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut owned = OwnedApp {
            child,
            logs: Arc::default(),
            readers: vec![],
            reaped: false,
            group_cleaned: false,
            stop_logs: Arc::default(),
        };
        let result = owned.cleanup().unwrap();
        assert_eq!(result["forced"], true);
        assert_eq!(result["reaped"], true);
        assert_eq!(unsafe { libc::kill(owned.pid() as i32, 0) }, -1);
        assert_eq!(owned.cleanup().unwrap()["forced"], false);
    }

    #[test]
    fn early_child_failure_is_not_scenario_success() {
        let child = Command::new("/bin/false").process_group(0).spawn().unwrap();
        let mut owned = OwnedApp {
            child,
            logs: Arc::default(),
            readers: vec![],
            reaped: false,
            group_cleaned: false,
            stop_logs: Arc::default(),
        };
        assert!(owned
            .wait_exit(Instant::now() + Duration::from_secs(2))
            .is_err());
        assert_eq!(owned.cleanup().unwrap()["reaped"], true);
    }

    #[test]
    fn expired_protocol_admission_and_caller_failure_still_reap_owned_app() {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut owned = OwnedApp {
            child,
            logs: Arc::default(),
            readers: vec![],
            reaped: false,
            group_cleaned: false,
            stop_logs: Arc::default(),
        };
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
        assert_eq!(unsafe { libc::kill(owned.pid() as i32, 0) }, -1);
        drop(session);
        std::fs::remove_dir_all(bundle.root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn exited_leader_keeps_group_owned_until_helpers_are_terminated() {
        use std::os::unix::process::ExitStatusExt;
        for mode in 0..3 {
            let through_check_alive = mode != 1;
            let child = Command::new("/bin/sh")
                .args(["-c", "read line; exit 0"])
                .process_group(0)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let mut owned = OwnedApp {
                child,
                logs: Arc::default(),
                readers: vec![],
                reaped: false,
                group_cleaned: false,
                stop_logs: Arc::default(),
            };
            // A directly waitable test helper joins the same owned group. This
            // permits independent reap/assertions without becoming a subreaper.
            let mut helper = Command::new("/bin/sleep")
                .arg("30")
                .process_group(owned.pid() as i32)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            assert_eq!(
                unsafe { libc::getpgid(helper.id() as i32) },
                owned.pid() as i32
            );
            drop(owned.child.stdin.take());
            let deadline = Instant::now() + Duration::from_secs(3);
            let outcome = if through_check_alive {
                loop {
                    if let Err(error) = owned.check_alive() {
                        break Err(error);
                    }
                    assert!(Instant::now() < deadline);
                    thread::sleep(Duration::from_millis(5));
                }
            } else {
                owned.wait_exit(deadline).unwrap();
                Ok(())
            };
            assert!(
                !owned.reaped,
                "exit observation must not release PID/PGID ownership"
            );
            assert!(helper.try_wait().unwrap().is_none());
            if mode == 2 {
                drop(owned);
                assert_eq!(helper.wait().unwrap().signal(), Some(libc::SIGKILL));
                continue;
            }
            let cleanup = owned.cleanup().unwrap();
            assert_eq!(cleanup["forced"], true);
            assert_eq!(cleanup["reaped"], true);
            assert_eq!(cleanup["process_group_drained"], true);
            assert_eq!(helper.wait().unwrap().signal(), Some(libc::SIGKILL));
            let result = crate::complete(outcome, Ok(cleanup));
            assert_eq!(result.is_err(), through_check_alive);
            assert_eq!(owned.cleanup().unwrap()["forced"], false);
        }
    }

    #[cfg(target_os = "linux")]
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
}
