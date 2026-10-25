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

use crate::{deployment::Deployment, wire, HostError, Result};

const LOG_LIMIT: usize = 1024 * 1024;
pub struct OwnedApp {
    child: Child,
    logs: Arc<Mutex<VecDeque<String>>>,
    readers: Vec<JoinHandle<()>>,
    reaped: bool,
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
        let mut command = Command::new(bundle.code.join("stateless-test-app"));
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &bundle.root)
            .env(
                "LD_LIBRARY_PATH",
                std::env::join_paths([native, dependencies])?,
            )
            .env("FabricConfigFileName", &bundle.config)
            .env("FabricDataRoot", bundle.root.join("native-data"))
            .env(
                "FabricLogRoot",
                bundle
                    .root
                    .join("deployment")
                    .join(wire::APP_ID)
                    .join("log"),
            )
            .env("FabricCodePath", native)
            .env("Fabric_ApplicationHostId", host)
            .env("Fabric_ApplicationHostType", "Activated_SingleCodePackage")
            .env("Fabric_IsContainerHost", "false")
            .env("Fabric_IsCodePackageActivatorHost", "false")
            .env("Fabric_RuntimeConnectionAddress", address)
            .env("Fabric_RuntimeConnectionUseSsl", "false")
            .env("Fabric_ApplicationId", wire::APP_ID)
            .env("Fabric_ApplicationName", wire::APP_NAME)
            .env("Fabric_ServicePackageName", wire::PACKAGE)
            .env("Fabric_CodePackageName", wire::CODE)
            .env("Fabric_CodePackageInstanceSeqNum", "1")
            .env("Fabric_ServicePackageInstanceSeqNum", "1")
            .env("Fabric_ServicePackageVersionInstance", "1.0:1.0:1")
            .env("Fabric_ServicePackageActivationId", "")
            .current_dir(&bundle.code)
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
        #[cfg(unix)]
        for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                let error = std::io::Error::last_os_error();
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
        let logs = Arc::new(Mutex::new(VecDeque::new()));
        let stop_logs = Arc::new(AtomicBool::new(false));
        let readers = vec![
            capture(stdout, logs.clone(), stop_logs.clone()),
            capture(stderr, logs.clone(), stop_logs.clone()),
        ];
        Ok(Self {
            child,
            logs,
            readers,
            reaped: false,
            stop_logs,
        })
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
        if let Some(status) = self.child.try_wait()? {
            self.reaped = true;
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
            if let Some(status) = self.child.try_wait()? {
                self.reaped = true;
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

    pub fn cleanup(&mut self) -> Result<serde_json::Value> {
        let forced = !self.reaped && self.child.try_wait()?.is_none();
        if forced {
            #[cfg(unix)]
            if unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }

            #[cfg(not(unix))]
            self.child.kill()?;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.reaped = true;
                self.stop_logs.store(true, Ordering::Release);
                for reader in self.readers.drain(..) {
                    let _ = reader.join();
                }
                return Ok(serde_json::json!({"pid":self.pid(), "forced":forced,
                    "reaped":true, "status":status.to_string()}));
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
        if !self.reaped {
            #[cfg(unix)]
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
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
            stop_logs: Arc::default(),
        };
        assert!(owned
            .wait_exit(Instant::now() + Duration::from_secs(2))
            .is_err());
        assert_eq!(owned.cleanup().unwrap()["reaped"], true);
    }
}
