use crate::profile::{err, private_dir, read_json, write_json, CleanupReceipt, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    deployment: String,
    clean: bool,
    predecessor: Option<String>,
    receipts: Option<Vec<CleanupReceipt>>,
}

pub struct Interlock {
    pub root: PathBuf,
    pub deployment: String,
    lock: File,
    predecessor: Option<String>,
}
impl Interlock {
    pub fn acquire(root: &Path) -> Result<Self> {
        if !root.is_absolute() {
            return Err("--root must be an absolute private profile root".into());
        }
        if !root.exists() {
            private_dir(root)?;
        }
        let metadata = fs::symlink_metadata(root).map_err(err)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("profile root must be owned, private (0700), and not a symlink".into());
        }
        let root = root.canonicalize().map_err(err)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join("owner.lock"))
            .map_err(err)?;
        if !lock.metadata().map_err(err)?.is_file()
            || unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
        {
            return Err("profile root already owned or invalid lock".into());
        }
        let authority_path = root.join("authority.json");
        let marker_path = root.join("handoff.json");
        let predecessor = if authority_path.exists() {
            let marker: Marker = read_json(&authority_path)?;
            let handoff: Marker = read_json(&marker_path)
                .map_err(|error| format!("missing predecessor handoff: {error}"))?;
            uuid::Uuid::parse_str(&marker.deployment).map_err(err)?;
            if marker.clean
                || !handoff.clean
                || handoff.deployment != marker.deployment
                || handoff.predecessor != marker.predecessor
                || handoff.receipts.as_ref().is_none_or(|receipts| {
                    receipts.len() > 3
                        || receipts
                            .iter()
                            .any(|receipt| receipt.deployment != marker.deployment)
                })
                || root.join("deployment").exists()
            {
                return Err(
                    "dirty predecessor: original cleanup proof required; no reset path".into(),
                );
            }
            Some(marker.deployment)
        } else {
            let unexpected = fs::read_dir(&root)
                .map_err(err)?
                .any(|entry| entry.map_or(true, |entry| entry.file_name() != "owner.lock"));
            if unexpected {
                return Err("missing predecessor handoff in nonempty profile root".into());
            }
            None
        };
        let deployment = uuid::Uuid::new_v4().to_string();
        write_json(
            &authority_path,
            &Marker {
                deployment: deployment.clone(),
                clean: false,
                predecessor: predecessor.clone(),
                receipts: None,
            },
        )?;
        write_json(
            &marker_path,
            &Marker {
                deployment: deployment.clone(),
                clean: false,
                predecessor: predecessor.clone(),
                receipts: None,
            },
        )?;
        Ok(Self {
            root,
            deployment,
            lock,
            predecessor,
        })
    }
    pub fn publish_clean(&self, receipts: &[CleanupReceipt]) -> Result<()> {
        if self.root.join("deployment").exists() {
            return Err("runtime tree remains; clean handoff forbidden".into());
        }
        if receipts.len() > 3
            || receipts
                .iter()
                .any(|receipt| receipt.deployment != self.deployment)
            || receipts.iter().enumerate().any(|(i, receipt)| {
                receipts[..i]
                    .iter()
                    .any(|old| old.node == receipt.node || old.session == receipt.session)
            })
        {
            return Err("clean handoff proof does not match original deployment children".into());
        }
        write_json(
            &self.root.join("handoff.json"),
            &Marker {
                deployment: self.deployment.clone(),
                clean: true,
                predecessor: self.predecessor.clone(),
                receipts: Some(receipts.to_vec()),
            },
        )
    }
}
impl Drop for Interlock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.lock.as_raw_fd(), libc::LOCK_UN) };
    }
}
