use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use fube_transport::{
    channel::{MessageChannel, MessageReceiver, RpcChannel},
    connection::{deadline_after, MAX_FRAME_TIME},
    message::{Header, Message},
    native_channel::{
        ChannelLimits, GatewayChannel, GatewayProfile, GatewayReceiver, GatewayTarget,
    },
};
use tokio::{
    fs::{self, File, ReadDir},
    io::AsyncReadExt,
    time::{timeout_at, Instant},
};

use crate::{
    image_store_protocol::{self as wire, FileTransferReply},
    Error, Result,
};

const MAX_DEPTH: usize = 128;
const MAX_ENTRIES: usize = 1_000_000;
const MAX_STALE_REPLIES: usize = 64;

/// Native `fabric:ImageStore` v1.2 uploader for explicitly unsecured development
/// numeric loopback gateways with a nonzero port. Owns a dedicated upload
/// GatewayChannel; no authentication, reconnect or protocol fallback.
pub struct ImageStoreClient {
    channel: GatewayChannel,
    target: Arc<GatewayTarget>,
    replies: GatewayReceiver,
    timeout: Duration,
}

struct OperationGuard {
    channel: GatewayChannel,
    armed: bool,
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.channel.abort();
        }
    }
}

enum Stage {
    Create,
    Chunk(u64),
    Commit(u64),
}

struct Directory {
    source: PathBuf,
    destination: String,
    entries: ReadDir,
}

impl ImageStoreClient {
    /// Connect without TLS/authentication; timeout also defaults later copies.
    pub async fn connect_clear(endpoint: SocketAddr, timeout: Duration) -> Result<Self> {
        let deadline = deadline_after(timeout)?;
        let channel = GatewayChannel::connect_unsecured_loopback(
            endpoint,
            GatewayProfile::Upload,
            ChannelLimits {
                max_ingress: 32,
                max_retained_bytes: 1024 * 1024 * 1024,
                ..ChannelLimits::default()
            },
            deadline,
        )
        .await?;
        let target = channel.resolve_target(&endpoint.to_string())?;
        let replies = channel.receiver()?;
        Ok(Self {
            channel,
            target,
            replies,
            timeout,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.channel.terminal_error().is_some()
    }

    pub fn terminal_error(&self) -> Option<Error> {
        self.channel.terminal_error()
    }

    pub async fn close(&self) -> Result<()> {
        self.channel
            .close(Instant::now() + MAX_FRAME_TIME + Duration::from_secs(1))
            .await
    }

    /// Replace the entire destination, upload descendants, then publish empty
    /// `_.dir` markers postorder. `None` uses the connection timeout, not infinity.
    ///
    /// Sources are not snapshotted. Symlinks/special entries are unsupported.
    /// Failures may leave partial remote content; session cleanup is not rollback.
    /// Cancellation or uncertain failure retires this socket synchronously.
    pub async fn copy_application_package(
        &mut self,
        source: &Path,
        destination: &str,
        timeout: Option<Duration>,
    ) -> Result<()> {
        if let Some(error) = self.channel.terminal_error() {
            return Err(error);
        }
        let deadline = deadline_after(timeout.unwrap_or(self.timeout))?;
        let destination = validate_destination(destination)?;
        let metadata = timeout_at(deadline, fs::symlink_metadata(source))
            .await
            .map_err(|_| Error::Timeout)??;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(Error::InvalidInput(
                "application package root must be a nonsymlink directory",
            ));
        }
        let entries = timeout_at(deadline, fs::read_dir(source))
            .await
            .map_err(|_| Error::Timeout)??;
        let mut guard = OperationGuard {
            channel: self.channel.clone(),
            armed: true,
        };
        let mut session = None;
        let result = self
            .copy(source, destination, entries, deadline, &mut session)
            .await;
        if let Err(error) = &result {
            // Only session-scoped cleanup, only inside the original budget.
            if let Some(activity) = session {
                if Instant::now() < deadline && !self.is_closed() {
                    let _ = self
                        .send(
                            "FileUploadDeleteSession",
                            activity,
                            vec![],
                            vec![],
                            deadline,
                        )
                        .await;
                }
            }
            self.channel.abort_with_error(error.clone());
        } else {
            guard.armed = false;
        }
        result
    }

    async fn copy(
        &mut self,
        source: &Path,
        destination: String,
        entries: ReadDir,
        deadline: Instant,
        session: &mut Option<[u8; 16]>,
    ) -> Result<()> {
        let activity = uuid::Uuid::new_v4().to_bytes_le();
        let (mut headers, body) = wire::delete(&destination, activity)?;
        headers.extend([
            Header::actor(5)?,
            Header::action("ForwardMessage")?,
            Header::timeout(wire::remaining_ticks(deadline)?)?,
        ]);
        let message = self
            .channel
            .request(&self.target, Message { headers, body }, deadline)
            .await
            .map_err(|error| self.terminal_error().unwrap_or(error))?;
        if let Some(error) = self.terminal_error() {
            return Err(error);
        }
        let deleted = wire::decode_delete(message)?;
        if deleted.is_some_and(|reply| reply != activity) {
            return Err(Error::Protocol(
                "Delete success Activity does not match request",
            ));
        }
        let mut stack = vec![Directory {
            source: source.into(),
            destination,
            entries,
        }];
        let mut visited = 0usize;
        while let Some(directory) = stack.last_mut() {
            let entry = timeout_at(deadline, directory.entries.next_entry())
                .await
                .map_err(|_| Error::Timeout)??;
            if let Some(entry) = entry {
                visited += 1;
                if visited > MAX_ENTRIES {
                    return Err(Error::ResourceLimit("application package entries"));
                }
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or(Error::InvalidInput("package filename is not UTF-8"))?;
                if name == "_.dir" {
                    continue;
                }
                if name.contains('\\') || name.contains('\0') {
                    return Err(Error::InvalidInput(
                        "package filename contains native path separator or NUL",
                    ));
                }
                let path = directory.source.join(name);
                let remote = format!("{}/{}", directory.destination, name);
                // Detect unsupported entries, including symlinks, without following.
                let metadata = timeout_at(deadline, fs::symlink_metadata(&path))
                    .await
                    .map_err(|_| Error::Timeout)??;
                if metadata.is_dir() {
                    if stack.len() == MAX_DEPTH {
                        return Err(Error::ResourceLimit("application package directory depth"));
                    }
                    let entries = timeout_at(deadline, fs::read_dir(&path))
                        .await
                        .map_err(|_| Error::Timeout)??;
                    stack.push(Directory {
                        source: path,
                        destination: remote,
                        entries,
                    });
                } else if metadata.is_file() {
                    let file = timeout_at(deadline, File::open(&path))
                        .await
                        .map_err(|_| Error::Timeout)??;
                    self.upload(&remote, Some(file), metadata.len(), deadline, session)
                        .await?;
                } else {
                    return Err(Error::InvalidInput(
                        "package contains unsupported symlink or special entry",
                    ));
                }
            } else {
                let directory = stack.pop().unwrap();
                self.upload(
                    &format!("{}/_.dir", directory.destination),
                    None,
                    0,
                    deadline,
                    session,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn send(
        &self,
        action: &str,
        activity: [u8; 16],
        extra: Vec<fube_transport::message::Header>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<()> {
        let mut headers = wire::headers(activity)?;
        headers.extend(extra);
        headers.extend([
            Header::actor(26)?,
            Header::action(action)?,
            Header::timeout(wire::remaining_ticks(deadline)?)?,
        ]);
        self.channel
            .send_one_way(&self.target, Message { headers, body }, deadline)
            .await
            .map_err(|error| self.terminal_error().unwrap_or(error))
    }

    async fn receive(
        &mut self,
        activity: [u8; 16],
        stage: Stage,
        deadline: Instant,
    ) -> Result<FileTransferReply> {
        let mut stale = 0;
        loop {
            let incoming = self
                .replies
                .receive(deadline)
                .await
                .map_err(|error| self.terminal_error().unwrap_or(error))?;
            if let Some(error) = self.terminal_error() {
                return Err(error);
            }
            let reply = wire::decode_reply(&incoming.message)?;
            if let Some(sequence) = reply.sequence {
                negotiated_size(sequence.buffer_size)?;
            }
            let obsolete = if reply.activity != activity {
                true
            } else {
                match (&stage, reply.actor, reply.sequence) {
                    (Stage::Create, 25, Some(sequence)) if sequence.number == u64::MAX => false,
                    (Stage::Chunk(expected), 25, Some(sequence))
                        if sequence.number == *expected =>
                    {
                        false
                    }
                    (Stage::Chunk(expected), 25, Some(sequence))
                        if sequence.number == u64::MAX || sequence.number < *expected =>
                    {
                        true
                    }
                    (Stage::Commit(last), 25, Some(sequence))
                        if sequence.number == u64::MAX || sequence.number <= *last =>
                    {
                        true
                    }
                    (Stage::Commit(_), 27, None) => false,
                    _ => {
                        return Err(Error::Protocol(
                            "native upload reply has future sequence or wrong stage",
                        ))
                    }
                }
            };
            if !obsolete {
                return Ok(reply);
            }
            stale += 1;
            if stale > MAX_STALE_REPLIES {
                return Err(Error::Protocol(
                    "too many consecutive stale native upload replies",
                ));
            }
        }
    }

    async fn upload(
        &mut self,
        path: &str,
        mut file: Option<File>,
        size: u64,
        deadline: Instant,
        session: &mut Option<[u8; 16]>,
    ) -> Result<()> {
        let activity = uuid::Uuid::new_v4().to_bytes_le();
        let create = wire::create(path, size)?;
        *session = Some(activity);
        self.send("FileCreate", activity, vec![create], vec![], deadline)
            .await?;
        let created = self.receive(activity, Stage::Create, deadline).await?;
        created.result?;
        let chunk_size = negotiated_size(created.sequence.unwrap().buffer_size)?;
        let mut number = 0;
        let mut remaining = size;
        loop {
            let count = remaining.min(chunk_size) as usize;
            let mut body = vec![0; count];
            if let Some(file) = &mut file {
                timeout_at(deadline, file.read_exact(&mut body))
                    .await
                    .map_err(|_| Error::Timeout)??;
                if remaining == count as u64 {
                    let mut probe = [0];
                    if timeout_at(deadline, file.read(&mut probe))
                        .await
                        .map_err(|_| Error::Timeout)??
                        != 0
                    {
                        return Err(Error::InvalidInput("package file grew during upload"));
                    }
                }
            }
            self.send(
                "FileContent",
                activity,
                vec![wire::sequence(number, count as u64)?],
                body,
                deadline,
            )
            .await?;
            let acknowledged = self
                .receive(activity, Stage::Chunk(number), deadline)
                .await?;
            acknowledged.result?;
            if negotiated_size(acknowledged.sequence.unwrap().buffer_size)? != chunk_size {
                return Err(Error::Protocol(
                    "native upload changed negotiated chunk size",
                ));
            }
            remaining -= count as u64;
            if remaining == 0 {
                break;
            }
            number = number
                .checked_add(1)
                .ok_or(Error::ResourceLimit("native chunk sequence"))?;
        }
        loop {
            self.send("FileUploadCommit", activity, vec![], vec![], deadline)
                .await?;
            let committed = self
                .receive(activity, Stage::Commit(number), deadline)
                .await?;
            match committed.result {
                Err(Error::Remote {
                    code: wire::OPERATIONS_PENDING,
                    ..
                }) => {
                    timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
                        .await
                        .map_err(|_| Error::Timeout)?;
                }
                result => {
                    result?;
                    break;
                }
            }
        }
        self.send("FileUploadCommitAck", activity, vec![], vec![], deadline)
            .await?;
        *session = None;
        Ok(())
    }
}

fn negotiated_size(size: u64) -> Result<u64> {
    let size = if size == 0 {
        wire::DEFAULT_CHUNK_SIZE
    } else {
        size
    };
    if size > wire::chunk_capacity()? {
        return Err(Error::Protocol(
            "native chunk threshold exceeds complete transport frame capacity",
        ));
    }
    Ok(size)
}

fn validate_destination(destination: &str) -> Result<String> {
    let check_root = |path: &str| {
        if path.is_empty()
            || path.starts_with(['/', '\\'])
            || path.contains('\0')
            || (path.as_bytes().get(1) == Some(&b':') && path.as_bytes()[0].is_ascii_alphabetic())
        {
            return Err(Error::InvalidInput(
                "ImageStore destination must be a nonrooted relative path",
            ));
        }
        Ok(())
    };
    let destination = destination.trim();
    check_root(destination)?;
    let mut normalized = destination.replace('\\', "/");
    loop {
        check_root(&normalized)?;
        let mut components = Vec::new();
        for component in normalized.split('/') {
            if component == ".." {
                return Err(Error::InvalidInput(
                    "ImageStore destination cannot traverse parents",
                ));
            }
            if !component.is_empty() && component != "." {
                components.push(component);
            }
        }
        // Collapsing components can expose outer spaces, which native FSS
        // trims. Trimming can in turn expose another dot/root/parent alias.
        let next = components.join("/").trim().to_owned();
        if next == normalized {
            break;
        }
        normalized = next;
    }
    check_root(&normalized)?;
    let top = normalized.split('/').next().unwrap();
    if top.eq_ignore_ascii_case("Store") || top.eq_ignore_ascii_case("WindowsFabricStore") {
        return Err(Error::InvalidInput(
            "ImageStore destination is a reserved system directory",
        ));
    }
    wire::preflight_root_marker(&normalized)?;
    wire::delete(&normalized, [1; 16])?;
    Ok(normalized)
}

#[cfg(test)]
#[path = "image_store_tests.rs"]
mod tests;
