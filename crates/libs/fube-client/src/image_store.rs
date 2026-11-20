use std::{
    fmt,
    future::{ready, Ready},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

use furpc::{
    header, ActorId, Body, Channel, HeaderRecord, Headers, IngressLimits, IngressOptions,
    IngressResponse, IngressService, Message, Peer, Request, ServiceBinding, Source,
};
use tokio::{
    fs::{self, File, ReadDir},
    io::AsyncReadExt,
    sync::{mpsc, Notify},
    time::{timeout_at, Instant},
};
use tower_service::Service;

use crate::{
    image_store_protocol::{self as wire, FileTransferReply},
    DomainFailure, FurpcClientError,
};

const MAX_DEPTH: usize = 128;
const MAX_ENTRIES: usize = 1_000_000;
const MAX_STALE_REPLIES: usize = 64;
const MAX_REPLIES: usize = 32;

/// Native upload failures retain their client or original local filesystem cause.
#[derive(Debug, Clone)]
pub enum ImageStoreError {
    Common(FurpcClientError),
    Io(Arc<std::io::Error>),
}

impl fmt::Display for ImageStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Common(error) => error.fmt(f),
            Self::Io(error) => write!(f, "ImageStore filesystem: {error}"),
        }
    }
}

impl std::error::Error for ImageStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Common(error) => Some(error),
            Self::Io(error) => Some(error.as_ref()),
        }
    }
}

impl From<FurpcClientError> for ImageStoreError {
    fn from(error: FurpcClientError) -> Self {
        Self::Common(error)
    }
}

impl From<furpc::Error> for ImageStoreError {
    fn from(error: furpc::Error) -> Self {
        FurpcClientError::from_rpc(error).into()
    }
}

impl From<fubst::Error> for ImageStoreError {
    fn from(error: fubst::Error) -> Self {
        FurpcClientError::Decode(error).into()
    }
}

impl From<std::io::Error> for ImageStoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

pub type ImageStoreResult<T> = std::result::Result<T, ImageStoreError>;
use ImageStoreResult as Result;

/// Local upload-operation defaults, independent of endpoint establishment.
#[derive(Debug, Clone, Copy)]
pub struct ImageStoreClientOptions {
    /// Whole-copy timeout used when an upload passes `None`; never infinite.
    pub default_timeout: Duration,
}

impl Default for ImageStoreClientOptions {
    fn default() -> Self {
        Self {
            default_timeout: crate::DEFAULT_TIMEOUT,
        }
    }
}

struct Connection {
    channel: Option<Channel>,
    error: Option<ImageStoreError>,
}

struct UploadState {
    connection: Mutex<Connection>,
    failed: Notify,
}

impl UploadState {
    fn check(&self) -> Result<()> {
        match &self.connection.lock().unwrap().error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn cause(&self, error: ImageStoreError) -> ImageStoreError {
        self.connection
            .lock()
            .unwrap()
            .error
            .clone()
            .unwrap_or(error)
    }

    fn fail(&self, error: ImageStoreError) {
        let channel = {
            let mut connection = self.connection.lock().unwrap();
            if connection.error.is_none() {
                connection.error = Some(error);
            }
            connection.channel.take()
        };
        drop(channel);
        self.failed.notify_waiters();
    }

    async fn failure(&self) -> ImageStoreError {
        loop {
            let failed = self.failed.notified();
            tokio::pin!(failed);
            failed.as_mut().enable();
            if let Some(error) = &self.connection.lock().unwrap().error {
                return error.clone();
            }
            failed.await;
        }
    }
}

/// Dedicated native `fabric:ImageStore` v1.2 uploader over furpc.
///
/// Captures one original-generation Peer. No authentication, reconnect or replay.
/// Last-owner release follows furpc lifecycle, not legacy close/abort guarantees.
pub struct ImageStoreClient {
    state: Arc<UploadState>,
    peer: Peer,
    replies: mpsc::Receiver<FileTransferReply>,
    timeout: Duration,
}

struct OperationGuard {
    state: Arc<UploadState>,
    armed: bool,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.state.fail(furpc::Error::Cancelled.into());
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
    /// Configure an explicit unsecured native endpoint without dialing.
    pub fn endpoint_clear(address: SocketAddr) -> furpc::Result<furpc::Endpoint> {
        furpc::Endpoint::try_from(address)
    }

    /// Capture the Channel's Peer and install authorized Actor 25/27 one-way ingress.
    ///
    /// No ImageStore session request is sent. Supply a dedicated Channel with no
    /// existing ingress service, and retain no outside clones if uploader failure
    /// or drop must release its last owner. Endpoint limits govern native execution.
    pub async fn new(channel: Channel, options: ImageStoreClientOptions) -> Result<Self> {
        let deadline = deadline_after(options.default_timeout)?;
        let peer = channel.peer(deadline).await?;
        let state = Arc::new(UploadState {
            connection: Mutex::new(Connection {
                channel: None,
                error: None,
            }),
            failed: Notify::new(),
        });
        let (sender, replies) = mpsc::channel(MAX_REPLIES);
        let ingress = IngressOptions::one_way(check_acknowledgment, IngressLimits::default());
        channel.register_service(IngressService::new(
            UploadIngress {
                state: Arc::downgrade(&state),
                generation: peer.info().generation,
                replies: sender,
            },
            vec![
                ServiceBinding::actor(ActorId::new(25)?, ingress.clone())?,
                ServiceBinding::actor(ActorId::new(27)?, ingress)?,
            ],
        )?)?;
        state.connection.lock().unwrap().channel = Some(channel);
        state.check()?;
        Ok(Self {
            state,
            peer,
            replies,
            timeout: options.default_timeout,
        })
    }

    /// Replace the entire destination, upload descendants, then publish empty
    /// `_.dir` markers postorder. `None` uses the configured whole-copy default.
    ///
    /// Sources are not snapshotted. Failures may leave partial remote content.
    /// Cancellation or uncertain failure releases this uploader's Channel owner
    /// and prevents reuse; it is not remote rollback or a physical teardown barrier.
    pub async fn copy_application_package(
        &mut self,
        source: &Path,
        destination: &str,
        timeout: Option<Duration>,
    ) -> Result<()> {
        self.state.check()?;
        let deadline = deadline_after(timeout.unwrap_or(self.timeout))?;
        let destination = validate_destination(destination)?;
        let metadata = timeout_at(deadline, fs::symlink_metadata(source))
            .await
            .map_err(|_| furpc::Error::Timeout("ImageStore source"))??;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(furpc::Error::InvalidInput(
                "application package root must be a nonsymlink directory",
            )
            .into());
        }
        let entries = timeout_at(deadline, fs::read_dir(source))
            .await
            .map_err(|_| furpc::Error::Timeout("ImageStore source"))??;
        let mut guard = OperationGuard {
            state: self.state.clone(),
            armed: true,
        };
        let mut session = None;
        let result = self
            .copy(source, destination, entries, deadline, &mut session)
            .await
            .map_err(|error| self.state.cause(error));
        if let Err(error) = &result {
            if let Some(activity) = session {
                if Instant::now() < deadline && self.state.check().is_ok() {
                    // Best effort within the original budget; never replace the primary failure.
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
            self.state.fail(error.clone());
        }
        guard.armed = false;
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
            header::actor(ActorId::new(5)?)?,
            header::action("ForwardMessage")?,
            wire::timeout(deadline)?,
        ]);
        let mut request = Request::new(body);
        for header in headers {
            request = request.append_header(header)?;
        }
        request.set_deadline(deadline);
        let response = self.peer.request(request, deadline).await?;
        self.state.check()?;
        let deleted = wire::decode_delete(response)?;
        if deleted.is_some_and(|reply| reply != activity) {
            return Err(FurpcClientError::Protocol(
                "Delete success Activity does not match request",
            )
            .into());
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
                .map_err(|_| furpc::Error::Timeout("ImageStore traversal"))??;
            if let Some(entry) = entry {
                visited += 1;
                if visited > MAX_ENTRIES {
                    return Err(furpc::Error::ResourceLimit("application package entries").into());
                }
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or(furpc::Error::InvalidInput("package filename is not UTF-8"))?;
                if name == "_.dir" {
                    continue;
                }
                if name.contains('\\') || name.contains('\0') {
                    return Err(furpc::Error::InvalidInput(
                        "package filename contains native path separator or NUL",
                    )
                    .into());
                }
                let path = directory.source.join(name);
                let remote = format!("{}/{}", directory.destination, name);
                let metadata = timeout_at(deadline, fs::symlink_metadata(&path))
                    .await
                    .map_err(|_| furpc::Error::Timeout("ImageStore traversal"))??;
                if metadata.is_dir() {
                    if stack.len() == MAX_DEPTH {
                        return Err(furpc::Error::ResourceLimit(
                            "application package directory depth",
                        )
                        .into());
                    }
                    let entries = timeout_at(deadline, fs::read_dir(&path))
                        .await
                        .map_err(|_| furpc::Error::Timeout("ImageStore traversal"))??;
                    stack.push(Directory {
                        source: path,
                        destination: remote,
                        entries,
                    });
                } else if metadata.is_file() {
                    let file = timeout_at(deadline, File::open(&path))
                        .await
                        .map_err(|_| furpc::Error::Timeout("ImageStore source"))??;
                    self.upload(&remote, Some(file), metadata.len(), deadline, session)
                        .await?;
                } else {
                    return Err(furpc::Error::InvalidInput(
                        "package contains unsupported symlink or special entry",
                    )
                    .into());
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
        extra: Vec<HeaderRecord>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<()> {
        self.state.check()?;
        let mut headers = wire::headers(activity)?;
        headers.extend(extra);
        headers.extend([
            header::actor(ActorId::new(26)?)?,
            header::action(action)?,
            wire::timeout(deadline)?,
        ]);
        let mut message = Message::builder().body(Body::from_bytes(body)?);
        for header in headers {
            message = message.append_header(header);
        }
        self.peer.send(message.build()?, deadline).await?;
        Ok(())
    }

    async fn receive(
        &mut self,
        activity: [u8; 16],
        stage: Stage,
        deadline: Instant,
    ) -> Result<FileTransferReply> {
        let mut stale = 0;
        loop {
            let reply = tokio::select! {
                biased;
                error = self.state.failure() => return Err(error),
                reply = timeout_at(deadline, self.replies.recv()) => {
                    reply.map_err(|_| furpc::Error::Timeout("ImageStore ACK"))?
                        .ok_or(furpc::Error::Closed)?
                }
            };
            self.state.check()?;
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
                        return Err(FurpcClientError::Protocol(
                            "native upload reply has future sequence or wrong stage",
                        )
                        .into())
                    }
                }
            };
            if !obsolete {
                return Ok(reply);
            }
            stale += 1;
            if stale > MAX_STALE_REPLIES {
                return Err(FurpcClientError::Protocol(
                    "too many consecutive stale native upload replies",
                )
                .into());
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
        created.result.map_err(FurpcClientError::Domain)?;
        let chunk_size = negotiated_size(created.sequence.unwrap().buffer_size)?;
        let mut number = 0;
        let mut remaining = size;
        loop {
            let count = remaining.min(chunk_size) as usize;
            let mut body = vec![0; count];
            if let Some(file) = &mut file {
                timeout_at(deadline, file.read_exact(&mut body))
                    .await
                    .map_err(|_| furpc::Error::Timeout("ImageStore file"))??;
                if remaining == count as u64 {
                    let mut probe = [0];
                    if timeout_at(deadline, file.read(&mut probe))
                        .await
                        .map_err(|_| furpc::Error::Timeout("ImageStore file"))??
                        != 0
                    {
                        return Err(
                            furpc::Error::InvalidInput("package file grew during upload").into(),
                        );
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
            acknowledged.result.map_err(FurpcClientError::Domain)?;
            if negotiated_size(acknowledged.sequence.unwrap().buffer_size)? != chunk_size {
                return Err(FurpcClientError::Protocol(
                    "native upload changed negotiated chunk size",
                )
                .into());
            }
            remaining -= count as u64;
            if remaining == 0 {
                break;
            }
            number = number
                .checked_add(1)
                .ok_or(furpc::Error::ResourceLimit("native chunk sequence"))?;
        }
        loop {
            self.send("FileUploadCommit", activity, vec![], vec![], deadline)
                .await?;
            let committed = self
                .receive(activity, Stage::Commit(number), deadline)
                .await?;
            match committed.result {
                Err(DomainFailure {
                    code: wire::OPERATIONS_PENDING,
                    ..
                }) => {
                    timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
                        .await
                        .map_err(|_| furpc::Error::Timeout("ImageStore commit"))?;
                }
                result => {
                    result.map_err(FurpcClientError::Domain)?;
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

fn deadline_after(timeout: Duration) -> furpc::Result<Instant> {
    if timeout.is_zero() {
        return Err(furpc::Error::InvalidInput(
            "upload timeout must be positive",
        ));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(furpc::Error::InvalidInput("upload timeout is too large"))
}

fn negotiated_size(size: u64) -> Result<u64> {
    let size = if size == 0 {
        wire::DEFAULT_CHUNK_SIZE
    } else {
        size
    };
    if size > wire::chunk_capacity()? {
        return Err(FurpcClientError::Protocol(
            "native chunk threshold exceeds complete transport frame capacity",
        )
        .into());
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
            return Err(furpc::Error::InvalidInput(
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
                return Err(furpc::Error::InvalidInput(
                    "ImageStore destination cannot traverse parents",
                )
                .into());
            }
            if !component.is_empty() && component != "." {
                components.push(component);
            }
        }
        let next = components.join("/").trim().to_owned();
        if next == normalized {
            break;
        }
        normalized = next;
    }
    check_root(&normalized)?;
    let top = normalized.split('/').next().unwrap();
    if top.eq_ignore_ascii_case("Store") || top.eq_ignore_ascii_case("WindowsFabricStore") {
        return Err(furpc::Error::InvalidInput(
            "ImageStore destination is a reserved system directory",
        )
        .into());
    }
    wire::preflight_root_marker(&normalized)?;
    wire::delete(&normalized, [1; 16])?;
    Ok(normalized)
}

fn check_acknowledgment(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if !matches!(common.actor.get(), 25 | 27)
        || !["ClientOperationSuccess", "ClientOperationFailure"]
            .iter()
            .any(|action| common.action().iter().copied().eq(action.encode_utf16()))
        || common.relates_to != furpc::MessageId::default()
        || common.expects_reply
        || common.uncorrelated_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(furpc::Error::Protocol(
            "unauthorized native upload envelope",
        ));
    }
    Ok(())
}

#[derive(Clone)]
struct UploadIngress {
    state: Weak<UploadState>,
    generation: u64,
    replies: mpsc::Sender<FileTransferReply>,
}

impl Service<Request<Body>> for UploadIngress {
    type Response = IngressResponse;
    type Error = ImageStoreError;
    type Future = Ready<Result<IngressResponse>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let Some(state) = self.state.upgrade() else {
            return ready(Err(furpc::Error::Closed.into()));
        };
        let result: Result<IngressResponse> = (|| {
            state.check()?;
            if request
                .context()
                .map(|context| context.source().generation())
                != Some(self.generation)
            {
                return Err(
                    FurpcClientError::Protocol("upload connection generation changed").into(),
                );
            }
            let reply = wire::decode_reply(request.headers(), request.body())?;
            self.replies.try_send(reply).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    ImageStoreError::from(furpc::Error::ResourceLimit("ImageStore ACK queue"))
                }
                mpsc::error::TrySendError::Closed(_) => furpc::Error::Closed.into(),
            })?;
            Ok(IngressResponse::NoReply)
        })();
        if let Err(error) = &result {
            state.fail(error.clone());
        }
        ready(result)
    }
}

#[cfg(test)]
#[path = "image_store_tests.rs"]
mod tests;
