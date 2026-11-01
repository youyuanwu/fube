//! Finite, node-local bootstrap-only Hosting IPC.
//!
//! The caller supplies prepared package metadata and expected host/PID admission.
//! Claimed loopback headers are not OS authentication. No factory, RA/RAP, report,
//! deployment or distributed Federation capability is implied by this service.
use crate::{lease::Authority, wire, Hosting};
use fube_transport::{
    frame::{Frame, FIXED_HEADER_SIZE},
    message::{self, Header, ListenInstance, Message, MessageId},
    serialization::serde::{from_slice_with_budget, to_vec},
    Error as TransportError, Result as TransportResult,
};
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinSet,
};

const IO_DEADLINE: Duration = Duration::from_secs(5);
const MAX_INPUT: usize = 64 * 1024;
const SCRATCH: usize = 64 * MAX_INPUT;
const ADMISSION_BYTES: usize = 8 * MAX_INPUT;
const MAX_RESULTS: usize = 16;

/// Production default matches the existing finite local-cluster profile.
pub const LOCAL_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Externally supplied complete node bootstrap reply. The server replaces the
/// lease fields/error per operation; the caller owns deployment/work artifacts.
pub type NodeContext = wire::StartReply;

/// The only admitted profile: activated single-code-package SharedProcess,
/// noncontainer/nonactivator, no factories/services/reports.
#[derive(Debug, Clone)]
pub struct Admission {
    pub host: wire::HostContext,
    pub code: wire::CodeContext,
    pub deadlines: RequestDeadlines,
}
/// Explicit producer profile: Rust propagates remaining Timeout headers; native
/// IpcClient/RequestReply owns its deadline locally and omits that header.
/// Both profiles retain finite node frame/write/registration/authority bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestDeadlines {
    Propagated,
    NativeClientLocal,
}

/// Logical session scratch reservations include frame/header duplication,
/// bounded DTO decode, semantic projection, serialization and one full write.
/// Slots remain charged until the connection task and accepted results terminate.
#[derive(Debug, Clone)]
pub struct Config {
    pub node: NodeContext,
    pub core_limits: crate::Limits,
    pub max_admissions: usize,
    pub scratch_bytes: usize,
    pub decode_bytes: usize,
    pub projection_bytes: usize,
    pub reply_bytes: usize,
    /// Explicit finite local test/cluster configuration; never greater than 30s.
    pub lease_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct OperationResult {
    pub action: &'static str,
    pub primary: crate::Result<()>,
    pub state_method: Option<crate::Result<()>>,
    pub cleanup: Option<crate::Result<()>>,
}

/// Loss is separate from logical cleanup and is retained even if output capacity
/// vetoes table removal. It does not assert observed OS exit or Residency release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Loss {
    ConnectionClosed,
    Protocol,
    RegistrationTimeout,
    Authority,
    Shutdown,
}

struct Entry {
    admission: Admission,
    authority: Arc<Authority>,
    connected: AtomicBool,
    ready: AtomicBool,
    registration_deadline: AtomicU64,
    cleanup_deadline: AtomicU64,
    _reservation: Reservation,
}
struct Record {
    entry: Arc<Entry>,
    claimed: bool,
    completed: bool,
    runtime: Option<String>,
    timer: Option<crate::Timer>,
    loss: Option<Loss>,
    cleanup: Option<crate::Result<()>>,
    results: VecDeque<OperationResult>,
}
struct State {
    core: Hosting,
    records: Vec<Record>,
}
struct Shared {
    config: Config,
    origin: Instant,
    state: Mutex<State>,
    stop: AtomicBool,
    wake: Notify,
    used: Arc<AtomicUsize>,
    node_bytes: usize,
}
struct Reservation {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Reservation {
    fn acquire(shared: &Shared, bytes: usize) -> crate::Result<Self> {
        shared
            .used
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= shared.config.scratch_bytes)
            })
            .map_err(|_| crate::Error::Capacity)?;
        Ok(Self {
            used: shared.used.clone(),
            bytes,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}
impl Shared {
    fn state(&self) -> crate::Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| crate::Error::ObjectClosed)
    }
    fn ticks(&self) -> i64 {
        i64::try_from(self.origin.elapsed().as_nanos() / 100).unwrap_or(i64::MAX)
    }
}

/// Already-owned control handle; withdrawal never waits on a queue, core,
/// writer, callbacks, diagnostics or cleanup.
#[derive(Clone)]
pub struct HostLease {
    entry: Arc<Entry>,
}
impl HostLease {
    pub fn stop_renewal(&self) {
        self.entry.authority.stop_renewal();
    }
    pub fn revoke(&self) {
        self.entry.authority.revoke();
    }
}

/// Synchronously transferred owned outputs. Taking these is not external ACK.
#[derive(Debug)]
pub struct Outputs {
    pub events: Vec<crate::ClosedRegisteredEvent>,
    pub statuses: Vec<crate::EnabledDisabledEvent>,
    pub obligations: Vec<crate::Obligation>,
    pub operations: Vec<OperationResult>,
}

#[derive(Debug)]
pub struct SessionStatus {
    pub connected: bool,
    pub loss: Option<Loss>,
    pub cleanup: Option<crate::Result<()>>,
    pub registered_host: bool,
    pub runtimes: Vec<crate::RuntimeContext>,
    pub timers: usize,
    pub outstanding_operations: usize,
    pub reserved_bytes: usize,
}

/// Owns the listener, dedicated executor and every connection task.
/// Drop stops authority/I/O and joins tasks; it never claims physical cleanup.
/// Owned core outputs remain available after `stop` until consuming transfer.
pub struct Server {
    address: SocketAddr,
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

fn protocol(why: &'static str) -> TransportError {
    TransportError::Protocol(why)
}
fn valid_string(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096 && !value.contains('\0')
}
fn code_strings(code: &wire::CodeContext) -> [&String; 5] {
    [
        &code.id.package.package.application.type_name,
        &code.id.package.package.name,
        &code.id.package.public_id,
        &code.id.name,
        &code.application_name,
    ]
}
fn admission_capacity(admission: &Admission) -> usize {
    code_strings(&admission.code).iter().fold(
        std::mem::size_of::<Admission>() + admission.host.id.capacity(),
        |bytes, value| bytes.saturating_add(value.capacity()),
    )
}
fn context_projection_bytes(context: &wire::RuntimeContext) -> usize {
    code_strings(&context.code).iter().fold(
        std::mem::size_of::<crate::RuntimeContext>() + context.id.len() + context.host.id.len(),
        |bytes, value| bytes.saturating_add(value.len()),
    )
}
fn reply_budget(shared: &Shared, bytes: usize) -> TransportResult<()> {
    if bytes > shared.config.reply_bytes {
        return Err(protocol("Hosting reply allocation budget"));
    }
    Ok(())
}
fn node_capacity(node: &NodeContext) -> usize {
    let strings = [
        &node.node_id,
        &node.node_name,
        &node.node_type,
        &node.client_address,
        &node.deployment,
        &node.ip,
        &node.work,
    ];
    let mut bytes = strings
        .iter()
        .fold(std::mem::size_of::<NodeContext>(), |bytes, value| {
            bytes.saturating_add(value.capacity())
        });
    for pairs in [&node.application_directories.0, &node.node_directories.0] {
        bytes = bytes.saturating_add(
            pairs
                .capacity()
                .saturating_mul(std::mem::size_of::<wire::Pair>()),
        );
        for pair in pairs {
            bytes = bytes
                .saturating_add(pair.key.capacity())
                .saturating_add(pair.value.capacity());
        }
    }
    for log in [&node.application_log.0, &node.system_log.0]
        .into_iter()
        .flatten()
    {
        bytes = bytes
            .saturating_add(std::mem::size_of::<wire::SharedLog>())
            .saturating_add(log.path.0.capacity());
    }
    bytes
}

impl Server {
    pub fn bind(address: SocketAddr, config: Config) -> crate::Result<Self> {
        if node_capacity(&config.node) > ADMISSION_BYTES / 2 {
            return Err(crate::Error::Capacity);
        }
        let node_bytes = to_vec(&config.node)
            .map_err(|_| crate::Error::InvalidInput)?
            .len();
        if !address.ip().is_loopback()
            || config.max_admissions == 0
            || config.max_admissions > 64
            || config.scratch_bytes / (SCRATCH + ADMISSION_BYTES) < config.max_admissions
            || config.decode_bytes == 0
            || config.decode_bytes > 4 * MAX_INPUT
            || config.projection_bytes == 0
            || config.projection_bytes > 4 * MAX_INPUT
            || config.reply_bytes == 0
            || config.reply_bytes > MAX_INPUT
            || config.lease_duration < Duration::from_millis(1)
            || config.lease_duration > LOCAL_LEASE_DURATION
            || !valid_string(&config.node.node_id)
            || !valid_string(&config.node.node_name)
            || !std::path::Path::new(&config.node.deployment).is_absolute()
            || !std::path::Path::new(&config.node.work).is_absolute()
            || node_bytes > MAX_INPUT - 64
        {
            return Err(crate::Error::InvalidInput);
        }
        let core = Hosting::new(config.core_limits.clone())?;
        let listener =
            std::net::TcpListener::bind(address).map_err(|_| crate::Error::OperationFailed)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| crate::Error::OperationFailed)?;
        let address = listener
            .local_addr()
            .map_err(|_| crate::Error::OperationFailed)?;
        let shared = Arc::new(Shared {
            config,
            origin: Instant::now(),
            state: Mutex::new(State {
                core,
                records: Vec::new(),
            }),
            stop: AtomicBool::new(false),
            wake: Notify::new(),
            used: Arc::new(AtomicUsize::new(0)),
            node_bytes,
        });
        let owned = shared.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| crate::Error::OperationFailed)?;
        let thread = std::thread::Builder::new()
            .name("fube-hosting-ipc".into())
            .spawn(move || {
                runtime.block_on(async move {
                    let listener = match TcpListener::from_std(listener) {
                        Ok(value) => value,
                        Err(_) => {
                            owned.stop.store(true, Ordering::SeqCst);
                            return;
                        }
                    };
                    serve(listener, owned).await;
                });
            })
            .map_err(|_| crate::Error::OperationFailed)?;
        Ok(Self {
            address,
            shared,
            thread: Some(thread),
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// Synchronous admission reserves one complete scratch/session/result slot.
    /// One connection incarnation only; reconnect requires explicit release and
    /// new admission after old work/timers/membership are disposed.
    pub fn admit(&self, admission: Admission) -> crate::Result<HostLease> {
        if self.shared.stop.load(Ordering::SeqCst) {
            return Err(crate::Error::ObjectClosed);
        }
        let host = &admission.host;
        if admission_capacity(&admission) > ADMISSION_BYTES / 2 {
            return Err(crate::Error::Capacity);
        }
        let reservation = Reservation::acquire(&self.shared, ADMISSION_BYTES)?;
        if uuid::Uuid::parse_str(&host.id).is_err()
            || host.pid == 0
            || host.kind != 1
            || host.container
            || host.activator
            || admission.code.id.package.activation.mode != 0
            || admission.code.id.package.activation.guid.0 != [0; 16]
            || !admission.code.id.package.public_id.is_empty()
            || !valid_string(&admission.code.application_name)
            || to_vec(&admission.code)
                .map_err(|_| crate::Error::InvalidInput)?
                .len()
                > MAX_INPUT
        {
            return Err(crate::Error::UnsupportedCapability);
        }
        // Validate H1 package premises without publishing a tentative row.
        crate::identity::validate_package(
            &wire::RuntimeContext {
                id: "admission".into(),
                host: host.clone(),
                code: admission.code.clone(),
            }
            .project()
            .package
            .package,
        )?;
        let mut state = self.shared.state()?;
        if state.records.len() == self.shared.config.max_admissions {
            return Err(crate::Error::Capacity);
        }
        if state
            .records
            .iter()
            .any(|r| crate::compare_names(&r.entry.admission.host.id, &host.id).is_eq())
        {
            return Err(crate::Error::HostAlreadyRegistered);
        }
        let entry = Arc::new(Entry {
            admission,
            authority: Authority::new(self.shared.config.lease_duration),
            connected: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            registration_deadline: AtomicU64::new(0),
            cleanup_deadline: AtomicU64::new(0),
            _reservation: reservation,
        });
        state.records.push(Record {
            entry: entry.clone(),
            claimed: false,
            completed: false,
            runtime: None,
            timer: None,
            loss: None,
            cleanup: None,
            results: VecDeque::new(),
        });
        Ok(HostLease { entry })
    }
    /// Release/replacement is explicit and fail-closed. A connected incarnation,
    /// retained failure/output or member host cannot be overwritten.
    pub fn release(&self, lease: HostLease) -> crate::Result<()> {
        lease.revoke();
        let mut state = self.shared.state()?;
        let index = record_index(&state, &lease.entry)?;
        let r = &state.records[index];
        if r.entry.connected.load(Ordering::SeqCst)
            || r.timer.is_some()
            || !r.results.is_empty()
            || r.cleanup.as_ref().is_some_and(|result| result.is_err())
            || state.core.find_host(&r.entry.admission.host.id).is_ok()
        {
            return Err(crate::Error::OperationFailed);
        }
        state.records.remove(index);
        Ok(())
    }
    pub fn status(&self, lease: &HostLease) -> crate::Result<SessionStatus> {
        let state = self.shared.state()?;
        let r = &state.records[record_index(&state, &lease.entry)?];
        Ok(SessionStatus {
            connected: r.entry.connected.load(Ordering::SeqCst),
            loss: r.loss.clone(),
            cleanup: r.cleanup.clone(),
            registered_host: match state.core.is_host_registered(&r.entry.admission.host.id) {
                Ok(value) => value,
                Err(crate::Error::HostNotRegistered) => false,
                Err(error) => return Err(error),
            },
            runtimes: state
                .core
                .runtime_records()
                .iter()
                .filter(|value| {
                    crate::compare_names(&value.context.host_id, &r.entry.admission.host.id).is_eq()
                })
                .map(|value| value.context.clone())
                .collect(),
            timers: state.core.timers().len(),
            outstanding_operations: state.core.outstanding_operations(),
            reserved_bytes: self.shared.used.load(Ordering::SeqCst),
        })
    }
    /// Cannot be abandoned between drain and transfer: synchronous owned return.
    pub fn take_outputs(&self) -> crate::Result<Outputs> {
        let mut state = self.shared.state()?;
        let operations = state
            .records
            .iter_mut()
            .flat_map(|r| r.results.drain(..))
            .collect();
        Ok(Outputs {
            operations,
            events: state.core.take_closed_registered_events(),
            statuses: state.core.take_enabled_disabled_events(),
            obligations: state.core.take_obligations(),
        })
    }
    pub fn stop(&mut self) -> crate::Result<()> {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.shared.wake.notify_one();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| crate::Error::OperationFailed)?;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
fn record_index(state: &State, entry: &Arc<Entry>) -> crate::Result<usize> {
    state
        .records
        .iter()
        .position(|r| Arc::ptr_eq(&r.entry, entry))
        .ok_or(crate::Error::HostNotRegistered)
}

async fn serve(listener: TcpListener, shared: Arc<Shared>) {
    let mut tasks = JoinSet::new();
    loop {
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        tokio::select! {
            _ = shared.wake.notified() => {},
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept(), if tasks.len() < shared.config.max_admissions => {
                let Ok((socket, peer)) = accepted else { break };
                if !peer.ip().is_loopback() { continue }
                let owned = shared.clone();
                let Ok(reservation) = Reservation::acquire(&shared, SCRATCH) else { continue };
                tasks.spawn(async move {
                    let entry = connection(socket, owned).await;
                    drop(reservation);
                    if let Some(entry) = entry {
                        entry.connected.store(false, Ordering::SeqCst);
                    }
                });
            }
        }
    }
    // Cancel sockets first; aborted work is joined, then synchronously cleaned.
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    if let Ok(mut state) = shared.state() {
        for i in 0..state.records.len() {
            state.records[i].entry.authority.revoke();
            state.records[i]
                .entry
                .connected
                .store(false, Ordering::SeqCst);
            cleanup(&mut state, i, Loss::Shutdown);
        }
    }
}

async fn read(socket: &mut TcpStream) -> TransportResult<Message> {
    // Idle has a finite deadline too; profile maximum is checked before allocation.
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut fixed = [0; FIXED_HEADER_SIZE];
        socket.read_exact(&mut fixed).await?;
        let (length, _) = fube_transport::frame::validate_fixed(&fixed)?;
        if length > MAX_INPUT {
            return Err(protocol("Hosting frame exceeds bootstrap profile"));
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(&fixed);
        bytes.resize(length, 0);
        socket.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
        Message::from_frame(Frame::decode(&bytes)?)
    })
    .await
    .map_err(|_| protocol("Hosting frame deadline"))?
}
async fn write(socket: &mut TcpStream, value: Message) -> TransportResult<()> {
    write_fenced(socket, value, None).await
}
async fn write_fenced<W: AsyncWrite + Unpin>(
    socket: &mut W,
    value: Message,
    fence: Option<&Authority>,
) -> TransportResult<()> {
    let bytes = value.to_frame()?.encode_with_body_crc(true)?;
    if bytes.len() > MAX_INPUT {
        return Err(protocol("Hosting reply exceeds bootstrap profile"));
    }
    if fence.is_some_and(|authority| !authority.live()) {
        return Err(TransportError::Closed);
    }
    tokio::time::timeout(IO_DEADLINE, socket.write_all(&bytes))
        .await
        .map_err(|_| protocol("Hosting write deadline"))??;
    Ok(())
}
fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> TransportResult<T> {
    let mut left = limit;
    from_slice_with_budget(bytes, &mut |capacity| {
        left = left
            .checked_sub(capacity)
            .ok_or(protocol("Hosting decode budget"))?;
        Ok(())
    })
}
fn unique_headers(message: &Message) -> TransportResult<()> {
    for kind in [
        message::ACTOR,
        message::ACTION,
        message::MESSAGE_ID,
        message::RELATES_TO,
        message::EXPECTS_REPLY,
        message::IPC,
        message::TIMEOUT,
        message::FAULT,
        message::UNCORRELATED_REPLY,
        message::HIGH_PRIORITY,
    ] {
        if message.headers.iter().filter(|h| h.id == kind).count() > 1 {
            return Err(protocol("duplicate Hosting routing header"));
        }
    }
    Ok(())
}
fn envelope(message: &Message, decode_limit: usize) -> TransportResult<(String, u32, bool)> {
    unique_headers(message)?;
    let p = message.properties()?;
    if p.id == MessageId::default()
        || p.relates_to != MessageId::default()
        || !p.expects_reply
        || p.fault.is_some()
        || p.uncorrelated_reply
        || !message.headers.iter().any(|h| h.id == message::ACTOR)
    {
        return Err(protocol("invalid Hosting request envelope"));
    }
    let timeout = message
        .headers
        .iter()
        .find(|h| h.id == message::TIMEOUT)
        .map(|h| h.read(|d| d.i64()))
        .transpose()?;
    if timeout.is_some_and(|value| value <= 0) {
        return Err(protocol("expired Hosting request"));
    }
    let header = message
        .headers
        .iter()
        .find(|h| h.id == message::IPC)
        .ok_or(protocol("missing Hosting identity"))?;
    #[derive(Deserialize)]
    struct Identity(String, u32);
    let Identity(host, pid) = decode(&header.value, decode_limit)?;
    Ok((host, pid, timeout.is_some()))
}
fn reply(id: MessageId, body: Vec<u8>) -> TransportResult<Message> {
    Ok(Message {
        headers: vec![
            Header::message_id(MessageId {
                guid: uuid::Uuid::new_v4().to_bytes_le(),
                index: 0,
            })?,
            Header::relates_to(id)?,
        ],
        body,
    })
}

async fn connection(mut socket: TcpStream, shared: Arc<Shared>) -> Option<Arc<Entry>> {
    let mut claimed: Option<Arc<Entry>> = None;
    let outcome = run_connection(&mut socket, &shared, &mut claimed).await;
    if let Some(entry) = claimed {
        let loss = match &outcome {
            Ok(()) | Err(TransportError::Io(_)) => Loss::ConnectionClosed,
            Err(TransportError::Closed) => Loss::Authority,
            Err(_) => Loss::Protocol,
        };
        entry.authority.revoke();
        if let Ok(mut state) = shared.state() {
            if let Ok(i) = record_index(&state, &entry) {
                cleanup(&mut state, i, loss);
            }
        }
        Some(entry)
    } else {
        None
    }
}
async fn run_connection(
    socket: &mut TcpStream,
    shared: &Arc<Shared>,
    claimed: &mut Option<Arc<Entry>>,
) -> TransportResult<()> {
    let first = read(socket).await?;
    unique_headers(&first)?;
    let properties = first.properties()?;
    let listen = ListenInstance::decode(&first.body)?;
    if properties.actor != 1
        || !properties.action.is_empty()
        || listen != ListenInstance::default()
        || properties.id == MessageId::default()
        || first.headers.iter().any(|h| {
            !matches!(
                h.id,
                message::ACTOR | message::ACTION | message::MESSAGE_ID | message::HIGH_PRIORITY
            )
        })
    {
        return Err(protocol("invalid client-only ListenInstance"));
    }
    write(
        socket,
        Message::listen_instance(
            MessageId {
                guid: uuid::Uuid::new_v4().to_bytes_le(),
                index: 0,
            },
            &ListenInstance {
                address: socket.local_addr()?.to_string(),
                instance: 1,
                nonce: uuid::Uuid::new_v4().to_bytes_le(),
            },
        )?,
    )
    .await?;
    let mut last_id: Option<MessageId> = None;
    loop {
        let request = if let Some(entry) = claimed.as_ref() {
            tokio::select! {
                _ = entry.authority.cancel.notified() => return Err(TransportError::Closed),
                value = read(socket) => value?,
                result = watch_deadlines(shared, entry) => return result,
            }
        } else {
            read(socket).await?
        };
        let (host, pid, has_timeout) = envelope(&request, shared.config.decode_bytes)?;
        let p = request.properties()?;
        if p.id.guid == [0; 16]
            || last_id.is_some_and(|last| p.id.guid != last.guid || p.id.index <= last.index)
        {
            return Err(protocol("stale Hosting correlation"));
        }
        // Native MessageId is process GUID/increasing index; Rust IPC is
        // connection GUID/increasing index. Gaps are valid; wrap/replay is not.
        last_id = Some(p.id);
        if claimed.is_none() {
            let mut state = shared.state().map_err(|_| protocol("Hosting closed"))?;
            let r = state
                .records
                .iter_mut()
                .find(|r| {
                    // Literal native reverse-route key is distinct from CI table identity.
                    r.entry.admission.host.id == host && r.entry.admission.host.pid == pid
                })
                .ok_or(protocol("unadmitted Hosting identity"))?;
            if !has_timeout && r.entry.admission.deadlines == RequestDeadlines::Propagated {
                return Err(protocol("missing propagated Hosting timeout"));
            }
            if r.claimed || !r.entry.authority.live() {
                return Err(protocol("obsolete Hosting session"));
            }
            r.claimed = true;
            r.entry.connected.store(true, Ordering::SeqCst);
            *claimed = Some(r.entry.clone());
        }
        let entry = claimed.as_ref().expect("claimed session");
        if !has_timeout && entry.admission.deadlines == RequestDeadlines::Propagated {
            return Err(protocol("missing propagated Hosting timeout"));
        }
        if entry.admission.host.id != host || entry.admission.host.pid != pid {
            return Err(protocol("Hosting identity changed"));
        }
        let lease_reply = p.actor == 15;
        let body = if lease_reply {
            if !p.action.is_empty() || !request.body.is_empty() {
                return Err(protocol("invalid actor-15 request"));
            }
            // No core lock or callback is involved in ongoing lease service.
            if !entry.ready.load(Ordering::SeqCst) {
                return Err(protocol("lease before completed registration"));
            }
            let ttl = entry.authority.poll().ok_or(TransportError::Closed)?;
            reply_budget(shared, 32)?;
            to_vec(&wire::LeaseReply { ttl })?
        } else {
            dispatch(shared, entry, p.actor, &p.action, &request.body)?
        };
        // No queued writer; this is the pre-frame fence. Cancellation discards
        // the connection after a partial frame, never reuses its writer.
        if lease_reply && !entry.authority.live() {
            return Err(TransportError::Closed);
        }
        let value = reply(p.id, body)?;
        tokio::select! {
            _ = entry.authority.cancel.notified() => return Err(TransportError::Closed),
            result = watch_deadlines(shared, entry) => return result,
            result = write_fenced(socket, value, lease_reply.then_some(entry.authority.as_ref())) => result?,
        }
    }
}

fn deadline(shared: &Shared, entry: &Arc<Entry>) -> TransportResult<()> {
    let cleanup_due = entry.cleanup_deadline.load(Ordering::SeqCst);
    if cleanup_due != 0 {
        if shared.ticks() as u64 >= cleanup_due {
            entry.authority.revoke();
            return Err(TransportError::Closed);
        }
        return Ok(());
    }
    if !entry.authority.live() {
        return Err(TransportError::Closed);
    }
    let due = entry.registration_deadline.load(Ordering::SeqCst);
    if due == 0 || (shared.ticks() as u64) < due {
        return Ok(());
    }
    let mut state = shared.state().map_err(|_| protocol("Hosting closed"))?;
    let i = record_index(&state, entry).map_err(|_| protocol("stale session"))?;
    if let Some(timer) = state.records[i].timer.clone() {
        if timer.due_ticks <= shared.ticks() {
            // Timer cannot survive this incarnation's disposal/release.
            state.records[i].timer = None;
            entry.registration_deadline.store(0, Ordering::SeqCst);
            entry.authority.revoke();
            let result = state.core.host_timeout(&timer);
            state.records[i].loss = Some(Loss::RegistrationTimeout);
            state.records[i].cleanup = Some(result);
            return Err(protocol("Hosting registration timeout"));
        }
    }
    Ok(())
}
async fn watch_deadlines(shared: &Shared, entry: &Arc<Entry>) -> TransportResult<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline(shared, entry)?;
    }
}
fn native_error(error: &crate::Error) -> TransportResult<i64> {
    let bits: u32 = match error {
        crate::Error::ObjectClosed => 0x80071bfe,
        crate::Error::InvalidState => 0x80071bfd,
        crate::Error::HostAlreadyRegistered => 0x80071d2d,
        crate::Error::HostNotRegistered => 0x80071d2e,
        crate::Error::RuntimeAlreadyRegistered => 0x80071d2f,
        crate::Error::RuntimeNotRegistered => 0x80071d30,
        crate::Error::OperationFailed => 0x80004005,
        crate::Error::Dependency(value) => return Ok(*value),
        _ => return Err(protocol("local Hosting operation failure")),
    };
    Ok(bits as i32 as i64)
}
fn dispatch(
    shared: &Shared,
    entry: &Arc<Entry>,
    actor: i64,
    action: &str,
    body: &[u8],
) -> TransportResult<Vec<u8>> {
    // Before mutation as well as in the independent wait supervisor.
    deadline(shared, entry)?;
    if entry.cleanup_deadline.load(Ordering::SeqCst) != 0
        && !matches!(
            (actor, action),
            (7, "UnregisterApplicationHostRequest") | (9, "UnregisterFabricRuntimeRequest")
        )
    {
        return Err(protocol("retired session accepts cleanup only"));
    }
    let mut state = shared.state().map_err(|_| protocol("Hosting closed"))?;
    let i = record_index(&state, entry).map_err(|_| protocol("stale session"))?;
    if state.records[i].results.len() == MAX_RESULTS {
        return Err(protocol("Hosting retained result capacity"));
    }
    let host_matches = |id: &str| crate::compare_names(id, &entry.admission.host.id).is_eq();
    let mut result = OperationResult {
        action: "",
        primary: Ok(()),
        state_method: None,
        cleanup: None,
    };
    let mut start_reply = false;
    match (actor, action) {
        (7, "StartRegisterApplicationHostRequest") => {
            let value: wire::StartRequest = decode(body, shared.config.decode_bytes)?;
            if !host_matches(&value.id)
                || value.pid != entry.admission.host.pid
                || value.kind != 1
                || value.container
                || value.activator
                || value.timeout <= 0
            {
                return Err(protocol("Start outside admitted profile"));
            }
            result.action = "StartRegisterApplicationHostRequest";
            result.primary = state.core.add_host(value.id.clone());
            if result.primary.is_ok() {
                match state.core.start_host(
                    &value.id,
                    &crate::ManualClock(shared.ticks()),
                    value.timeout,
                ) {
                    Ok(timer) => {
                        entry
                            .registration_deadline
                            .store(timer.due_ticks as u64, Ordering::SeqCst);
                        state.records[i].timer = Some(timer);
                    }
                    Err(error) => result.primary = Err(error),
                }
            }
            start_reply = true;
        }
        (7, "FinishRegisterApplicationHostRequest") => {
            let value: wire::IdRequest = decode(body, shared.config.decode_bytes)?;
            if !host_matches(&value.id) {
                return Err(protocol("foreign Finish target"));
            }
            result.action = "FinishRegisterApplicationHostRequest";
            match state.core.finish_host(&value.id, false) {
                Ok(finish) => {
                    result.primary = finish.reply;
                    result.state_method = finish.state_method;
                }
                Err(error) => result.primary = Err(error),
            }
            state.records[i].completed = match state.core.is_host_registered(&value.id) {
                Ok(value) => value,
                Err(crate::Error::HostNotRegistered) => false,
                Err(_) => return Err(protocol("Finish membership failure")),
            };
            if state.records[i].completed {
                state.records[i].timer = None;
                entry.registration_deadline.store(0, Ordering::SeqCst);
            }
        }
        (9, "RegisterFabricRuntimeRequest") => {
            let value: wire::RuntimeRequest = decode(body, shared.config.decode_bytes)?;
            let context = &value.context;
            let mut expected_host = entry.admission.host.clone();
            expected_host.id = context.host.id.clone();
            if !host_matches(&context.host.id)
                || context.host != expected_host
                || context.code != entry.admission.code
                || !valid_string(&context.id)
            {
                return Err(protocol("runtime context outside admitted profile"));
            }
            result.action = "RegisterFabricRuntimeRequest";
            if state.records[i]
                .runtime
                .as_ref()
                .is_some_and(|id| !crate::compare_names(id, &context.id).is_eq())
            {
                return Err(protocol("multiple runtimes outside bootstrap-only profile"));
            }
            let projection = context_projection_bytes(context);
            if projection > shared.config.projection_bytes {
                return Err(protocol("Hosting projection budget"));
            }
            let registration = state.core.register_runtime(context.project());
            result.primary = registration.result;
            result.cleanup = registration.cleanup;
            if result.primary.is_ok() {
                state.records[i].runtime = Some(context.id.clone());
                entry.ready.store(true, Ordering::SeqCst);
            }
        }
        (9, "UnregisterFabricRuntimeRequest") => {
            let value: wire::IdRequest = decode(body, shared.config.decode_bytes)?;
            if !state.records[i]
                .runtime
                .as_ref()
                .is_some_and(|id| crate::compare_names(id, &value.id).is_eq())
            {
                return Err(protocol("foreign runtime unregister target"));
            }
            result.action = "UnregisterFabricRuntimeRequest";
            entry.ready.store(false, Ordering::SeqCst);
            let _ = entry.cleanup_deadline.compare_exchange(
                0,
                shared.ticks().saturating_add(4_000_000) as u64,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            entry.authority.retire();
            result.primary = state.core.unregister_runtime(&value.id);
            if result.primary.is_ok() {
                state.records[i].runtime = None;
            }
        }
        (7, "UnregisterApplicationHostRequest") => {
            let value: wire::UnregisterHost = decode(body, shared.config.decode_bytes)?;
            if !host_matches(&value.id) {
                return Err(protocol("foreign host unregister target"));
            }
            result.action = "UnregisterApplicationHostRequest";
            entry.ready.store(false, Ordering::SeqCst);
            let _ = entry.cleanup_deadline.compare_exchange(
                0,
                shared.ticks().saturating_add(4_000_000) as u64,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            entry.authority.retire();
            let activity = format!(
                "{}:{}:{}",
                uuid::Uuid::from_bytes_le(value.activity.id.guid.0),
                value.activity.id.index,
                value.activity.kind
            );
            match state.core.unregister_host(&value.id, &activity) {
                Ok(removed) => {
                    result.primary = removed.reply;
                    result.cleanup = removed.runtime_removal.map(|value| value.map(|_| ()));
                }
                Err(error) => result.primary = Err(error),
            }
            if result.primary.is_ok() {
                state.records[i].runtime = None;
                state.records[i].completed = false;
                state.records[i].timer = None;
            }
        }
        _ => return Err(protocol("unsupported Hosting capability")),
    }
    // Accepted operation outcomes remain owned even when serialization, local
    // mapping or the subsequent socket write fails.
    state.records[i].results.push_back(result.clone());
    let error = match &result.primary {
        Ok(()) => 0,
        Err(error) => native_error(error)?,
    };
    let response = if start_reply {
        reply_budget(shared, shared.node_bytes + 64)?;
        let mut node = shared.config.node.clone();
        node.lease_handle = 1; // Opaque local marker, never a native lease handle.
        node.lease_duration = (shared.config.lease_duration.as_nanos() / 100) as i64;
        node.initial_ttl = node.lease_duration;
        node.error = error;
        to_vec(&node)?
    } else {
        reply_budget(shared, 32)?;
        to_vec(&wire::ErrorReply { error })?
    };
    Ok(response)
}
fn cleanup(state: &mut State, i: usize, loss: Loss) {
    let r = &mut state.records[i];
    r.loss.get_or_insert(loss);
    r.entry.authority.revoke();
    r.entry.ready.store(false, Ordering::SeqCst);
    r.entry.registration_deadline.store(0, Ordering::SeqCst);
    if let Some(timer) = r.timer.take() {
        // Dispose only the timer owned by this incarnation, not a successor's
        // name-based timeout callback.
        let cancelled = state.core.change(|s| {
            s.cancel_timer(timer.id);
            Ok(())
        });
        if let Err(error) = cancelled {
            r.cleanup = Some(Err(error));
        }
    }
    r.completed = false;
    if r.cleanup.as_ref().is_some_and(|result| result.is_err()) {
        return;
    }
    if state.core.find_host(&r.entry.admission.host.id).is_ok() {
        let result = state
            .core
            .unregister_host(&r.entry.admission.host.id, "ipc-session-loss");
        r.cleanup = Some(result.and_then(|value| {
            value.reply?;
            value.runtime_removal.transpose()?;
            Ok(())
        }));
    }
}

#[cfg(test)]
mod fence_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn queued_production_reply_fence_observes_no_frame_after_terminal_loss() {
        for expire in [false, true] {
            let authority = Authority::new(Duration::from_millis(10));
            let (mut sender, mut receiver) = tokio::io::duplex(64);
            let correlation = MessageId {
                guid: uuid::Uuid::new_v4().to_bytes_le(),
                index: 123,
            };
            // Queue the exact production writer future, before its pre-frame
            // boundary. Do not replace that writer with a flag-inspection shim.
            let pending = write_fenced(
                &mut sender,
                reply(
                    correlation,
                    to_vec(&wire::LeaseReply { ttl: 100_000 }).unwrap(),
                )
                .unwrap(),
                Some(authority.as_ref()),
            );
            if expire {
                authority.stop_renewal();
                tokio::time::sleep(Duration::from_millis(20)).await;
            } else {
                authority.revoke();
            }
            assert!(matches!(pending.await, Err(TransportError::Closed)));
            drop(sender);
            let mut observed = Vec::new();
            receiver.read_to_end(&mut observed).await.unwrap();
            assert!(observed.is_empty(), "a queued correlated frame started");
        }
    }
}
