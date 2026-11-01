//! Operational, owned native NamingGateway channels for unsecured loopback development.
//!
//! This is a separate backend for [`crate::channel`], not an adapter around
//! Connection's envelope-building APIs. Only actor-5 outbound messages and
//! ordinary gateway completions are supported. Explicit notification opt-in
//! additionally authorizes native `ServiceNotificationRequest` envelopes and
//! consuming `ServiceNotificationReply` capabilities. No listener, arbitrary
//! reverse RPC, IPC, upload, replication, TLS, reconnect or replay is supplied.
//!
//! Clear loopback is an explicit trust assumption, not authentication of local
//! processes. Domain headers/body, including TimeoutHeader, remain caller-owned.
//! See `docs/transport/NativeChannel.md` for limits and qualification.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::size_of,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
    task::Poll,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, Notify},
    task::JoinHandle,
    time::{timeout, timeout_at, Instant},
};

use crate::{
    channel::{
        ChannelFuture, IncomingMessage, MessageChannel, MessageReceiver, ReplyContext, RpcChannel,
    },
    connection::{write_message, MAX_FRAME_TIME},
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE, MAX_FRAME_SIZE, MAX_HEADER_SIZE},
    message::{self, Header, ListenInstance, Message, MessageId, Properties, MAX_HEADER_COUNT},
    Error, Result,
};

#[cfg(test)]
#[path = "native_channel_tests.rs"]
mod tests;

const ORDINARY: usize = 0;
const REPLY: usize = 1;
const INGRESS: usize = 2;
const RECEIVE: usize = 3;
const CLOSE: usize = 4;

/// Immutable finite limits, validated before connection establishment.
///
/// Counts include queued, pending and completed-undelivered work. Bytes are
/// conservative allocation reservations, not wire lengths. Each data/ingress
/// slot reserves [`Self::slot_bytes`]; receive/close waiters each reserve 256
/// bytes and are separately bounded by `max_operations`. One bounded reader
/// scratch allocation is additional to these retained-slot reservations.
#[derive(Debug, Clone, Copy)]
pub struct ChannelLimits {
    pub max_operations: usize,
    pub max_replies: usize,
    pub max_ingress: usize,
    pub max_frame_bytes: usize,
    pub max_retained_bytes: usize,
    /// Lifetime outbound + notification ID history; exhaustion is terminal.
    pub max_history: usize,
}

impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_operations: 8,
            max_replies: 4,
            max_ingress: 8,
            max_frame_bytes: MAX_FRAME_SIZE,
            max_retained_bytes: 512 * 1024 * 1024,
            max_history: 4096,
        }
    }
}

impl ChannelLimits {
    /// Worst-case reservation covering input capacities, encoding copies, a
    /// started frame and maximum undelivered reply/ingress plus decode scratch.
    pub fn slot_bytes(&self) -> Result<usize> {
        self.max_frame_bytes
            .checked_mul(5)
            .and_then(|n| n.checked_add(MAX_HEADER_COUNT * size_of::<Header>()))
            .and_then(|n| n.checked_add(6 * MAX_HEADER_SIZE))
            .ok_or(Error::InvalidInput("channel allocation limit overflow"))
    }

    fn validate(&self) -> Result<()> {
        if self.max_operations == 0
            || self.max_replies == 0
            || self.max_ingress == 0
            || self.max_history == 0
            || !(FIXED_HEADER_SIZE..=MAX_FRAME_SIZE).contains(&self.max_frame_bytes)
            || self.max_retained_bytes < self.slot_bytes()?
        {
            return Err(Error::InvalidInput("invalid finite channel limits"));
        }
        self.max_operations
            .checked_mul(3)
            .and_then(|n| n.checked_add(self.max_replies))
            .and_then(|n| n.checked_add(self.max_ingress))
            .and_then(|n| n.checked_mul(self.slot_bytes().ok()?))
            .and_then(|n| n.checked_add(self.max_history.checked_mul(128)?))
            .ok_or(Error::InvalidInput("channel count limit overflow"))?;
        Ok(())
    }
}

#[derive(Default)]
struct Usage {
    counts: [usize; 5],
    bytes: usize,
}

struct Budget {
    limits: ChannelLimits,
    usage: Mutex<Usage>,
}

struct Lease {
    budget: Arc<Budget>,
    class: usize,
    bytes: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap();
        usage.counts[self.class] -= 1;
        usage.bytes -= self.bytes;
    }
}

impl Budget {
    fn reserve(self: &Arc<Self>, class: usize) -> Result<Arc<Lease>> {
        let count_limit = match class {
            REPLY => self.limits.max_replies,
            INGRESS => self.limits.max_ingress,
            _ => self.limits.max_operations,
        };
        let bytes = if class >= RECEIVE {
            256
        } else {
            self.limits.slot_bytes()?
        };
        let mut usage = self.usage.lock().unwrap();
        let total = usage
            .bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit("channel bytes"))?;
        if usage.counts[class] >= count_limit || total > self.limits.max_retained_bytes {
            return Err(Error::ResourceLimit("channel retained count/bytes"));
        }
        usage.counts[class] += 1;
        usage.bytes = total;
        Ok(Arc::new(Lease {
            budget: self.clone(),
            class,
            bytes,
        }))
    }
}

/// Private-construction identity of exactly one established backend/socket.
///
/// Resolution reuses this identity. Retaining it keeps only inert identity
/// alive, never an owner, socket, worker or reconnection capability.
pub struct GatewayTarget {
    identity: Arc<()>,
    address: SocketAddr,
}

enum Outcome {
    Written,
    Message(Message),
}

struct Entry {
    deadline: Instant,
    id: Option<MessageId>,
    rpc: bool,
    bytes: Option<Vec<u8>>,
    result: Option<Result<Outcome>>,
    lease: Arc<Lease>,
    tracking: Option<Arc<Mutex<RequestState>>>,
}

#[derive(Default)]
struct RequestState {
    token: Option<u64>,
    visible: bool,
    cancelled: bool,
}

/// Nonowning cancellation observation for one tracked request.
///
/// Visibility means an actual positive socket write, not queue selection.
/// It survives reply settlement, expiry and abandonment of the request future.
#[derive(Clone)]
pub struct RequestCancellation {
    shared: Weak<Shared>,
    state: Arc<Mutex<RequestState>>,
}

impl RequestCancellation {
    /// Retract only if no byte has become visible. A `false` result means
    /// visibility cannot be rolled back; it does not imply a remote outcome.
    /// Retraction before the lazy request's first poll prevents submission.
    pub fn cancel_unsent(&self) -> bool {
        let shared = self.shared.upgrade();
        let mut inner = shared.as_ref().map(|s| s.inner.lock().unwrap());
        let mut state = self.state.lock().unwrap();
        if state.visible {
            return false;
        }
        state.cancelled = true;
        if let (Some(inner), Some(token)) = (inner.as_mut(), state.token) {
            retire(inner, token);
        }
        drop(state);
        drop(inner);
        if let Some(shared) = shared {
            shared.changed.notify_waiters();
        }
        true
    }
}

struct Ingress {
    message: Message,
    id: MessageId,
    _lease: Arc<Lease>,
}

struct Inner {
    terminal: Option<Error>,
    next_token: u64,
    next_index: u32,
    remote: ListenInstance,
    entries: HashMap<u64, Entry>,
    pending: HashMap<MessageId, u64>,
    ordinary: VecDeque<u64>,
    replies: VecDeque<u64>,
    ingress: VecDeque<Ingress>,
    history: HashSet<MessageId>,
    unmatched: usize,
}

struct Shared {
    inner: Mutex<Inner>,
    budget: Arc<Budget>,
    target: Arc<GatewayTarget>,
    notifications: bool,
    process_id: [u8; 16],
    changed: Notify,
    stop: Notify,
    stopped: watch::Sender<bool>,
}

impl Shared {
    fn error(&self) -> Option<Error> {
        self.inner.lock().unwrap().terminal.clone()
    }

    fn terminate(&self, error: Error) {
        let mut inner = self.inner.lock().unwrap();
        if inner.terminal.is_none() {
            inner.terminal = Some(error);
            inner.entries.clear();
            inner.pending.clear();
            inner.ordinary.clear();
            inner.replies.clear();
            inner.ingress.clear();
            inner.history.clear();
        }
        drop(inner);
        self.stop.notify_waiters();
        self.changed.notify_waiters();
    }

    fn finish(&self, token: u64, result: Result<Outcome>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(&token) {
            if entry.result.is_none() {
                entry.result = Some(if Instant::now() >= entry.deadline {
                    Err(Error::Timeout)
                } else {
                    result
                });
                let id = entry.id;
                if let Some(id) = id {
                    inner.pending.remove(&id);
                }
            }
        }
        drop(inner);
        self.changed.notify_waiters();
    }

    fn remember(&self, inner: &mut Inner, id: MessageId) -> Result<()> {
        if inner.history.contains(&id) {
            return Err(Error::InvalidInput("message ID used or retired"));
        }
        if inner.history.len() == self.budget.limits.max_history {
            return Err(Error::ResourceLimit("channel lifetime ID history"));
        }
        inner.history.insert(id);
        Ok(())
    }
}

struct Owner {
    shared: Arc<Shared>,
    /// Borrow the handle while awaiting so timeout/drop cannot detach cleanup.
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.terminate(Error::Closed);
    }
}

/// Cloneable lifecycle owner of one native gateway foundation.
///
/// Only these owner clones retain live ownership. GatewayReceiver, target and
/// reply contexts are nonowning. A typed façade should borrow this owner, not
/// clone it silently. Last owner drop initiates bounded terminal cleanup.
#[derive(Clone)]
pub struct GatewayChannel {
    owner: Arc<Owner>,
}

/// Nonowning authorized pull capability, minted only by notification opt-in.
#[derive(Clone)]
pub struct GatewayReceiver {
    shared: Weak<Shared>,
}

/// Finite ingress handoff retaining each reservation until individual transfer.
///
/// This owns no channel lifecycle or socket. Drop releases untaken envelopes;
/// no new traffic is added to the snapshot.
pub struct GatewayIngressSnapshot {
    shared: Arc<Shared>,
    pending: VecDeque<Ingress>,
}

impl GatewayIngressSnapshot {
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl Iterator for GatewayIngressSnapshot {
    type Item = IncomingMessage<GatewayTarget>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pending
            .pop_front()
            .map(|ingress| GatewayReceiver::transfer(&self.shared, ingress))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.pending.len(), Some(self.pending.len()))
    }
}

impl ExactSizeIterator for GatewayIngressSnapshot {}

impl GatewayChannel {
    /// Connect a dedicated, explicitly trusted unsecured-development gateway.
    ///
    /// Only numeric loopback endpoints are allowed. `notifications` authorizes
    /// the exact native notification protocol, not arbitrary reverse requests.
    /// One deadline includes TCP connect and the ListenInstance exchange.
    pub async fn connect_unsecured_loopback(
        endpoint: SocketAddr,
        notifications: bool,
        limits: ChannelLimits,
        deadline: Instant,
    ) -> Result<Self> {
        limits.validate()?;
        if !endpoint.ip().is_loopback() || endpoint.port() == 0 {
            return Err(Error::InvalidInput(
                "unsecured gateway requires explicit loopback endpoint",
            ));
        }
        check_deadline(deadline)?;
        let process_id = uuid::Uuid::new_v4().to_bytes_le();
        let (stream, remote) = timeout_at(deadline, async {
            let mut stream = TcpStream::connect(endpoint).await?;
            stream.set_nodelay(true)?;
            write_message(
                &mut stream,
                &Message::listen_instance(
                    MessageId {
                        guid: process_id,
                        index: 1,
                    },
                    &ListenInstance::default(),
                )?,
            )
            .await?;
            let message = read_bounded(&mut stream, limits.max_frame_bytes).await?;
            let props = envelope(&message, true)?;
            control_instance(&message, &props).map(|remote| (stream, remote))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        check_deadline(deadline)?;
        Ok(Self::start(
            stream,
            endpoint,
            notifications,
            limits,
            process_id,
            remote,
        ))
    }

    fn start<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        stream: S,
        endpoint: SocketAddr,
        notifications: bool,
        limits: ChannelLimits,
        process_id: [u8; 16],
        remote: ListenInstance,
    ) -> Self {
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                terminal: None,
                next_token: 1,
                next_index: 2,
                remote,
                entries: HashMap::new(),
                pending: HashMap::new(),
                ordinary: VecDeque::new(),
                replies: VecDeque::new(),
                ingress: VecDeque::new(),
                history: HashSet::from([MessageId {
                    guid: process_id,
                    index: 1,
                }]),
                unmatched: 0,
            }),
            budget: Arc::new(Budget {
                limits,
                usage: Mutex::new(Usage::default()),
            }),
            target: Arc::new(GatewayTarget {
                identity: Arc::new(()),
                address: endpoint,
            }),
            notifications,
            process_id,
            changed: Notify::new(),
            stop: Notify::new(),
            stopped: watch::channel(false).0,
        });
        let join = tokio::spawn(drive(stream, shared.clone()));
        Self {
            owner: Arc::new(Owner {
                shared,
                join: tokio::sync::Mutex::new(Some(join)),
            }),
        }
    }

    /// Obtain the paired nonowning receiver; disabled mode fails explicitly.
    pub fn receiver(&self) -> Result<GatewayReceiver> {
        if self.owner.shared.error().is_some() {
            return Err(Error::Closed);
        }
        if !self.owner.shared.notifications {
            return Err(Error::InvalidInput("notification ingress is not enabled"));
        }
        Ok(GatewayReceiver {
            shared: Arc::downgrade(&self.owner.shared),
        })
    }

    pub fn terminal_error(&self) -> Option<Error> {
        self.owner.shared.error()
    }

    /// Initiate irreversible bounded shutdown without awaiting driver exit.
    pub fn abort(&self) {
        self.abort_with_error(Error::Closed);
    }

    /// Initiate shutdown retaining the first terminal cause.
    pub fn abort_with_error(&self, error: Error) {
        self.owner.shared.terminate(error);
    }

    /// Noninitiating bounded wait for the owned socket/driver to exit.
    ///
    /// The join handle stays owned if this observer times out or is dropped.
    pub fn closed(&self, deadline: Instant) -> ChannelFuture<'_, ()> {
        Box::pin(async move {
            let _waiter = self.owner.shared.budget.reserve(CLOSE)?;
            timeout_at(deadline, async {
                let mut join = self.owner.join.lock().await;
                if let Some(handle) = join.as_mut() {
                    let result = handle.await;
                    *join = None;
                    result.map_err(|_| Error::Protocol("channel driver task failed"))?;
                }
                Ok(())
            })
            .await
            .map_err(|_| Error::Timeout)?
        })
    }

    /// Lazy ordinary RPC plus independently retained first-write cancellation.
    /// Domain decoding and uncertain-mutation policy remain caller-owned.
    pub fn request_tracked<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
    ) -> (ChannelFuture<'a, Message>, RequestCancellation) {
        let state = Arc::new(Mutex::new(RequestState::default()));
        let cancellation = RequestCancellation {
            shared: Arc::downgrade(&self.owner.shared),
            state: state.clone(),
        };
        let future = Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Rpc,
                Some(state),
            )
            .await?
            {
                Outcome::Message(message) => Ok(message),
                Outcome::Written => unreachable!(),
            }
        });
        (future, cancellation)
    }

    /// Latest native ListenInstance; diagnostic only, not renewed authority.
    pub fn remote_instance(&self) -> ListenInstance {
        self.owner.shared.inner.lock().unwrap().remote.clone()
    }
}

fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Timeout)
    } else {
        Ok(())
    }
}

fn local(error: Error) -> Error {
    match error {
        Error::Protocol(reason) => Error::InvalidInput(reason),
        other => other,
    }
}

/// Caller fields remain unique. Received native common-header scanning uses
/// the final RelatesTo (MessageHeaders.cpp:765-824 at pinned SF 3988b451).
/// Preserve and validate every record under the existing header/frame bounds.
/// Only ExpectsReply retains the second-byte-identical exception; other fields
/// stay unique. Domain reply eligibility belongs to the caller.
fn envelope(message: &Message, native_inbound: bool) -> Result<Properties> {
    let ids = [
        message::ACTOR,
        message::ACTION,
        message::MESSAGE_ID,
        message::RELATES_TO,
        message::EXPECTS_REPLY,
        message::UNCORRELATED_REPLY,
        message::FAULT,
        message::HIGH_PRIORITY,
        message::IPC,
    ];
    let mut seen: [Option<&Header>; 9] = [None; 9];
    let mut repeated = [false; 9];
    if message.headers.len() > MAX_HEADER_COUNT {
        return Err(Error::Protocol("header count exceeds limit"));
    }
    for header in &message.headers {
        if let Some(index) = ids.iter().position(|id| *id == header.id) {
            if native_inbound && header.id == message::RELATES_TO {
                continue;
            }
            if let Some(previous) = seen[index] {
                if !native_inbound
                    || !matches!(header.id, message::RELATES_TO | message::EXPECTS_REPLY)
                    || previous.value != header.value
                    || repeated[index]
                {
                    let reason = if native_inbound
                        && repeated[index]
                        && matches!(header.id, message::RELATES_TO | message::EXPECTS_REPLY)
                    {
                        "more than two native correlation headers"
                    } else {
                        match header.id {
                            message::RELATES_TO => "contradictory native RelatesTo header",
                            message::EXPECTS_REPLY => "contradictory native ExpectsReply header",
                            message::MESSAGE_ID => "duplicate native MessageId header",
                            message::ACTOR => "duplicate native Actor header",
                            message::ACTION => "duplicate native Action header",
                            _ => "duplicate or contradictory transport envelope header",
                        }
                    };
                    return Err(Error::Protocol(reason));
                }
                repeated[index] = true;
            }
            seen[index] = Some(header);
        }
    }
    message.properties()
}

fn has(message: &Message, id: u16) -> bool {
    message.headers.iter().any(|header| header.id == id)
}

fn domain_gateway(message: &Message, props: &Properties) -> Result<()> {
    if props.actor != 5 || props.action.is_empty() || has(message, message::IPC) {
        return Err(Error::InvalidInput(
            "only native actor-5 gateway domain messages are authorized",
        ));
    }
    Ok(())
}

fn allocation_check(message: &Message, limits: ChannelLimits) -> Result<()> {
    let input_limit =
        limits.max_frame_bytes + MAX_HEADER_COUNT * size_of::<Header>() + MAX_HEADER_SIZE;
    let retained = message.headers.iter().try_fold(
        message
            .body
            .capacity()
            .checked_add(
                message
                    .headers
                    .capacity()
                    .checked_mul(size_of::<Header>())
                    .ok_or(Error::ResourceLimit("header allocation overflow"))?,
            )
            .ok_or(Error::ResourceLimit("message allocation overflow"))?,
        |n, h| {
            n.checked_add(h.value.capacity())
                .ok_or(Error::ResourceLimit("header allocation overflow"))
        },
    )?;
    let wire = message.headers.iter().try_fold(
        FIXED_HEADER_SIZE
            .checked_add(message.body.len())
            .ok_or(Error::ResourceLimit("frame size overflow"))?,
        |n, h| {
            n.checked_add(4)
                .and_then(|n| n.checked_add(h.value.len()))
                .ok_or(Error::ResourceLimit("frame size overflow"))
        },
    )?;
    if retained > input_limit || wire > limits.max_frame_bytes {
        return Err(Error::ResourceLimit(
            "channel message allocation/frame limit",
        ));
    }
    Ok(())
}

enum Submission {
    Raw,
    Rpc,
    Reply(MessageId),
}

struct OperationGuard {
    shared: Arc<Shared>,
    token: u64,
}

fn retire(inner: &mut Inner, token: u64) {
    if let Some(entry) = inner.entries.remove(&token) {
        if let Some(id) = entry.id {
            inner.pending.remove(&id);
        }
    }
    inner.ordinary.retain(|queued| *queued != token);
    inner.replies.retain(|queued| *queued != token);
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        let mut inner = self.shared.inner.lock().unwrap();
        retire(&mut inner, self.token);
        drop(inner);
        self.shared.changed.notify_waiters();
    }
}

async fn submit(
    shared: &Arc<Shared>,
    target: &GatewayTarget,
    mut message: Message,
    deadline: Instant,
    submission: Submission,
    tracking: Option<Arc<Mutex<RequestState>>>,
) -> Result<Outcome> {
    if shared.error().is_some() {
        return Err(Error::Closed);
    }
    check_deadline(deadline)?;
    if !Arc::ptr_eq(&shared.target.identity, &target.identity)
        || target.address != shared.target.address
    {
        return Err(Error::InvalidInput("foreign channel target"));
    }
    allocation_check(&message, shared.budget.limits)?;
    let props = envelope(&message, false).map_err(local)?;
    domain_gateway(&message, &props)?;
    let rpc = matches!(submission, Submission::Rpc);
    let class = if matches!(submission, Submission::Reply(_)) {
        REPLY
    } else {
        ORDINARY
    };
    match submission {
        Submission::Raw
            if props.expects_reply
                || props.uncorrelated_reply
                || props.relates_to != MessageId::default() =>
        {
            return Err(Error::InvalidInput(
                "raw send cannot bypass RPC or reply authority",
            ));
        }
        Submission::Rpc => {
            if props.uncorrelated_reply
                || props.relates_to != MessageId::default()
                || (has(&message, message::EXPECTS_REPLY) && !props.expects_reply)
                || (has(&message, message::MESSAGE_ID) && props.id == MessageId::default())
            {
                return Err(Error::InvalidInput("contradictory RPC correlation"));
            }
        }
        Submission::Reply(id) => {
            if props.action != "ServiceNotificationReply"
                || props.expects_reply
                || props.uncorrelated_reply
                || (has(&message, message::RELATES_TO) && props.relates_to != id)
            {
                return Err(Error::InvalidInput(
                    "contradictory or unauthorized notification reply",
                ));
            }
            if !has(&message, message::RELATES_TO) {
                message.headers.push(Header::relates_to(id)?);
            }
        }
        Submission::Raw => {}
    }
    let lease = shared.budget.reserve(class)?;
    let token;
    {
        let mut inner = shared.inner.lock().unwrap();
        if let Some(error) = &inner.terminal {
            return Err(error.clone());
        }
        check_deadline(deadline)?;
        let mut tracked = tracking.as_ref().map(|state| state.lock().unwrap());
        if tracked.as_ref().is_some_and(|state| state.cancelled) {
            return Err(Error::Closed);
        }
        let mut id = (props.id != MessageId::default()).then_some(props.id);
        if rpc && id.is_none() {
            loop {
                let Some(next) = inner.next_index.checked_add(1) else {
                    drop(tracked);
                    drop(inner);
                    let error = Error::ResourceLimit("channel generated ID exhaustion");
                    shared.terminate(error.clone());
                    return Err(error);
                };
                let candidate = MessageId {
                    guid: shared.process_id,
                    index: inner.next_index,
                };
                inner.next_index = next;
                if !inner.history.contains(&candidate) {
                    id = Some(candidate);
                    break;
                }
            }
            message.headers.push(Header::message_id(id.unwrap())?);
        }
        if rpc && !has(&message, message::EXPECTS_REPLY) {
            message
                .headers
                .push(Header::boolean(message::EXPECTS_REPLY, true)?);
        }
        allocation_check(&message, shared.budget.limits)?;
        let bytes = message
            .to_frame()
            .and_then(|frame| frame.encode())
            .map_err(local)?;
        check_deadline(deadline)?;
        if let Some(id) = id {
            if let Err(error) = shared.remember(&mut inner, id) {
                if matches!(error, Error::ResourceLimit(_)) {
                    drop(tracked);
                    drop(inner);
                    shared.terminate(error.clone());
                }
                return Err(error);
            }
        }
        token = inner.next_token;
        inner.next_token = token.checked_add(1).ok_or(Error::ResourceLimit(
            "channel operation identity exhaustion",
        ))?;
        inner.entries.insert(
            token,
            Entry {
                deadline,
                id,
                rpc,
                bytes: Some(bytes),
                result: None,
                lease,
                tracking: tracking.clone(),
            },
        );
        if let Some(state) = tracked.as_mut() {
            state.token = Some(token);
        }
        if rpc {
            inner.pending.insert(id.unwrap(), token);
        }
        if class == REPLY {
            inner.replies.push_back(token);
        } else {
            inner.ordinary.push_back(token);
        }
    }
    drop(message);
    let _guard = OperationGuard {
        shared: shared.clone(),
        token,
    };
    shared.changed.notify_waiters();
    timeout_at(deadline, async {
        loop {
            let notified = shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut inner = shared.inner.lock().unwrap();
                if let Some(error) = &inner.terminal {
                    return Err(error.clone());
                }
                if let Some(result) = inner
                    .entries
                    .get_mut(&token)
                    .and_then(|entry| entry.result.take())
                {
                    return result;
                }
                check_deadline(deadline)?;
                if !inner.entries.contains_key(&token) {
                    return Err(Error::Closed);
                }
            }
            notified.await;
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}

impl MessageChannel for GatewayChannel {
    type Target = GatewayTarget;

    fn resolve_target(&self, address: &str) -> Result<Arc<GatewayTarget>> {
        if self.owner.shared.error().is_some() {
            return Err(Error::Closed);
        }
        let endpoint = address
            .parse::<SocketAddr>()
            .map_err(|_| Error::InvalidInput("numeric gateway endpoint required"))?;
        if endpoint != self.owner.shared.target.address {
            return Err(Error::InvalidInput(
                "only the established gateway target is supported",
            ));
        }
        Ok(self.owner.shared.target.clone())
    }

    fn send_one_way<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
    ) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Raw,
                None,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                Outcome::Message(_) => unreachable!(),
            }
        })
    }

    fn close(&self, deadline: Instant) -> ChannelFuture<'_, ()> {
        Box::pin(async move {
            // Shutdown is irreversible even when its wait cannot be admitted.
            self.abort();
            self.closed(deadline).await
        })
    }
}

impl RpcChannel for GatewayChannel {
    fn request<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
    ) -> ChannelFuture<'a, Message> {
        Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Rpc,
                None,
            )
            .await?
            {
                Outcome::Message(message) => Ok(message),
                Outcome::Written => unreachable!(),
            }
        })
    }
}

struct NotificationReply {
    shared: Weak<Shared>,
    target: Arc<GatewayTarget>,
    id: MessageId,
}

impl ReplyContext for NotificationReply {
    fn reply(self: Box<Self>, message: Message, deadline: Instant) -> ChannelFuture<'static, ()> {
        Box::pin(async move {
            let shared = self.shared.upgrade().ok_or(Error::Closed)?;
            match submit(
                &shared,
                &self.target,
                message,
                deadline,
                Submission::Reply(self.id),
                None,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                Outcome::Message(_) => unreachable!(),
            }
        })
    }
}

impl GatewayReceiver {
    fn transfer(shared: &Arc<Shared>, ingress: Ingress) -> IncomingMessage<GatewayTarget> {
        IncomingMessage {
            message: ingress.message,
            source: shared.target.clone(),
            reply: Some(Box::new(NotificationReply {
                shared: Arc::downgrade(shared),
                target: shared.target.clone(),
                id: ingress.id,
            })),
        }
    }

    /// Consume a finite snapshot of already-admitted ingress in wire order.
    ///
    /// Does not wait for new traffic. Intended for a sole receiver's handoff
    /// across an RPC completion; concurrent receivers may consume other items.
    /// Each transferred envelope retains only its original one-use authority.
    pub fn drain_pending(&self) -> Result<GatewayIngressSnapshot> {
        let shared = self.shared.upgrade().ok_or(Error::Closed)?;
        let mut inner = shared.inner.lock().unwrap();
        if let Some(error) = &inner.terminal {
            return Err(error.clone());
        }
        let pending = std::mem::take(&mut inner.ingress);
        drop(inner);
        Ok(GatewayIngressSnapshot { shared, pending })
    }
}

impl MessageReceiver for GatewayReceiver {
    type Target = GatewayTarget;

    fn receive(&self, deadline: Instant) -> ChannelFuture<'_, IncomingMessage<GatewayTarget>> {
        Box::pin(async move {
            let shared = self.shared.upgrade().ok_or(Error::Closed)?;
            if shared.error().is_some() {
                return Err(Error::Closed);
            }
            check_deadline(deadline)?;
            let _waiter = shared.budget.reserve(RECEIVE)?;
            timeout_at(deadline, async {
                loop {
                    let notified = shared.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    {
                        let mut inner = shared.inner.lock().unwrap();
                        if let Some(error) = &inner.terminal {
                            return Err(error.clone());
                        }
                        check_deadline(deadline)?;
                        if let Some(ingress) = inner.ingress.pop_front() {
                            return Ok(Self::transfer(&shared, ingress));
                        }
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| Error::Timeout)?
        })
    }
}

async fn read_bounded<R: AsyncRead + Unpin>(reader: &mut R, limit: usize) -> Result<Message> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    reader.read_exact(&mut fixed[..1]).await?;
    timeout(MAX_FRAME_TIME, async {
        reader.read_exact(&mut fixed[1..]).await?;
        let (size, _) = validate_fixed(&fixed)?;
        if size > limit {
            return Err(Error::ResourceLimit("channel inbound frame limit"));
        }
        let mut bytes = vec![0; size];
        bytes[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
        reader.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
        Message::from_frame(Frame::decode(&bytes)?)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

fn control_instance(message: &Message, props: &Properties) -> Result<ListenInstance> {
    if let Some(fault) = props.fault.filter(|fault| fault.code != 0) {
        return Err(Error::Remote {
            code: fault.code,
            message: "native connection control fault".into(),
        });
    }
    if props.actor != 1
        || !props.action.is_empty()
        || props.expects_reply
        || props.uncorrelated_reply
        || props.relates_to != MessageId::default()
    {
        return Err(Error::Protocol("expected native ListenInstance control"));
    }
    ListenInstance::decode(&message.body)
}

async fn receive_loop<R: AsyncRead + Unpin>(mut reader: R, shared: &Arc<Shared>) -> Result<()> {
    loop {
        let message = read_bounded(&mut reader, shared.budget.limits.max_frame_bytes).await?;
        let props = envelope(&message, true)?;
        if props.actor == 1 {
            let remote = control_instance(&message, &props)?;
            shared.inner.lock().unwrap().remote = remote;
            continue;
        }
        if matches!(props.actor, 21 | 29 | 35 | 36) {
            if let Some(fault) = props.fault.filter(|fault| fault.code != 0) {
                return Err(Error::Remote {
                    code: fault.code,
                    message: "native transport control fault".into(),
                });
            }
            return Err(Error::Protocol(
                "unsupported role/security/reconnect control; no recovery",
            ));
        }
        if props.uncorrelated_reply {
            if !shared.notifications
                || props.actor != 5
                || props.action != "ServiceNotificationRequest"
                || props.id == MessageId::default()
                || props.relates_to != MessageId::default()
                || !props.expects_reply
                || props.fault.is_some()
                || has(&message, message::IPC)
            {
                return Err(Error::Protocol("unauthorized native notification envelope"));
            }
            let lease = shared.budget.reserve(INGRESS)?;
            let mut inner = shared.inner.lock().unwrap();
            if inner.terminal.is_some() {
                return Err(Error::Closed);
            }
            shared
                .remember(&mut inner, props.id)
                .map_err(|error| match error {
                    Error::InvalidInput(_) => {
                        Error::Protocol("duplicate or colliding notification ID")
                    }
                    other => other,
                })?;
            inner.ingress.push_back(Ingress {
                message,
                id: props.id,
                _lease: lease,
            });
            drop(inner);
            shared.changed.notify_waiters();
            continue;
        }
        if props.relates_to == MessageId::default()
            || props.expects_reply
            || has(&message, message::IPC)
        {
            return Err(Error::Protocol("unauthorized ordinary gateway reply"));
        }
        let token = {
            let mut inner = shared.inner.lock().unwrap();
            // Properties validates every occurrence and exposes the final
            // native-effective ID. Never try an earlier ID, even if final is
            // unknown/retired and an earlier one names a live neighbor.
            let token = inner.pending.remove(&props.relates_to);
            if token.is_none() {
                inner.unmatched += 1;
                if inner.unmatched > 1024 {
                    return Err(Error::Protocol("too many unmatched replies"));
                }
            } else {
                inner.unmatched = 0;
            }
            token
        };
        if let Some(token) = token {
            let result = match props.fault.filter(|fault| fault.code != 0) {
                Some(fault) => Err(Error::Remote {
                    code: fault.code,
                    message: "native transport Fault".into(),
                }),
                None => Ok(Outcome::Message(message)),
            };
            shared.finish(token, result);
        }
    }
}

async fn send_loop<W: AsyncWrite + Unpin>(mut writer: W, shared: &Arc<Shared>) -> Result<()> {
    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let work = {
            let mut inner = shared.inner.lock().unwrap();
            if inner.terminal.is_some() {
                return Ok(());
            }
            let token = inner
                .replies
                .pop_front()
                .or_else(|| inner.ordinary.pop_front());
            token.and_then(|token| {
                let entry = inner.entries.get_mut(&token)?;
                if Instant::now() >= entry.deadline || entry.result.is_some() {
                    entry.bytes = None;
                    if entry.result.is_none() {
                        entry.result = Some(Err(Error::Timeout));
                    }
                    let id = entry.id;
                    if let Some(id) = id {
                        inner.pending.remove(&id);
                    }
                    return None;
                }
                Some((
                    token,
                    entry.bytes.take()?,
                    entry.lease.clone(),
                    entry.rpc,
                    entry.deadline,
                ))
            })
        };
        if let Some((token, bytes, _lease, rpc, deadline)) = work {
            // Selection is not frame start. Until a positive write, validate
            // ownership/deadline under the same lock as cancellation/close.
            let first = timeout_at(deadline, async {
                loop {
                    let changed = shared.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    tokio::select! {
                        result = std::future::poll_fn(|cx| {
                            let inner = shared.inner.lock().unwrap();
                            let eligible = inner.terminal.is_none()
                                && inner.entries.get(&token).is_some_and(|entry|
                                    entry.result.is_none() && Instant::now() < entry.deadline);
                            if !eligible { return Poll::Ready(Ok(None)); }
                            match std::pin::Pin::new(&mut writer).poll_write(cx, &bytes) {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(Ok(0)) => Poll::Ready(Err(Error::Io(std::io::ErrorKind::WriteZero.into()))),
                                Poll::Ready(Ok(size)) => {
                                    if let Some(tracking) = &inner.entries[&token].tracking {
                                        tracking.lock().unwrap().visible = true;
                                    }
                                    Poll::Ready(Ok(Some(size)))
                                },
                                Poll::Ready(Err(error)) => Poll::Ready(Err(Error::from(error))),
                            }
                        }) => return result,
                        _ = changed => {},
                    }
                }
            }).await;
            let offset = match first {
                Ok(Ok(Some(offset))) => offset,
                Ok(Err(error)) => {
                    shared.finish(token, Err(error.clone()));
                    return Err(error);
                }
                Ok(Ok(None)) | Err(_) => {
                    shared.finish(
                        token,
                        Err(if Instant::now() >= deadline {
                            Error::Timeout
                        } else {
                            Error::Closed
                        }),
                    );
                    continue;
                }
            };
            // A positive first write is irrevocable. Preserve the complete
            // suffix despite observer abandonment, expiry or terminal close.
            let result = timeout(MAX_FRAME_TIME, writer.write_all(&bytes[offset..]))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(Error::from);
            if !rpc || result.is_err() {
                shared.finish(token, result.clone().map(|_| Outcome::Written));
            }
            result?;
        } else {
            // Queue disposal/expiry may have left more keys to process.
            let more = {
                let inner = shared.inner.lock().unwrap();
                !inner.ordinary.is_empty() || !inner.replies.is_empty()
            };
            if more {
                continue;
            }
            notified.await;
        }
    }
}

async fn expire_loop(shared: &Arc<Shared>) {
    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (deadline, did_expire) = {
            let mut inner = shared.inner.lock().unwrap();
            if inner.terminal.is_some() {
                return;
            }
            let now = Instant::now();
            let expired: Vec<_> = inner
                .entries
                .iter()
                .filter(|(_, entry)| entry.result.is_none() && entry.deadline <= now)
                .map(|(token, _)| *token)
                .collect();
            let did_expire = !expired.is_empty();
            for token in expired {
                let entry = inner.entries.get_mut(&token).unwrap();
                entry.result = Some(Err(Error::Timeout));
                entry.bytes = None;
                let id = entry.id;
                if let Some(id) = id {
                    inner.pending.remove(&id);
                }
            }
            (
                inner
                    .entries
                    .values()
                    .filter(|entry| entry.result.is_none())
                    .map(|entry| entry.deadline)
                    .min(),
                did_expire,
            )
        };
        if did_expire {
            shared.changed.notify_waiters();
        }
        match deadline {
            Some(deadline) => {
                tokio::select! { _ = tokio::time::sleep_until(deadline) => {}, _ = notified => {} }
            }
            None => notified.await,
        }
    }
}

struct DriverExit(Arc<Shared>);
impl Drop for DriverExit {
    fn drop(&mut self) {
        self.0.terminate(Error::Closed);
        self.0.stopped.send_replace(true);
    }
}

async fn drive<S: AsyncRead + AsyncWrite + Unpin>(stream: S, shared: Arc<Shared>) {
    let _exit = DriverExit(shared.clone());
    let (reader, writer) = tokio::io::split(stream);
    let reading = async {
        let stop = shared.stop.notified();
        tokio::pin!(stop);
        stop.as_mut().enable();
        if shared.error().is_some() {
            return;
        }
        tokio::select! {
            _ = stop => {},
            result = receive_loop(reader, &shared) => shared.terminate(result.err().unwrap_or(Error::Closed)),
        }
    };
    let writing = async {
        if let Err(error) = send_loop(writer, &shared).await {
            shared.terminate(error);
        }
    };
    tokio::join!(reading, writing, expire_loop(&shared));
}
