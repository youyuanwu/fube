//! Operational, owned native channels over plain TCP.
//!
//! This is the operational backend for [`crate::channel`], not an adapter around
//! the retired legacy envelope-building APIs. Connect-time composition separates native
//! actor ingress, ordinary correlation and authorized special duplex handling.
//! Consumer actor/action/envelope authorization is supplied by client adapters.
//! IPC session envelopes are composed by [`crate::native_ipc`]; separately
//! authorized normal-request replies are shared request/reply machinery.
//! The private raw-session engine owns socket/task progress, admitted write
//! jobs, common controls and joining. Channel policy shares its coordinator
//! for atomic initial-write eligibility, but keeps correlation, IDs, grants
//! and session/actor authorization above that engine.
//! No listener, replication, TLS, reconnect or replay is supplied.
//!
//! Connections are unencrypted and unauthenticated. Domain headers/body,
//! including TimeoutHeader, remain caller-owned.
//! See `docs/transport/NativeChannel.md` for limits and qualification.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::size_of,
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, Mutex, Weak},
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::oneshot,
    time::{timeout, timeout_at, Instant},
};

use crate::{
    channel::{
        ChannelFuture, IncomingMessage, MessageChannel, MessageReceiver, ReplyContext, RpcChannel,
    },
    connection::MAX_FRAME_TIME,
    frame::{FIXED_HEADER_SIZE, MAX_FRAME_SIZE, MAX_HEADER_SIZE},
    message::{self, Header, ListenInstance, Message, MessageId, Properties, MAX_HEADER_COUNT},
    native_demux::{ChannelComposition, Dispatch, EnvelopeValidator, OutboundKind},
    raw_session::{
        self,
        engine::{self, control_instance, envelope, has},
    },
    Error, Result,
};

#[cfg(test)]
#[path = "native_channel_tests.rs"]
mod tests;

#[path = "native_request_reply.rs"]
mod request_reply;
use request_reply::NativeReplyContext;

#[cfg(test)]
use crate::frame::{validate_fixed, Frame};

#[derive(Clone, Copy, PartialEq, Eq)]
enum BudgetClass {
    Ordinary,
    Reply,
    Ingress,
    Receive,
    Close,
}

impl BudgetClass {
    fn index(self) -> usize {
        match self {
            Self::Ordinary => 0,
            Self::Reply => 1,
            Self::Ingress => 2,
            Self::Receive => 3,
            Self::Close => 4,
        }
    }
}

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
    /// Lifetime outbound + authorized duplex ID history; exhaustion is terminal.
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

    pub(crate) fn validate(&self) -> Result<()> {
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
    class: BudgetClass,
    bytes: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap();
        usage.counts[self.class.index()] -= 1;
        usage.bytes -= self.bytes;
    }
}

impl Budget {
    fn reserve(self: &Arc<Self>, class: BudgetClass) -> Result<Arc<Lease>> {
        let count_limit = match class {
            BudgetClass::Reply => self.limits.max_replies,
            BudgetClass::Ingress => self.limits.max_ingress,
            BudgetClass::Ordinary | BudgetClass::Receive | BudgetClass::Close => {
                self.limits.max_operations
            }
        };
        let bytes = match class {
            BudgetClass::Receive | BudgetClass::Close => 256,
            BudgetClass::Ordinary | BudgetClass::Reply | BudgetClass::Ingress => {
                self.limits.slot_bytes()?
            }
        };
        let mut usage = self.usage.lock().unwrap();
        let total = usage
            .bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit("channel bytes"))?;
        if usage.counts[class.index()] >= count_limit || total > self.limits.max_retained_bytes {
            return Err(Error::ResourceLimit("channel retained count/bytes"));
        }
        usage.counts[class.index()] += 1;
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
    Held(Message, CompletionGuard),
}

/// Bounded, nonblocking per-attempt validity sample, not a domain decoder.
///
/// Runs outside the shared-state mutex. It may inspect diagnostics but must
/// not mutate/cancel this channel or perform I/O, blocking work or user callbacks.
pub type WritePredicate = Arc<dyn Fn() -> bool + Send + Sync>;

/// Trusted, bounded session envelope policy, never domain work or I/O.
///
/// Common control, correlation and consuming reply guards cannot be overridden.
pub(crate) trait SessionEnvelope: Send + Sync {
    fn header(&self, _: &Message, _: bool) -> Result<Option<Header>> {
        Ok(None)
    }
    fn requires_message_id(&self) -> bool {
        false
    }
    fn incoming(&self, message: &Message) -> Result<()> {
        if has(message, message::IPC) {
            return Err(Error::Protocol("IPC envelope on native channel"));
        }
        Ok(())
    }
}

pub(crate) struct NativeEnvelope;
impl SessionEnvelope for NativeEnvelope {}

/// Opaque completion acknowledgment and retained result reservation.
///
/// An opted-in reader waits until acknowledgment/drop before delivering later
/// frames. Keeping this after acknowledgment retains only the result budget,
/// never socket ownership. It cannot grant sender or reply authority.
pub struct CompletionGuard {
    release: Option<oneshot::Sender<()>>,
    _lease: Arc<Lease>,
}

impl CompletionGuard {
    pub fn acknowledge(&mut self) {
        self.release.take();
    }
}

struct Entry {
    deadline: Instant,
    id: Option<MessageId>,
    rpc: bool,
    result: Option<Result<Outcome>>,
    lease: Arc<Lease>,
    tracking: Option<Arc<Mutex<RequestState>>>,
    hold_reply: bool,
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
        let _visibility = shared.as_ref().map(|s| s.visibility.lock().unwrap());
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
        if let Some(shared) = &shared {
            shared.changed.notify_waiters();
        }
        true
    }
}

struct Ingress {
    message: Message,
    received_at: Instant,
    reply_id: Option<MessageId>,
    reply_authorize: Option<EnvelopeValidator>,
    handoff: Option<oneshot::Sender<()>>,
    _lease: Arc<Lease>,
}

struct ChannelState {
    next_token: u64,
    next_index: u32,
    entries: HashMap<u64, Entry>,
    pending: HashMap<MessageId, u64>,
    ingress: VecDeque<Ingress>,
    history: HashSet<MessageId>,
    unmatched: usize,
}

type Inner = engine::Coordinated<ChannelState>;

impl ChannelState {
    fn retire_pending(&mut self, id: Option<MessageId>) {
        if let Some(id) = id {
            self.pending.remove(&id);
        }
    }

    fn settle(&mut self, token: u64, result: Result<Outcome>, now: Instant) {
        if let Some(entry) = self.entries.get_mut(&token) {
            if entry.result.is_none() {
                entry.result = Some(if now >= entry.deadline {
                    Err(Error::Timeout)
                } else {
                    result
                });
                let id = entry.id;
                self.retire_pending(id);
            }
        }
    }
}

impl Inner {
    fn settle(&mut self, token: u64, result: Result<Outcome>, now: Instant) {
        self.policy.settle(token, result, now);
        // An early reply/expiry retires queued bytes atomically. Selected jobs
        // remain engine-owned and recheck this policy before a positive write.
        self.transport.remove(token);
    }
}

impl engine::Policy for ChannelState {
    type Charge = Arc<Lease>;

    fn eligible(&self, token: u64, now: Instant) -> bool {
        self.entries
            .get(&token)
            .is_some_and(|entry| entry.result.is_none() && now < entry.deadline)
    }

    fn started(&self, token: u64) {
        if let Some(tracking) = &self.entries[&token].tracking {
            tracking.lock().unwrap().visible = true;
        }
    }

    fn receipt(&mut self, token: u64, receipt: engine::Receipt, now: Instant) {
        let result = match receipt {
            engine::Receipt::Written => {
                if self.entries.get(&token).is_none_or(|entry| entry.rpc) {
                    return;
                }
                Ok(Outcome::Written)
            }
            engine::Receipt::Rejected => Err(
                if self
                    .entries
                    .get(&token)
                    .is_some_and(|entry| now >= entry.deadline)
                {
                    Error::Timeout
                } else {
                    Error::Closed
                },
            ),
            engine::Receipt::Failed(error) => Err(error),
        };
        self.settle(token, result, now);
    }

    fn retire(&mut self) {
        self.entries.clear();
        self.pending.clear();
        self.ingress.clear();
        self.history.clear();
    }
}

struct Shared {
    core: Arc<engine::Core<ChannelState>>,
    budget: Arc<Budget>,
    target: Arc<GatewayTarget>,
    composition: ChannelComposition,
    process_id: [u8; 16],
    session_envelope: Arc<dyn SessionEnvelope>,
}

impl Deref for Shared {
    type Target = engine::Core<ChannelState>;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Shared {
    #[cfg(test)]
    fn selected_unsettled(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .entries
            .iter()
            .any(|(token, entry)| !inner.transport.queued(*token) && entry.result.is_none())
    }

    fn error(&self) -> Option<Error> {
        self.core.error()
    }

    fn terminate(&self, error: Error) {
        self.end(error, false);
    }

    fn end(&self, error: Error, dispose: bool) {
        self.core.end(error, dispose);
    }

    fn finish(&self, token: u64, result: Result<Outcome>) {
        let mut inner = self.inner.lock().unwrap();
        inner.settle(token, result, Instant::now());
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
    connection: engine::Owner<ChannelState>,
}

/// Cloneable lifecycle owner of one established native connection.
///
/// Also exported as [`crate::Channel`], for ordinary clients, raw upload and
/// outgoing/accepted IPC. Shared type does not imply interchangeable session
/// state or permission; construction of a typed stub grants no authorization.
///
/// Only these owner clones retain live ownership. GatewayReceiver, target and
/// reply contexts are nonowning. A typed façade should borrow this owner, not
/// clone it silently. Last owner drop initiates bounded terminal cleanup.
#[derive(Clone)]
pub struct GatewayChannel {
    owner: Arc<Owner>,
}

/// Nonowning pull capability for configured, authorized application ingress.
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
    pub(crate) async fn connect_with_envelope(
        endpoint: SocketAddr,
        composition: ChannelComposition,
        limits: ChannelLimits,
        deadline: Instant,
        session_envelope: Arc<dyn SessionEnvelope>,
    ) -> Result<Self> {
        limits.validate()?;
        crate::endpoint::validate_tcp_endpoint(endpoint)?;
        check_deadline(deadline)?;
        let process_id = uuid::Uuid::new_v4().to_bytes_le();
        let (stream, message) = raw_session::connect(
            endpoint,
            ListenInstance::default(),
            MessageId {
                guid: process_id,
                index: 1,
            },
            raw_limits(limits.max_frame_bytes),
            deadline,
            |_, _| Ok(raw_session::Admission::Retain(())),
        )
        .await?;
        let props = envelope(&message.message, true)?;
        let remote = control_instance(&message.message, &props)?;
        check_deadline(deadline)?;
        Self::adopt_established(
            stream,
            composition,
            limits,
            process_id,
            remote,
            deadline,
            session_envelope,
        )
    }

    /// Publish exactly one established plain-TCP socket.
    ///
    /// The session adapter must complete and validate its hello before calling.
    /// Actual endpoint/finite-limit/deadline checks are repeated here; callers
    /// cannot assert a different target or gain domain admission from adoption.
    pub(crate) fn adopt_established(
        stream: TcpStream,
        composition: ChannelComposition,
        limits: ChannelLimits,
        process_id: [u8; 16],
        remote: ListenInstance,
        deadline: Instant,
        session_envelope: Arc<dyn SessionEnvelope>,
    ) -> Result<Self> {
        limits.validate()?;
        let endpoint = stream.peer_addr()?;
        crate::endpoint::validate_tcp_endpoint(endpoint)?;
        crate::endpoint::validate_tcp_endpoint(stream.local_addr()?)?;
        if process_id == [0; 16] {
            return Err(Error::InvalidInput("invalid established TCP session"));
        }
        check_deadline(deadline)?;
        stream.set_nodelay(true)?;
        Self::publish_before_deadline(
            Self::start_with_envelope(
                stream,
                endpoint,
                composition,
                limits,
                process_id,
                remote,
                session_envelope,
            ),
            deadline,
        )
    }

    pub(crate) fn publish_before_deadline(channel: Self, deadline: Instant) -> Result<Self> {
        // Owner allocation/driver publication belongs to establishment too.
        // On expiry, normal drop of this unpublished owner initiates cleanup.
        check_deadline(deadline)?;
        Ok(channel)
    }

    #[cfg(test)]
    fn start<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        stream: S,
        endpoint: SocketAddr,
        composition: ChannelComposition,
        limits: ChannelLimits,
        process_id: [u8; 16],
        remote: ListenInstance,
    ) -> Self {
        Self::start_with_envelope(
            stream,
            endpoint,
            composition,
            limits,
            process_id,
            remote,
            Arc::new(NativeEnvelope),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_envelope<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        stream: S,
        endpoint: SocketAddr,
        composition: ChannelComposition,
        limits: ChannelLimits,
        process_id: [u8; 16],
        remote: ListenInstance,
        session_envelope: Arc<dyn SessionEnvelope>,
    ) -> Self {
        let shared = Arc::new(Shared {
            core: engine::Core::new(
                ChannelState {
                    next_token: 1,
                    next_index: 2,
                    entries: HashMap::new(),
                    pending: HashMap::new(),
                    ingress: VecDeque::new(),
                    history: HashSet::from([MessageId {
                        guid: process_id,
                        index: 1,
                    }]),
                    unmatched: 0,
                },
                remote,
            ),
            budget: Arc::new(Budget {
                limits,
                usage: Mutex::new(Usage::default()),
            }),
            target: Arc::new(GatewayTarget {
                identity: Arc::new(()),
                address: endpoint,
            }),
            composition,
            process_id,
            session_envelope,
        });
        let connection =
            engine::Owner::start(stream, shared.clone(), raw_limits(limits.max_frame_bytes));
        Self {
            owner: Arc::new(Owner { shared, connection }),
        }
    }

    /// Clone this connection's original, inert target after a terminal sample.
    ///
    /// This does no I/O and never derives identity from advertised metadata.
    /// Success is not lasting liveness or permission: every submission still
    /// checks connection affinity, terminal state and installed policies.
    pub fn target(&self) -> Result<Arc<GatewayTarget>> {
        if self.owner.shared.error().is_some() {
            return Err(Error::Closed);
        }
        Ok(self.owner.shared.target.clone())
    }

    /// Obtain the paired nonowning receiver; disabled mode fails explicitly.
    pub fn receiver(&self) -> Result<GatewayReceiver> {
        if self.owner.shared.error().is_some() {
            return Err(Error::Closed);
        }
        if self.owner.shared.composition.actors.is_empty()
            && self.owner.shared.composition.duplex.is_none()
        {
            return Err(Error::InvalidInput("gateway ingress is not enabled"));
        }
        Ok(GatewayReceiver {
            shared: Arc::downgrade(&self.owner.shared),
        })
    }

    pub fn terminal_error(&self) -> Option<Error> {
        self.owner.shared.error()
    }

    /// Observe first-cause terminal state without initiating close or joining
    /// a started frame. Waiter admission is finite and nonowning.
    pub fn wait_terminal(&self, deadline: Instant) -> ChannelFuture<'_, Error> {
        Box::pin(async move {
            let _waiter = self.owner.shared.budget.reserve(BudgetClass::Close)?;
            timeout_at(deadline, async {
                loop {
                    let stopped = self.owner.shared.stop.notified();
                    tokio::pin!(stopped);
                    stopped.as_mut().enable();
                    if let Some(error) = self.terminal_error() {
                        return Ok(error);
                    }
                    stopped.await;
                }
            })
            .await
            .map_err(|_| Error::Timeout)?
        })
    }

    /// Stop admission and initiate ordinary shutdown without awaiting driver exit.
    ///
    /// A started frame retains its suffix until completion or its independent
    /// watchdog. Use [`Self::dispose`] only to retire the entire socket instead.
    pub fn abort(&self) {
        self.abort_with_error(Error::Closed);
    }

    /// Initiate ordinary shutdown retaining the first terminal cause.
    pub fn abort_with_error(&self, error: Error) {
        self.owner.shared.terminate(error);
    }

    /// Irreversibly dispose the entire owned socket, including a started frame.
    ///
    /// Unlike ordinary abort/close, this permits truncating an uncertain suffix.
    /// No part of the stream can be reused. It can escalate ordinary shutdown
    /// without replacing its first cause. Use [`Self::closed`] to observe driver
    /// exit; timeout/drop of that observer does not relinquish join ownership.
    pub fn dispose(&self) {
        self.owner.shared.end(Error::Closed, true);
    }

    /// Noninitiating bounded wait for the owned socket/driver to exit.
    ///
    /// The join handle stays owned if this observer times out or is dropped.
    pub fn closed(&self, deadline: Instant) -> ChannelFuture<'_, ()> {
        Box::pin(async move {
            let _waiter = self.owner.shared.budget.reserve(BudgetClass::Close)?;
            self.owner.connection.closed(deadline).await
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
        self.request_tracked_fenced(target, message, deadline, None)
    }

    pub fn request_tracked_fenced<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
        valid: Option<WritePredicate>,
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
                valid,
                false,
            )
            .await?
            {
                Outcome::Message(message) => Ok(message),
                _ => unreachable!(),
            }
        });
        (future, cancellation)
    }

    /// RPC with opaque, ordered validation handoff; no domain code runs here.
    /// The caller must poll independently of its observer, acknowledge a valid
    /// result, or abort before releasing malformed protocol completion.
    pub fn request_held<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
        valid: Option<WritePredicate>,
    ) -> ChannelFuture<'a, (Message, CompletionGuard)> {
        Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Rpc,
                None,
                valid,
                true,
            )
            .await?
            {
                Outcome::Held(message, guard) => Ok((message, guard)),
                _ => unreachable!(),
            }
        })
    }

    pub fn send_one_way_fenced<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
        valid: WritePredicate,
    ) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Raw,
                None,
                Some(valid),
                false,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                _ => unreachable!(),
            }
        })
    }

    /// Semantic one-way control traffic using separately bounded reply slots.
    /// Requires explicit Reserved authorization; never adds RelatesTo.
    pub fn send_reserved<'a>(
        &'a self,
        target: &'a GatewayTarget,
        message: Message,
        deadline: Instant,
        valid: Option<WritePredicate>,
    ) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            match submit(
                &self.owner.shared,
                target,
                message,
                deadline,
                Submission::Reserved,
                None,
                valid,
                false,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                _ => unreachable!(),
            }
        })
    }

    /// Latest native ListenInstance; diagnostic only, not renewed authority.
    pub fn remote_instance(&self) -> ListenInstance {
        self.owner
            .shared
            .inner
            .lock()
            .unwrap()
            .transport
            .remote
            .clone()
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
    Reserved,
    Rpc,
    Reply(MessageId, EnvelopeValidator),
}

struct OperationGuard {
    shared: Arc<Shared>,
    token: u64,
}

fn retire(inner: &mut Inner, token: u64) {
    if let Some(entry) = inner.entries.remove(&token) {
        inner.retire_pending(entry.id);
    }
    inner.transport.remove(token);
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        let _visibility = self.shared.visibility.lock().unwrap();
        let mut inner = self.shared.inner.lock().unwrap();
        retire(&mut inner, self.token);
        drop(inner);
        self.shared.changed.notify_waiters();
    }
}

#[allow(clippy::too_many_arguments)]
async fn submit(
    shared: &Arc<Shared>,
    target: &GatewayTarget,
    mut message: Message,
    deadline: Instant,
    submission: Submission,
    tracking: Option<Arc<Mutex<RequestState>>>,
    valid: Option<WritePredicate>,
    hold_reply: bool,
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
    if crate::native_demux::is_control(props.actor) || has(&message, message::IPC) {
        return Err(Error::InvalidInput("reserved native transport envelope"));
    }
    let session_header = shared
        .session_envelope
        .header(&message, matches!(submission, Submission::Reply(..)))?;
    match submission {
        Submission::Raw => (shared.composition.outbound)(&message, &props, OutboundKind::Raw)?,
        Submission::Reserved => {
            (shared.composition.outbound)(&message, &props, OutboundKind::Reserved)?
        }
        Submission::Rpc => (shared.composition.outbound)(&message, &props, OutboundKind::Rpc)?,
        Submission::Reply(_, authorize) => authorize(&message, &props)?,
    }
    let rpc = matches!(submission, Submission::Rpc);
    let class = if matches!(submission, Submission::Reply(..) | Submission::Reserved) {
        BudgetClass::Reply
    } else {
        BudgetClass::Ordinary
    };
    match submission {
        Submission::Raw | Submission::Reserved
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
        Submission::Reply(id, _) => {
            if props.expects_reply
                || props.uncorrelated_reply
                || (has(&message, message::RELATES_TO) && props.relates_to != id)
            {
                return Err(Error::InvalidInput("contradictory duplex reply"));
            }
            if !has(&message, message::RELATES_TO) {
                message.headers.push(Header::relates_to(id)?);
            }
        }
        Submission::Raw | Submission::Reserved => {}
    }
    if let Some(header) = session_header {
        message.headers.push(header);
    }
    let requires_id = rpc || shared.session_envelope.requires_message_id();
    let lease = shared.budget.reserve(class)?;
    let token;
    {
        let mut inner = shared.inner.lock().unwrap();
        if let Some(error) = &inner.transport.terminal {
            return Err(error.clone());
        }
        check_deadline(deadline)?;
        let mut tracked = tracking.as_ref().map(|state| state.lock().unwrap());
        if tracked.as_ref().is_some_and(|state| state.cancelled) {
            return Err(Error::Closed);
        }
        let mut id = (props.id != MessageId::default()).then_some(props.id);
        if requires_id && id.is_none() && !matches!(submission, Submission::Reply(..)) {
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
                result: None,
                lease: lease.clone(),
                tracking: tracking.clone(),
                hold_reply,
            },
        );
        if let Some(state) = tracked.as_mut() {
            state.token = Some(token);
        }
        if rpc {
            inner.pending.insert(id.unwrap(), token);
        }
        inner.transport.enqueue(
            engine::WriteJob {
                token,
                bytes,
                deadline,
                valid,
                charge: lease,
            },
            class == BudgetClass::Reply,
        );
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
                if let Some(error) = &inner.transport.terminal {
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
                None,
                false,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                _ => unreachable!(),
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
                None,
                false,
            )
            .await?
            {
                Outcome::Message(message) => Ok(message),
                _ => unreachable!(),
            }
        })
    }
}

impl GatewayReceiver {
    fn transfer(shared: &Arc<Shared>, ingress: Ingress) -> IncomingMessage<GatewayTarget> {
        IncomingMessage {
            message: ingress.message,
            received_at: ingress.received_at,
            handoff: ingress.handoff.map(|release| CompletionGuard {
                release: Some(release),
                _lease: ingress._lease.clone(),
            }),
            source: shared.target.clone(),
            reply: ingress.reply_id.map(|id| {
                Box::new(NativeReplyContext {
                    shared: Arc::downgrade(shared),
                    target: shared.target.clone(),
                    id,
                    // This is private ingress state, populated only after
                    // configured duplex authorization in the receive loop.
                    authorize: ingress.reply_authorize.unwrap(),
                }) as Box<dyn ReplyContext>
            }),
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
        if let Some(error) = &inner.transport.terminal {
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
            let _waiter = shared.budget.reserve(BudgetClass::Receive)?;
            timeout_at(deadline, async {
                loop {
                    let notified = shared.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    {
                        let mut inner = shared.inner.lock().unwrap();
                        if let Some(error) = &inner.transport.terminal {
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

fn raw_limits(max_frame_bytes: usize) -> raw_session::Limits {
    raw_session::Limits {
        max_frame_bytes,
        frame_time: MAX_FRAME_TIME,
    }
}

async fn receive_message(
    message: Message,
    props: Properties,
    received_at: Instant,
    shared: &Arc<Shared>,
) -> Result<()> {
    shared.session_envelope.incoming(&message)?;
    let mut reply_authorize = None;
    let reply_id = match shared.composition.actors.select(&props)? {
        Dispatch::Actor(authorize) => {
            authorize(&message, &props)?;
            if props.expects_reply {
                if let Some(policy) = &shared.composition.requests {
                    if props.id == MessageId::default() || props.fault.is_some() {
                        return Err(Error::Protocol("invalid native actor request"));
                    }
                    (policy.incoming)(&message, &props)?;
                    reply_authorize = Some(policy.reply);
                    Some(Some(props.id))
                } else {
                    Some(None)
                }
            } else {
                Some(None)
            }
        }
        Dispatch::Reply if props.uncorrelated_reply => {
            let duplex = shared
                .composition
                .duplex
                .as_ref()
                .ok_or(Error::Protocol("native duplex ingress is not enabled"))?;
            if props.id == MessageId::default() {
                return Err(Error::Protocol(
                    "duplex ingress requires original MessageId",
                ));
            }
            (duplex.incoming)(&message, &props)?;
            reply_authorize = Some(duplex.reply);
            Some(Some(props.id))
        }
        Dispatch::Reply => None,
    };
    if let Some(reply_id) = reply_id {
        let lease = shared.budget.reserve(BudgetClass::Ingress)?;
        let (handoff, acknowledgment) = if shared.composition.ordered_ingress {
            let (send, receive) = oneshot::channel();
            (Some(send), Some(receive))
        } else {
            (None, None)
        };
        {
            let mut inner = shared.inner.lock().unwrap();
            if inner.transport.terminal.is_some() {
                return Err(Error::Closed);
            }
            if let Some(id) = reply_id {
                shared
                    .remember(&mut inner, id)
                    .map_err(|error| match error {
                        Error::InvalidInput(_) => {
                            Error::Protocol("duplicate or colliding request ID")
                        }
                        other => other,
                    })?;
            }
            inner.ingress.push_back(Ingress {
                message,
                received_at,
                reply_id,
                reply_authorize,
                handoff,
                _lease: lease,
            });
        }
        shared.changed.notify_waiters();
        if let Some(acknowledgment) = acknowledgment {
            let _ = timeout(MAX_FRAME_TIME, acknowledgment)
                .await
                .map_err(|_| Error::Timeout)?;
        }
        return Ok(());
    }
    if props.relates_to == MessageId::default()
        || props.expects_reply
        || has(&message, message::IPC)
    {
        return Err(Error::Protocol("invalid ordinary native reply"));
    }
    (shared.composition.ordinary_reply)(&message, &props)?;
    let token = {
        let mut inner = shared.inner.lock().unwrap();
        // Properties validates every occurrence and exposes the final
        // native-effective ID. Never try an earlier ID, even if final is
        // unknown/retired and an earlier one names a live neighbor.
        let token = inner.pending.get(&props.relates_to).copied();
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
        let held = {
            let inner = shared.inner.lock().unwrap();
            inner
                .entries
                .get(&token)
                .filter(|entry| entry.hold_reply)
                .map(|entry| (entry.deadline, entry.lease.clone()))
        };
        let mut acknowledgment = None;
        let result = match props.fault.filter(|fault| fault.code != 0) {
            Some(fault) => Err(Error::Remote {
                code: fault.code,
                message: "native transport Fault".into(),
            }),
            None => match held {
                Some((deadline, lease)) => {
                    let (release, receive) = oneshot::channel();
                    acknowledgment = Some((deadline, receive));
                    Ok(Outcome::Held(
                        message,
                        CompletionGuard {
                            release: Some(release),
                            _lease: lease,
                        },
                    ))
                }
                None => Ok(Outcome::Message(message)),
            },
        };
        shared.finish(token, result);
        if let Some((deadline, receive)) = acknowledgment {
            // Generic ordered handoff, never a domain callback. Deadline
            // and outer driver-stop selection bound abandoned validation.
            let _ = timeout_at(deadline, receive)
                .await
                .map_err(|_| Error::Timeout)?;
        }
    }
    Ok(())
}

impl engine::Driver for Shared {
    type Policy = ChannelState;

    fn core(&self) -> &Arc<engine::Core<ChannelState>> {
        &self.core
    }

    async fn receive(
        self: Arc<Self>,
        message: Message,
        props: Properties,
        received_at: Instant,
    ) -> Result<()> {
        receive_message(message, props, received_at, &self).await
    }

    async fn maintain(self: Arc<Self>) {
        expire_loop(&self).await;
    }
}

async fn expire_loop(shared: &Arc<Shared>) {
    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (deadline, did_expire) = {
            let mut inner = shared.inner.lock().unwrap();
            if inner.transport.terminal.is_some() {
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
                inner.settle(token, Err(Error::Timeout), now);
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
