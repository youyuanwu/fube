//! **Declarations only: not implemented or wired to any existing transport.**
//!
//! This proposed actor+action framework is distinct from the operational
//! actor-only [`native_demux`](crate::native_demux). Its routing-header uniqueness
//! and handler/executor contracts do not govern ordinary native RPC completion.
//!
//! An axum-inspired composition target for native actor/action handlers and
//! shared state **over** the [`channel`](crate::channel) foundation, not HTTP
//! routing, an `IFabricTransport` port, an SF ABI, or an operational framework.
//! No constructor, registration storage, matching, dispatch, closure adapter,
//! codec, executor or lifecycle implementation is supplied. Existing
//! [`Connection`](crate::connection::Connection) behavior/security/roles and
//! client/runtime/data protocol schemas remain unchanged.
//!
//! # Native evidence, not Rust guarantees
//!
//! Links pin locally inspected Service Fabric revision
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; this is source evidence, not
//! executed interoperability qualification.
//!
//! - [Actor.h:14–134][actors] defines numeric header routing values, including
//!   test actors above 16 bits. Actors are not destinations, channel instances,
//!   SF Reliable Actors or security principals.
//! - [DemuxerT.h:17–29][callbacks] registers **one callback per actor**, with
//!   message/context unique-pointer references and a transport-thread flag.
//!   [Registration:139–185][registration] asserts on duplicates and silently
//!   returns when closed; it is mutable, not a fallible build/ready API.
//! - [Ingress:188–227][ingress] sends empty-RelatesTo, non-special messages to
//!   actor dispatch, without testing ExpectsReply. Ordinary completions go to
//!   the reply handler, never actor fallback. [Dispatch:230–278][dispatch]
//!   invokes inline or clones the message/moves the context onto a thread pool.
//!   Unknown actors log/drop; Action is diagnostic, not a lookup key.
//! - [Demuxer.cpp:16–24][context] constructs a receiver context even for
//!   ordinary one-way actor ingress. [ReceiverContext.cpp:25–49][reply] has
//!   reusable **const** Reply, captured MessageId/target/transport and explicitly
//!   installed remaining timeout/default expiration, not automatically
//!   inherited inbound timeout. The Rust consuming capability is stronger.
//! - [DuplexRequestReply.cpp:35–77][notifications] dispatches special
//!   IsUncorrelatedReply notifications separately, with context based on the
//!   notification's own MessageId. This is neither arbitrary reverse RPC nor a
//!   rule that all notifications are fire-and-forget.
//! - [ClientServerRequestMessage.h:28–38,59–106][domain] binds actor/action/body
//!   in domain messages. [RequestTable.cpp:46–70][correlation] handles RelatesTo
//!   and native Fault; [ClientConnectionManager.cpp:150–199][domain-errors]
//!   interprets client domain failure action/body above transport completion.
//!   No generic protocol/codec abstraction per actor is needed.
//! - [TCP controls:2181–2195][controls] bypass external ingress; security/target
//!   operations are [separate transport concerns:26–73][security].
//!   [Replication ACKs:168–200][acks] are protocol progress, not ordinary RPC
//!   completion. Registration grants no sender permission or reply authority.
//! - [Demux cleanup:62–119][cleanup] clears the map/reply handler; callbacks
//!   already copied out of the map can still run. It is not a drain barrier.
//!
//! # Proposed Rust-profile contracts
//!
//! ```text
//! configured validated MessageReceiver -> unchanged IncomingMessage
//!   -> immutable Router / Registry (exact actor + action)
//!   -> Handler + Arc<State> (domain interpretation)
//!   -> explicit optional consuming ReplyContext, owned/sent by handler
//! RegistryBuilder -> fallible registration -> consuming build(state) -> Router
//! ```
//!
//! The fluent `.route(...).with_state(...)` style is an ergonomic target, not a
//! working facade here. Trait declarations express exclusive construction and
//! immutable ready capabilities without manufacturing constructors.
//!
//! ## Trust and matching
//!
//! Dispatch is a **trusted handoff**: the caller transfers the unchanged
//! envelope from the router's configured validated
//! [`MessageReceiver`](crate::channel::MessageReceiver), preserving the
//! message/source/reply association. Public fields, equal Target types,
//! ExpectsReply, actor/action and reply presence cannot prove provenance,
//! instance affinity or protocol authorization. A future implementation checks
//! current eligibility/affinity and rejects known violations. Exposing this API
//! to untrusted assembled envelopes requires additional configured authority to
//! validate that association; absent that authority it must fail closed, not
//! pretend field-shape checks establish trust. No validator/wiring is supplied.
//!
//! Require exactly one valid Actor and Action header, decoded under the
//! existing wire profile, before matching [`RouteKey`]. Missing, duplicate or
//! malformed routing headers are [`RoutingError::MalformedMessage`]; a decoded
//! invalid route is [`RoutingError::InvalidRoute`]. Do not use the existing
//! last-visible [`Message::properties`](crate::message::Message::properties)
//! behavior as a uniqueness validator.
//! Transport controls and ordinary correlated completions are ineligible even
//! if their identifiers are registered; their progress must remain independent
//! of full application admission. Authorized special notifications/reverse
//! requests require the configured lower-layer protocol policy, not a generic
//! interpretation of reply presence or IsUncorrelatedReply alone.
//!
//! Exact actor/action matches only: no wildcard, case folding, Unicode
//! normalization, whitespace trimming, prefix lookup or fallback. Unknown
//! actors/actions return explicit local errors, unlike native log/drop.
//! Rejection never silently succeeds, manufactures a domain fault/ACK, or
//! widens existing roles. Protocol handlers own body/action/domain semantics;
//! ordinary RPC correlation and source/role validation remain below.
//!
//! ## Ownership, bounds and progress
//!
//! Calling dispatch moves ingress into a lazy future, with no admission,
//! handler invocation or I/O until poll. Acceptance transfers it once into
//! router-owned handler work, only after eligibility/matching and finite
//! reservations. Before acceptance, rejection/expiry/drop disposes the envelope
//! and unused reply capability, invokes/sends nothing and releases reservations.
//! Rejection is an explicit local result, not a wire response; callers receive
//! no recovered envelope. Unknown actor/action and malformed input never become
//! successful no-reply outcomes.
//!
//! [`RoutingLimits`] bounds router-controlled retention. Reservations, accepted
//! work and completed-but-undelivered results remain charged until disposal or
//! transfer to the caller; no hidden unbounded queue. Handler calls may overlap,
//! including calls to the same handler; no implicit serialization, ordering or
//! fairness. Application synchronization belongs to shared thread-safe state.
//! Handler construction, polls and destruction must be bounded/nonblocking and
//! cooperative. Handler-owned allocations need their own documented finite
//! policy; child work must stay in the same finite owned scope, not detach.
//! Send/Sync/static alone do not establish any of these progress guarantees.
//!
//! One absolute [`Instant`] covers admission, handler work and a chosen reply's
//! complete local-write boundary. No stage renews the budget. Expiry/close races
//! settle once. After acceptance, timeout or observer drop is local abandonment,
//! not remote cancellation, rollback or proof of nonexecution; retire ownership
//! safely and dispose/join owned work without retaining unobserved results
//! forever. Cancellation of cooperative handler work may drop its future and
//! unused context, but cannot undo prior effects. Deadlines cannot preempt
//! arbitrary blocking application code. A reply already accepted by its channel
//! follows channel retirement rules: never truncate a started frame, implicitly
//! retry/replay/reconnect, or claim exactly-once remote execution.
//!
//! ## Separate lifecycle ownership
//!
//! A live router owns only its handler/task scope, not the whole shared channel
//! backend. Every strong handle retaining its live allocation, including
//! `Arc<dyn Registry>` upcasts, is a lifecycle owner: a changed trait view does
//! not detach ownership. Only independently retained immutable registry data is
//! inert. Last-owner abandonment initiates terminal scope cleanup; Drop need
//! not join. State/handlers/source handles must not form live-router ownership
//! cycles or retain its worker tasks after terminal close. Inert state/identity
//! may outlive work. Do not implicitly adopt/close unrelated backend owners;
//! caller-owned envelopes were already outside backend ingress accounting,
//! and become router-accounted only while retained by dispatch.
//!
//! ## Compile-only consumers
//!
//! These uncalled functions typecheck erased construction/state/handler/dispatch
//! and ownership. They are not mock implementations or operational examples,
//! and do not establish runtime admission, authorization, deadlines or disposal.
//!
//! ```
//! use fube_transport::{
//!     channel::{ChannelFuture, IncomingMessage},
//!     routing::{
//!         DispatchError, DispatchFuture, Handler, HandlerError, HandlerFuture,
//!         HandlerOutcome, RegistrationError, Registry, RegistryBuilder,
//!         RouteKey, SharedHandler, SharedRouter,
//!     },
//! };
//! use std::sync::Arc;
//! use tokio::time::Instant;
//!
//! fn build<T, S, E>(
//!     mut builder: Box<dyn RegistryBuilder<Target = T, State = S, Error = E>>,
//!     route: RouteKey, handler: SharedHandler<T, S, E>, state: Arc<S>,
//! ) -> Result<SharedRouter<T, S, E>, RegistrationError>
//! where T: Send + Sync + 'static, S: Send + Sync + 'static, E: Send + 'static {
//!     let _limits = builder.limits();
//!     builder.register(route, handler)?;
//!     builder.build(state) // consumes construction authority
//! }
//!
//! fn inspect<T, S, E>(router: SharedRouter<T, S, E>, route: &RouteKey)
//! where T: Send + Sync + 'static, S: Send + Sync + 'static, E: Send + 'static {
//!     let registry: Arc<dyn Registry<Target = T, State = S, Error = E>> =
//!         router.clone(); // upcast retains the LIVE router allocation/ownership
//!     let _handler = registry.lookup(route);
//!     drop(router); // registry is still a lifecycle owner, not an inert snapshot
//! }
//!
//! fn handler_call<T, S, E>(
//!     handler: &dyn Handler<Target = T, State = S, Error = E>,
//!     state: Arc<S>, incoming: IncomingMessage<T>, deadline: Instant,
//! ) -> HandlerFuture<E>
//! where T: Send + Sync + 'static, S: Send + Sync + 'static, E: Send + 'static {
//!     // Direct handler calls are caller-owned work, not registry authorization.
//!     handler.handle(state, incoming, deadline)
//! }
//!
//! async fn dispatch<T, S, E>(
//!     router: SharedRouter<T, S, E>, incoming: IncomingMessage<T>, deadline: Instant,
//! ) -> (Result<HandlerOutcome, DispatchError<E>>, fube_transport::Result<()>)
//! where T: Send + Sync + 'static, S: Send + Sync + 'static, E: Send + 'static {
//!     // Precondition: unchanged trusted handoff from the configured receiver.
//!     let pending: DispatchFuture<'_, E> = router.dispatch(incoming, deadline);
//!     let outcome = pending.await;
//!     let closing: ChannelFuture<'_, ()> = router.close(deadline);
//!     let close_result = closing.await;
//!     let interpreted = match outcome {
//!         Ok(HandlerOutcome::NoReply) => Ok(HandlerOutcome::NoReply),
//!         Ok(HandlerOutcome::ReplySent) => Ok(HandlerOutcome::ReplySent),
//!         Err(DispatchError::Routing(error)) => Err(DispatchError::Routing(error)),
//!         Err(DispatchError::Handler(HandlerError::Domain(error))) =>
//!             Err(DispatchError::Handler(HandlerError::Domain(error))),
//!         Err(DispatchError::Handler(HandlerError::Reply(error))) =>
//!             Err(DispatchError::Handler(HandlerError::Reply(error))),
//!     };
//!     (interpreted, close_result) // dispatch and disposal are separate results
//! }
//!
//! fn require_send<F: Send>(future: F) -> F { future }
//! fn own_handler_future<E: Send + 'static>(future: HandlerFuture<E>) {
//!     let _: HandlerFuture<E> = require_send(future); // owned 'static Send work
//! }
//! ```
//!
//! [actors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L14-L134
//! [callbacks]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L17-L29
//! [registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L139-L185
//! [ingress]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L188-L227
//! [dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L230-L278
//! [context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Demuxer.cpp#L16-L24
//! [reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L25-L49
//! [notifications]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77
//! [domain]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerRequestMessage.h#L28-L106
//! [correlation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L70
//! [domain-errors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L150-L199
//! [controls]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2181-L2195
//! [security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L26-L73
//! [acks]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L168-L200
//! [cleanup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L62-L119

use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc};
use tokio::time::Instant;

use crate::{
    channel::{ChannelFuture, IncomingMessage},
    Error,
};

/// Application route actor in the existing [`Message`](crate::message::Message)
/// header profile (`i64`), not a header identifier (`u16`) or destination.
///
/// **Rust policy:** positive values only for routes, including native test
/// actors and unknown positive values; zero/negative are invalid. This is not a
/// native production-enum range check or permission to dispatch controls.
/// **Native fact:** [Actor.h:118–134](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L118-L134)
/// includes production markers and test actors `0xFFFF` and `0x10000+`.
pub type ActorId = i64;

/// Owned exact actor/action key; fields do not confer validity or authority.
///
/// **Rust policy:** validate actor and nonempty action at registration/dispatch;
/// compare action UTF-8 bytes exactly, under [`RoutingLimits::max_action_bytes`].
/// No trimming, case folding, normalization, wildcard or fallback matching.
/// This pair is a Rust convenience: native
/// [DemuxerT.h:253–278](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L253-L278)
/// looks up actor only, with action used for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteKey {
    /// Native header routing number, not source identity.
    pub actor: ActorId,
    /// Exact native action text under the existing Rust string profile.
    pub action: String,
}

/// Finite immutable admission profile fixed before exposing a builder.
///
/// **Rust policy, not native callback guarantees.** Check all accounting
/// arithmetic; overflow/inability is a resource-limit error. Registered action
/// allocation capacities together must not exceed `max_routes * max_action_bytes`:
/// charge retained capacity or compact/copy into bounded storage and release
/// oversized supplied allocations. No registration implementation is supplied.
///
/// Byte accounting below covers retained ingress, not arbitrary application
/// state, handler captures/future/domain-error allocations or reply-channel
/// queues. Those owners must document separate finite policies; router task and
/// result counts remain charged regardless of those byte-accounting boundaries.
/// Implementations also document finite per-route/task/accounting overhead and
/// bounded temporary header decoding. No hidden unbounded admission queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingLimits {
    /// Maximum distinct bindings, including all registered actors/actions.
    pub max_routes: NonZeroUsize,
    /// Maximum action UTF-8 byte length and per-key retained allocation budget.
    pub max_action_bytes: NonZeroUsize,
    /// Maximum outstanding dispatch reservations/work/undelivered results.
    ///
    /// Charge from reservation until disposal/result transfer, not just while
    /// the handler is polling. Pre-acceptance waiters must also be bounded.
    pub max_in_flight: NonZeroUsize,
    /// Aggregate retained ingress allocation budget, including reservations.
    ///
    /// Charge body capacity, header-vector capacity times Header size and each
    /// header-value capacity with checked arithmetic; lengths are insufficient.
    /// Charge any router-retained copies likewise until retired/transferred.
    /// Caller-owned envelopes outside dispatch are not charged to this router.
    pub max_retained_bytes: NonZeroUsize,
}

/// Explicit successful handler completion, not generic domain success.
///
/// The handler is the sole owner/sender of the original optional reply
/// capability; the router never sends a response. Presence of that capability
/// does not classify the protocol as ordinary RPC, or require a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerOutcome {
    /// No consuming reply attempt occurred; dropping unused authority sent nothing.
    ///
    /// Valid only when the domain protocol intentionally requires no correlated
    /// reply. Raw one-way/semantic ACK work is separate and must not be forced
    /// into ReplyContext; this outcome makes no claim about that work.
    /// Never use it as success fallback for a missing required reply capability.
    NoReply,
    /// The single selected ReplyContext attempt completed its full local write.
    ///
    /// Only return after awaiting successful reply completion. Domain-error
    /// bodies are still domain failures; local write is not remote receipt,
    /// processing, ACK, durability or exactly-once execution.
    ReplySent,
}

/// Handler-local failures; never automatically encoded as wire responses.
#[derive(Debug)]
pub enum HandlerError<E> {
    /// Domain validation/processing failure before a reply attempt.
    ///
    /// Includes a missing required reply capability as a domain decision.
    /// A protocol handler may instead explicitly encode/send its domain-error
    /// response and report ReplySent; the router knows no domain fault schema.
    Domain(E),
    /// Failure of the explicit single consuming reply attempt.
    ///
    /// Preserve the original transport error; timeout/drop/failure after
    /// channel acceptance may leave the remote outcome uncertain.
    Reply(Error),
}

/// Owned non-borrowing Send handler work with a domain-specific failure family.
///
/// Lazy like [`ChannelFuture`], but reports [`HandlerError`] and the explicit
/// reply outcome rather than conflating domain failure with transport success.
pub type HandlerFuture<E> =
    Pin<Box<dyn Future<Output = Result<HandlerOutcome, HandlerError<E>>> + Send + 'static>>;

/// Shared erased handler binding, with explicit foundation/state/error types.
pub type SharedHandler<T, S, E> = Arc<dyn Handler<Target = T, State = S, Error = E>>;

/// Protocol handler with shared application state and owned authorized ingress.
///
/// **Rust policy:** shared calls may overlap without ordering/serialization or
/// fairness; State is responsible for synchronization. Invocation, polls and
/// destruction must be bounded/nonblocking/cooperative, with finite allocations
/// and no detached router-owned work. Direct calls through a looked-up handler
/// are caller-owned work, not registry authorization or router admission.
///
/// **Native distinction:** [DemuxerT.h:17–29,230–251](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L17-L251)
/// has synchronous callback pointer references and inline/thread-pool selection,
/// not async state injection, this future or a bounded cooperative policy.
pub trait Handler: Send + Sync + 'static {
    /// Paired receiver's source type; type equality is not foundation affinity.
    type Target: Send + Sync + 'static;
    /// Shared state retained by the ready router and cloned into accepted calls.
    ///
    /// Unit state is valid. State/handler captures must not own live-router
    /// cycles or detach worker scopes; retaining inert data is allowed.
    type State: Send + Sync + 'static;
    /// Opaque domain error, interpreted by the consumer, not a generic codec.
    type Error: Send + 'static;

    /// Move ingress/state into one lazy owned future under the original deadline.
    ///
    /// The returned future cannot borrow `self`; own/clone needed dependencies.
    /// The original message/source/reply transfer together; retain them only
    /// under finite handler/router policies. Validate domain action/body and
    /// semantic ACK rules here, without changing generic RPC correlation.
    ///
    /// Handler alone may consume the original reply capability once and await
    /// it with this same deadline (or an earlier one), never a renewed budget.
    /// Do not substitute a context/source, manufacture authority, synthesize an
    /// automatic reply, or send twice. Domain errors before that attempt use
    /// Domain; attempted reply failures use Reply. The channel owns correlation
    /// headers/attempt retirement and rejects contradictory reply envelopes.
    ///
    /// The original envelope's authority cannot be copied/reused:
    ///
    /// ```compile_fail,E0382
    /// use fube_transport::{channel::IncomingMessage, message::Message};
    /// use tokio::time::Instant;
    /// fn reuse(incoming: IncomingMessage<()>, response: Message, deadline: Instant) {
    ///     if let Some(context) = incoming.reply {
    ///         let _attempt = context.reply(response, deadline);
    ///         let _again = context; // reply consumed the original erased box
    ///     }
    /// }
    /// ```
    fn handle(
        &self,
        state: Arc<Self::State>,
        incoming: IncomingMessage<Self::Target>,
        deadline: Instant,
    ) -> HandlerFuture<Self::Error>;
}

/// Atomic registration/finalization failures, with no live-router side effects.
///
/// These returned errors are Rust policy, **not** native duplicate behavior:
/// [DemuxerT.h:139–185](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L139-L185)
/// asserts on duplicate actor registration and returns silently when closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationError {
    /// Route actor is zero/negative under this Rust profile.
    InvalidActor,
    /// Action is empty or exceeds the configured UTF-8 byte length.
    InvalidAction,
    /// Exact actor/action already registered; no replacement occurs.
    DuplicateRoute,
    /// Count/allocation bound, checked arithmetic or allocation cannot be met.
    ResourceLimit,
}

/// Consuming construction result: one shared ready router or explicit failure.
pub type BuildResult<T, S, E> = Result<SharedRouter<T, S, E>, RegistrationError>;

/// Exclusive construction authority; no constructors or working fluent facade.
///
/// Bounds and trusted receiver/foundation association are fixed by a future
/// implementation before this capability is exposed. Registration is local,
/// never network I/O, handler invocation, sender authorization or worker start.
/// No mutation/unregistration API exists on the immutable ready registry.
pub trait RegistryBuilder: Send + 'static {
    /// Source type of the configured validated foundation.
    type Target: Send + Sync + 'static;
    /// State type supplied once on finalization and shared with every handler.
    type State: Send + Sync + 'static;
    /// Common domain-error family of the registry's handler bindings.
    type Error: Send + 'static;

    /// Fixed finite profile; the built registry/router retains these same limits.
    fn limits(&self) -> RoutingLimits;

    /// Consume a key/handler and atomically add one exclusive exact binding.
    ///
    /// Validate the profile, duplicate and count/allocation bounds. Success
    /// retains bounded key storage and the handler; failure leaves every prior
    /// binding/limit unchanged and releases only the supplied key/handler
    /// ownership (other caller Arc clones are unaffected). No input recovery,
    /// replacement, partial insertion or authorization side effect.
    fn register(
        &mut self,
        route: RouteKey,
        handler: SharedHandler<Self::Target, Self::State, Self::Error>,
    ) -> Result<(), RegistrationError>;

    /// Consume construction authority and transfer immutable bindings/shared state.
    ///
    /// Empty registries are valid: all eligible routes then reject explicitly.
    /// On success, publish one ready router capability with fixed bounds and
    /// state; no dispatch/receive task or network I/O starts just by building.
    /// On failure, no ready object escapes: release all builder-owned bindings
    /// and the supplied state Arc, without affecting external clones. No retry
    /// through the consumed builder; ready capabilities have no registration API.
    ///
    /// ```compile_fail,E0382
    /// use fube_transport::routing::RegistryBuilder;
    /// use std::sync::Arc;
    /// fn consumed(builder: Box<dyn RegistryBuilder<Target=(), State=(), Error=()>>) {
    ///     let _ready = builder.build(Arc::new(()));
    ///     let _reuse = builder.limits(); // build consumed the erased box
    /// }
    /// ```
    fn build(
        self: Box<Self>,
        state: Arc<Self::State>,
    ) -> BuildResult<Self::Target, Self::State, Self::Error>;
}

/// Immutable exact route bindings; inspection does not authorize dispatch.
///
/// A view of a live router allocation retains lifecycle ownership, including
/// Arc upcasts. An independently stored registry can be inert; this trait alone
/// does not imply a detached snapshot or weaken the live router's owner count.
pub trait Registry: Send + Sync + 'static {
    /// Paired foundation source type.
    type Target: Send + Sync + 'static;
    /// Common thread-safe application state type.
    type State: Send + Sync + 'static;
    /// Common opaque domain-error family.
    type Error: Send + 'static;

    /// Immutable profile identical to the finalized builder's limits.
    fn limits(&self) -> RoutingLimits;

    /// Inspect an exact binding without admission, handler invocation or I/O.
    ///
    /// Invalid/missing keys return None; no fallback. A returned handler Arc
    /// retains that handler's data, not authorized ingress or router-owned
    /// tasks. Direct invocation belongs to the caller's own bounded scope.
    fn lookup(
        &self,
        route: &RouteKey,
    ) -> Option<SharedHandler<Self::Target, Self::State, Self::Error>>;
}

/// Explicit local routing/admission failure; no automatic wire fault or ACK.
#[derive(Debug)]
pub enum RoutingError {
    /// Missing/duplicate/malformed Actor or Action routing header.
    MalformedMessage,
    /// Decoded actor/action violates the configured Rust route profile.
    InvalidRoute,
    /// Eligible actor has no registered bindings; test/unknown positives are allowed keys.
    UnknownActor(ActorId),
    /// Actor exists, but this exact action has no binding; key is profile-bounded.
    UnknownAction(RouteKey),
    /// Control/completion, known trust/affinity/role violation or unvalidated ingress.
    IneligibleIngress,
    /// Finite reservation/count/byte/allocation bound could not be met.
    ResourceLimit,
    /// Whole-operation deadline expired, before or after acceptance.
    Timeout,
    /// Router scope is terminal; this does not imply the channel backend closed.
    Closed,
}

/// Separate router/admission and handler/domain/explicit-reply failure layers.
///
/// Timeout/close can race accepted work; this error is not proof of absence of
/// handler effects or remote processing. Errors never become NoReply success.
#[derive(Debug)]
pub enum DispatchError<E> {
    /// Local routing, admission, deadline or router-lifecycle outcome.
    Routing(RoutingError),
    /// Handler's domain failure or explicit consuming reply transport failure.
    Handler(HandlerError<E>),
}

/// Lazy Send dispatch observation; the accepted work belongs to the router scope.
pub type DispatchFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<HandlerOutcome, DispatchError<E>>> + Send + 'a>>;

/// Shared erased ready capability; every live-allocation strong handle is an owner.
pub type SharedRouter<T, S, E> = Arc<dyn Router<Target = T, State = S, Error = E>>;

/// Immutable registry plus bounded, deadline-bound application dispatch scope.
///
/// **Proposed Rust policy, not implemented.** See the module for trusted handoff,
/// completion/control bypass, exact matching, accounting and ownership. Sharing
/// this capability does not adopt/close unrelated backend owners. Handler/state
/// allocations are separately bounded; no hidden executor/task framework exists.
pub trait Router: Registry {
    /// Transfer unchanged trusted ingress into lazy dispatch observation.
    ///
    /// Enforce eligibility/unique routing/profile/matching and reservations
    /// before acceptance, then own/poll one handler future with the built shared
    /// state and original deadline. No route registration grants authority.
    /// Exhaustion rejects ResourceLimit or backpressures within the same
    /// deadline and finite waiter limits. Rejection disposes ingress/unused
    /// context with explicit local error; never silent success or domain reply.
    ///
    /// Successful handler outcome is forwarded unchanged; router sends nothing.
    /// After acceptance, observer abandonment is local only: work/effects may
    /// already have occurred. Retire/join work and undelivered result ownership
    /// under the module's finite/cooperative rules; no implicit replay or ACK.
    fn dispatch(
        &self,
        incoming: IncomingMessage<Self::Target>,
        deadline: Instant,
    ) -> DispatchFuture<'_, Self::Error>;

    /// On first poll, close new admission and await disposal of owned task scope.
    ///
    /// Close retires reservations/undelivered results, stops/disposes/joins owned
    /// handler and child work, and succeeds without waiting for inert state,
    /// registry data, sources or unused reply contexts to be dropped. Accepted
    /// channel writes retire under channel rules, never truncate a started frame.
    /// It does not close the shared MessageChannel or other owners' direct work.
    ///
    /// Concurrent/repeated calls share terminal state. Timeout/drop after
    /// initiation does not undo close or prove disposal; a later close can wait
    /// again. A deadline bounds observation, not arbitrary application-code
    /// preemption. Last live-allocation owner abandonment initiates cleanup;
    /// Drop need not join. Retained live handles remain closed after success.
    ///
    /// **Native distinction:** [DemuxerT.h:62–119,230–278](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L62-L278)
    /// clears registrations but copied/posted callbacks can still execute;
    /// that cleanup is not this stronger owned-scope disposal contract.
    fn close(&self, deadline: Instant) -> ChannelFuture<'_, ()>;
}
