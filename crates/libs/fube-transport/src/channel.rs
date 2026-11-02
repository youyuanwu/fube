//! **Operational for the explicit native loopback composition in
//! [`native_channel`](crate::native_channel).**
//!
//! Bounded Rust-profile contracts for a shared native message/RPC
//! foundation, **not** an `IFabricTransport` application-service API or an SF ABI.
//! [`GatewayChannel`](crate::native_channel::GatewayChannel) implements raw/RPC
//! and owned shutdown; its configured receiver and consuming context implement
//! authorized actor ingress, distinct normal-request and special duplex replies.
//! Client/runtime adapters install
//! domain actor/action/envelope rules through [`native_demux`](crate::native_demux).
//! Actor registration alone grants neither outbound nor reply authority.
//! IPC identity is an explicit clear-loopback opt-in, not a general listener,
//! replication or secure transport implementation. Legacy Connection is retired;
//! runtime domain policy remains above this foundation.
//!
//! # Native component boundary (source facts)
//!
//! All native links below pin locally inspected Service Fabric revision
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; they are source evidence, not
//! executed interoperability qualification.
//!
//! - The abstract [ClientServerTransport][client-interface] separates domain
//!   wrappers from transport. [ClientServerTcpTransport][client-components]
//!   composes a TCP datagram transport, Demuxer and DuplexRequestReply.
//!   "Datagram" here does not mean UDP.
//! - [IDatagramTransport][raw] resolves targets and sends owned messages.
//!   [RequestReply][request] adds ordinary correlation above raw delivery.
//!   [IPC][ipc] also composes demux, request/reply and message transport.
//! - [Demuxer][demux] sends empty-RelatesTo, non-special messages to actor
//!   handlers, and other messages to the reply handler. That actor branch
//!   includes one-way ingress; it does not test ExpectsReply.
//!   [TCP transport controls][controls] are handled below external ingress.
//! - [DuplexRequestReply][duplex] treats IsUncorrelatedReply notifications
//!   separately, with a context based on the notification's own MessageId.
//!   This is not arbitrary application reverse RPC.
//! - [ReplicationTransport][replication] uses raw SendOneWay;
//!   [replication ACKs][ack] carry protocol progress and a fresh MessageId, not
//!   ordinary RelatesTo request-table replies.
//!
//! # Rust-profile component contracts (not native guarantees)
//!
//! ```text
//! protocol actor/action/body, domain validation, semantic ACKs
//!   -> RpcChannel (ordinary correlation, when needed)
//!   -> MessageChannel (raw outbound delivery and owned shutdown)
//! inbound authorized dispatch -> MessageReceiver -> IncomingMessage
//!                                               -> optional ReplyContext
//! ```
//!
//! Each backend configures security, role permissions, dispatch and finite
//! resource limits before exposing ready capabilities. The operational gateway
//! constructor/limits live in [`native_channel`](crate::native_channel), not in
//! these generic traits. Raw-only protocols need not implement [`RpcChannel`].
//! Components sharing a foundation bind the same backend [`MessageChannel::Target`]
//! type, but type equality does **not** establish instance affinity or authority.
//! Implementations must reject foreign/retired targets before admission and
//! must not widen existing concrete connection roles.
//!
//! ## Ownership, progress and deadlines
//!
//! All operation futures are lazy: calling transfers owned inputs into the
//! future but performs no admission or I/O; polling begins progress. Before
//! acceptance, expiry/drop sends nothing and releases reservations. After
//! acceptance, dropping the observer is local abandonment, not remote
//! cancellation, rollback or proof of nonexecution. Retire local correlation
//! and resource ownership safely; never truncate a started frame. Retained
//! backend work, reservations and undelivered results count toward documented
//! finite limits until retired or transferred to the caller.
//!
//! A Tokio [`Instant`] deadline covers admission and the operation's local
//! completion boundary; do not restart its budget at each stage. An expired
//! operation must not be admitted. Expiry returns [`Error::Timeout`](crate::Error::Timeout); terminal
//! shutdown returns [`Error::Closed`](crate::Error::Closed); capacity exhaustion may reject with
//! [`Error::ResourceLimit`](crate::Error::ResourceLimit) or backpressure within that same deadline.
//! Invalid input and malformed wire envelopes remain distinct
//! ([`Error::InvalidInput`](crate::Error::InvalidInput) and [`Error::Protocol`](crate::Error::Protocol)). No new error mapping,
//! cancellation signal, automatic retry/reconnect/replay or exactly-once remote
//! execution is supplied or promised. Implementations must settle local
//! completion races once, including deadline, disconnect and close races.
//!
//! ## Declaration usability
//!
//! This uncalled consumer checks object safety, target binding, owned ingress,
//! a consuming erased reply and its independent Send future. It is **not** a
//! backend implementation or an operational example. Arguments must be paired
//! capabilities of the same ready foundation; their equal Target types alone
//! cannot prove that.
//!
//! ```
//! use fube_transport::{
//!     channel::{ChannelFuture, MessageChannel, MessageReceiver, ReplyContext, RpcChannel},
//!     message::Message, Result,
//! };
//! use std::sync::Arc;
//! use tokio::time::Instant;
//!
//! async fn consume<T: Send + Sync + 'static>(
//!     raw: &dyn MessageChannel<Target = T>,
//!     rpc: &dyn RpcChannel<Target = T>,
//!     ingress: &dyn MessageReceiver<Target = T>,
//!     one_way: Message, request: Message, response: Message, deadline: Instant,
//! ) -> Result<Message> {
//!     let target: Arc<T> = raw.resolve_target("backend-defined-address")?;
//!     raw.send_one_way(target.as_ref(), one_way, deadline).await?;
//!     let result = rpc.request(target.as_ref(), request, deadline).await?;
//!     let incoming = ingress.receive(deadline).await?;
//!     let _source: Arc<T> = incoming.source;
//!     let _message: Message = incoming.message;
//!     if let Some(context) = incoming.reply {
//!         let context: Box<dyn ReplyContext> = context;
//!         let pending: ChannelFuture<'static, ()> = context.reply(response, deadline);
//!         pending.await?;
//!     }
//!     raw.close(deadline).await?;
//!     Ok(result)
//! }
//!
//! fn check_send<'a, T: Send + Sync + 'static>(
//!     rpc: &'a dyn RpcChannel<Target = T>, target: &'a T,
//!     message: Message, deadline: Instant,
//! ) {
//!     fn requires_send<F: Send>(_: F) {}
//!     requires_send(rpc.request(target, message, deadline));
//! }
//! ```
//!
//! [client-interface]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/IClientServerTransport.h#L22-L66
//! [client-components]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L34-L40
//! [raw]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L51-L73
//! [request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L57-L122
//! [ipc]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcClient.h#L75-L83
//! [demux]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L188-L227
//! [controls]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2181-L2196
//! [duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77
//! [replication]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L880-L890
//! [ack]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L168-L200

use std::{future::Future, pin::Pin, sync::Arc};
use tokio::time::Instant;

use crate::{message::Message, Result};

/// Object-safe Send future returning the existing transport [`Result`].
///
/// **Rust-profile contract.** The lifetime permits
/// borrowing a channel/target; consuming reply futures instead own their state
/// and use `'static`. See the [module](self) for lazy progress, acceptance,
/// cancellation, finite resource accounting and deadline obligations.
pub type ChannelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Raw outbound message delivery and lifecycle ownership, independent of RPC.
///
/// **Rust-profile contract, implemented by the configured GatewayChannel.** Concurrent calls
/// through shared references are supported; no cross-target ordering/fairness
/// guarantee is imposed. A backend must document its per-target ordering,
/// affinity, finite admission/byte limits and authorized roles.
///
/// **Native facts:** [IDatagramTransport.h:39–73](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L39-L73)
/// separates receive handlers, target resolution and raw SendOneWay;
/// [ISendTarget.h:10–38](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ISendTarget.h#L10-L38)
/// represents a shared resolved destination, not an application actor.
pub trait MessageChannel: Send + Sync + 'static {
    /// Backend-owned destination identity/state, separate from message routing.
    ///
    /// A retained target is not proof of a live connection or permission to
    /// reopen a closed foundation. Equal types do not prove instance affinity.
    /// Implementations document validity and reject foreign/retired targets.
    type Target: Send + Sync + 'static;

    /// Resolve a backend-defined address to shared target identity/state.
    ///
    /// **Rust-profile contract:** synchronous, no connection establishment or
    /// network wait required; actual readiness/connection failures can occur on
    /// send. Resolution grants no security or role privilege. Reject invalid
    /// addresses, resource exhaustion or terminal closure using existing errors.
    /// Address syntax and target reuse/identity policy are backend-defined.
    ///
    /// **Native fact:** [IDatagramTransport.h:51–62](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L51-L62)
    /// has synchronous ResolveTarget with address and optional target metadata;
    /// this declaration does not import those settings or promise a connection.
    fn resolve_target(&self, address: &str) -> Result<Arc<Self::Target>>;

    /// Transfer one owned raw message for deadline-bound local delivery.
    ///
    /// **Rust-profile contract:** preserve protocol headers/body; do not add
    /// ordinary RPC correlation or infer a protocol ACK. Enforce the configured
    /// role before admission; this is not an adapter for the retired legacy
    /// IPC-only envelope-building API.
    /// Success means the complete local message write finished, not that the
    /// peer received, decoded, processed, acknowledged or durably stored it.
    /// Failure/timeout after writing began may have an uncertain remote outcome.
    /// See the [module](self) for lazy progress, ownership and cancellation.
    ///
    /// **Native distinction:** [IDatagramTransport.h:69–73](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L69-L73)
    /// accepts an owned message with expiration, but its synchronous return
    /// [does not report send completion (170–172)](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L170-L172).
    /// The local-write future boundary here is a stronger Rust design rule.
    fn send_one_way<'a>(
        &'a self,
        target: &'a Self::Target,
        message: Message,
        deadline: Instant,
    ) -> ChannelFuture<'a, ()>;

    /// Begin terminal shutdown on first poll and wait for owned work to stop.
    ///
    /// **Rust-profile contract:** close new admission, terminate pending RPCs,
    /// cease ingress delivery, explicitly retire queued/undelivered ownership,
    /// and dispose/join backend-owned work before returning success. Concurrent
    /// or repeated closes share terminal state. A deadline bounds waiting, not
    /// cleanup itself: timeout/drop after initiation does not undo shutdown or
    /// prove disposal; a subsequent close can wait again.
    ///
    /// All capabilities of this owned logical foundation observe closure;
    /// targets, caller-owned envelopes and unused reply contexts cannot keep
    /// backend work running or prevent successful close. A later reply attempt
    /// fails Closed without send. Identify lifecycle-owning handles and initiate
    /// terminal shutdown when the last is abandoned; Drop alone need not join.
    /// Retained capabilities may keep inert identity/state alive. Do not adopt
    /// an externally owned backend or shut down unrelated owners implicitly.
    ///
    /// **Native distinctions:** [IDatagramTransport.h:16–18](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IDatagramTransport.h#L16-L18)
    /// declares Start/CompleteStart/Stop, not this lazy async disposal contract.
    /// [ClientServerTcpTransport.cpp:96–131](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L96-L131)
    /// has different close/abort teardown and skips externally owned transport.
    /// This is neither native Abort nor runtime service disposal, and does not
    /// change runtime service-disposal authority.
    fn close(&self, deadline: Instant) -> ChannelFuture<'_, ()>;
}

/// Ordinary correlated request/reply above a raw [`MessageChannel`].
///
/// **Rust-profile contract, implemented by the configured GatewayChannel.** Correlation,
/// bounded pending ownership and reply completion are distinct from protocol
/// actor dispatch/domain validation. The inherited channel and this RPC
/// capability belong to the same foundation; no automatic Connection adapter is supplied.
///
/// **Native facts:** [requestreply.cpp:57–122](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L57-L122)
/// adds ExpectsReply, generates a missing MessageId, starts/inserts pending work
/// before send and observes send failure.
/// [RequestTable.cpp:46–70](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L70)
/// removes by RelatesTo, stores a reply only on successful native Fault status,
/// and completes the operation; an absent entry cannot complete another ID.
/// [requestreply.cpp:281–286](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L281-L286)
/// explicitly ignores the sender in ordinary matching. Peer/role authorization
/// below is a Rust obligation, **not** a native request-table guarantee.
pub trait RpcChannel: MessageChannel {
    /// Submit an owned ordinary request and await its owned transport reply.
    ///
    /// **Rust-profile contract:** one deadline spans admission/send/reply.
    /// Own ExpectsReply and MessageId: add true/missing unique ID, preserve a
    /// valid supplied ID only if unused and unretired on this live foundation.
    /// Reject duplicate/contradictory correlation, nonempty RelatesTo and
    /// IsUncorrelatedReply before admission. Reserve bounded completion state
    /// before outbound visibility. Validate source/role separately from
    /// RelatesTo; unknown/late replies must never satisfy another operation.
    /// Retired IDs cannot be reused while late traffic could match them;
    /// identity tracking must itself be bounded (retire the channel if needed).
    ///
    /// A well-formed nonzero native Fault status yields [`Error::Remote`](crate::Error::Remote),
    /// preserving its code. Malformed transport envelopes yield
    /// [`Error::Protocol`](crate::Error::Protocol). Otherwise return the opaque Message: success is
    /// transport correlation, **not** domain success, durability or exactly-once
    /// execution. Domain headers/body and optional fault-body diagnostics are
    /// not generically decoded here.
    ///
    /// Actor/action/body and domain TimeoutHeader propagation remain above RPC.
    /// **Native fact:** [ClientConnectionManager.cpp:98–118](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L98-L118)
    /// adds remaining TimeoutHeader upstream; it is not added by the cited
    /// generic RequestReply send path.
    /// [RequestAsyncOperation.cpp:39–51](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestAsyncOperation.cpp#L39-L51)
    /// removes local state on timeout/cancel, not a remote cancellation packet.
    ///
    /// **Rust-profile contract:** timeout/drop does not prove nonexecution; use
    /// the [module](self) ownership rules. Disconnect terminates affected local
    /// pending work; no implicit replay. **Native qualification:**
    /// [requestreply.cpp:193–227](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L193-L227)
    /// fails target-specific requests on disconnect but exempts native session
    /// expiration. [RequestTable.cpp:73–80](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L73-L80)
    /// cancels remaining entries on Close. Neither is this Rust lifecycle.
    fn request<'a>(
        &'a self,
        target: &'a Self::Target,
        message: Message,
        deadline: Instant,
    ) -> ChannelFuture<'a, Message>;
}

/// Pull delivery of authorized non-completion ingress.
///
/// **Rust-profile contract, implemented by the opted-in GatewayReceiver.** A receiver is paired
/// with its foundation's lifecycle owner and source Target type; it need not
/// require ordinary RPC. Concurrent receives each deliver one distinct owned
/// envelope, with no fairness guarantee. Transport controls and, where RPC is
/// composed, ordinary correlated completions remain below application ingress,
/// including when the application ingress queue is full.
///
/// Accept ingress only after authorized validation and finite delivery-resource
/// reservation. Network receipt alone is not acceptance or remote receipt.
/// Document bounded bytes/counts/resources for reservations, queues and
/// completed-but-undelivered backend envelopes. Before acceptance, exhaustion
/// must backpressure or explicitly reject/terminate; accepted ingress must not
/// silently vanish. Shutdown can explicitly retire undelivered work. Delivered
/// envelopes become caller-owned, outside backend-retained accounting.
///
/// **Native distinction:** [DemuxerT.h:188–249](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L188-L249)
/// dispatches actor callbacks on the transport thread or posts them to a pool;
/// this pull interface and its bounded/cancellation rules are Rust design.
pub trait MessageReceiver: Send + Sync + 'static {
    /// Source identity type of the paired foundation, not an application actor.
    ///
    /// Same-type targets still require backend instance-affinity validation.
    type Target: Send + Sync + 'static;

    /// Wait until the deadline for one authorized owned ingress envelope.
    ///
    /// Progress begins on poll. Expiry/drop before delivery must not consume
    /// or lose an envelope: preserve/requeue undelivered ownership and retire
    /// reservations. Ready transfers ownership once to the caller; abandonment
    /// thereafter sends no implicit reply/ACK. Terminal closure yields Closed.
    /// A reply capability is granted only by configured, validated protocol
    /// policy, not inferred solely from actor, ExpectsReply or source address.
    fn receive(&self, deadline: Instant) -> ChannelFuture<'_, IncomingMessage<Self::Target>>;
}

/// Owned ingress plus source identity and optional authorized reply capability.
///
/// **Rust-profile contract.** Raw one-way messages,
/// authorized reverse requests and special notifications can be represented
/// without inventing another wire format. Inspect existing Message properties
/// under protocol policy; presence of a context is permission for one reply
/// attempt, not a domain success claim. No public fields mint backend authority:
/// implementations must validate that authority at send admission.
///
/// **Native facts:** [Demuxer.cpp:16–24](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Demuxer.cpp#L16-L24)
/// builds ReceiverContext from MessageId, sender target and transport without
/// testing ExpectsReply. [DuplexRequestReply.cpp:35–77](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77)
/// separately creates a notification context. Neither automatically grants
/// arbitrary reverse-request permission in this bounded Rust profile.
pub struct IncomingMessage<Target: Send + Sync + 'static> {
    /// Original owned wire headers/body; domain decoding remains above ingress.
    pub message: Message,
    /// Monotonic complete-frame receipt time, preserved across queued handoff.
    /// Diagnostic/deadline input only; editing it cannot mint reply authority.
    pub received_at: Instant,
    /// Optional ordered ingress handoff acknowledgment, not a reply grant.
    /// Acknowledge only after the configured bounded handoff; reject/abort
    /// before releasing malformed ingress. Dropping also releases the barrier.
    pub handoff: Option<crate::native_channel::CompletionGuard>,
    /// Shared backend source identity/state, not a live-session guarantee.
    pub source: Arc<Target>,
    /// One authorized reply attempt, absent for ordinary one-way ingress.
    ///
    /// Semantic ACKs remain protocol-defined raw traffic, not this capability.
    /// Dropping an unused context emits no automatic response or cancellation.
    pub reply: Option<Box<dyn ReplyContext>>,
}

/// Consuming capability bound to an original destination and correlation ID.
///
/// **Rust-profile contract, implemented by the private consuming gateway context.** Send permits moving
/// the capability to a worker; Sync and Clone are not required. Consuming one
/// context permits at most one local attempt through that value, not exactly-one
/// remote execution or protection against separately minted backend authority.
///
/// **Native distinction:** [ReceiverContext.h:15–35](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.h#L15-L35)
/// exposes reusable `Reply(...) const` and shared target/transport ownership;
/// [ReceiverContext.cpp:25–39](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L25-L39)
/// adds original RelatesTo and uses explicitly installed remaining timeout or
/// default reply expiration. Native Reply is **not** inherently single-use, nor
/// does generic context creation automatically inherit a request TimeoutHeader.
///
/// The consuming receiver cannot be reused (an uncalled ownership witness):
/// ```compile_fail
/// use fube_transport::{channel::ReplyContext, message::Message};
/// use tokio::time::Instant;
/// fn reuse(context: Box<dyn ReplyContext>, first: Message, second: Message, deadline: Instant) {
///     let pending = context.reply(first, deadline);
///     let another = context.reply(second, deadline); // context was moved
/// }
/// ```
pub trait ReplyContext: Send + 'static {
    /// Consume the authorized reply attempt and its owned response message.
    ///
    /// **Rust-profile contract:** the returned Send `'static` future owns the
    /// context state; no borrowed moved receiver/target. Calling consumes the
    /// attempt even if never polled. Apply the [module](self) lazy admission,
    /// cancellation and deadline rules and [`MessageChannel::send_one_way`]'s
    /// complete-local-write result meaning. No automatic response on drop.
    ///
    /// Retain the original authorized source target and message ID. Own
    /// RelatesTo: add the captured ID if absent; accept one matching value;
    /// reject duplicate/contradictory correlation, ExpectsReply=true and
    /// IsUncorrelatedReply envelopes before admission. Domain actor/action/body
    /// remain caller-owned, subject to the original authorized protocol policy.
    /// The explicit deadline bounds admission/write, not remote processing;
    /// do not assume it came from the inbound TimeoutHeader. A terminal
    /// foundation returns Closed without sending, even for a retained context.
    fn reply(self: Box<Self>, message: Message, deadline: Instant) -> ChannelFuture<'static, ()>;

    /// Consume an attempt with a bounded nonblocking pre-positive-write check.
    ///
    /// This is distinct from envelope authorization. Implementations without
    /// this mechanism reject explicitly rather than silently ignoring a fence.
    fn reply_fenced(
        self: Box<Self>,
        _message: Message,
        _deadline: Instant,
        _valid: crate::native_channel::WritePredicate,
    ) -> ChannelFuture<'static, ()> {
        Box::pin(async { Err(crate::Error::InvalidInput("fenced reply unsupported")) })
    }
}
