//! Operational bounded typed ordinary calls over an explicitly bound RPC channel.
//!
//! Rust-profile contracts for typed ordinary request/reply over Service Fabric
//! native actors/actions and Fabric serialization. Handwritten sealed adapters
//! can resemble a typed gRPC stub conceptually;
//! this is **not** gRPC, protobuf, HTTP/2, an SF ABI, or a public COM-IDL-generated
//! network stub. [`FabricClient`](crate::FabricClient) is the explicit channel
//! owner; [`GatewayClient`](crate::GatewayClient) is a borrowing typed observer.
//! Both implement [`Client`] through the shared call layer. Notification and
//! image-transfer clients and runtime IPC compositions remain independently owned.
//!
//! # Native boundary (source facts)
//!
//! All native links pin locally inspected SF revision
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. Source evidence is not executed
//! interoperability or live qualification.
//!
//! - [ClientServerRequestMessage][request-wrapper] binds actor/action/activity
//!   and a domain body into a native transport message.
//!   [ClientServerReplyMessage][reply-wrapper] owns that message and exposes
//!   headers and typed body reading. These are native message wrappers, not
//!   generated protobuf stubs.
//! - [ClientServerTransport][interface] separates those wrappers from
//!   target request/reply and one-way delivery. The [TCP bridge][bridge] delegates
//!   to generic RequestReply and wraps its successful raw reply.
//! - [RequestReply][correlation] adds ExpectsReply and a missing MessageId.
//!   [RequestTable][fault] matches RelatesTo and interprets a nonzero transport
//!   Fault (including optional native diagnostic decoding).
//!   [ClientConnectionManager][domain] separately interprets
//!   ClientOperationFailure from its domain action/body after transport success.
//! - [Client operation][client-time] passes remaining time and conditionally
//!   adds client role metadata; [connection manager][manager-time] adds
//!   TimeoutHeader with remaining time and passes it into transport.
//!   These sources do not promise arbitrary Rust preparation/final validation
//!   inside a single absolute deadline.
//! - Native [resolution state][resolution-state] retains request data/previous
//!   metadata; [reply processing][resolution-reply] uses domain-specific cache
//!   and partition interpretation. This is not static operation-name decoding.
//! - Native [timeout/cancel handlers][cancel] remove local pending state without
//!   a remote cancellation packet in those handlers. Ordinary
//!   [reply matching][sender] explicitly ignores the sender target; stronger
//!   instance/source/role checks below are Rust policies, not that table's
//!   guarantees (nor a claim that native security lacks authentication).
//! - Native [TCP close/abort][ownership] skips externally supplied transport
//!   teardown. Its broader [gateway management][gateway] includes settings and
//!   connection/notification handling; these are not promised here.
//!
//! # Operational Rust-profile responsibilities (not native guarantees)
//!
//! ```text
//! owned typed request + CallContext
//!   -> encode -> (existing Message, owned request-derived ReplyState)
//!   -> bound RpcChannel (ordinary correlation / transport Fault)
//!   -> decode with ReplyState -> typed response / protocol or domain error
//! ```
//!
//! [`Client`] is a typed façade **observer**, not a lifecycle owner. It uses one
//! [`ClientBinding`] snapshot on one explicitly supplied foundation throughout
//! a call. Equal Target types, an address, actor or action do not prove affinity,
//! readiness or privilege. The backend must reject foreign/retired targets and
//! preserve channel source/role authorization. There is no default connection,
//! owner adoption, shared-owner close on façade drop/error, pool or resolver
//! hierarchy. Only the designated lifecycle owner invokes
//! [`MessageChannel::close`]; retaining inert bindings, validation state or
//! delivered results must not keep that owner's workers running after close.
//!
//! Calling [`Client::call`] transfers the owned request/context into a lazy
//! future: no encoding, admission, reservation or I/O until first poll.
//! One absolute deadline covers preparation, RPC admission/send/wait and final
//! local validation/result completion, without restarted budgets or late
//! success. Preparation/validation are bounded, nonblocking local work, not
//! detached tasks. [`ClientLimits`](crate::ClientLimits) specifies finite count/byte limits
//! covering all client/channel-retained requests, encoded messages, validation
//! state, pending work and undelivered results until retirement or transfer.
//! Enforce those limits without unbounded allocations from untrusted lengths;
//! reject with a local resource error or backpressure inside the same budget.
//!
//! Expiry/drop before admission sends nothing and releases local ownership.
//! After channel acceptance, timeout/drop is local abandonment, **not** remote
//! cancellation, rollback or proof of nonexecution. Retire local ownership and
//! correlation safely; never truncate a started frame. Settle local completion
//! once under reply/deadline/close races. No automatic retry, reconnect, replay,
//! failover or exactly-once execution; no second pending table or client reply
//! route registry. The channel owns transport correlation and native Fault;
//! typed decoding remains above it.
//!
//! Ordinary replies preserve all header records and use native's final valid
//! RelatesTo in wire order, within the channel's header/frame bounds. There is
//! no forwarding-count or repetition-pattern restriction; each typed operation
//! validates its ordinary reply envelope/body independently of correlation.
//! Unknown final IDs cannot complete an earlier neighbor. Caller duplicates
//! and unrelated owned fields remain strict; opaque unowned extensions confer
//! no retry/ACK/role authority. See the maintained NativeChannel guide and
//! pinned native common-header/cache references.
//!
//! Encoding selects operation-appropriate actor/action/header/body schemas
//! already housed in concrete protocol modules. It must not supply ordinary
//! MessageId, ExpectsReply, RelatesTo or special notification envelopes reserved
//! by [`RpcChannel`]. Activity is not MessageId. Activity/version/authorized role
//! and remaining TimeoutHeader are protocol-specific, not blanket channel
//! injection; any native time metadata uses the same remaining budget.
//! Decoding checks the operation's legitimate success **or failure** envelope,
//! required/duplicate fields and body shape, and request-derived conditions
//! (e.g. requested service name/partition selection) before typed completion.
//! Do not impose one universal actor/action/version schema on every protocol.
//!
//! Malformed/mismatched required reply data returns [`ClientError::Protocol`],
//! discards that result/state, and never fabricates success or silently closes
//! the shared foundation. Owner-level escalation is outside this façade;
//! the channel can independently become terminal under its own policy. A valid
//! operation-appropriate failure yields [`ClientError::Domain`], preserving its
//! required native code/data; missing/malformed required error data is Protocol,
//! not zero-HRESULT success. This observer policy deliberately replaces ordinary
//! FabricClient's previous malformed-domain abort policy, not the excluded
//! runtime IPC lifecycle/error mappings.
//!
//! Ordinary typed RPC does not cover every native semantic protocol.
//! [File transfer][file-transfer] accesses transport for its own protocol;
//! [replication ACKs][ack] express progress and replication
//! [sends one-way][replication]. They can continue through [`MessageChannel`].
//! [Demuxer][demux] separates ordinary replies from actor ingress, including
//! one-way; [DuplexRequestReply][duplex] handles special notifications separately.
//! Unsolicited notifications/authorized reverse requests use the existing
//! inbound foundation/router, not a new generic client listener. Generated
//! stubs, codecs, pools, middleware and gateway recovery are deferred.
//!
//! # Generic type guarantees
//!
//! These uncalled consumers check typed response/error relationships, Send,
//! owned request-derived validation state, and erased binding/channel usability.
//! Built-in adapters supply backend-independent codecs. Constructing a binding
//! cannot prove runtime affinity, authorization, budgets or lifecycle readiness.
//!
//! ```
//! use fube_client::contracts::{
//!     CallContext, Client, ClientBinding, ClientError, ClientFuture, ClientRequest,
//!     ReplyError,
//! };
//! use fube_transport::{channel::{MessageChannel, RpcChannel}, message::Message};
//!
//! fn typed<'a, C: Client, R: ClientRequest>(
//!     client: &'a C, request: R, context: CallContext,
//! ) -> ClientFuture<'a, R::Response, R::DomainError> {
//!     let pending = client.call(request, context);
//!     fn requires_send<F: Send>(_: &F) {}
//!     requires_send(&pending);
//!     pending
//! }
//!
//! fn paired<R: ClientRequest>(
//!     request: R, context: &CallContext, reply: Message,
//! ) -> Result<R::Response, ClientError<R::DomainError>> {
//!     let (_message, state) = request.encode(context).map_err(ClientError::Local)?;
//!     R::decode(state, reply, context).map_err(|error| match error {
//!         ReplyError::Protocol(reason) => ClientError::Protocol(reason),
//!         ReplyError::Domain(error) => ClientError::Domain(error),
//!     })
//! }
//!
//! fn erased<T: Send + Sync + 'static>(
//!     client: &dyn Client<Channel = dyn RpcChannel<Target = T>>,
//! ) {
//!     let binding: ClientBinding<'_, dyn RpcChannel<Target = T>> = client.binding();
//!     let _rpc: &dyn RpcChannel<Target = T> = binding.channel;
//!     let _raw: &dyn MessageChannel<Target = T> = binding.channel;
//!     let _target: &T = binding.target;
//!     // The generic typed call deliberately requires a statically known Client.
//! }
//!
//! fn error_origin<D>(error: ClientError<D>) -> &'static str {
//!     match error {
//!         ClientError::Local(_) => "local preparation/admission/client budget",
//!         ClientError::Transport(_) => "RPC transport (including native Fault)",
//!         ClientError::Protocol(_) => "reply validation",
//!         ClientError::Domain(_) => "legitimate typed domain failure",
//!     }
//! }
//! ```
//!
//! [request-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerRequestMessage.h#L59-L106
//! [reply-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerReplyMessage.h#L14-L77
//! [interface]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/IClientServerTransport.h#L22-L66
//! [bridge]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L190-L228
//! [correlation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L57-L122
//! [fault]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L70
//! [domain]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L150-L199
//! [client-time]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.RequestReplyAsyncOperation.cpp#L102-L117
//! [manager-time]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L79-L118
//! [resolution-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/LruClientCacheManager.ResolveServiceAsyncOperation.cpp#L20-L30
//! [resolution-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/LruClientCacheManager.ResolveServiceAsyncOperation.cpp#L221-L267
//! [cancel]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestAsyncOperation.cpp#L39-L51
//! [sender]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L281-L286
//! [ownership]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L51-L129
//! [gateway]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.h#L42-L80
//! [file-transfer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.h#L108-L117
//! [ack]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L168-L200
//! [replication]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L880-L890
//! [demux]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L188-L227
//! [duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77

use std::{future::Future, pin::Pin};

use fube_transport::{
    channel::{MessageChannel, RpcChannel},
    message::Message,
    Error, Result,
};
use tokio::time::Instant;

/// One owned whole-call budget, not a duplicate wire-metadata envelope.
///
/// **Rust-profile contract.** The caller chooses the absolute
/// deadline before preparation. [`Client::call`] checks it before/after encode,
/// across RPC, and before/after decode and local result completion. Expiry at
/// client stages is [`ClientError::Local`] with [`Error::Timeout`]; an RPC
/// timeout retains [`ClientError::Transport`] provenance. Either can occur
/// after send and therefore does not prove remote nonexecution.
/// Operation-specific metadata must derive remaining time from this deadline;
/// the context does not inject activity, version, role or TimeoutHeader.
#[derive(Debug)]
pub struct CallContext {
    /// Absolute Tokio deadline; never restart or extend it at a later stage.
    pub deadline: Instant,
}

/// Borrowed snapshot of an explicitly supplied channel and its target.
///
/// **Rust-profile contract.** Both references must belong to the same foundation;
/// the backend still validates affinity/retirement/role at admission. The
/// snapshot is stable for that call; type equality alone is insufficient.
/// Borrows/shared handles do not grant this observer ownership of shutdown.
/// The designated lifecycle owner remains responsible for its own close.
pub struct ClientBinding<'a, C: RpcChannel + ?Sized> {
    /// Existing ordinary RPC capability, not an adopted lifecycle owner.
    pub channel: &'a C,
    /// Target issued by that exact foundation, not merely the same Target type.
    pub target: &'a <C as MessageChannel>::Target,
}

/// Client-stage error provenance, distinct from the unchanged transport Error.
///
/// Display/source retain origin; there is no implicit flattening conversion,
/// fallback, close or recovery logic. Returning an error after acceptance
/// cannot promise nonexecution. See the [module](self) for malformed-reply policy.
#[derive(Debug)]
pub enum ClientError<D> {
    /// Input/encoding, client-side admission/resource/closed or budget failure.
    ///
    /// Use the existing InvalidInput/Protocol/ResourceLimit/Closed/Timeout as
    /// appropriate; do not place a remote domain or transport Fault here.
    Local(Error),
    /// Unmodified error from RpcChannel, including native Fault as Error::Remote.
    ///
    /// Do not reinterpret its code as a decoded domain response.
    Transport(Error),
    /// Malformed/mismatched required reply envelope, body or validation state.
    Protocol(&'static str),
    /// Legitimate operation-specific failure decoded from an allowed reply.
    Domain(D),
}

impl<D: std::fmt::Display> std::fmt::Display for ClientError<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(error) => write!(f, "local client: {error}"),
            Self::Transport(error) => write!(f, "RPC transport: {error}"),
            Self::Protocol(reason) => write!(f, "reply protocol: {reason}"),
            Self::Domain(error) => write!(f, "{error}"),
        }
    }
}
impl<D: std::error::Error + 'static> std::error::Error for ClientError<D> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Local(error) | Self::Transport(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}

/// Outcomes allowed from operation-specific reply interpretation.
///
/// **Rust-profile contract.** Required malformed failure data is
/// Protocol; a readable, valid domain failure is Domain, never transport success
/// coerced into a fake success result. RPC Fault never reaches this decoder.
#[derive(Debug)]
pub enum ReplyError<D> {
    /// Invalid required reply shape or request-derived validation mismatch.
    Protocol(&'static str),
    /// Valid operation-specific failure with required native code/data retained.
    Domain(D),
}

/// Boxed Send call future with typed client errors, borrowing its façade.
///
/// **Rust-profile contract.** Unlike ChannelFuture, its result
/// preserves domain provenance. Owned payloads do not make this future `'static`.
/// See [`Client::call`] for lazy progress and whole-call completion obligations.
pub type ClientFuture<'a, T, D> =
    Pin<Box<dyn Future<Output = std::result::Result<T, ClientError<D>>> + Send + 'a>>;

/// One owned typed request and its operation-specific request/reply adapter.
///
/// Built-in request representations implement this sealed shared trait, not a
/// new trait per operation/actor. Sealing makes the allocation proof enforceable;
/// arbitrary user codecs cannot silently claim a bound. Concrete schemas stay
/// in protocol modules; no universal codec, dynamic registry or type erasure.
///
/// The request, response, one-shot validation state and domain error are owned
/// and Send + 'static; they need not be Sync. Static adaptation is intentionally
/// not object-safe; the response/state/error association stays statically typed.
/// A client future can still borrow its façade for less than 'static.
pub trait ClientRequest: crate::typed::sealed::Request + Sized + Send + 'static {
    /// Owned successful operation result, after all required validation.
    type Response: Send + 'static;
    /// Owned per-call request-derived data retained across RPC for validation.
    ///
    /// May include service name/partition selection/previous metadata. Use ()
    /// only when the operation genuinely has no request-derived conditions.
    /// Retention is finite and must not keep owner tasks running after close.
    type ReplyState: Send + 'static;
    /// Owned typed domain failure preserving required native error code/data.
    type DomainError: Send + 'static;

    /// Consume the request into its wire message and paired validation state.
    ///
    /// Declared bounded synchronous local preparation, executed inside the lazy
    /// call after first poll, before RPC admission. Use the same context budget;
    /// no I/O, task spawning, retries or reply correlation. Select only the
    /// operation's allowed actor/action/header/body schema. Reject invalid input,
    /// spoofed/reserved correlation and special envelopes rather than rewriting
    /// contradictions. Failures use existing local Error variants, not Remote.
    fn encode(self, context: &CallContext) -> Result<(Message, Self::ReplyState)>;

    /// Consume the correlated opaque reply and this request's retained state.
    ///
    /// Declared bounded synchronous local validation/decoding under the original
    /// budget. RPC has already handled correlation/source/role and native Fault;
    /// do not install another request table. Validate allowed success/failure
    /// envelopes and required body/header/request-derived conditions before
    /// returning Response or Domain. Malformed/mismatched required data is
    /// Protocol; do not swallow it or close a shared owner.
    ///
    /// Without an explicit Copy/Clone bound, a generic consumer cannot reuse
    /// consumed validation state (the eventual client decodes each call once):
    ///
    /// ```compile_fail
    /// use fube_client::contracts::{CallContext, ClientRequest};
    /// use fube_transport::message::Message;
    /// fn reuse<R: ClientRequest>(
    ///     state: R::ReplyState, first: Message, second: Message, context: &CallContext,
    /// ) {
    ///     let _ = R::decode(state, first, context);
    ///     let _ = R::decode(state, second, context); // state was moved
    /// }
    /// ```
    fn decode(
        state: Self::ReplyState,
        reply: Message,
        context: &CallContext,
    ) -> std::result::Result<Self::Response, ReplyError<Self::DomainError>>;
}

/// Typed observer over an explicitly bound existing RPC foundation.
///
/// Implemented by the borrowing GatewayClient and designated FabricClient owner.
/// Shared concurrent calls are permitted under documented finite owner limits.
/// The binding accessor is object-safe with Channel fixed (including an erased
/// RpcChannel). The generic call requires Self: Sized; no erased universal call
/// or codec is supplied. This trait has no constructor, close or drop lifecycle.
pub trait Client: Send + Sync {
    /// Existing channel capability; Target inherits its foundation constraints.
    type Channel: RpcChannel + ?Sized;

    /// Borrow one same-foundation channel/target snapshot; no I/O or allocation.
    ///
    /// This observer accessor does not resolve/connect/adopt a target, guarantee
    /// live readiness, or grant authority to close the channel.
    fn binding(&self) -> ClientBinding<'_, Self::Channel>;

    /// Transfer an owned request/context into a lazy typed call.
    ///
    /// The future borrows self, owns request/state/results, and performs no
    /// encoding/reservation/admission/I/O until first poll. Take one binding
    /// snapshot, keep state paired through RPC and consume it in R::decode.
    /// Enforce finite retention and the unchanged deadline through final local
    /// completion; forward RPC errors as Transport and decoder errors as
    /// Protocol/Domain. Client preparation/admission/budget failures are Local.
    /// Follow the [module](self) for cancellation, owner and protocol policies.
    ///
    /// Owning the request does not permit detaching a borrowed call as 'static:
    ///
    /// ```compile_fail
    /// use fube_client::contracts::{CallContext, Client, ClientRequest};
    /// fn detach<C: Client, R: ClientRequest>(
    ///     client: &C, request: R, context: CallContext,
    /// ) {
    ///     tokio::spawn(client.call(request, context)); // borrowed client escapes
    /// }
    /// ```
    ///
    /// The response type cannot be chosen independently of the request:
    ///
    /// ```compile_fail
    /// use fube_client::contracts::{CallContext, Client, ClientFuture, ClientRequest};
    /// fn wrong<'a, C: Client, R: ClientRequest>(
    ///     client: &'a C, request: R, context: CallContext,
    /// ) -> ClientFuture<'a, (), R::DomainError> {
    ///     client.call(request, context) // R::Response is not generically ()
    /// }
    /// ```
    fn call<'a, R: ClientRequest>(
        &'a self,
        request: R,
        context: CallContext,
    ) -> ClientFuture<'a, R::Response, R::DomainError>
    where
        Self: Sized;
}
