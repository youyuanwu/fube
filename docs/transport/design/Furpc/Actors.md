# furpc: Application ingress and receive capabilities

**Status:** proposed; see the [design summary](README.md).

Application ingress selects either explicit raw delivery or optional ordinary
Actor dispatch, with [Tower-service handlers](Services.md) as the canonical
API for both dialed reverse traffic and accepted connections. Native
notifications use that same service boundary, with distinct classification
and authorization. There is no built-in application pull receiver; applications
can compose a bounded queue inside their service.
An ActorId is a native numeric routing code, not a Reliable
Actors identity, task, or durable mailbox. Registration capacity is resource
policy, not a native 32-actor limit. No implicit per-actor serialization,
cross-connection ordering, activation, or scheduling framework is added.
[Routing](Routing.md) adds Action matching within the Actor-dispatched path.

## Classification and authority

One inbound classifier processes native controls first, then applies native
classification using effective common properties:

```text
native control                         -> connection driver
native special notification            -> notification bindings/services
nonempty RelatesTo                     -> RPC/notification reply table
otherwise                              -> ordinary bindings/services
```

Native notification classification precedes ordinary correlation and dispatch.
Each application traffic class selects its own configured raw delivery or Actor
dispatch. Ordinary bindings never receive native notifications, controls, or
unmatched replies; notification bindings never catch an ordinary route miss.
Acknowledgments are correlated replies, not new notification deliveries.
Unknown correlation never falls back to an earlier RelatesTo occurrence or to
application ingress. `raw_only` disables outgoing managed RPC/notification
correlation and leaves ordinary one-way IDs caller-owned; it does not select
a dispatcher, authorize notification ingress, or turn replies into application
messages. Explicit incoming acknowledgment tickets remain private to the runner.

Actor dispatch uses effective native Actor properties and does not depend on
Action or ExpectsReply. It diagnoses and drops unknown actors. With explicit raw
ingress, Actor is not a delivery requirement; the raw binding's bounded
header/source check authorizes the application envelope instead. With no
application binding, ordinary application messages are diagnosed and dropped.
The request table ignores unmatched/late replies without an arbitrary
consecutive-reply terminal counter. Explicit flood/resource policy can still
retire an abusive connection. Domain envelope restrictions must be labeled as
policy, not additional native wire rules.

Registration selects delivery; it grants neither outbound permission nor reply
authority. ExpectsReply and caller-assembled message fields are not proof of
authorization. A validated ingress envelope retains private provenance binding
its message, source, connection generation, and any reply grant together.

Raw transport is a trusted low-level capability, not a sandbox against its own
caller. Restricted domain facades must not expose an unrestricted Peer
handle that bypasses their policy. Concrete composition determines the
capabilities available to a consumer.

## Application dispatcher selection

Raw and Actor-dispatched ingress, whole-Actor bindings, and the optional exact
Actor/Action Router are implemented for ordinary and notification traffic.
Notification origination, service-defined acknowledgments, and typed unary
adaptation use the same managed core and service runner.

Each logical Channel or server scope installs at most one IngressService.
Within each traffic class, its bindings choose raw ingress or Actor dispatch.
A raw dispatcher has one
[`ServiceBinding::raw`](Services.md#service-boundary-and-native-bindings)
for that class.
Actor dispatch can contain multiple actor bindings owned by that same service
or Router. Ordinary and notification bindings have disjoint namespaces, but
share the runner and HandlerLimits. No new public dispatcher trait, driver,
or staging queue is needed.

`Endpoint::service`, `Channel::register_service`, and `ServerBuilder::service`
install the same IngressService adapter and select its raw or Actor bindings.
The installed service's bindings select the dispatcher: ServiceBinding::raw
selects raw delivery; ServiceBinding::actor and Router::into_service select
Actor dispatch. Raw and Actor bindings cannot be mixed within one traffic class.
An ordinary raw binding may coexist with notification Actor bindings, or vice
versa, without creating a fallback between classes.
Conflicting or duplicate service/binding installation returns InvalidInput
without changing existing configuration; server build/serve and Endpoint connect revalidate
before enabling traffic. A second raw binding in the same class rejects,
never broadcasts.
Ordinary RPC/notification correlation and `on_peer_ready` are independent of
that dispatcher choice.

Without bindings for a traffic class, its application delivery remains
unconfigured, not implicitly raw. A Channel may select its dispatchers by later
service installation, but cannot recover previously dropped traffic. Once
installed, the service and both classes' bindings are fixed for that logical
scope; there is no live replacement/unregistration API. Reconnection
preserves the selection and current registry; it cannot switch dispatchers or
redeliver an old registration's messages to a different target.

The shared driver validates native headers and handles controls/correlation.
At its header-stage admission boundary it runs only the configured bounded
dispatcher matching and header/source checks, before body retention. Application
processing and custom domain demultiplexing run after full applicable integrity,
outside the driver. Optional dispatch does not defer byte admission until
after an unbounded body allocation or move reply authority into application code.

## Ingress policies and send capabilities

The same policies apply to raw and Actor service bindings. Service installation
is defined in [Services](Services.md#service-boundary-and-native-bindings);
handlers receive `Request<Body>` and return IngressResponse. Any pull-style
application queue is [service-level composition](Services.md#application-owned-work-queues),
not another native receiver API.

```rust,ignore
impl IngressOptions {
    pub fn one_way(check: EnvelopeCheck, limits: IngressLimits) -> Self;
    pub fn requests(policy: RequestPolicy, limits: IngressLimits) -> Self;
    pub fn notifications(policy: NotificationPolicy, limits: IngressLimits) -> Self;
    pub fn with_send_policy(self, policy: SendPolicy) -> Self;
}

pub type EnvelopeCheck = fn(&Headers, &Source) -> Result<()>;

pub enum SendKind { OneWay, Request, Notification }
pub type SendCheck = fn(SendKind, &Headers, &Source) -> Result<()>;

impl SendPolicy {
    pub fn new(check: SendCheck) -> Self;
}

impl RequestPolicy {
    pub fn new(request: EnvelopeCheck, response: EnvelopeCheck) -> Self;
}

impl NotificationPolicy {
    pub fn new(notification: EnvelopeCheck, acknowledgment: EnvelopeCheck) -> Self;
}
```

NotificationPolicy is cloneable immutable configuration; cloning copies checks,
not live invocation or correlation state.

`IngressOptions` configures authorization/admission for the installed service.
Endpoint stores inert service configuration and bounds binding metadata,
not live queues. Connect installs bindings before application traffic;
Channel clones share one service runner rather than competing receivers.
Independent explicit connects have their own runners and native accounting.
Dynamic Channel service installation cannot recover traffic admitted earlier.
Server configuration is complete before serving; the initial server has no
live service-registration controller. Invalid/mixed bindings and reserved
native control actors reject before traffic. Raw checks cannot override native
control classification. Configuration validation applies the actual scope's
finite limits, including after Endpoint limits are lowered.

The registry owns bounded binding metadata, not the facade's live scope or
application queues. An optional actor ID
does not identify a destination, domain object, security principal, or task.
Wire order is preserved within an individual connection's admitted delivery;
no cross-connection total order, service processing order, or handler-completion
order is promised.

`one_way` authorizes ordinary delivery without a reply ticket. `requests`
separately authorizes ordinary request and response envelopes.
`notifications` separately authorizes native notification and acknowledgment
envelopes; it is not an alias for either ordinary kind. Core native fields and
ordinary-versus-notification classification checks remain mandatory. User checks are
trusted nonblocking header/source checks, not body decoders or driver callbacks
that may await. Raw or Actor registration alone never authorizes sending.

Separately, [`ServerBuilder::on_peer_ready`](Connections.md#accepted-peer-readiness-hook)
can receive a restricted Peer after native establishment, before any
application ingress is required. Its explicit SendPolicy is independent of
ingress registration and authorizes no inbound delivery or reply grant.
Installing it does not make RequestContext::peer Some for bindings
that did not opt in.

`with_send_policy` independently opts into a received Peer capability.
The check is captured with the original driver/generation and re-applied to
every outbound envelope before commitment. `SendKind` is selected by the
method used, never a wire field. A policy can allow only semantic one-way ACKs,
or explicitly permit requests/notifications too. It cannot waive managed IDs,
reply-ticket rules, or native control restrictions. `Peer::notify` adds its
explicit NotificationPolicy; it cannot replace a captured send restriction.

Without this opt-in, RequestContext::peer returns None even when the runner
holds a reply ticket. All services return IngressResponse data and never
extract that grant, including when application work is queued.
`Source` remains passive provenance, not a way to recover more send authority.
The same rules apply to reverse ingress received by a client.

For a semantic ACK consumer, trusted composition installs
`IngressOptions::one_way(check_packet, limits).with_send_policy(ack_policy)`.
The ACK policy must restrict send kind and envelope to the domain's actual
ACK protocol. After the domain has validated/processed the packet and earned
its acknowledgment, it can use the captured path:

```rust,ignore
let context = request.context()
    .ok_or(Error::Unsupported("native ingress context required"))?;
let peer = context.peer().cloned()
    .ok_or(Error::Unsupported("source-bound send not authorized"))?;
peer.send(ack, deadline).await?;
```

This neither performs a lookup nor turns source metadata into authority.
The connection may already have retired; send then fails Closed without
dialing a replacement.

## Actor-less protocols and custom demultiplexing

The ordinary Actor header is optional native metadata, not a framing
requirement. Absence remains visible as `Headers::first_actor() == None`;
native effective-property lookup can retain its Empty/default-zero semantics
without inserting a record or requiring applications to register Actor 0.
Malformed recognized occurrences still fail native validation in either path.
The Actor dispatcher continues to use effective native properties; an absent
or unregistered actor is not an implicit request to try raw delivery.

An explicitly configured ordinary raw binding receives authorized application
messages with or without a valid ordinary Actor header. It is not a fallback
for rejected actor routes, native controls, special traffic, or unknown
correlation. Its check runs before body retention, with the same bounded
header/source policy, byte admission, complete-frame integrity, provenance,
and optional send/reply authority as an Actor binding.

Classic replication normally installs IngressService with ServiceBinding::raw
through Endpoint/Channel or ServerBuilder. The same domain service runs on
initiating and accepting roles; any replication-owned work queue is composed
inside that service rather than exposed as a second native ingress API.
Select `ConnectionOptions::raw_only` independently when replication owns its
wire MessageIds. The domain validates native ReplicationActor/ReplicationFrom
headers and, after verified delivery, demultiplexes by ReplicationEndpointId
under its own bounded endpoint/processing policy. It sends earned semantic
ACKs through the original request context's authorized Peer; no ordinary Actor
header, generic Router, or transport-owned replication table is needed.

This follows the separation in native
[`ReplicationDemuxer`][sf-replication-demuxer], which selects its endpoint key
from ReplicationActor instead of ordinary Actor, and
[`CreateSharedHeaders`][sf-replication-headers], which emits replication
headers and Action without an ordinary Actor. Furpc keeps those domain schemas
and semantic ACKs above its shared driver. Replication owns operation queues,
ACK/RequestAck dispatch, and retry under the
[native flow-control split](Resources.md#replication-flow-control-boundary).
Session behavior still needs its own replication-framework qualification.

## Ingress across connection generations

Channel IngressService configuration, including notification bindings, survives
a physical disconnect. Service readiness/handler invocations and private
reply/acknowledgment tickets admitted to the failed generation are cancelled
and never replayed; only configuration persists for new messages.
Reconnection binds the current logical registry before releasing application
traffic on its new generation. Runtime service installation remains effective.
A separate explicit Endpoint connect creates a completely independent registry.
The runner and HandlerLimits are shared by all Channel clones, traffic classes
and generations, not recreated as fresh per-generation capacity.

Application-owned queues may retain old requests, source metadata, optional
Peers and shared bytes, but cannot keep a cancelled invocation or its private
completion ticket live. Late worker output cannot acknowledge on a replacement
generation. There is no total order across generations or implicit domain-session
continuity. Source checks run for each newly admitted delivery.

Installing notification ingress or waiting on an application queue does not
demand reconnect. Later RPC readiness or Channel peer acquisition may join the
same bounded reconnect attempt; a Peer notification never reconnects itself.
Retain an owning Channel or live server execution while native work is needed.

Transport establishment does not restore IPC registration, notification
subscriptions, replication sessions, or earned semantic ACK state. Consumers
must gate domain work and recover those protocols explicitly on a fresh Peer
or new source generation; transport does not fabricate a session-ready event.

## Native notifications

Native push notifications are request/acknowledgment exchanges, not ordinary
one-way delivery or arbitrary reverse RPC. Their native special flags, including
UncorrelatedReply, select a distinct traffic class. That name means the push is
not answering an earlier request; it does not mean fire-and-forget.

Use the same native-operation and service programming model as existing sends
and requests. Do not add a separate notification client, transport-owned
receiver/queue, received-message wrapper, or public acknowledgment grant.
These APIs replace the previous separate duplex design. They are implemented;
independent native interoperability is not yet qualified.

The wire mapping follows native [BeginNotification][sf-notification],
[request setup][sf-notification-request],
[special notification dispatch][sf-duplex], and
[ReceiverContext::Reply][sf-receiver-reply]: the originator adds
UncorrelatedReply and ExpectsReply, while the acknowledgment relates to the
original MessageId without those flags. Ingress special classification does
not itself require ExpectsReply; it requires a nonempty original MessageId
and explicit notification authorization, even when RelatesTo is also present.

```rust,ignore
impl Peer {
    pub fn notify(
        &self, request: Request<Body>, policy: NotificationPolicy, deadline: Instant,
    ) -> Operation<Response<Body>>;
}
```

`notify` parallels `Peer::request`: it is inert until polled, uses the earlier
explicit/local deadline, is bound to the Peer generation, and retains
Operation/Attempt cancellation and commitment semantics. It requires managed
mode and an explicit NotificationPolicy. The notification check and any captured
SendPolicy apply to submission; the acknowledgment check applies to matched
responses. Neither can waive native field/control rules.
At the caller, a matched acknowledgment's header/source check runs before body
retention. Rejected envelopes are diagnosed and integrity-checked on discard;
they do not settle the pending operation or fall back to ingress.

The core assigns the native ID and special flags, registers correlation before
commitment, and awaits the acknowledgment using the shared pending-only table
and existing pending-request/result allowance, not a separate notification pool.
Caller-supplied managed fields reject; special flags are not enabled by editing
raw send headers. Acknowledgment lookup remains generation-bound, verifies
complete frame integrity, and processes native Faults before successful body
decoding. Timeout, cancellation and generation retirement remove pending state
without replay or lifetime ID history.

Peer remains byte-facing, like Peer::request; it has no body-codec convenience
method. Typed notification callers reuse RpcClient<T> over an explicit
byte-facing Tower service that delegates to Peer::notify with its captured
NotificationPolicy. That adapter forwards the Request's original absolute
deadline, native Response parts and errors, and adds no replay, correlation
table or driver. It does not make Peer implement Service or introduce another
built-in transport. RpcClient never infers native notification kind from edited
flags or changes the ordinary Channel service's request semantics.

Typed notification calls therefore use per-call encoder/decoder values and
ClientCodecLimits contract, encoding before byte-service/native admission and
decoding outside the driver. They return ordinary RpcClient futures without an
Attempt; callers needing native disposition use the byte notify operation.
Response parts preserve native source and settlement timing. An adapter that
captures only a Peer remains nonowning, so retain the originating Channel or
server execution. No allocation ledger or additional construction cap is added.

Ingress uses `IngressOptions::notifications(policy, limits)` with
ServiceBinding::raw or ServiceBinding::actor, or the Router's
`notification(..., NotificationRouteOptions, service)` registration.
Install these through the existing Endpoint::service, Channel::register_service
or ServerBuilder::service APIs. There is no separate take/register/unregister
notification-receiver lifecycle. Duplicate/conflicting bindings reject within
their traffic class before serving.

At header admission, the core validates the original native notification kind
and required ID, selects only an authorized notification binding, and captures
the original invocation/acknowledgment authority before body retention.
Ordinary one-way/request flags or mutable Actor/Action values cannot mint this
admission. After integrity verification the service receives Request<Body> with
the same private original-source, kind, route, policy and deadline context.

The service returns IngressResponse::Reply as acknowledgment data.
`server::unary` adapts its typed Request<Q> -> Reply<R> inside that same
invocation; it accepts either an originally authorized ordinary request or
notification. The runner alone canonicalizes the acknowledgment's native
flags/RelatesTo using the notification's original MessageId and generation.
NotificationPolicy's acknowledgment check is applied at submission and
commitment. `one_way` adaptation rejects notification invocations;
NoReply for a notification is a missing-acknowledgment handler error.
Failures, cancellation and dropped work send no implicit acknowledgment or Fault.

On ingress, notification readiness, codec work, handlers, queue waits and
acknowledgment writes use the original invocation deadline and shared HandlerLimits.
Handler concurrency and readiness admission are consumer middleware/queue
policy, not an implicit furpc quota.
Application-owned bounded queues compose inside services while the runner
retains its ticket. They receive no send authority, and late worker output
cannot revive a cancelled invocation. The optional SendPolicy alone decides
whether RequestContext also exposes a restricted Peer.

Graceful server drain includes already-admitted notification handlers and their
ticket-authorized acknowledgments just like ordinary replying handlers.
General Peers gain no drain exemption for new notifications or semantic sends.
Dropping the owner/execution cancels notification work; retaining requests,
parts, a Peer or an unpolled operation cannot keep the scope alive.

[sf-replication-demuxer]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationDemuxer.cpp#L12-L28
[sf-replication-headers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L919-L929
[sf-notification]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/requestreply.cpp#L263-L271
[sf-notification-request]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/requestreply.cpp#L61-L105
[sf-duplex]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77
[sf-receiver-reply]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/ReceiverContext.cpp#L25-L41
