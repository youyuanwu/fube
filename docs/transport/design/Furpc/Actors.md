# furpc: Application ingress and receive capabilities

**Status:** proposed; see the [design summary](README.md).

Application ingress selects either explicit raw delivery or optional ordinary
Actor dispatch, with [Tower-service handlers](Services.md) as the canonical
API for both dialed reverse traffic and accepted connections. Ordinary ingress
has no built-in pull receiver; applications can compose a bounded queue inside
their service. Specialized native duplex ingress remains separate.
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
special duplex                         -> authorized duplex machinery
nonempty RelatesTo                     -> ordinary reply table
otherwise                              -> configured application dispatcher
```

Only the last branch selects raw delivery or Actor dispatch. Raw ingress does
not expose native controls, special duplex, or unmatched ordinary replies.
Unknown correlation never falls back to an earlier RelatesTo occurrence or to
application ingress. `raw_only` disables managed correlation/ID ownership; it
does not select a dispatcher or turn replies into ordinary application messages.

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
Actor/Action Router are implemented. Specialized duplex remains deferred.

Each logical Channel or server scope has at most one ordinary application
dispatcher: raw ingress or Actor dispatch. A raw dispatcher has one
[`ServiceBinding::raw`](Services.md#service-boundary-and-native-bindings).
Actor dispatch can contain multiple actor bindings owned by the installed
IngressService/Router. No new public dispatcher trait, driver, or staging queue
is needed.

`Endpoint::service`, `Channel::register_service`, and `ServerBuilder::service`
install the same IngressService adapter and select its raw or Actor bindings.
The installed service's bindings select the dispatcher: ServiceBinding::raw
selects raw delivery; ServiceBinding::actor and Router::into_service select
Actor dispatch. Raw and Actor bindings cannot be mixed in one scope.
Conflicting or duplicate service/binding installation returns InvalidInput
without changing existing configuration; server build/serve and Endpoint connect revalidate
before enabling traffic. A second raw binding rejects, never broadcasts.
Ordinary RPC correlation, special-duplex registration, and `on_peer_ready`
are independent of this choice.

Without ordinary ingress configuration, application delivery remains
unconfigured, not implicitly raw. A Channel may select its dispatcher by later
service installation, but cannot recover previously dropped traffic. Once
installed, the ordinary service and bindings are fixed for that logical scope;
there is no live replacement/unregistration API. Reconnection
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
```

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

`one_way` authorizes delivery without a reply grant. `requests` separately
authorizes request and response envelopes; core MessageId/ExpectsReply and
normal-versus-special classification checks remain mandatory. User checks are
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
reply-grant rules, or native control restrictions. `Peer::duplex` adds
its operation policy; it cannot replace a captured send restriction.

Without this opt-in, RequestContext::peer returns None even when the runner
holds a reply grant. Ordinary services return IngressResponse data and never
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

An explicitly configured raw binding receives authorized ordinary application
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

Channel IngressService configuration and optional special-duplex registration
survive a physical disconnect. Service readiness/handler invocations already
admitted to the failed generation are
cancelled and never replayed; only configuration persists for new messages.
Reconnection binds the current logical registry before releasing application
traffic on its new generation.
It preserves the installed ordinary service and chosen dispatcher. Special
duplex queues/receivers are not recreated; take_duplex remains take-once.
Runtime service installation remains effective, while dropped/unregistered
special-duplex entries are not resurrected from Endpoint configuration. A separate
explicit Endpoint connect still creates a completely independent registry.
Registration epochs and physical connection generations are distinct.
The service runner and HandlerLimits are shared by all Channel clones and
generations, not recreated as fresh per-generation capacity. An installed
service is not a new public owner or continuous-reconnect demand.

Queued old-generation special-duplex deliveries retain their immutable source, optional Peer,
reply grant, shared storage, and original policy/deadline. They may be drained without
being reassigned to the replacement. Replying or ACKing through a retired
generation fails with its cause; never send that work through a fresh Peer
automatically. Source checks run for each newly admitted delivery. There is
no total order across generations and no implicit domain-session continuity.
Ingress limits cover the combined old/current special-duplex queue, not a new
allowance per connection. Application-owned queues are outside the registry;
queued requests retain their original context/storage and cannot keep a
cancelled service invocation live or replay it on a new generation.

When its queue is empty, a pending Channel-backed DuplexReceiver::recv may demand
the same bounded reconnect attempt as RPC readiness or Channel peer acquisition.
It is a nonowning demand: last-owner release closes it. Cancellation consumes
no queued message and grants no replay authority. Reconnect failure is returned
explicitly for that wait, leaving the registration usable for later receives;
a physical disconnect alone is not terminal registration Closed. Already
queued messages can still be consumed with their original provenance.
An accepted-server DuplexReceiver never originates a reconnect.

Transport establishment does not restore IPC registration, notification
subscriptions, replication sessions, or earned semantic ACK state. Consumers
must gate domain work and recover those protocols explicitly on a fresh Peer
or new source generation; transport does not fabricate a session-ready event.

## Duplex origination and ingress

Special duplex origination and delivery have explicit entry points:

```rust,ignore
impl Endpoint {
    pub fn duplex_ingress(
        self, policy: DuplexPolicy, limits: IngressLimits,
        sends: Option<SendPolicy>,
    ) -> Result<Self>;
}

impl Channel {
    pub fn take_duplex(&self) -> Result<DuplexReceiver>;
    pub fn register_duplex(
        &self, policy: DuplexPolicy, limits: IngressLimits,
        sends: Option<SendPolicy>,
    ) -> Result<DuplexReceiver>;
}

impl Server {
    pub fn register_duplex(
        &self, policy: DuplexPolicy, limits: IngressLimits,
        sends: Option<SendPolicy>,
    ) -> Result<DuplexReceiver>;
}

impl DuplexReceiver {
    pub async fn recv(&mut self, deadline: Instant) -> Result<ReceivedMessage>;
    pub fn unregister(&mut self);
}

impl ReceivedMessage {
    pub fn message(&self) -> &Message;
    pub fn source(&self) -> &Source;
    pub fn peer(&self) -> Option<&Peer>;
    pub fn take_reply(&mut self) -> Option<ReplyGrant>;
    pub fn into_message(self) -> Message;
}

impl ReplyGrant {
    pub fn reply(self, message: Message, deadline: Instant)
        -> Operation<WriteReceipt>;
}

impl Peer {
    pub fn duplex(&self, policy: DuplexPolicy) -> Result<DuplexClient>;
}

impl DuplexPolicy {
    pub fn new(notification: EnvelopeCheck, acknowledgment: EnvelopeCheck) -> Self;
}

impl DuplexClient {
    pub fn notify<C>(
        &self, request: Request<C::Encode>, codec: C, deadline: Instant,
    ) -> Operation<Response<C::Decode>>
    where
        C: Codec + Send + 'static,
        C::Encode: Send + 'static,
        C::Decode: Send + 'static,
        C::Encoder: Send + 'static,
        C::Decoder: Send + 'static;
}
```

Initially one bounded special-duplex receiver may be registered per Channel
or server scope; its policy selects authorized notification envelopes.
`Endpoint::duplex_ingress` preconfigures it before application traffic;
it stores an inert descriptor and connect installs the bounded queue before
special traffic is released. `Channel::take_duplex` transfers that receiver
once; missing/already-taken is InvalidInput. Clones share one registration
and cannot obtain duplicate receivers. Dynamic Channel registration remains explicit.
Duplicate duplex configuration/registration rejects instead of silently
replacing its policy.
Server registers before serving; the application drives its separately owned
duplex processing concurrently with the live Channel/server.

DuplexReceiver and ReceivedMessage are not Clone and retain no scope owner.
Cancellation/timeout of recv consumes no message; exclusive mutable receive
permits no unbounded receive-waiter list. Unregister stops new admission,
leaving already assigned/queued work on its original registration epoch until
drained, then recv returns Closed. Drop unregisters and discards retained work
without acknowledgment. New registration cannot inherit old deliveries.

ReceivedMessage is the direct special-duplex delivery envelope, not ordinary
service ingress. Its constructors are private; extracting a Message cannot
reconstruct validation or manufacture reply authority. take_reply moves the
original grant once, while into_message drops unused context/grant without
emitting output. ReplyGrant::reply consumes the grant even when its lazy
operation is unpolled, applies captured response policy/flags and original
RelatesTo, and never targets a replacement generation. Owner shutdown rejects
unused grants; retaining a grant cannot hold drain open.

Special duplex remains separately classified with either ordinary dispatcher.
Origination requires managed mode
and explicit policy; the notification and acknowledgment checks apply in their
respective directions. `notify` owns the special flags and awaits correlation.
`duplex` returns a cloneable codec-independent adapter retaining only its
bound Peer and explicit policy. Each `notify` consumes a codec for that
notification/acknowledgment pair, using the same lazy operation, body budgets,
and native per-call ownership rules. This specialized Peer-bound path
retains `Operation`/Attempt and actual scope-owned codec admission; it is not
the generic `RpcClient<T>` transport service, whose codecs run above byte
admission. Both paths reuse the same Codec traits without storing a codec on
the adapter; client codecs need neither Clone nor Sync. DuplexClient and its
operations are nonowning; keep the originating Channel or server execution
alive. Native convenience methods
may supply `FubstCodec` explicitly, without imposing its schemas on unrelated
notifications.
Receiving special traffic does not use `IngressOptions::requests`. This shares
body codecs and attempt receipts, not an unrestricted raw flag setter.
The optional send policy controls whether those received contexts also expose
a restricted Peer; None grants only the separately authorized reply.

[sf-replication-demuxer]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationDemuxer.cpp#L12-L28
[sf-replication-headers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L919-L929
