# furpc: Ingress services and bounded execution

**Status:** proposed; see the [design summary](README.md).

Defines the canonical Tower ingress boundary for Channel and Server, native
bindings, typed RPC method adapters, runner-owned reply completion, handler limits,
and handler-aware drain.
The [router](Routing.md) is one implementation of this boundary.
Applications needing queued processing compose a
[bounded work queue](#application-owned-work-queues) inside their service.

## Placement and ownership

```text
native connection driver
    -> common-header/control/reply/notification classification
        -> ordinary OR notification bindings
            -> class-local raw binding OR optional Actor dispatcher
                -> header-only authorization and bounded admission
                    -> complete-frame integrity verification
                        -> scope-owned service readiness and handler work
                            -> optional Router: native class + Actor + Action
                                -> selected Service<Request<Body>>
                                    -> optional server::unary/one_way adaptation
                                        -> decode -> typed service -> encode
                        -> private original-connection reply/ack completion
```

Native controls and correlated replies do not enter this application service
path. Native notifications do, but only through explicitly authorized
notification bindings, never ordinary request/one-way bindings.
A raw binding requires no ordinary Actor header. Actor/Action routing is an
optional class-local application dispatcher, not a replacement for native
classification. All bindings share the same runner and admission; no path
routes through an additional inbox.

The Channel's private logical scope or the `serve_with_shutdown` execution
owns ingress-service readiness, handler futures, response completion, and
their ordinary storage ownership. Channel's outbound RPC Service remains a separate
direction; inbound services do not call it or create another correlation table.
User code never runs in the socket driver beyond the existing bounded
nonblocking header/source authorization checks.
The optional [`on_peer_ready` service](Connections.md#accepted-peer-readiness-hook)
is separate server-owned `Service<Peer, Response = ()>` work, not an actor
request, route, or codec adapter. It shares the accepted connection's lifetime
and resource accounting, but not request-service `HandlerLimits` permits.
Application state can be captured in `Arc<State>`. Ingress configuration must
not capture its own owning Channel and create an ownership cycle; use the
independently authorized context Peer for original-generation native sends.

## Service boundary and native bindings

Use the standard trait, not a parallel handler-service trait:

```rust,ignore
tower_service::Service<Request<Body>, Response = IngressResponse>
```

The service is cloneable and Send, its future is owned Send, and its error
converts into `BoxError`. The runner clones configuration before readiness,
polls readiness, and calls that same prepared instance. It never clones a
service after readiness and discards its capacity reservation.

A standard service cannot declare its native ingress or authorize a reply
grant. Carry that separate trusted configuration in a small adapter:

```rust,ignore
impl<S> IngressService<S>
where
    S: tower_service::Service<Request<Body>, Response = IngressResponse>
        + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    pub fn new(service: S, bindings: Vec<ServiceBinding>) -> Result<Self>;
    pub fn map_service<T>(
        self, layer: impl FnOnce(S) -> T,
    ) -> IngressService<T>
    where
        T: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + 'static,
        T::Future: Send + 'static,
        T::Error: Into<BoxError>;
}

impl ServiceBinding {
    pub fn raw(options: IngressOptions) -> Result<Self>;
    pub fn actor(actor: ActorId, options: IngressOptions) -> Result<Self>;
}

impl Endpoint {
    pub fn service<S>(self, service: IngressService<S>) -> Result<Self>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;
    pub fn handler_limits(self, limits: HandlerLimits) -> Self;
}

impl Channel {
    pub fn register_service<S>(&self, service: IngressService<S>) -> Result<()>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;
}

impl ServerBuilder {
    pub fn service<S>(self, service: IngressService<S>) -> Result<Self>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;
    pub fn handler_limits(self, limits: HandlerLimits) -> Self;
}
```

`IngressService<S>` and `IngressResponse` replace the server-specific service
and response names: the same inbound handler role applies to dialed reverse
traffic and accepted connections. server::unary/one_way adapt typed methods in
either role; it does not require an accepted Server owner.

`IngressService` is configuration, not another task owner or queue. Its bindings
are immutable once installed. `map_service` preserves them while adding
compatible Tower middleware. Clone is implemented when S is Clone and copies
only configuration/bindings, never live invocation or readiness state.
Each scope installs at most one explicit user
ingress service; compose its routes or domain dispatch inside that service.
Repeated installation rejects explicitly. There is initially no live service
replacement/unregistration API; ordinary and notification bindings belong to
this same service.

`Endpoint::service` stores an inert cloneable prototype and bounded binding
metadata. Constructors, setters, and unpolled connect futures perform no
readiness polling, handler invocation, I/O, or task startup. Each explicit
connect clones unprepared service configuration into an independent logical
scope, installs bindings and reserves execution metadata before application
traffic is released, then drives reverse handlers without a user receive loop.
There is no receiver handoff, hidden inbox, or native application-message pull API.
Initial connect failure/cancellation retires its unpublished ingress work too.

Endpoint remains Clone + Send + Sync without requiring S to be Sync. Its
private erased prototype may use short synchronization for synchronous
configuration cloning; no service future is polled or lock held across an
await. Never share a mutable ready service or duplicate its reservation by
cloning. Separate explicit connects have independent runners and budgets;
application state shared intentionally by S::clone remains application-owned.

`Channel::register_service` atomically installs the same adapter into its
current logical registry under the Endpoint's actual HandlerLimits and
resource policy. It is synchronous and performs no readiness or handler work.
It can run while disconnected and takes effect for later admissions, including
the next generation, but cannot recover earlier dropped messages. It adds
neither an owner nor a replacement driver. Prefer preconnect installation to
avoid early reverse-ingress races. Server installs the same adapter before
serving through its existing builder method.

Bindings select the [application dispatcher](Actors.md#application-dispatcher-selection).
Within each native traffic class, raw service composition has one
ServiceBinding::raw and no Actor bindings. IngressOptions selects ordinary
one-way, ordinary request, or notification authorization. Its check authorizes
only that original class/kind independently of Actor presence. This is explicit
configuration, not a wildcard fallback for a missed route or traffic class.

With Actor dispatch, all actor bindings belong to the installed service.
A custom service can bind a whole actor and
interpret Action itself. A router generates Actor bindings with immutable
action selectors and traffic classes from its routes. Class-local conflicting
raw/Actor or duplicate binding claims reject before serving, never race to
consume the same message or silently widen a binding. IngressService::new
validates its class-local raw-versus-Actor bindings; installing it and serving
revalidate scope-wide compatibility.

These bindings perform only header/source matching, independent authorization,
and generic native admission. They do not decode domain bodies or invoke
async services. Request and notification policies create private completion
tickets only after their respective core envelope checks; Actor, Action,
ExpectsReply and special flags alone grant nothing.
Before handing a request to middleware, the runner records the selected
binding/route identity, native classification, exchange kind, correlation,
policies, limits, and deadline independently of its editable headers.
Later metadata edits never replace this admission snapshot or select another
binding's permissions.

No ingress service is required for outbound-only composition. A notification-only
scope installs a service with only notification bindings; there is no separate
receive registration. A custom raw service can interpret ReplicationActor or
another domain key without a generic Router or Actor binding. Domain
demultiplexing and body work run outside the driver;
its original raw binding still fixes native classification, policies, limits,
and response authority.
Likewise, peer-ready work does not require installing a request service.
When it needs a response handled by a raw or Actor service, that service remains
independently runnable while the hook waits; the hook cannot hold its handler
capacity or block its dispatch.

## Typed RPC adapters

`server::unary`, `server::one_way`, and `Reply<T>` are implemented. They work
inside raw, whole-Actor, or routed ingress services on either connection role.
The optional codec-free Router is implemented independently of these adapters.
Unary accepts authorized native notifications as well as ordinary requests;
notification bindings and origination are implemented with runner-owned
acknowledgment tickets.

Keep typed body adaptation outside `Router`, following
[tonic's method-level `server::Grpc<C>`][tonic-server-rpc]. A byte-facing method
service supplies actual encoder/decoder state values for that invocation.
Ordinary Tower composition owns their construction; the router has no
codec/factory parameter or default body format. The same method adapter can
also run inside a custom raw or whole-actor service without any router.

```rust,ignore
pub async fn unary<S, D, E>(
        service: S, request: Request<Body>, decoder: D, encoder: E,
    ) -> Result<IngressResponse>
    where
        E: Encoder + Send,
        E::Item: Send + 'static,
        D: Decoder + Send,
        D::Item: Send + 'static,
        S: tower_service::Service<
            Request<D::Item>, Response = Reply<E::Item>,
        > + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;

pub async fn one_way<S, D>(
        service: S, request: Request<Body>, decoder: D,
    ) -> Result<IngressResponse>
    where
        D: Decoder + Send,
        D::Item: Send + 'static,
        S: tower_service::Service<Request<D::Item>, Response = ()>
            + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;
impl<T> Reply<T> {
    pub fn new(body: T) -> Self;
    pub fn append_header(self, header: HeaderRecord) -> Result<Self>;
}
```

These functions own no socket, admission pool, reply grant, or
execution scope. Invoke them inside the
byte-facing service future, supplying independent encoder/decoder
state per invocation. Neither that codec nor the consumed typed service needs
Clone or Sync; sharing requirements apply only to stored outer service
configuration. Constructing a method adapter cannot create live invocation
context.

`unary` adapts one replying exchange: an ordinary native request or an explicitly
authorized native notification requiring acknowledgment. Before typed-service
work, verify live core-created context and the original replying-kind
authorization recorded by the runner, and validate the current header collection
under its original scope. Do not infer authorization from editable flags or
IDs. Use that invocation's actual body budgets and original deadline,
not defaults captured when the service was constructed. Decode into
`Request<D::Item>` while preserving parts, then invoke the typed service and
encode its `Reply<E::Item>` into `IngressResponse::Reply(Message)`.
The runner's private ticket, not the codec or typed service, chooses ordinary
reply versus native notification acknowledgment canonicalization.
Native convenience method code supplies `FubstDecoder<Q>` and `FubstEncoder<R>` for a
`Request<Q>` / `Reply<R>` handler; custom protocols supply their own codec.
Supported fubst serialization/deserialization schemas are required only by
that concrete codec, not by routing or custom method codecs.
The router does not select either one.

The supplied typed service is prepared and called on the same instance.
Readiness, including readiness of an outer routed service, uses the runner's
same bounded waiter accounting and original deadline. No additional unbounded
queue or independent handler task is introduced. Decoding/encoding and user
work remain outside the socket driver.

`one_way` applies the same live-state and edited-header validation using the
original ordinary one-way admission, rejects notification/request contexts,
decodes once, calls a typed service returning `()`,
and produces `IngressResponse::NoReply`. It does not construct an encoder or
impose bounds on unused encode types. Successful handler completion is not a
semantic ACK; domain processing and an authorized send remain explicit.

Header-aware decoding uses these same unary/one_way methods. Normal decoders
read current headers through DecodeBuf::headers(), only after original
invocation admission and input-byte limits. They pass the actual typed body to
the service without header capture/cloning. Unary can compose its two real
directions as actual values; one-way uses only a decoder.
Metadata cannot alter original routing permissions or create authority.
The reply encoder also sees the typed reply's current headers through EncodeBuf.
See [header-aware adaptation](Codecs.md#header-aware-method-adaptation).

The methods return response data only. They never call `Peer::send`,
mint a grant, or insert reply correlation. The runner retains the original
ticket and alone submits an authorized reply. Missing/stale/wrong-kind
invocation context rejects before decoding or calling user code. Preserve
readiness, codec, handler, and encoding error phases/sources; no automatic
native Fault or application error response is generated.

`Reply<T>` supplies a body and ordered headers through generic `append_header`.
Appending is fallible under native representation limits; scope-specific
policy is checked on submission. Actor/Action values are explicit domain
headers, never inferred from request names.

## Requests, responses, and reply authority

The shared [`Request<B>` envelope](Requests.md#request-envelope)
serves both outbound calls and inbound service dispatch. Services receive
Request<Body> for ordinary messages and notifications; typed handlers receive
Request<Q>.
There is no separate `ServerRequest` type or context handler argument.
Headers and optional received context are accessed through the request.
`Body` is the existing shared segmented native body, not an HTTP stream.

```rust,ignore
pub enum IngressResponse {
    Reply(Message),
    NoReply,
}
```

The runner converts admitted ingress into a service request while retaining
the single-use reply/acknowledgment authority in its private dispatch ticket.
Every live inbound service request has immutable core-created context; public outbound
construction supplies none. Context is data and any independently authorized
send capability, not proof of reply authority. A custom service cannot
manufacture, duplicate, or redirect the runner's grant.

`into_parts`/`from_parts` and `map` preserve native headers and received
context during body adaptation. Middleware may separately edit
`Request::headers_mut` or public `RequestParts::headers` through bounded,
fallible collection operations. None of these operations creates a new
admission, changes the captured binding/correlation, or resets its deadline.
Trusted context stays private and read-only; only application metadata is
mutable. RPC-adapter decoding uses the existing bounded codec buffers,
moving the parts to `Request<Q>` without copying headers or adding a staging copy
of the encoded body. Decoded values use ordinary application ownership.
Decode failure retires the original ticket.

Tower services/middleware are trusted cooperative application code, not a
sandbox. Public request construction or reassembly cannot bypass the owning scope's
ingress admission, captured response policy, or original-connection reply
completion. Directly calling a service does not create a live completion
ticket. Received context has a private nonowning link to runner-owned invocation
state, including the actual admitted body budget and deadline. The router
checks liveness before service dispatch; the method adapter checks it before
decoding or invoking a typed service. Absent, completed, or cancelled state
rejects explicitly. The runner invalidates the state when its invocation
settles. This is internal dispatch bookkeeping, not a public
extensions bag, permission to retain admission, or a second reply ticket.

`IngressResponse::Reply` is data, not authority. The runner submits it through
the original grant, applying captured response policy, correlation, deadline,
and complete-local-write semantics. Reply headers supplied by user code cannot
override this association.

`NoReply` is successful only for delivery without a reply ticket. Returning it
for an authorized request or notification is an explicit missing-response or
missing-acknowledgment handler failure, not a fabricated acknowledgment.
Returning Reply without a ticket similarly
fails without sending. Dropping a failed/timed-out ticket sends no response.

`request.context()` exposes read-only source/deadline information;
its `RequestContext::peer` exposes only a capability independently granted
by the binding's `SendPolicy`. All of its send restrictions remain in force;
it is not a way around private completion tickets. The service context does not
offer a source-to-peer lookup.
Constructing response data uses standalone `Message::builder` and header
constructors; it needs no context or live scope owner. The runner applies its
actual scope policy and original grant during response submission.

Services cannot extract the runner's ticket: they return
IngressResponse data and the runner completes it on the original generation.
Normal server drain includes those service responses; Channel last-owner
release cancels ingress work rather than creating a client drain API.
The separately classified [native notification path](Actors.md#native-notifications)
uses this same runner; it exposes no public receiver or acknowledgment grant.

## Application-owned work queues

Pull-style application processing is ordinary service composition: put the
admitted Request into a bounded application queue and return a future waiting
for that worker's response, for example through a per-request Tokio oneshot.
The worker returns `Result<IngressResponse, E>`; it receives no completion ticket or
authority to transmit a native reply itself. The service future returns that
result, and only the original runner completes correlation and output.
There is no core ticket transfer/disarm path for either traffic class.

The original absolute invocation deadline, response-size policy, context, and
shared storage ownership remain in force across enqueue, worker processing,
response waiting, and reply completion. The application owns queue capacity
and any readiness/concurrency admission policy. Queueing preserves
Request parts/context; it does not reconstruct a request or renew admission.
Application queues/workers may keep byte storage alive after timeout or
cancellation; this does not keep transport queue credit occupied.

The application owns the finite queue and workers; furpc creates no queue or
forwarding task for it. Keep workers running while admitted service futures
need them, and explicitly coordinate their shutdown. Workers should check
the original context deadline and whether the response waiter is closed before
starting stale work. Handler timeout, connection retirement, or scope
cancellation invalidates the invocation and abandons its response wait; late
worker output cannot revive the private ticket or send a second reply.
Cancellation cannot undo work already performed, and failed/closed queue or
response channels must surface as service errors rather than success fallbacks.

For one-way protocols with independent later ACKs, bounded domain queue
admission may itself be the complete handler outcome NoReply; it is not proof
of application completion. That separately owned processing and its actual
ACK policy follow the [replication boundary](#replication-service-admission).

## Consumer execution policy and invocation deadlines

### Handler limits

```rust,ignore
#[derive(Clone, Debug)]
pub struct HandlerLimits {
    pub max_encoded_body_bytes_per_response: usize,
    pub timeout: Duration,
}

impl HandlerLimits {
    pub fn validate(&self) -> Result<()>;
}
```

`HandlerLimits` implements `Default`. These are explicit initial Rust-host
defaults, not native SF constants, hard wire limits, eager allocations, or
performance qualification. [Route configuration](Routing.md#routing-limits)
validates keys/conflicts without another construction-only quota.

These timeout/output limits configure all installed user-service ingress in one logical
Channel or server scope as a whole, not independent per-clone, per-route, or
per-generation pools. Endpoint configures them for dialed ingress; ServerBuilder
configures them for accepted ingress. Route options supply native ingress/send
policies, not additional handler timeout or allocation limits.

| Handler field | Default | Meaning |
| --- | --- | --- |
| `max_encoded_body_bytes_per_response` | Native frame body capacity | Optional caller-selected encoded-output restriction, not backing capacity or a heap quota. The full frame must still fit after headers. |
| `timeout` | 30 seconds | One total service-invocation budget, not a timeout restarted for each phase. |

Furpc imposes no handler concurrency or service-readiness waiter cap. Consumers
can wrap their service with Tower concurrency/load-shedding middleware or use
their own bounded queues and admission state. Shared limits require shared
consumer middleware state; furpc does not manufacture a quota per route,
connection or clone. Pending readiness retains the same prepared service and
remains subject to the original invocation deadline and cancellation.

MiB means a power-of-1024 unit. Byte arithmetic is checked; overflow is
InvalidInput. Server build and Endpoint connect validate timeout/output policy
before starting ingress: timeout must be positive and fit the monotonic clock
range. A zero encoded-body cap permits
empty output. Decoded Rust allocations are not tracked.

Explicit per-invocation output limits cannot widen native frame or send-queue
policy. They are not a total-frame allowance: native headers and the fixed
header must still fit. No aggregate retained-allocation guarantee is made,
and custom codecs/handlers own their heap usage and cooperative execution.

### Invocation lifetime

`HandlerLimits` supplies a positive finite handler timeout. Its one absolute
deadline starts at service-ingress admission and includes body acquisition,
queue/readiness delay, decoding, handler work, encoding, and reply submission.
Store it in the request's private received context; `Request::context` exposes
the read-only `RequestContext::deadline`. Body mapping, reconstruction, and
middleware must not restart it.

A protocol-specific timeout header may narrow this budget through explicit
domain policy. It does not silently select infinity or control every raw
protocol. Partial-frame and committed-write watchdogs remain separate.

Consumers must check their own ingress/handler capacity before accepting work.
Furpc adds no router inbox or consumer worker queue. Ordinary ownership follows readiness, decoding,
handler state, and completed-but-unsent responses, without allocation charges.

Service readiness and body/handler code run outside the socket driver.
Pending service readiness must not stop unrelated native control/reply
processing. Transport send/correlation guards remain independent. This does not eliminate
TCP head-of-line blocking behind a frame already arriving.

Body codecs use the existing bounded buffers. User handlers and custom
serializers remain trusted cooperative code; type bounds cannot cap arbitrary
private allocations or preempt blocking CPU work. The owning Channel/server
scope tracks the returned service future and response path, not every task the application spawns.
Application child jobs require their own cancellation/ownership; spawning a
job does not enroll it in server drain. Use an explicit domain scope for
longer-lived external processing, and await the processing promised by the
handler's result before returning. A one-way replication NoReply can mean
only domain queue admission, not completion of the replicated operation.

Timeouts retire the ticket once and dispose cooperative handler work. They do
not undo application side effects. An already committed reply retains its
writer and native send-queue occupancy until completion or irreversible connection failure.

### Replication service admission

Use the [native flow-control split](Resources.md#replication-flow-control-boundary).
The replication-framework service in fube-data, not the user application,
dispatches ACK/RequestAck outside its replicated-operation queue. Its readiness
must not wait for space in that data queue: doing so would also block control
messages before their Action can be examined.

For replication data, validate and try to admit the operation into a bounded,
replication-owned queue, then promptly return IngressResponse::NoReply.
Do not retain a live handler invocation while waiting for later state-provider processing,
application acknowledgment, or quorum completion. The domain owns that work,
ACK timers, and retry. It may retain shared storage and an independently
authorized original-generation Peer for later semantic sends, but not a live
invocation ticket. Reference counting keeps retained storage alive after the
handler returns without transport allocation charges.

If the operation queue is full, an explicit native-compatible domain policy
can diagnose/drop the unadmitted one-way data without falsely advancing
acknowledged progress. NoReply then settles only that no-wire-reply handler,
not the operation or an ACK. ACK/RequestAck can still report truthful progress
for earlier admitted operations. This is a declared overload outcome, not a
catch-all conversion of malformed input or arbitrary service errors to success.

Generic frame, byte, send-queue, invocation-time and response-size guards still apply. The framework
must quiesce its deferred processing and required ACKs before owner shutdown.

## Errors and shutdown

Routing, readiness, decode, handler, encode, and response failures retain phase,
source, and bounded route/peer diagnostics. They are distinct from malformed
native framing or controls. Handler failures are normally call-local and
recorded without terminating unrelated connections.
Peer-ready readiness/call failure instead retires its accepted connection
under the [hook contract](Connections.md#accepted-peer-readiness-hook);
it is not a request error that can be ignored to continue a failed bootstrap.

No automatic native Fault or domain error reply is synthesized from an
arbitrary Rust error. A domain can deliberately return a typed error payload
or raw response matching its protocol. Native RPC Fault completion on the
receiving side remains unchanged.

On physical generation failure, invalidate and cancel its unsettled ingress
invocations and original reply tickets; do not replay them on a replacement.
Channel retains only the configured service/registry for later-generation
messages. Completed results are not reclassified. Active/retiring work and
shared storage retain their normal owners; HandlerLimits belong to the same
logical runner. A service configuration being installed does not itself
redial an idle disconnected Channel.

Ingress services and their runner futures retain no public Channel owner
guard. Last-owner release closes inbound admission and cancels outstanding
ingress readiness/handlers with the rest of the logical scope; it neither
flushes nor joins them. No service registration handle, second client owner,
or public client shutdown controller is added.

Graceful server shutdown must include the service scope:

1. Stop acceptance and new application dispatch, while continuing native
   controls and RPC/notification acknowledgment completions needed by admitted work.
2. Drain already-admitted readiness/handler work within the original request
   deadlines capped by the single server drain deadline.
3. Permit the retained tickets' original replies despite closure of new general
   submission. Context-derived Peer handles do not exempt new sends,
   requests, or notifications from that closure.
4. Drain resulting output and await active connection/service completion.
   Expiry cancels remaining work and returns a shutdown-drain timeout without
   promising every background task has exited.

The private ticket is the bridge between admitted work and allowed completion
traffic. Context does not carry a separate drain-time send capability.
Application queue workers remain separately owned: keep them available for
admitted service responses during drain, then stop/dispose them explicitly.
Deferred one-way processing needing new semantic sends
must quiesce before signaling shutdown if those sends must complete.
Admitted notification acknowledgments instead complete through their original
runner tickets under the same drain rules as ordinary replies.

Normal serving completion waits for admitted service work and responses,
not merely accept-loop exit. Dropping serving or returning a serving error
does not guarantee joined destruction of all background work; follow the
[cancellation rules](Lifecycle.md#cancellation-and-drop).
Handler-returned errors remain invocation-local and observable through their
error paths and bounded diagnostics; they do not poison normal serving
completion. A fatal failure observed by the serving supervisor returns through
its `Result<()>` with its cause. Unexpected task failures must be diagnosed,
but no retained scope report collects every task result.

The [lifecycle contract](Lifecycle.md) owns the scope-level APIs, drop behavior,
and normal graceful-completion boundary. This section adds handler-specific
work to that drain; it does not create another shutdown controller.

[tonic-server-rpc]: https://docs.rs/tonic/latest/tonic/server/struct.Grpc.html
