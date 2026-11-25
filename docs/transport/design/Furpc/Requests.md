# furpc: Request envelopes and RPC

**Status:** proposed; see the [design summary](README.md).

Defines the shared `Request<B>` envelope, typed RPC/one-way clients, correlation, and
reply authority. [Codecs](Codecs.md) own body representation, while
[services](Services.md) own inbound handler completion.

## Request envelope

```rust,ignore
impl<B> Request<B> {
    pub fn new(body: B) -> Self;
    pub fn append_header(self, header: HeaderRecord) -> Result<Self>;
    pub fn headers(&self) -> &Headers;
    pub fn headers_mut(&mut self) -> &mut Headers;
    pub fn deadline(&self) -> Option<Instant>;
    pub fn set_deadline(&mut self, deadline: Instant);
    pub fn context(&self) -> Option<&RequestContext>;
    pub fn body(&self) -> &B;
    pub fn body_mut(&mut self) -> &mut B;
    pub fn into_body(self) -> B;
    pub fn into_parts(self) -> (RequestParts, B);
    pub fn from_parts(parts: RequestParts, body: B) -> Self;
    pub fn map<T>(self, f: impl FnOnce(B) -> T) -> Request<T>;
}

pub struct RequestParts {
    pub headers: Headers,
    pub deadline: Option<Instant>,
    context: Option<RequestContext>,
}

impl RequestParts {
    pub fn context(&self) -> Option<&RequestContext>;
}

impl RequestContext {
    pub fn source(&self) -> &Source;
    pub fn peer(&self) -> Option<&Peer>;
    pub fn deadline(&self) -> Instant;
}
```

`Request<B>` is furpc's generic application request envelope, not an HTTP
request and not a second wire-message format. `B` is the current body
representation, with no universal body trait or Serde bound:

| Boundary | Request body |
| --- | --- |
| Typed outbound RPC, one-way send or native notification | `Request<C::Encode>`, before body encoding. |
| Client transport service / Channel / original-Peer adapters | `Request<Body>`, after application encoding and before native submission/correlation processing. |
| Channel/server ingress service and router entry | `Request<Body>`, containing the admitted native message's shared, segmented bytes. |
| Typed request or one-way handler | `Request<C::Decode>`, after its selected body codec. |

`Request::new(body)` creates an outbound envelope with empty headers and no
received context, following http/tonic's body-first request shape. Native Actor,
Action, and other domain headers use generic `append_header` with the
[native header constructors](Messages.md#raw-messages-headers-and-sending);
there are no Actor/Action arguments or dedicated setters on the envelope.
Appending delegates to the same bounded header operations as `headers_mut`
and is allowed on both outbound and received requests. Middleware can append,
remove, or explicitly replace application metadata without discarding context.
Full native-header encoding, size, managed-field, and send-policy checks still
run under the selected connection's context before commitment. Construction
performs no network I/O or driver admission. A received request reused as
outbound data is subject to all the same submission checks; its context cannot
supply new send authority, change the destination, or override the explicit
call deadline.

The optional `deadline` is local client-call metadata, not a native header or
trusted received context. `new` leaves it None; `set_deadline` and public
`RequestParts::deadline` let direct byte-service callers and middleware carry
an absolute deadline through standard Tower calls. RPC uses the earlier of
this value and its explicit call deadline. Middleware may shorten it but must
not reset or extend an enclosing call's budget. On ingress, the runner's
original deadline remains authoritative regardless of edits to this metadata.
Service ingress leaves local client-call metadata None; method adapters use the
private context deadline, not this field. Forwarding received data through a
new client call still requires that call's explicit deadline.
The byte-facing client service requires Some; missing deadlines fail
InvalidInput instead of selecting an unbounded wait.

The runner constructs `Request<Body>` from admitted ingress without copying
the native body or routing it through another inbox. Request headers are an
editable ordered native collection, preserving duplicates and unknown records
unless explicitly removed. Individual encoded records remain immutable;
editing collection metadata must not change another request or raw message
sharing the original storage. Lookup retains the first-visible/effective
distinction and recomputes derived properties after edits. They are not an HTTP
HeaderMap, and there is no request-line, URI, method, or HTTP version.
Endpoint's URI selects byte I/O only; it is not a request field and its path
does not become native Actor/Action routing.
An ordinary Actor header is not mandatory for a request envelope or a raw
server binding. Explicit Actor/Action routing may require a matching key;
custom raw dispatch instead interprets its own domain headers. Both paths
retain the original admitted binding and independent native reply authority.

Mutable headers are application data, not proof of admission. Before invoking
middleware, the runner captures the original native classification, admitted
binding/route identity, exchange kind, source, correlation, response/send
policies, resource allowances, and deadline in private invocation state.
Header edits cannot change them, repeat ingress authorization, mint a reply
grant, or turn a one-way invocation into a request. A changed Timeout header
does not extend the admitted deadline. Furpc dispatch/adaptation boundaries
validate edited headers under the original scope before using them;
malformed records and exceeded native/configured limits
return explicit errors. Routing remains within the
[original admitted binding](Routing.md#matching-and-unknown-routes).

Only core ingress can create `RequestContext`. On either Channel or Server's ingress-service path,
`context()` is Some and contains original source identity, the independently
authorized optional Peer, and the effective absolute handler deadline.
Raw response construction uses `Message::builder`, not received context.
Outbound `context()` is None; absence is explicit, never a fabricated source
or received deadline. The context contains no public reply/acknowledgment ticket, admission permit,
or lifecycle owner. No public context setter, mutable context view, or source-to-peer
lookup is provided.

Internally, received context carries a nonowning link to the runner's live
invocation state. The runner and RPC method adapter use it to check the original
deadline and access the actual admitted body budget, including limits configured
after service construction. The router checks liveness and dispatches bytes,
without accessing application-body serialization state.
The runner owns admission, response completion, and state
invalidation on completion/failure/cancellation. Retaining context does not
keep that state live or create another permit.

Like [`http::Request`][http-request], `into_body` consumes the request and drops
its metadata; `into_parts` transfers the headers, local deadline, context, and
body separately. `RequestParts::headers` is public, like
`http::request::Parts::headers`, while
its context remains private with no public constructor or setter.
`from_parts` reassembles those parts with another body without reclassifying,
reauthorizing, or renewing the invocation. Callers may edit `parts.headers`
through its bounded operations before reassembly. `map` changes only the body,
preserving all current metadata and context. Fallible codec adaptation uses
`into_parts`/`from_parts`, with errors retiring the original invocation.
`body_mut` edits the body; neither it nor `headers_mut` exposes mutable trusted
provenance. Replacing or swapping header collections never transfers context,
admission, or reply authority with them.

Request/parts do not initially implement `Clone`. Moving them preserves
shared backing storage and private context. Body adaptation uses the existing
codec buffers without adding allocation accounting.
Arbitrary application allocations remain the application's responsibility.
Retaining parts or a context-derived Peer cannot extend the invocation
deadline, revive its private completion ticket, or hold graceful drain open.

There is no public `ServerRequest` wrapper or alias and no separate
`RequestContext` handler parameter. Message remains immutable raw data.
All application ingress, including native notifications, uses `Request<B>` and
runner-owned reply/acknowledgment authority, including
through application work queues. `Response<T>`, `Reply<T>`, and `IngressResponse::NoReply` keep
their distinct existing roles. A generic extensions bag, extractor framework,
and streaming-body trait are not required for this initial envelope.

## RPC calls and responses

```rust,ignore
impl Peer {
    pub fn request(
        &self, request: Request<Body>, deadline: Instant,
    ) -> Operation<Response<Body>>;
}

impl<T> RpcClient<T> {
    pub fn new(transport: T) -> Self;
    pub fn codec_limits(self, limits: ClientCodecLimits) -> Result<Self>;
    pub fn into_inner(self) -> T;
    pub fn get_mut(&mut self) -> &mut T;
}

impl<T> RpcClient<T>
where
    T: tower_service::Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    pub async fn call<E, D>(
        &mut self, request: Request<E::Item>, encoder: E, decoder: D, deadline: Instant,
    ) -> Result<Response<D::Item>>
    where
        E: Encoder + Send,
        E::Item: Send,
        D: Decoder + Send,
        D::Item: Send;

}

pub struct ClientCodecLimits {
    pub max_encoded_body_bytes: usize,
    pub max_response_body_bytes: usize,
}

impl<T> Response<T> {
    pub fn new(body: T) -> Self;
    pub fn headers(&self) -> &Headers;
    pub fn headers_mut(&mut self) -> &mut Headers;
    pub fn source(&self) -> Option<&Source>;
    pub fn body(&self) -> &T;
    pub fn body_mut(&mut self) -> &mut T;
    pub fn into_body(self) -> T;
    pub fn into_parts(self) -> (ResponseParts, T);
    pub fn from_parts(parts: ResponseParts, body: T) -> Self;
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Response<U>;
}

pub struct ResponseParts {
    pub headers: Headers,
    source: Option<Source>,
}

impl ResponseParts {
    pub fn source(&self) -> Option<&Source>;
}
```

`Peer::request` is the explicit native byte-RPC capability: it captures the
request and earlier explicit/local deadline without doing I/O at construction.
First poll validates managed mode, captured request send policy, headers, and
actual connection/queue admission before any native transmission. Raw-only mode
fails Unsupported. It returns an owned native Operation with an Attempt;
unlike generic RPC, it performs no application encoding or decoding. It neither
implements Tower Service nor owns the driver. Dialed native callers obtain it
from `Channel::peer(deadline)`; accepted callers require an independently authorized
Peer from received ingress or the
[peer-ready hook](Connections.md#accepted-peer-readiness-hook).
Keep the original Channel or server execution alive.

## One-way client and Peer service adapters

**Status:** implemented. RpcClient supports request/reply exchanges, OneWayClient
adapts typed one-way sends, and Peer::send still accepts an already-built Message.
Runtime's typed RapClient now uses OneWayClient over its SF policy middleware
and PeerSendService; HostingClient uses PeerRequestService.

### Typed one-way client

The optional `OneWayClient<T>` is parallel to RpcClient, over an explicitly
one-way byte-facing Tower service. This is codec composition above native sends,
not a new connection owner or a request/reply method disguised as a send.

```rust,ignore
// Implemented API.
impl<T> OneWayClient<T> {
    pub fn new(transport: T) -> Self;
    pub fn max_encoded_body_bytes(self, limit: usize) -> Result<Self>;
    pub fn into_inner(self) -> T;
    pub fn get_mut(&mut self) -> &mut T;
}

impl<T> OneWayClient<T>
where
    T: tower_service::Service<Request<Body>, Response = WriteReceipt> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    pub async fn send<E>(
        &mut self, request: Request<E::Item>, encoder: E, deadline: Instant,
    ) -> Result<WriteReceipt>
    where
        E: Encoder + Send,
        E::Item: Send;
}
```

The adapter consumes an encoder value per send and uses only that direction. There
are no bounds on unused decode types/state and no response-body limit. The
default encoded-byte allowance is native body capacity; explicit restrictions
validate before use. It uses the
[public body helpers](Codecs.md#public-body-helpers), preserves
request parts/ordered headers, and stamps the earlier of the request-local
and explicit absolute deadlines into the byte-facing request.

One unchanged deadline covers encoding, readiness and local-write observation.
Prepare and call the same mutable byte-service instance; never clone after
readiness or duplicate its reservation. Construction and unpolled sends are
inert. Encoding failure occurs before submission and is call-local. No response
decoder, pending RPC entry, ExpectsReply flag, RelatesTo, notification
acknowledgment or semantic reply is introduced.

Success returns the native WriteReceipt: complete local transmission, not peer
receipt or domain acknowledgment. Preserve its written_at timestamp rather
than treating delayed observation of an on-time write as a late native result.
Middleware forwards receipts unchanged; there is no public constructor for
fabricated native receipts.
Like RpcClient, the ordinary async future exposes no native Attempt. Dropping
it abandons the wrapped future, not a confirmed retraction or remote rollback;
committed output remains driver-owned. Callers needing explicit attempt
disposition keep using Peer::send with a body encoded separately.

Frozen retry bodies can continue through the byte service directly without
re-encoding. Do not add a replaying send method or make OneWayClient own a retry
cache, reconnection policy, or application acknowledgment worker.

### Explicit original-Peer services

`PeerRequestService::new(peer)` and `PeerSendService::new(peer)` construct
explicit adapters from an already-authorized Peer:

| Adapter | Tower boundary | Native operation |
| --- | --- | --- |
| PeerRequestService | Service<Request<Body>, Response = Response<Body>> | Peer::request |
| PeerSendService | Service<Request<Body>, Response = WriteReceipt> | Peer::send after native Message assembly |

Both require the byte request's absolute deadline; absence fails explicitly.
Preserve its headers and body, apply normal native managed-field/send-policy
checks, and propagate the original operation result and error source. The send adapter rejects MessageId, RelatesTo, ExpectsReply and UncorrelatedReply
records even in raw-only mode, rather than reinterpreting them or manufacturing
correlation. Direct raw Peer::send retains its caller-owned-ID contract.

The adapters retain the captured Peer, not an owning Channel or an Endpoint.
They never reconnect, acquire a replacement generation, retry or replay.
Clones copy only that same nonowning capability and configuration. They create
no private admission semaphore, queue or background task; native operation
admission remains where it is today. Peer itself stays byte-facing and does
not implement Service, and Channel remains the reconnecting logical transport.
Readiness is immediate and reserves no native operation capacity; the underlying
Peer operation performs its existing admission when first polled.
PeerSendService supplies the one-way client's native byte service.
PeerRequestService replaces runtime's former request-only PeerService adapter.
Peer::notify remains a separate
explicit operation and is not silently selected by either adapter.

SF actors/actions, IPC host/PID and timeout headers, allowed-action policy such
as runtime's outbound(), serving-authority checks, cleanup exemptions, and
ordinary versus reserved reply admission remain in consumer middleware.
A dedicated runtime RapClient composes OneWayClient over that middleware;
none of those domain rules belongs in furpc. Local-write success still does not
mean a successful RAP semantic reply, and no first-write authority fence is added.

## Response envelope

Follow [`http::Response<T>`][http-response]'s head/body ownership pattern,
not its HTTP semantics. `ResponseParts::headers` is public and editable through
the same bounded, fallible collection operations as `Response::headers_mut`.
Source remains read-only. The parts sketch above omits additional private
native settlement fields; there is no public constructor,
source setter, or mutable provenance view. Obtain synthetic parts by splitting
`Response::new(body)`. Responses and their parts do not initially implement
`Clone`.

`Response::new` creates an application/custom-service response with empty
headers, no source, and no native settlement receipt. Only native ingress can
originate source and settlement metadata; source is Some for native Channel/Peer
responses, None for synthetic ones. Moving existing parts preserves that
metadata but cannot manufacture it.

`into_parts` consumes the response and separates its head and body;
`from_parts` reassembles those parts with a possibly different body type.
`map` changes only the body. All three preserve current headers, private
provenance and original native settlement timing.
Use parts for fallible body conversion, propagating conversion errors rather
than constructing a successful fallback. Neither reassembly nor header/body
edits rerun native Fault classification, change correlation, or create send or
reply authority. Provenance describes the original exchange, not authentication
of application-transformed headers or bodies. There are no HTTP status/version
fields or unrestricted extensions bag.

The returned `Response<T>` owns an ordinary decoded value. `into_parts` and
`from_parts` preserve metadata while moving that value; no hidden allocation
charge follows the parts. `into_body` returns the body and drops the head.
Shared byte views independently keep their backing storage alive.

`body_mut` provides exclusive body access without exposing mutable trusted
metadata. Body replacement and mapping grant no native send or reply authority.
On conversion failure, ordinary ownership drops parts and temporary storage
unless the caller retains them. Application allocations remain the
application's responsibility.

## Codec and call behavior

Follow [tonic's `client::Grpc<T>`][tonic-client]: `RpcClient<T>` owns a
byte-facing transport service, not a codec or native connection implementation.
`RpcClient<Channel>` is the default composition; Tower middleware or a custom
service can supply T without another client transport trait. Channel is the
only built-in client transport service; Peer is not interchangeable with it.
Its logical destination survives reconnect for later calls; the client need not
be reconstructed after each socket failure. Constructors do no I/O, readiness
polling, or codec work.

`RpcClient<T>` implements Clone only when T does, preserving codec-limit
configuration. Native transport clones never duplicate readiness reservations;
custom T implementations must uphold the same-instance service contract.
A generic transport needs neither Clone nor Sync nor a static lifetime. `call` borrows
the client mutably, like tonic; clone a cloneable client before starting
concurrent calls. Do not silently clone T inside `call`.

Each `call` consumes its own encoder and decoder and returns an ordinary async future, not
`Operation<T>`. One client supports unrelated body types and formats. The codec
and its encode/decode state need Send, not Clone or Sync, and stay in the
caller's future. First poll checks the effective deadline and encodes into a
bounded body, then awaits T's readiness under that same absolute deadline and
calls that exact ready instance. No codec work, admission, or submission occurs
for an unpolled call. Readiness can wait for Channel's one shared reconnect
attempt under this same call deadline; no encode or readiness stage restarts
the budget. Failure/expiry before transport submission drops the encoded body
without creating native correlation. A transient reconnect failure can be
delivered by T's call future after readiness, without native submission.

The transport exchanges complete `Request<Body>`/`Response<Body>` values.
The native Channel service validates current headers, enforces
send authority, assigns managed IDs, installs pending correlation, and
processes native Faults before returning a successful body. RPC adaptation
neither inserts native correlation fields nor creates another reply pump.
Custom transports must honor the same deadline and error contract; middleware
must preserve request metadata, response provenance/storage, and cancellation
rather than insert hidden retries or unbounded queues. A custom service may
produce a response without a native source; see below.

Channel binds each prepared native call to one physical generation. If that
generation fails, pending calls fail rather than being moved/re-encoded on the
replacement, even when they had not committed output. Old correlation entries
are not copied. Later independent calls on the same RpcClient can trigger
reconnection; this is not retry of the failed invocation, and domain code
decides whether retry is semantically safe. An already-ready old-generation
reservation also cannot silently retarget another generation.
For protocols requiring per-connection initialization, a domain-specific T
can acquire/initialize a Peer and submit through `Peer::request` on that exact
generation. Merely initializing a Peer before a separate Channel call would
race Channel reconnection; see [domain recovery](Connections.md#identity-and-domain-recovery).

The generic RPC adapter takes explicit encoder/decoder values. Native domain or
convenience methods supply FubstEncoder<Q> and FubstDecoder<R> when appropriate;
supported fubst schemas are required only for those calls. There is no
codec-bound client type, codec-selecting connection constructor, or codec
replacement method. T parameterizes transport, not serialization. Codec choice
cannot open another socket or create a competing reply pump. Domain success
and any required Actor/Action on replies remain the domain adapter's responsibility.

Client codec limits belong to `RpcClient<T>`, not T's hidden configuration.
`ClientCodecLimits::default()` adds no ceiling below native frame capacity.
Applications may explicitly narrow encoded request and response-input sizes;
zero permits an empty body. Check response input before decoding and encoded
output before submission. There is no decoded-allocation budget or mandatory
extra depth/work cap. The fixed native serializer's settings remain independent.

Generic client codec work is application-future-owned and cannot implicitly
acquire an arbitrary T's private transport state. Channel applies native frame,
send-queue and pending-operation policy at submission. Server and Peer notification
adapters retain their own execution/deadline rules. No aggregate heap quota or
allocation-budget handoff is implied; see the
[resource boundary](Resources.md#serialization-and-body-budgets).

`Request` exposes outbound domain envelope construction, not setters for
managed IDs, reply flags, or source identity. Encoding and decoding run outside
the socket driver. Native Fault completion precedes successful body decoding;
codec errors are call-local.

One absolute deadline covers encoding, readiness, native admission/write, and
reply receipt; it is copied into the byte request, not restarted after encoding
or at each middleware. Bound the service wait with that deadline and poll an
already-ready result before declaring timeout. A native response retains its
actual settlement time, so direct native calls settled on time do
not become late just because decoding/polling is delayed. Compare that native
receipt with the original effective call deadline, even if an intervening
service altered local deadline metadata. For a synthetic
response with no native receipt, service-result observation is the completion
boundary; observing it after the effective deadline returns Timeout before
decoding. Middleware that holds a result pending can itself cause a service-wait
timeout; it cannot promise transparent access to a hidden native completion.
User codec work remains cooperative, and decoding an on-time response does
not retroactively time out its network result. Check expiry before transport
submission and again in the native commit gate.
The shared reconnect attempt has its own finite establishment timeout. A
caller's shorter deadline only ends that caller's wait; it cannot lengthen
the request or cancel establishment needed by other Channel users.

Dropping the async RPC future drops its underlying service future. This
abandons the response, not remote execution; after native commitment the
driver retains output until completion or irreversible failure. Generic T
does not provide an `Attempt`, so this API cannot report confirmed retraction.
Use explicit `Peer::request` for a native byte RPC operation with an
explicit attempt observer.

Custom codecs use the [bounded buffer contract](Codecs.md#bounded-buffers-and-failures).
Arbitrary additional user allocations remain the codec author's responsibility.

## Correlation and native IDs

Install pending correlation and reserve its result capacity before output can
commit. Match only on the authorized connection generation and final effective
RelatesTo. Decode native transport Faults at this layer, including the
RejectFaultBody behavior and error details identified in the gap report.
Domain error envelopes remain above RPC.

Generation binding is an explicit Rust safety policy, stronger than the native
ordinary request table's sender-independent ID matching. It is not evidence
that a connection is authenticated. Channel logical identity is not a
correlation key: new generations get independent native connection state and
old replies cannot settle their calls.

The ordinary RPC API owns MessageId and ExpectsReply. Generate native
`GUID + u32 index` IDs from a process-scoped allocator. Rotate to a new GUID
namespace before index reuse; this is an explicit Rust rollover adaptation,
not a claim to reproduce native counter wrap behavior. IDs are not credentials.

A pending-only request table then needs no connection-lifetime tombstone set.
Completed IDs are not reused by the allocator; late replies are unmatched.
Caller-selected RPC IDs are excluded initially.

Raw-only compositions may support caller-supplied native IDs. Mixing
unrestricted caller-selected IDs with managed correlation on one connection is
unsupported until there is an explicit nonreuse contract. Managed raw sends
sharing an RPC connection must use its allocator when an ID is required.

The shared messaging core does not perform general ingress deduplication.
Repeated semantic work and notification pages belong to their domain protocol.
A private runner ticket authorizes one local response attempt for one accepted delivery,
not exactly-once remote processing.

## Notification and reply authority

Special uncorrelated-reply notifications need an independently authorized
origination API and their own correlated acknowledgment lifetime. They are
not enabled by removing raw-send flag guards. The
[Peer notification operation](Actors.md#native-notifications) parallels the existing
byte request operation, with explicit NotificationPolicy. It shares managed IDs,
pending-only correlation, source and settlement metadata, commitment/cancellation
semantics, and generation failure. Peer stays byte-facing.
Typed notification callers reuse RpcClient<T> over an explicit byte service
delegating to Peer::notify, forwarding the original absolute deadline and native
response unchanged. They use ClientCodecLimits and ordinary-future semantics,
not a second codec API or native Attempt. Generic RpcClient never implicitly
changes a Channel request into a notification; the supplied byte service
explicitly selects that native operation and policy.

Notification handlers use the same Request<Body>/IngressResponse service
boundary as ordinary handlers. `server::unary` accepts either original
replying kind; `one_way` accepts neither. IngressOptions/route options explicitly
select notification authorization before body retention. Their private ticket
captures the original notification MessageId, generation, acknowledgment policy,
native flags and deadline before editable request publication.

Only the runner may consume that ticket to submit acknowledgment data.
It cannot be forged from public headers, transferred into application queues,
or redirected through a replacement connection. NoReply, errors, dropped
work and cancellation generate no implicit acknowledgment. Admitted
acknowledgments drain like ordinary replies; retained request parts do not
keep a cancelled ticket live. This private single-completion contract is a Rust
adaptation, not ABI equivalence to reusable native receiver contexts.

[http-request]: https://docs.rs/http/latest/http/request/struct.Request.html
[http-response]: https://docs.rs/http/latest/http/response/struct.Response.html
[tonic-client]: https://docs.rs/tonic/latest/tonic/client/struct.Grpc.html
