# furpc: Implementation, migration, and evidence

**Status:** proposed; see the [design summary](README.md).

Maps existing gaps to owners and describes staged delivery, consumer
migration, acceptance, and unresolved qualification work. These are proposed
milestones, not a record of delivered implementation or qualified support.

## Current implementation

The crate now includes native frames/ordered headers, shared segmented messages,
incremental admission-gated framing, and a private clear-connection driver.
Initiating and accepted owned Tokio I/O use the same driver. It exchanges and
verifies ListenInstance before opening application submission/delivery, accepts
verified ListenInstance updates, and keeps transmit/receive wire contexts
separate. Clear transport does not authenticate the advertised address.

The driver owns cooperative preparation, scalar/vectored partial output,
queued serialized-frame bytes, precommit cancellation/deadlines, local-write
receipts, partial-frame/discard/committed-write watchdogs, and drain/abort.
Queue bytes remain charged until the whole unbatched frame is consumed or
removed, independently of surviving body aliases. Zero disables the native
queue-byte limit; disabling outgoing frame-size policy preserves the native
empty-queue exception. Pending-operation counts are explicit execution policy,
not construction or retained-allocation quotas.

One scope-owned poll loop consumes private verified-message/discard events;
there is no built-in application inbox, per-frame task, or second I/O owner.
The native classifier handles controls first, routes authorized native
notifications separately from ordinary ingress/correlation, diagnoses unmatched
replies and unconfigured delivery, and checks discarded-frame integrity. Unsupported
security/send-target controls and transport actions fail explicitly.
Send expiry/cancellation retracts only uncommitted output; a committed suffix
remains owned until full write or connection retirement. Drain rejects new
submission, flushes accepted output, and shuts down under one fixed deadline.

Deterministic I/O cases, backpressured in-memory exchange, and a loopback clear-TCP
exchange exercise this foundation. They are not native interoperability
qualification.

The separate `fube-client::FurpcClient` now qualifies native Ping through this
driver against a fresh Service Fabric 11.8.121.4 onebox. The existing baseline
test runs three overlapping pings on one Channel and compares complete gateway
identities with fresh independent HTTP observations and the legacy native client.
Its required `furpc_ping` report records all three results' successful comparison.
This is initiating clear-TCP Ping evidence only; it does not qualify accepted-side
serving, notifications, TLS, or the rest of the gateway API. See the
[client slice](../../RustFabricClient.md#separate-furpc-backed-client).

The public facade supports managed byte RPC by default and explicitly selected
`ConnectionOptions::raw_only()` for caller-owned one-way IDs. Endpoint owns inert URI,
limit and service configuration; connect returns an owning Channel. Channel
clones share one retained URI-input Tower connector and demand-driven reconnect
attempt, without replay. Peer, Source, native Operation and Attempt values never
own that logical lifetime or retarget another physical generation.
Raw-only mode rejects RPC/readiness explicitly rather than enabling correlation.

The managed core allocates process-scoped GUID/index IDs, rotating the GUID
namespace before index reuse. Its generation-bound request tables contain only
pending entries, installed before queued output can commit. Request expiry is
driver-owned, not dependent on continued observer polling; cancellation,
generation retirement and successful completion remove correlation without
lifetime tombstones or replay. Replies are matched by final effective RelatesTo,
admitted before body storage independently of HandlerLimits, and settled only
after complete-frame integrity. Unknown/late replies are diagnosed/discarded
without a consecutive-reply terminal cutoff.

`Peer::request` returns an inert native Operation and Channel is the sole
`Service<Request<Body>, Response = Response<Body>>`. Each ready Channel instance
owns one generation-bound operation/result reservation; clones copy neither a
ready permit nor a failed-attempt outcome. Transient reconnect/capacity outcomes
are delivered by the next call without native submission. Byte-service calls
require local deadline metadata and retain a Channel owner guard, while native
Peer operations remain nonowners. Response parts/body mapping preserve original
source and settlement metadata; synthetic responses have neither. On-time native
settlement remains on time when observed after the deadline or generation exit.

Native nonzero Faults are returned as RemoteFault with header/effective codes,
raw UTF-16 diagnostic text and the shared received body. As in native RequestTable,
a valid RejectFaultBody overrides error details only when its message is nonempty;
malformed bodies retain the header failure and expose their decode error rather
than creating a successful empty response. Application bodies are never decoded
by this layer.

Application-body adaptation is now available above the byte service through
public Codec/Encoder/Decoder traits, fallible segmented EncodeBuf/DecodeBuf
buffers, and FubstCodec. Custom codecs need no Serde, Clone or Sync bounds.
Encoding can adopt shared body segments without copying; decoding must consume
the whole body. FubstCodec uses the shared native Serde implementation, borrows
contiguous input directly and copies fragmented input only when needed by that
implementation. Its optional byte/depth settings are independent of native
header/control/Fault decoding and add no default depth/work ceiling.

RpcClient<T> adapts any conforming Tower byte service, borrowing the exact
transport for readiness and call without cloning, retry or replay. It consumes
one codec per call and preserves editable envelope parts and native provenance.
One original absolute deadline covers encoding, readiness and receipt; native
settlement time remains authoritative when an on-time result is observed late.
Synthetic results use observation time. ClientCodecLimits optionally narrow
encoded output and response input, default to native body capacity, and allow
zero for empty bodies. Codec errors retain encode/decode phase, original source,
and resource-limit classification without becoming connection-protocol failures.
No decoded-allocation ledger or transport-budget handoff is introduced.

Typed ingress methods now use `server::Rpc<C>::unary` or `one_way` inside the
same byte-facing services. `Reply<T>` supplies response data and ordered headers;
only the existing runner owns reply tickets, correlation and original-generation
submission. Adapters validate live core-created context, original exchange kind,
current native headers, and the admitted input/frame policy before decoding.
They use the actual scope response-output limit and shared readiness-waiter
capacity rather than independent method pools or construction-time defaults.
One-way adaptation never constructs an encoder or requires unused encode
types/state to be Send.

Invocation context retains only a weak link to private state, not handler/reply
permits or a scope owner. Queued application workers preserve the original
deadline and are notified when the owning runner retires their invocation.
Typed readiness/handler futures are dropped on that cancellation or deadline;
no method task or work queue is created by the crate. Application codec work
remains trusted and cooperative. Admitted typed replies can complete during
graceful drain without a general-purpose Peer.

TCP/custom connectors, Accept, TCP/stream acceptors and
`Server::serve_with_shutdown` use the same physical engine. Canonical
IngressService bindings admit bounded work at the header gate, before body
storage, and run readiness/handlers outside the driver. Header authorization
and HandlerLimits are shared by all connections in a server or generations in
one Channel. Handler deadlines begin at that admission, including partial-body
waiting. Dynamic Channel service installation survives reconnect. Received
Request parts preserve their original read-only source/deadline/context through
header edits and body mapping. Only an independently configured SendPolicy
supplies a source-bound Peer; policy is rechecked at the commitment gate.
`IngressOptions::requests(RequestPolicy, ...)` separately authorizes request and
response envelopes. Private runner-owned tickets capture the original request
ID, generation, response policy and deadline before editable Request publication.
Only those tickets can send IngressResponse::Reply; missing responses and replies
without a ticket are diagnosed handler failures and generate no automatic Fault.

Ordinary ingress now selects one explicit raw binding or multiple disjoint
whole-Actor/exact-action bindings. Header-stage matching uses sorted native
Actor/UTF-16 Action keys before body retention, policy checks, handler permits
or reply tickets. Unknown actors/actions are diagnosed and discarded with full
frame integrity rather than becoming connection-fatal errors or falling back
to raw delivery. Correlation/control classification remains earlier and separate
from application routing, including in raw-only mode.

The codec-free Router installs immutable exact-action bindings and distinct
RequestRouteOptions/OneWayRouteOptions. Each invocation retains its original
private binding and route identity: edited keys and independently configured
router tables cannot borrow another route's permissions. Whole-Actor services
may rewrite Action within their original Actor; explicit raw services remain
actor-less when appropriate. Routed and typed services share the existing
readiness allowance, original deadline and runner-owned reply completion.
Configuration rejects reserved control actors, duplicate/overlapping bindings
and raw/Actor mixtures without adding route-count or action-storage quotas.
Router clones share configuration, not prepared services; selected services
are cloned before readiness and called on that same instance.

The optional accepted-peer hook runs once after establishment, with its own
timeout and no handler-slot dependency. Shutdown drops acceptance, rejects
new sends/dispatch and waits for admitted handlers, hook work, pending RPC and
output under one drain deadline. Native shutdown is held until original-path
replies finish; general Peers have no drain exemption. Reply count credit is
reserved from handler capacity and follows the driver's queued frame after
commitment, independently of a timed-out handler or dropped observer. Native
queue-byte accounting still applies. Finite accept-source exhaustion is not a
shutdown signal. Dropping initial connect or the last Channel owner cancels
owned work; retaining Endpoint configuration or a Peer does not keep it alive.
Runtime errors are cloneable and retain original sources, so Error is no longer
Copy. Automatic diagnostics are rate-limited static categories/phases, not
arbitrary handler/connector error text or custom URI paths/queries.

Current TransportLimits defaults use native u32 frame capacity, a disabled
queue-byte limit, and no retained-allocation accounting. Execution defaults are
explicit Rust policy: 1,024 pending sends, 1,024 pending RPC/results per logical
scope, 128 server connections, 16 simultaneous
handshakes, 128 peer waiters, 64 KiB/32-step cooperative turns, 30-second frame
and committed-write watchdogs, and a 5-second discard watchdog. Connect, server
handshake, peer-ready and server drain default to 30 seconds; HandlerLimits
retain their documented 8 in-flight/4 readiness-waiter/30-second defaults.
These are not native constants or performance qualification.

Native notifications are implemented through byte-facing Peer::notify,
NotificationPolicy, IngressOptions::notifications, and
Router::notification/NotificationRouteOptions. They share managed pending-call
and result capacity with RPC, and keep native correlation installed before
commitment. Both initiating and accepted Peers can originate notifications
under their captured send policy; raw-only mode rejects origination.

Ordinary and notification bindings select raw or Actor dispatch independently.
Notification classification precedes ordinary RelatesTo lookup; native controls
still take precedence. Matched ACK policies run at header admission before
retention. A rejected ACK is diagnosed/discarded with full integrity and leaves
the pending operation awaiting an authorized response or expiry.

The existing service runner reserves notification handler/reply capacity and
owns the original acknowledgment ticket. IngressResponse::Reply(Message) or
typed unary Reply<T> supplies application-defined ACK data; only the ticket
supplies native RelatesTo and ordinary reply flags. Edited flags cannot change
the original route class or turn notification context into one-way authority.
NoReply, failure, cancellation and dropped work send no automatic ACK or Fault.
Admitted ACK writes retain the ordinary protected completion/drain behavior.
No public notification receiver/grant, hidden queue or extra execution pool
was introduced. Typed callers reuse RpcClient::call over an explicit byte
service forwarding Peer::notify and the original deadline.

Source qualification at the pinned native baseline covers BeginNotification,
DuplexRequestReply's special-first dispatch, and ReceiverContext::Reply.
In-memory exchanges cover both directions, typed ACK data, class-local routing,
policy rejection/commit checks, shared capacity, cancellation, Faults and drain;
independent native notification interoperability remains unqualified. TLS remains
unimplemented.

## Gap ownership and implementation order

| Existing gap | Primary owner in the new design |
| --- | --- |
| Fixed frame and serializer ceilings | `wire`, `fubst`, explicit resource configuration. |
| Duplicate common headers, Retry/Idempotent | Ordered header scan and common schemas. |
| Missing trailing defaults and unknown suffixes | `fubst` plus schema-specific defaults. |
| Lost correlated fault-body details | `messaging::request_reply`. |
| Lifetime ID history and unmatched-reply cutoff | Managed allocator and pending-only correlation. |
| Unknown-actor terminal behavior | Native demux plus separately named domain/resource policy. |
| Close discarding queued accepted work | Connection drain state and owned shutdown. |
| Missing notification origination | Authorized Peer notification operations and runner-owned service acknowledgments. |
| Body flattening and repeated copies | Immutable storage, segmented preparation, vectored writer. |
| Listener/connection/recovery duplication | `transport` supervision over Endpoint values, connector services, and Accept, with domain recovery above it. |
| TLS/claims and unsupported controls | Explicit driver security/control phases; TLS/claims implementation and qualification are deferred. |

Deliver the initial clear-TCP implementation in this order:

1. Establish the source-to-behavior inventory and ownership contracts. Record
   native rules, deliberate Rust adaptations, limits, and unsupported features.
2. Extend `fubst` and implement pure frames/headers/messages. Native
   header/control/Fault decode paths use one internal context derived from
   `TransportLimits`; no higher layer compensates for broken shared decoding.
   Add standalone message/header constructors with native width/schema checks;
   send/reply admission must still validate the actual scope policy.
   Add the public body-codec contract and default `FubstCodec` with separate
   body-specific operation budgets, not inherited native decode settings.
3. Implement a clear physical connection for both initiating and accepted
   sockets, with staged admission, receipts, segmented output, drain, and abort.
   Establish the security transition design early rather than retrofitting a
   `TlsStream` abstraction later.
4. Implement URI-based Endpoint configuration for destination, native options, finite
   limits, timeout, and reverse-ingress descriptors; default TCP connect,
   reusable connector injection through `connect_with_connector`, public Accept, TCP/stream adapters, and
   bounded raw listeners/connections; connect a replication adapter.
   Endpoint establishes an owning Channel through that same dialed engine;
   physical connections and supervision stay private. Expose Peer only for
   native/accepted-peer capabilities, never another client transport service.
   Add optional `on_peer_ready` Tower work with explicit SendPolicy so an
   accepted connection can initiate bootstrap before application ingress.
   Publish it after establishment/security release, not at native raw-accept
   timing, with bounded connection-owned execution outside the driver.
   Include coalesced demand-driven reconnect for later calls, without copying
   failed native attempts/correlation or granting new per-generation capacity.
   Demonstrate that raw consumers need neither gateway composition nor RPC
   and custom I/O does not require another connection engine.
   Reuse http::Uri and standard URI-input Tower services without HTTP framing,
   retaining original logical URIs across reconnect rather than pinning the
   first DNS result or introducing another public address enum.
5. Add native byte request/reply, the sole Channel client Tower service, and
   canonical IngressService execution through Endpoint::service,
   Channel::register_service, and ServerBuilder::service. Share native bindings,
   IngressResponse, HandlerLimits, and runner-owned reply completion across
   dialed reverse ingress and accepted connections. All application ingress
   exposes services only; bounded application work queues retain runner-owned
   replies/acknowledgments. Notification ingress uses explicit bindings within
   that same service, without a separate receiver or public completion grant.
   Include explicit raw ingress and optional Actor dispatch, notification
   origination, and gateway/IPC adapters. `RpcClient<T>` adapts any conforming byte service through normal
   readiness and mutable async calls, with bounded application-owned codecs
   and native admission at transport handoff. Peer notifications retain
   byte-facing native operations; typed notifications reuse RpcClient over an
   explicit notify byte-service adapter. Each typed call consumes its codec; domain convenience
   methods supply `FubstCodec`. Preserve domain/capability boundaries.
   Keep application dispatcher selection independent of managed/raw-only
   correlation mode. Qualify actor-less replication in initiating and accepted
   roles without synthesizing ordinary Actor headers or installing Actor 0.
6. Add the optional codec-free service router and separate `server::Rpc<C>`
   ordinary request/one-way and notification method adaptation inside byte-facing
   services. Qualify shared
   bounded readiness, handler lifetime/response completion, and graceful
   shutdown without forcing raw peers through typed adaptation.

TLS and claims implementation are deferred follow-ups, not a required final
step of the initial implementation. When scheduled, deliver each security
profile against its own native acceptance requirements and the phase design in
[security phases](Security.md#security-phases). Any later domain-recovery profile likewise needs its own native acceptance
requirements. Advanced handler ergonomics remain separate from the initial
minimal server service/router and are not required to retire the old raw core.

Vectored correctness and ownership belong in the initial writer. Multi-message
batching and tuning can follow without changing the public completion contract.

## Migration and acceptance

Add `furpc` as a new explicit workspace member when implementation begins.
Do not rename `fube-transport` in place or make the new core depend on it.
Move/reimplement shared responsibilities against this design; reuse proven
algorithms and source evidence without inheriting accidental policy.

Migrate consumers individually through narrow adapters outside the core.
An adapter may implement existing channel traits while its consumer migrates,
but their cancellation, close, ID, and error contracts must be reconciled
explicitly. Do not claim compatibility because method signatures match.
Keep old types and new types from circulating interchangeably inside one
connection/session.

Preserve independent gateway/notification owners, IPC host/PID and domain
admission, native replication operation-queue/ACK semantics, and original-source
ACK/reply affinity. Semantic replication ACKs and retransmission stay in `fube-data`;
runtime/RAP, service lifecycle, and transaction logic do not move into `furpc`.
See the [source-qualified flow-control boundary](Resources.md#replication-flow-control-boundary).

Acceptance evidence has distinct levels:

- Pure codec cases for native boundaries, duplicate semantics, schema evolution,
  malformed input, and native-permitted messages beyond old fixed ceilings.
- Private framing-contract cases for both admission gates, retained/discarded
  integrity, partial-frame EOF, segmented write cursors, and commitment
  boundaries, as specified in [framing contracts](Framing.md#required-evidence).
- Deterministic driver cases for partial I/O, cancellation/commit races, bounded
  retention, slow peers, drain/abort, and actual task/resource release. Internal
  cleanup tests do not imply a public post-drop retirement barrier.
- Consumer cases for raw replication, correlated requests, accepted-server
  traffic, IPC identity, and native notification acknowledgment without competing readers.
- Native-aligned replication service flow control: dispatch ACK/RequestAck
  independently of replicated-operation queues, keep service readiness free
  of data-queue-space waits, and return NoReply promptly after a bounded domain
  admission/drop decision instead of holding a handler until durable/quorum
  completion. Deferred operation processing, ACK scheduling, and retry have
  explicit domain ownership and shutdown. An intentional queue-full data drop
  is diagnosed and cannot advance acknowledgment for unadmitted operations;
  truthful ACKs for earlier progress remain valid. Shared buffers survive
  retention outside a completed handler. Exercise ordinary frame/queue-limit
  exhaustion with its documented error/drop/retirement outcome. Preserve internal native
  control, managed RPC-completion, and authorized-reply protection.
- Public header views/builders and raw/Actor-dispatched receive, including
  preserved unknown records, rejection of conflicting managed fields, bounded
  registrations, and no authority gained by setting Actor or RelatesTo.
  Exercise standalone construction without a facade/runtime, native-width encoding,
  shared backing ownership, sticky builder errors, and combined native-header
  limits including managed-field overhead. A successfully built message must
  still reject under stricter submission limits without committing output.
  Reused received storage stays shared through construction and submission.
  Verify body-first request construction, generic ordered Actor/Action
  insertion on messages/requests/replies, typed helper encodings against native
  schemas, and first-visible versus effective duplicate lookup. Action views
  preserve raw UTF-16; constructing a header does not select a handler.
- Optional application dispatch with a missing ordinary Actor header.
  Exercise raw Endpoint/Channel and accepted-server service paths
  using native classic ReplicationActor/ReplicationFrom/Action envelopes,
  including data and semantic ACKs. Preserve absent Actor records and native
  effective Empty semantics without inserting a header or requiring Actor 0.
  Domain endpoint demultiplexing remains bounded and outside the driver.
  Malformed recognized headers still fail; Actor-routed unknown actors retain
  diagnosed drops, and route misses do not fall back to raw ingress.
  Raw checks, body admission, integrity, captured source, response grants,
  and restricted original-generation ACK sends retain existing contracts.
  Verify managed RPC plus raw ingress and raw-only plus Actor dispatch to
  prove dispatcher selection and correlation mode are independent.
  Class-local raw/Actor mixtures, duplicate raw targets or Actor bindings,
  repeated service installation, and dispatcher replacement reject
  atomically. Cross-class raw/Actor composition is allowed without fallback.
  No configuration means no implicit raw catch-all. Notification bindings and
  on_peer_ready are independently authorized; native controls and
  unmatched replies never leak into application delivery. Check before-traffic
  service installation, binding/byte bounds,
  stable dispatcher selection/registry across reconnect, and no new queue,
  decoder, or reader beneath the existing admission path.
- Canonical ingress services on both Channel and Server, using the same raw
  and Actor/Router adapters without native ordinary receiver or manual receive
  loops, hidden forwarding workers, or another correlation table. Cover
  IngressResponse::Reply and NoReply with runner-owned original reply authority,
  actual admitted typed-codec budgets, and source-bound semantic sends.
  Endpoint configuration/unpolled connect must not poll service readiness or
  invoke handlers; setup installs before early reverse traffic. Its cloned
  service prototypes preserve Endpoint Clone + Send + Sync without requiring
  S: Sync, cloning a ready service, or polling a future under a lock.
  Separate explicit connects have independent runners/budgets even when the
  application intentionally shares service state.
  Test live/disconnected Channel registration, one explicit service per scope,
  atomic binding conflicts, and ownership of all Actor bindings by the one
  installed service. Dynamic installation cannot recover previously dropped traffic.
  HandlerLimits is one logical Channel/server allowance, not a fresh pool per
  clone, binding, or physical generation. Old-generation ingress futures and
  reply tickets cancel without replay while configuration survives reconnect.
  Inbound readiness/handler futures retain no public Channel owner guard;
  last-owner/failed-connect cancellation must retire unpublished/active work
  without adding a client drain API or post-drop join promise.
  Application work-queue composition preserves Request context, absolute
  deadlines, and shared storage while the runner retains reply authority.
  Enqueue/worker/oneshot waits stay inside existing bounded handler capacity;
  no second transport queue or waiting pool is created. Closed queue/worker
  channels surface as service errors. Timeout, disconnect, and shutdown races
  invalidate the original ticket; late output cannot resurrect it, forge a
  NoReply handoff, or send a duplicate response. Retained queued storage stays
  alive through ordinary references. Workers are application-owned and must remain usable
  during admitted service drain, then be explicitly stopped/disposed.
- http-shaped request decomposition and reassembly, including mutable headers
  on received requests and public parts headers while trusted context stays
  read-only. Exercise append, remove-all, explicit replacement, failed-edit
  atomicity, current duplicate lookups, copy-on-write metadata, and
  shared ownership without mutating raw messages. Repeated edits do not
  accumulate hidden tombstones. Header swaps and edited records
  must pass original-scope validation before routing/adaptation;
  edits cannot retarget an admitted route, change classification/correlation,
  mint a reply grant, widen policies/limits, or extend the original deadline.
- http-shaped response body mutation, decomposition, reassembly, and mapping
  for native and synthetic responses. Verify public bounded parts headers and
  read-only optional source, preserved original settlement timing, and
  explicit propagation of fallible body-conversion errors. Header/body edits
  must not change native Fault classification or mint provenance/reply authority.
  Splitting/reassembling responses preserves metadata and ordinary decoded-value
  ownership without hidden charges. Shared byte views retain their backing
  independently. Body replacement cannot silently grant native authority.
- Channel byte service composition beneath `RpcClient<T>`, including
  middleware and an external non-Clone/non-Sync transport that constructs
  `Response::new` without private Source access. Generic clients must not
  require physical connection access, a custom transport trait, or hidden admission
  handles. Verify conditional client Clone, mutable calls, same-instance
  readiness/call, deadline metadata through mapping/reassembly, bounded waiter
  registrations, and no duplicated ready permits, queues, or progress tasks.
  Check stricter native byte policy can reject an encoded request after
  count readiness without partial output or leaked reservations.
- Endpoint TCP/custom connects share one native establishment path and finite
  timeout, with no second public owner or Channel connect API. Channel
  clones/submitted service futures retain their private dialed scope;
  extracted Peer/source/request/response values and native operations do not.
  Last-owner release requests cancellation without an idle driver ownership
  cycle or a post-drop join promise. Peer cannot implement the built-in
  transport service, dial, widen captured policy, or become an owning Channel.
  Dropping an unpublished connect retires its scope/result, ingress-service
  work, including all configured notification-service execution.
- Accepted-side initiation through `on_peer_ready` without any incoming
  application message, including finite single-stream acceptance and the
  RA-node harness's NodeUp/NodeUpAck then ReplicaUp/semantic-reply workflow.
  Verify same-generation submission and domain validation without another
  socket reader, driver, owning Channel, or a new connection direction.
  Hook readiness/call cannot block native completion or exhaust request-handler
  capacity needed by bootstrap replies. Check explicit restricted policy,
  unchanged ingress permissions, raw-only request rejection, and no fabricated
  reply grant or authentication. Each established accepted generation admits
  at most one hook; failed establishment, dialed reconnect, repeated readiness
  polls, or hook failure must not create another call.
  Verify same-instance Tower readiness/call, non-Sync service support, finite
  connection/shared metadata accounting, fail-fast admission failure, and one
  nonrenewable readiness-plus-execution deadline. Check the 30-second default
  and rejection of zero/unrepresentable timeout configuration.
  Success leaves the connection usable without an automatic domain ACK.
  Readiness/call/timeout failure preserves its phase/source and retires only
  that connection; escaped Peers cannot remain usable or dial replacements.
  Shutdown races prevent new hook admission, bound tracked hooks by drain,
  grant no new-send exemption, and cancel work on connection/forced-scope
  failure without a post-drop join promise. Record that post-establishment
  timing is deliberately later than native's connection-accepted callback.
- URI Endpoint construction from shared text/bytes, parsed http::Uri, and
  optional SocketAddr conversion. Verify pure/fallible construction,
  parser errors, absolute scheme/authority requirements,
  userinfo rejection, and from_shared fragment rejection before parser truncation.
  Built-in TCP accepts explicit valid ports, hostnames, IPv4, and bracketed
  supported IPv6 with empty/root path and no query. Missing/zero/overflow ports,
  malformed authorities, unsupported paths/queries, and non-TCP schemes fail
  before dialing without HTTP port/scheme defaults or component stripping.
  Socket conversion must never silently discard IPv6 scope IDs.
  Custom URI services explicitly validate their own components; URI path/query
  never implicitly chooses Actor/Action, credentials, or native TLS.
  Initial/reconnect attempts receive the same original URI and can redo DNS
  within each unchanged establishment budget; resolved SocketAddr/source is
  not logical identity. TCP listener binding retains SocketAddr.
- Stable logical Channel reconnection through one retained reusable connector.
  Concurrent RPC readiness and Peer acquisition must
  coalesce into one attempt; idle disconnected handles must not redial in a
  loop. Verify eager initial failure, fresh per-attempt connect budgets,
  unchanged caller deadlines, shorter-waiter cancellation without aborting
  shared work, and last-owner cancellation/publication races.
  Transient reconnect/capacity errors pass through a ready instance's next
  call without permanently poisoning the service. They are never RPC success.
  Cached failure outcomes, zero-waiter fail-fast, diagnostics, and repeated
  failed attempts must remain bounded and preserve original causes/phases.
- Disconnect races with held readiness, unpolled native operations, pending
  correlation, and complete settled replies. Readiness reserved for one
  generation cannot silently target another; admitted requests never replay,
  including uncommitted ones. Peer/Source/Attempt/SendPermit provenance,
  notification operations and private completion tickets stay generation-bound,
  while an existing RpcClient
  can issue a later independent call. Accepted capabilities must never dial.
  Preserve old settled results and reject cross-generation late replies.
- Stable ingress-service configuration, including notification bindings,
  survive physical reconnect with the current registry, not a replay of the
  initial Endpoint descriptors or failed-generation invocations. Dynamic service
  installation survives; there is no notification receiver or registration epoch
  to recreate. Shared HandlerLimits cover all classes/generations. Old
  application-queued requests retain source/storage but cannot keep cancelled
  acknowledgment tickets live or ACK on a replacement automatically.
  Revalidate native setup/security and per-delivery policies on each generation
  without fabricating IPC registration, notification, or replication readiness.
  Connection-bound domain initialization/submission must use the same Peer,
  even if Channel changes generation between separate application awaits.
  Reconnect/current/retiring work keeps its actual owners; old results retain
  byte storage without occupying allocation-budget or new-generation queue credit.
- Preconfigured Endpoint services install ordinary/notification bindings and
  bounded execution before application traffic. Both classes invoke canonical
  services without a transport-owned notification queue or receiver handoff.
  Endpoint clones/connects never share live invocation state. Exercise
  configuration limit lowering, dynamic service installation, shared readiness
  exhaustion, last-owner drop, and application-owned bounded notification queues.
  Notifications use distinct route-option types and class-qualified keys:
  equal Actor/Action values may coexist with ordinary routes without policy
  borrowing, fallback, or retargeting through edited flags/headers.
- Peer::notify requires managed mode, explicit notification/
  acknowledgment checks and captured send restrictions. Verify native special
  flags and managed IDs, pending correlation before commitment, header-stage
  admission, acknowledgment policy/integrity, native Faults, timeout/cancellation
  and generation failure without replay. Notification services use Request<Body>
  and return IngressResponse::Reply; typed handlers reuse unary. NoReply, wrong-kind
  adaptation, failed/dropped handlers and stale queue output must never fabricate
  an acknowledgment. Admitted acknowledgments share reply-completion counts
  and drain with the original ticket; general notification sends gain no drain
  exemption. Installed notification services and application queue waits do not
  demand reconnect or own a Channel.
- End-to-end consumers of the public API sketches, including client/server ownership,
  repeated service rejection, notification classification/policies, connection/source affinity, explicit capacity
  exhaustion, client-owner drop, and signal-and-await server shutdown without
  separate closure observers.
- Awaitable server execution with no hidden startup, concurrent handler work,
  signal-driven drain, active-runner cancellation, and finite accept-source
  exhaustion that does not lose its final establishment.
- Unit-success normal serving completion and structured serving errors;
  ordinary request/peer errors do not poison normal shutdown. Distinguish a
  configured drain timeout from an outer timeout dropping the execution.
  Neither errors nor cancellation imply every background task has exited;
  retained work and byte views follow their ordinary owners.
- Raw service bindings or optional router Actor/Action selection, conflicting
  or duplicate class-local bindings,
  request versus one-way reply rules, byte-facing services, bounded readiness,
  and handler-aware drain without fabricated ACKs or generic error replies.
  Routing has no application serialization bounds or format defaults. Verify
  chosen-service readiness/call on the same clone and bounded waiter reuse
  through outer, routed, and typed services without another admission pool.
  Separate method adapters must use actual configured body budgets and reject
  stale/wrong-kind invocation state before decoding or calling typed services.
  Original admitted replies can drain; context-derived Peers do not
  exempt new application sends from shutdown.
- Reverse client ingress and server-originated sends over the same shared
  messaging components, explicit received-context send grants that cannot be
  widened through typed adapters, and independent native queue/frame limits
  for initiating and accepted roles.
- External connector/acceptor implementations and stream input, including
  a non-Clone/non-Sync reusable URI-input connector transferred once into Channel,
  repeated serialized readiness/call on that same retained instance without
  transport cloning, including later reuse after error/timeout/cancellation;
  default/custom connect sharing one unchanged budget within each attempt,
  cancellation, clean exhaustion with pending establishment, source-preserving
  accept errors including `BoxError`, absent
  socket metadata, and the same framing/handshake/retirement contracts as TCP.
- Default `fubst` and externally implemented non-Serde body codecs over the same
  driver, including distinct encode/decode types, bounded output, malformed
  payloads, and call-local codec failures without connection corruption.
  RPC calls and typed notifications through RpcClient byte-service adapters must
  support unrelated per-call codecs and
  body types, including a non-Clone/non-Sync client codec. Cloneable RPC
  transports support concurrent calls through independent client clones;
  codec state must not be shared.
  Server method services must likewise support non-Clone/non-Sync codecs and
  per-invocation typed services through `server::Rpc`; shared route-service
  bounds cannot leak onto values created inside its future. Exercise unary and
  decoder-only one-way adaptation with both fubst and external formats.
  Unpolled calls must not execute codecs or acquire new transport admission.
  Byte-service futures may only transfer a readiness slot already reserved by
  their own instance and must release it if dropped unpolled.
  Generic typed RPC/notification calls return ordinary futures without Attempt
  guarantees; byte request/notification operations retain synchronized retraction
  receipts. Notification byte-service adapters must preserve the Request deadline,
  native Response parts/source/settlement, error categories, and cancellation
  without adding replay or a hidden native pump.
  Client codecs may use explicit per-call input/output restrictions outside T's
  scope, while Channel applies native frame/send-queue policy on submission.
  Preserve shared backing through decoding and native settlement timing through response mapping.
  Synthetic responses have no source/settlement receipt and use service-result
  observation; result-holding middleware may cause its own wait timeout.
  Verify ordinary decoded-value ownership and shared backing for zero-copy views,
  with no decoded-allocation ledger or constructor-only depth/work ceilings.
- Native limit changes leave body-codec settings unchanged and vice versa;
  default/custom body codecs satisfy their invocation budgets while fixed
  native serialization continues to enforce its own resource ceilings.
- Independent native exchanges for each claimed runtime/role/security profile.
  Internal round trips alone cannot establish native interoperability.

For the deferred TLS/claims implementation, security cases must cover final
wrapped-flight ordering, coalesced wire-phase transitions, authorization
release, and idle TLS output. These are future security release gates, not
initial clear-TCP acceptance gates. Initial acceptance requires explicit
rejection of unsupported security configuration and no downgrade behavior.

Measure allocations, retained bytes, copies, throughput, and tail latency under
representative frame sizes and contention. Segmented buffers alone do not prove
native-equivalent performance. Keep verification resource-bounded and reuse
existing repository tooling.

Retire the old backend only after its consumers have migrated and their
documented contracts have an accepted replacement. Avoid permanent
legacy/native branches throughout the new codec and driver.

## Decisions still requiring evidence

Before claiming production support for a role, settle its concrete finite
defaults and native close/control details, and qualify connection retirement
and explicit re-establishment behavior. Resolve TLS/provider and trust profiles
when the deferred security work is scheduled. Every implementation milestone
must resolve its relevant items rather than hide them behind successful
defaults.

Deferred capabilities include TLS and claims implementation, caller-selected
RPC IDs, alternate protection formats, TLS session renewal, general connection
pools, generated stubs, and a complete actor36 service API. None is implied by
implementing the common raw transport.
