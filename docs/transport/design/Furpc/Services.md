# furpc: Ingress services and execution

The canonical application boundary is a Tower service from `Request<Body>`
to `IngressResponse`. `IngressService` combines that service with trusted
native bindings. The same runner handles Channel reverse ingress and Server
accepted ingress. Definitions and invocation machinery live in
[service.rs](../../../../crates/libs/furpc/src/service.rs).

## Placement and ownership

Header classification, binding selection, and authorization happen before body
retention. Full frame verification precedes user service execution.
Native controls and correlated replies do not enter this handler path.
Notification traffic enters only its independently authorized bindings.

The owning Channel/session or serving future tracks service readiness,
handler futures, and response writing. There is no transport application inbox,
second receive pump, or per-handler public connection owner.
The private session drives handlers alongside I/O and correlation in
[session.rs](../../../../crates/libs/furpc/src/transport/session.rs).

`on_peer_ready` is separate accepted-connection work with its own deadline,
not an ingress invocation or request-handler permit. Application queues and
spawned tasks have their own owners.

## Service boundary and native bindings

Ingress service configuration is Clone + Send with owned Send futures; it
does not require Sync. The runner clones the service before readiness, then
calls the exact prepared instance. No reservation is duplicated by cloning
after readiness. `map_service` adds compatible middleware while preserving
the binding set.

Each scope permits one installed service. Its bindings select class-local raw,
whole-Actor, or exact-action dispatch with explicit policy and body limits.
Installation and configuration are synchronous; they neither poll readiness
nor start application work.

Endpoint stores an inert prototype, and each explicit connect gets independent
logical execution. Channel clones share the installed registry. Dynamic
installation affects only later admissions and survives physical reconnect.
Server configuration is installed before acceptance.
The installation methods live in
[channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs) and
[server.rs](../../../../crates/libs/furpc/src/transport/server.rs).

Bindings are immutable after installation. No service is required for
outbound-only composition. An unconfigured traffic class is diagnosed/discarded,
not implicitly delivered raw.

## Typed RPC adapters

[server.rs](../../../../crates/libs/furpc/src/server.rs) defines the typed
adapters used inside byte-facing service futures:

| Adapter | Required original invocation | Typed service outcome |
| --- | --- | --- |
| `server::unary` | Authorized ordinary request or native notification. | `Reply<T>`, encoded into response data. |
| `server::one_way` | Authorized ordinary one-way. | Unit, returned as NoReply. |

Each adapter consumes actual decoder/encoder values for its needed directions.
It validates live original context, current headers, and exchange kind before
decoding. Input/output budgets and the deadline come from the actual admitted
invocation, not a separately captured configuration default.

Body adaptation preserves request parts/context and supplies current immutable
headers to codecs. The typed service is prepared and called on the same
instance. Reply encoding sees the typed reply's headers. Codec/user work runs
outside the socket driver under the original invocation deadline.

Adapters need no socket, admission pool, or reply grant. They return data and
never send through Peer or insert reply correlation. Context-free, stale,
wrong-kind, or retargeted invocations reject explicitly before user work.
Inner typed services/codecs do not inherit Clone/Sync bounds merely because
outer route configuration is shared.

The Router is codec-free; raw and whole-Actor services can use these same
adapters without routing.

## Requests, responses, and reply authority

The runner captures a private single-completion ticket before exposing request
metadata. It retains original source/generation, kind, correlation, response
policy, allowances, and deadline. Editable headers and RequestContext do not
contain a public copy of that grant.

`IngressResponse::Reply` is a raw Message to complete through that original
ticket. `Reply<T>` is its typed precursor. `IngressResponse::NoReply` completes
only ordinary one-way work. Returning Reply without a grant or NoReply for a
request/notification is an explicit response-contract error.

Request body mapping, parts reassembly, and header edits preserve context but
cannot reauthorize, renew, or retarget it. A context Peer is an independently
granted general send capability, not a way to manufacture correlated replies.
Standalone response construction needs no live context; authorized submission
still belongs exclusively to the runner.

Dropping, failing, or cancelling an invocation invalidates its state and sends
no automatic Fault or acknowledgment. Arbitrary Rust errors are not mapped
into universal wire error responses.

## Application-owned work queues

A service can enqueue an admitted Request into a bounded domain queue and
await a worker's result through a response channel. The runner still owns
completion, and the original deadline covers queueing, processing, and reply
writing. Workers receive data/context, not a transferable completion ticket.
Late results cannot revive cancelled work.

Alternatively, a one-way service can return NoReply after bounded domain
enqueue. In that case transport invocation completion means enqueue, not later
decoding, application execution, persistence, quorum, or semantic ACK.
Longer-lived processing is separately owned and cannot later reuse a live
server adapter merely by retaining context.

The application owns queue capacity, worker lifetime, cancellation, and shutdown.
Queue/response channel failures must be explicit service failures.
Retained byte storage can outlive invocation cancellation without retaining
transport queue credit.

## Consumer execution policy and invocation deadlines

`IngressLimits` bounds admitted body bytes. `HandlerLimits` supplies encoded
response-body policy and one finite invocation timeout. Their definitions,
defaults, and validation are in
[service.rs](../../../../crates/libs/furpc/src/service.rs).

Furpc imposes no handler concurrency or service-readiness waiter quota.
Consumers supply shared Tower middleware, load shedding, or bounded queues
when needed. There is no quota reset per route, clone, or physical generation.
Pending readiness retains its prepared service and remains subject to
cancellation and the original deadline.

The invocation deadline begins at header-stage service admission and includes
body acquisition, readiness/queue delay, decoding, handler work, encoding, and
reply writing. Editing local metadata or a Timeout header cannot extend it.
Body/output limits cannot widen native frame or send-queue constraints.
They do not track decoded heap allocations or preempt blocking user code.

### Replication service admission

The replication framework dispatches ACK/RequestAck independently of its data
queue. Readiness must not wait for data-queue space before the action can be
examined.

Data handling validates and makes an explicit bounded domain admission
decision, then promptly returns NoReply. Later processing, earned ACKs, and
retransmission are domain-owned. Queue-full policy must not advance progress
for an unadmitted operation; it is not permission to hide malformed input or
arbitrary service failures as success.

See [Resources](Resources.md#replication-flow-control-boundary) and the
[consumer adapter](Implementation.md#classic-replication).

## Errors and shutdown

Readiness, codec, handler, and response errors preserve their phase/source and
are diagnosed without automatically terminating unrelated work. Native frame
and control failures remain connection failures. Peer-ready failure instead
retires its accepted connection under its separate hook contract.

Physical failure cancels that generation's unsettled handlers and tickets.
Channel retains service configuration, not old invocation state. Last-owner
release cancels dialed ingress and does not flush or join it.

Graceful server shutdown stops new admission but allows admitted handlers and
their original replies/notification acknowledgments to finish within the
drain deadline. A general context Peer gains no new-send exemption. Deferred
domain work needing semantic sends must quiesce before the domain signals
transport shutdown.

Keep application workers available while admitted service responses drain,
then stop them explicitly. Furpc does not adopt application-spawned child jobs.
Scope-level completion and cancellation are defined in [Lifecycle](Lifecycle.md).

Executable typed and queued composition lives in
[typed_server.rs](../../../../crates/libs/furpc/tests/transport/typed_server.rs);
readiness/drain behavior also appears in
[transport.rs](../../../../crates/libs/furpc/tests/transport.rs).
