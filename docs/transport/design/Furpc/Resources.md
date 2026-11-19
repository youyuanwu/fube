# furpc: Resources, admission, and scheduling

**Status:** proposed; see the [design summary](README.md).

Defines allocation leases, local/shared budgets, bounded admission,
and output scheduling. Budgets share accounting, not connection or
task lifetime. [Operations](Operations.md) define commitment; the
[framing contracts](Framing.md) implement staged receive and partial writes.

## Shared budget construction

```rust,ignore
impl ResourceBudget {
    pub fn new(limits: AggregateLimits) -> Result<Self>;
}
```

## Serialization and body budgets

Codec choice and resource policy are separate. Endpoint and the server builder take
`TransportLimits`; they do not take a native codec or serializer object.
Generic client codec limits are configured above the transport on RpcClient.
Native serialization, transport admission, and typed-body allowances have
explicitly different owners:

| Work | Budget and ownership |
| --- | --- |
| Standalone message/header construction | Caller-owned data under finite construction-only header/storage/work ceilings; no scope reservation. Submission separately enforces the actual scope's native limits and capacity. |
| Endpoint URI construction | Finite URI input/work and retained backing/metadata ceilings before parse/adoption/copy; no DNS/I/O or scope reservation. A Channel accounts for URI/configuration metadata it retains, preserving the original logical destination across reconnect rather than allocating unbounded URI history. |
| Endpoint ingress configuration | Inert cloneable IngressService prototype, HandlerLimits, raw/Actor bindings, and optional special-duplex descriptor under finite configuration ceilings; no live runner, queue, or admission. Each connect reserves live metadata before traffic. Ordinary ingress has no native receive queue. Arbitrary application service state is not universal transport heap accounting. |
| Native headers and transport controls/Faults | Native decode ceilings in `TransportLimits`, validated when the scope is built and applied by the internal fixed serializer. Bound strings, collections, object/nesting counts, extension retention, and decode work before allocation. |
| Generic client RPC codecs | Each RpcClient call owns its codec under finite `ClientCodecLimits`: encoded body, response input, and tracked decoded allocations. This work belongs to application futures, not an arbitrary T's hidden connection scope. Per-call bounds are not aggregate admission across caller-created futures. |
| Channel / native Peer byte RPC | Actual `TransportLimits` and local/shared admission bound operation/result slots, encoded storage, native headers, pending correlation, and retained byte responses. Only Channel exposes Tower readiness; Peer operations admit directly on first poll. |
| Channel reconnection | One retained reusable connector and at most one pending establishment per logical Channel. Bound waiters, ready/failure outcomes, current/retiring driver work, and all old-result leases under the existing scope/local/shared limits, not fresh per-generation budgets. |
| Native duplex application bodies | Each notification owns its codec under body-specific per-operation ceilings in `TransportLimits`, intersected with operation admission, result-retention allowances, and codec-local work limits. |
| Channel/server service ingress | One scope-owned runner using HandlerLimits for readiness, in-flight work, tracked decoded request storage, encoded response storage, and completion. Endpoint configures reverse ingress; ServerBuilder configures accepted ingress. Clones/generations share the actual scope policy and native local/shared admission, not fresh execution pools. |
| Application work queues | Optional finite service-owned composition, not a native pull API. For request/reply, enqueue/worker/response waiting remain inside the original HandlerLimits slot and deadline; the runner keeps its grant. Retained native bytes keep their actual leases after cancellation. Independent one-way domain processing has its own queue/work policy. |
| Accepted-peer ready work | At most one hook readiness/execution per admitted accepted connection. Service-clone/future/timer/completion metadata uses finite connection/local/shared capacity reserved before retention, under one `peer_ready_timeout`. Outbound messages and received replies retain their usual independent admission. |

The default `FubstCodec` and user-defined body codecs operate under the
applicable body budget. Sharing `fubst` primitives with native serialization
does not share its header/control context or limits. Codec configuration may
restrict its own body format/work further; it cannot raise an admitted byte
allowance or reinterpret native headers.

Generic RPC encoding finishes in a bounded caller-owned buffer before native
admission; it cannot extract a private budget from arbitrary middleware or a
custom transport service. Channel must admit its full backing allocation and
metadata before retaining it. Received byte bodies keep their native leases
through generic decoding; new decoded objects use the client's tracked per-call
allowance and transfer it into Response, not into an unrelated connection's
ledger. Applications bound the number of codec futures they create. This is
the tonic-style client transport boundary, not a promise that generic
application codec allocations are included in Channel's aggregate budget.

All admitted transport stages still satisfy frame-size, per-scope, and shared
`ResourceBudget` limits. Native-header storage and body storage have separate
charges, not independent unaccounted pools. Builders validate native/body limit
values as resource policy; no codec selection is required to increase an
appropriate finite allowance. Changing one limit family does not silently
reset the other.

## Storage, admission, and scheduling

### Immutable storage and accounting

Represent messages using immutable shared storage and segmented bodies. Prepare
outgoing frames as fixed-header, native-header, and body segments; compute CRCs
over segments without flattening the body.

Count retained backing allocations, not just visible slice lengths. Shared
views charge their storage once, including a large receive allocation retained
by a small slice. A storage owner carries its opaque allocation lease through
clones and slices; exposing byte views must not detach that lease.

Standalone construction performs no scope admission. New caller-provided
storage is caller-owned until submission; admission must reserve its retained
backing/metadata capacity before the transport retains it. Any pre-existing
leases on reused received headers/bodies survive construction and cloning.
Preserve those charges and obtain missing reservations without double-charging
the same allocation within a shared ledger. Construction ceilings bound one
builder/header operation, not the application's total number of messages or
allocations.

Request header edits use the same finite construction ceilings, including
copy-on-write metadata and retained backing capacity. Existing leases survive
edits, removal, and parts reassembly. Newly supplied application storage does
not become admitted merely by insertion: before routing or RPC adaptation
retains/uses an edited collection, validate it under the original invocation's
native-header limits and reserve missing local/shared header and metadata
charges. Replacing a collection with standalone headers cannot bypass this
check, release still-retained old allocations, or obtain a larger admission.
Failure is explicit before dispatch/decoding, without a new permit or queue.

Moving a message between driver, RPC completion, and ingress queues transfers
its charge rather than reserving the same bytes again. New decoded objects,
encryption output, or copied buffers require their applicable owner-specific
charges; generic client codec allocations use their separate per-call policy.
Retained completed-but-undelivered results still count.

The implementation must publish where each lease ends. Leased byte storage
keeps its charge through application ownership; extracting a slice cannot
release it. Tracked plain decoded allocations instead use the
[invocation/response handoff](Codecs.md#bounded-buffers-and-failures): the
invocation or response retains their charge until settlement, response drop,
or explicit extraction to application-managed ownership. Retained
completed-but-undelivered results cannot release their charges early.
Response decomposition moves tracked decoded charges into
[`ResponseParts`](Requests.md#response-envelope); keeping the parts retains
those charges, and reassembly transfers them back without a new reservation.
Dropping parts while retaining a plain body is explicit application handoff,
not lease detachment: embedded leased storage remains charged independently.
Arbitrary caller allocations and decoded domain state beyond that handoff
are outside the transport budget, as are OS/runtime overheads. This is not a
process-RSS promise.

### Bounded admission

Bound outbound operations/bytes, inbound retained messages/bytes, pending RPCs,
completion state, establishment, connections, and any waiting callers.
An async semaphore alone does not bound the number of waiting futures.

Channel and server scopes have finite local limits. Channel clones share
one logical admission ledger and current-generation command path, not an additional Tower buffer.
Clone-held readiness permits and pending waiter registrations are themselves
bounded; a reserved slot does not imply capacity for an arbitrary body.
Ready reservations identify their physical generation; a cached failed-attempt
outcome grants no native send/admission capacity. Failure-only state is bounded
per caller handle/waiter without retaining request bodies or an unbounded
history. Shared reconnect outcomes may preserve one causal error through bounded
references; they must not retain old sockets or arbitrary completed payloads.

Reconnection charges current and retiring generations, pending establishment,
late results, write suffixes, and stable ingress queues to the same ledger.
Keep at most one current generation and one establishing replacement; retiring
work remains bounded by the actual connection/task/storage limits. Capacity
does not become free merely because another socket is desired. Reconnect may
fail ResourceExhausted or wait only through an existing bounded allowance and
its finite attempt timeout until cleanup releases capacity. Never create an
uncharged replacement or wait for retirement through an unbounded queue.
Each Channel shares one reconnect attempt across RPC readiness, Peer
acquisition, and raw receivers; waiter cancellation cannot mint capacity,
restart the shared connect clock, or abort other callers' work.
A peer can provide one shared `ResourceBudget` so accepted and initiating
sessions, retained work,
and bytes also satisfy a combined limit. Facade separation must not silently
double a peer's allowance. Shared accounting neither owns I/O nor shares
mutable connection/session state. Native-control/reply progress reservations must
remain protected at the aggregate level as well.

Configured Channel/server ingress services additionally bound readiness waiters, in-flight
handlers, decoded/output retention, and handler deadlines. They reuse admitted
ingress storage rather than create a second unbounded router inbox. All of
that owned work remains counted through response completion.
Channel service configuration persists across reconnect, but failed-generation
ingress invocations are cancelled without replay. Retiring futures and byte
leases keep their charges; a new generation does not mint another HandlerLimits
allowance. Inbound service futures retain no public Channel owner guard.
Optional [peer-ready work](Connections.md#accepted-peer-readiness-hook) is
bounded by admitted connection capacity, not a second unbounded task/ready-event
queue. It does not consume request-service handler/readiness permits that its
own bootstrap traffic may need. Connection/hook bookkeeping and retained
storage remain charged until actually released, including cancelled work
still exiting; a replacement connection cannot reuse those charges early.

Offer fail-fast admission and, where needed, a separately bounded
deadline-aware reservation path. Reserve required metadata and completion
capacity before accepting work. Do not encode into an unbounded temporary
before checking the relevant allowance.

Retain staged receive:

```text
fixed-header validation
    -> bounded header read and common-property decoding
    -> select internal native resource category and admit
    -> retain body, or drain/dispose according to explicit policy
```

Draining rejected bodies uses bounded scratch and performs applicable integrity
checks. It is not successful delivery. Oversized or inadmissible traffic has a
documented connection/protocol outcome.

The [receive contract](Framing.md#incremental-receive-and-admission-gates)
also gates header storage after fixed-header validation. Header-stage routing
and policy outcomes remain provisional: publish no message, reply authority,
RPC settlement, or security transition before complete-frame verification.

Reserved capacity belongs to internal native traffic categories, not simply
to an untrusted HighPriority bit. Keep enough bounded capacity for native controls,
admitted RPC completion, and authorized replies so ordinary output cannot
exhaust every progress path.
Replication ACK/RequestAck remains ordinary one-way application traffic, not
native control or an authorized RPC reply. It uses ordinary admission and
send limits.

### Ordering and overflow

Preserve byte order on each connection. Schedule internal native control/reply traffic
ahead of eligible ordinary frames at frame boundaries, with a policy preventing
unbounded starvation. Never insert a new frame into a committed frame's suffix.

Batching and vectored writes are bounded optimizations. Track offsets across
segments and frame boundaries, preserve each operation's receipt, and apply
deadline/cancellation eligibility before irreversible inclusion in a batch.

A full application inbox cannot cause an unbounded read-ahead queue. Choose
overflow behavior explicitly for the consumer: bounded backpressure, terminal
resource error, or a protocol-permitted diagnosed drop. Generic frame
discard uses the existing bounded integrity path and is not successful delivery.

Fabric has no general HTTP/2-style per-stream flow control. Reserved queues
cannot bypass a large frame already arriving on the same TCP stream. Separate
connections are required where a consumer needs stronger traffic isolation.

## Replication flow-control boundary

Follow native classic replication's ownership split: the replication-framework
service in fube-data
owns operation-queue limits, sender/session validation, ACK scheduling, and
retransmission. ACK/RequestAck is dispatched independently of the replicated
operation queue. [Service admission](Services.md#replication-service-admission) must
finish promptly after the domain's bounded queue-admission decision, rather
than hold a handler slot until application acknowledgment or quorum completion.

In the pinned native source, [Replicator message processing][sf-repl-dispatch]
dispatches complete messages by Action before the secondary's
[operation-queue admission][sf-repl-operation-admission].
[RequestAck handling][sf-repl-request-ack] delegates timing to the domain's
AckSender. ACKs and data share [ReplicationTransport::SendMessage][sf-repl-send]
without selecting a special priority; [SendOneWay's default][sf-send-default]
is Normal.

Native's common message processor is created with the
[default JobQueue configuration][sf-repl-message-queue], whose
[maximum queue size is UINT64_MAX][sf-job-queue-default]. Furpc does not copy
that effectively unbounded allowance. Its configurable finite native,
execution, and aggregate limits remain explicit Rust resource policy; under
exhaustion an application message may fail admission or its connection may
retire. Normal frame-atomic writes and TCP head-of-line blocking still apply.

## Peers and aggregate resource limits

Replication and other bidirectional protocols compose Channels and a server
under their domain supervisor. They may share immutable protocol configuration and
a `ResourceBudget`, but not a socket driver or mutable security/session state.
The same actor can be configured independently on each Channel and server.
Alternatively each scope can independently install explicit raw ingress,
without an ordinary Actor header or a synthetic Actor-0 binding. Raw and Actor
dispatch reuse the same admission/lease accounting; custom domain endpoint
maps, processing concurrency, and any downstream queues need their own bounded
application policy, not another transport reader or copied inbox.
The domain retains Channels and drives server execution. Both can run the
same ingress service contract without native receive loops; bounded application
queues can be composed inside it. Neither needs another I/O pump,
multi-connection client owner, or duplicated inbox. Separate Endpoint connects create independent bounded
registries, but reconnect within a Channel reuses its current registry/queues
and aggregate allowance. It neither duplicates old ingress nor resurrects
unregistered Endpoint descriptors.

`TransportLimits` applies local per-scope/per-connection policy.
`AggregateLimits` bounds combined establishment, connections, retained work,
and bytes across facades sharing a budget. A reservation must satisfy both.
Without an explicit budget, a facade creates its own finite budget.

Pass one shared budget when accepted and initiating sessions compete for the
same resource allowance. Splitting facades must not silently double that
allowance. Budget clones are inert accounting handles; closing a Channel or
completing a server execution affects only that scope, neither stopping the
other nor waiting for the shared budget to become empty. Cleanup still exiting
after cancellation and caller-retained storage keep their leases until those
resources are actually released.

## Bounded reservations

```rust,ignore
impl Peer {
    pub fn try_reserve(
        &self, budget: SendBudget, deadline: Instant,
    ) -> Result<SendPermit>;
    pub async fn reserve(
        &self, budget: SendBudget, deadline: Instant,
    ) -> Result<SendPermit>;
}

impl SendPermit {
    pub fn send(self, message: Message) -> Operation<WriteReceipt>;
}
```

`SendBudget` bounds frame bytes, retained backing bytes, and segment
bookkeeping for one ordinary raw send. A permit binds transport, connection
generation, captured send policy, deadline, and ordinary send allowance. Oversized work rejects before
commitment; unused or expired permits release their reservations. Permits
cannot be retargeted or converted into control/reply/RPC authority.

`try_reserve` performs explicit synchronous admission; normal lazy operation
construction does not. `reserve` has a separately bounded waiter class
and one unchanged deadline. Waiter exhaustion rejects rather than queueing
without bound. Passing a permit into an unpolled operation transfers its
reservation; dropping that operation releases it.

Native byte RPC/duplex calls use their own RPC/completion admission, not a raw
`SendPermit`. Channel service readiness additionally exposes bounded
waiting for one such count slot; bytes remain separately checked at submission.
Generic RpcClient uses that service contract after bounded application-owned
encoding. No raw send permit can be converted into a ready RPC slot or bypass
pending/result accounting. A permit from a retired generation cannot target a
replacement, including when its frame never committed.

[sf-repl-dispatch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/Replicator.cpp#L618-L746
[sf-repl-operation-admission]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicationReceiver.cpp#L396-L426
[sf-repl-request-ack]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.cpp#L1214-L1262
[sf-repl-send]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L880-L892
[sf-send-default]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/IDatagramTransport.h#L67-L72
[sf-repl-message-queue]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/Replicator.cpp#L123-L126
[sf-job-queue-default]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Common/JobQueue.h#L16-L28
