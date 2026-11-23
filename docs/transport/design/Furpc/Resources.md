# furpc: Limits, admission, and scheduling

**Status:** proposed; see the [design summary](README.md).

Use native frame-size and send-queue semantics, ordinary shared buffer ownership,
and explicit execution/backpressure policy. Do not introduce allocation-owned
byte leases, local/shared allocation ledgers, transactional allocation admission,
or an aggregate retained-heap guarantee. Those are not native transport contracts.

## Serialization and body budgets

Native serialization is fixed by the supported wire schemas, not selected by
the application. Standalone headers/messages enforce native representation
limits: the u16 combined header area, nonzero record lengths, u32 frame size,
and checked arithmetic. They impose no extra retained-capacity, segment-count,
nesting-depth, or declared-field/element ceilings. `fubst` defaults remain
native-aligned. Explicit caller-selected codec restrictions are application
policy, never additional native format rules.

Application body codecs run above byte transport, outside the connection
driver. Encoding must finish before submission. A custom codec owns its work
and allocations; furpc does not track decoded Rust values or move hidden
allocation charges through request/response parts. Native frame validation and
configured send-queue admission apply when encoded messages reach transport.

Endpoint/URI and router construction validate syntax, native representability,
and binding conflicts without arbitrary construction-only storage/work caps.
Runtime limits on active connections, operations, service invocations and
waiting callers remain explicit policies of their corresponding owners.
Per-turn reader/CRC work quanta provide cooperative fairness, not a message
construction limit or preemptive CPU quota.

## Storage, admission, and scheduling

### Immutable storage and accounting

Headers and bodies use immutable shared storage. Clones and slices keep their
backing allocations alive by reference counting, as native Message/ByteBique
ownership does. Small views can retain large backing allocations; there is no
byte-budget charge attached to those references.

Send-queue accounting measures queued serialized frame bytes, including the
fixed header, rather than Vec capacity or allocation metadata. Two queued sends
of a shared body each contribute their full wire length. Decrement that
counter as queued frames are consumed or removed, following the selected native
send-buffer behavior. A surviving application alias does not keep send-queue
credit occupied. Queue capacity and backing-buffer lifetime are distinct.
The pinned native [enqueue check][sf-send-enqueue], [send consumption][sf-send-consume]
and [shared message clone][sf-message-clone] demonstrate this separation.
Preserve native disabled-limit semantics, including the empty-queue exception
for one large message when outgoing frame-size limiting is disabled; do not
silently replace those rules with a hard allocation quota.

Standalone construction and request/response decomposition neither reserve
transport capacity nor grant authority. Submission validates the actual frame
and connection policy after managed headers are resolved. Header edits retain
ordered records and atomic failure behavior; they cannot change trusted
provenance, admitted routing, correlation, or a reply grant.

Fallible buffer growth reports allocation failures. Do not promise universal
heap accounting, allocator-overhead measurement, cross-facade deduplication,
or limits on storage held by application code after handoff.

### Bounded admission

The connection owns configured maximum frame size, queued wire bytes, pending
operations/correlation, and any explicit waiter/count limits. Channel clones
share one logical command and operation path, not a new pool per clone.
Readiness reservations and send permits identify their physical generation;
they cannot retarget a replacement connection or replay a failed request.

Reconnection coalesces demand into one attempt and keeps existing operation
deadlines. Current and retiring work remain owned until completion/cancellation,
but no allocation ledger follows old results into application ownership.
Ingress timeout/response-size policy applies to one logical Channel/server
runner across its bindings and generations. Handler concurrency and readiness
waiter limits belong exclusively to consumers, including their bounded queues.

Retain staged receive:

```text
fixed-header validation and configured frame-size check
    -> native-header accumulation and decoding
    -> classification and destination admission
    -> retain body, or explicitly discard/retire
```

Header/body decisions remain private driver gates. They are not allocation
budget permits. No application delivery, reply authority, RPC settlement or
security transition precedes complete-frame verification. Discard performs
incremental integrity checks and observes the driver's nonrenewable drain
deadline; it does not manufacture an empty successful message.

Native controls, admitted RPC completion and authorized replies retain their
own scheduling/admission protections. An untrusted HighPriority header cannot
select those categories. Replication ACK/RequestAck remains ordinary one-way
domain traffic subject to native frame/send-queue limits.

### Ordering and overflow

Preserve byte order on each connection. Schedule eligible control/reply traffic
at frame boundaries without unbounded starvation. Never insert a new frame
into a committed frame's suffix. Vectored writes do not require flattening.
Batching preserves each operation's commitment and local-write receipt.

Choose overload behavior explicitly: backpressure, a reported resource error,
or a protocol-permitted diagnosed drop. Fabric has no general HTTP/2-style
per-stream flow control. A large incoming frame can block later traffic on the
same TCP stream; use separate connections where domain isolation requires it.

## Replication flow-control boundary

Follow native classic replication's ownership split: the replication-framework
service in fube-data owns operation queues, sender/session validation, ACK
scheduling and retransmission. Dispatch ACK/RequestAck independently of data
queue capacity. A service returns promptly after its bounded domain admission
decision rather than holding a handler until durable/quorum completion.

The pinned [Replicator message processor][sf-repl-dispatch] dispatches by Action
before secondary [operation-queue admission][sf-repl-operation-admission].
[RequestAck][sf-repl-request-ack] delegates timing to the domain AckSender.
ACKs and data share [ReplicationTransport::SendMessage][sf-repl-send], and
[SendOneWay defaults][sf-send-default] to Normal priority.

Native's common processor uses the [default JobQueue configuration][sf-repl-message-queue],
whose [maximum queue size is UINT64_MAX][sf-job-queue-default]. A stricter
application execution/queue policy is explicit Rust-host policy, not native
compatibility or allocation accounting. Intentional queue-full drops cannot
advance acknowledgment for unadmitted operations; ACKs for earlier progress
remain valid. Deferred processing and required ACKs have domain-owned shutdown.

## Peers and queue limits

A domain supervisor may compose initiating Channels and an accepting server,
but each connection retains its own native frame/send-queue state. There is no
shared `ResourceBudget` or `AggregateLimits` API. A process-wide or domain-wide
resource governor belongs outside furpc if an application needs one.

Raw and Actor-dispatched ingress reuse the same driver/classification path.
Custom endpoint maps and downstream queues do not add another native reader.
Channel/server lifetimes remain independent.

## Bounded reservations

`SendPermit` reserves queue/operation capacity, not backing allocations. Its
budget describes serialized frame bytes. It binds generation, send policy,
deadline and ordinary traffic class; it cannot grant control/reply/RPC authority.
Unused permits return their queue reservation, and first submission performs
the actual native message checks.

Fail-fast admission and any explicit waiting path preserve one absolute
deadline. Bound waiter registrations separately when waiting is supported.
Native RPC/notification operations retain their transport operation/count
admission; none creates a general allocation-budget capability. Furpc imposes
no service-readiness or handler concurrency quota. Notification handlers share
invocation-time/response-size policy with ordinary handlers, not a separate
transport-owned notification queue or per-class allowance.

[sf-repl-dispatch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/Replicator.cpp#L618-L746
[sf-repl-operation-admission]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicationReceiver.cpp#L396-L426
[sf-repl-request-ack]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.cpp#L1214-L1262
[sf-repl-send]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L880-L892
[sf-send-default]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/IDatagramTransport.h#L67-L72
[sf-repl-message-queue]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/Replicator.cpp#L123-L126
[sf-job-queue-default]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Common/JobQueue.h#L16-L28
[sf-send-enqueue]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/SendBuffer.cpp#L179-L249
[sf-send-consume]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpSendBuffer.cpp#L350-L398
[sf-message-clone]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/Message.cpp#L194-L203
