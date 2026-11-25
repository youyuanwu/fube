# furpc: Limits, admission, and scheduling

Furpc separates native representation, transport admission, shared storage,
and consumer execution policy. It provides no aggregate heap ledger or
allocation-owned byte lease.

## Serialization and body budgets

| Policy | Authority | Meaning |
| --- | --- | --- |
| Native representation | [body.rs](../../../../crates/libs/furpc/src/body.rs), [headers](../../../../crates/libs/furpc/src/wire/headers.rs), [frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs) | Native header/frame widths and checked construction. |
| TransportLimits | [transport/mod.rs](../../../../crates/libs/furpc/src/transport/mod.rs) | Frame sizes, queue bytes, operation/connection/handshake/Peer-waiter counts, work quanta, and watchdogs. |
| ClientCodecLimits and one-way output limit | [client.rs](../../../../crates/libs/furpc/src/client.rs) | Encoded request and response-input restrictions. |
| IngressLimits | [service.rs](../../../../crates/libs/furpc/src/service.rs) | Admitted body bytes for a binding. |
| HandlerLimits | [service.rs](../../../../crates/libs/furpc/src/service.rs) | Invocation timeout and encoded response-body restriction. |
| Codec-specific limits | [codec.rs](../../../../crates/libs/furpc/src/codec.rs) | Explicit format/body work policy, independent of native envelope decoding. |
| Domain backpressure | Consumer middleware and queues | Handler concurrency, readiness waiters, deferred operations, and semantic completion. |

Definitions and defaults live at those implementation locations, not in a
second configuration table here. Finite execution policy is not a native
format rule. A representable frame may fail a configured size or capacity
check without being malformed.

Standalone body/header/message, Endpoint, and Router construction do not
reserve native admission or add arbitrary retained-capacity/work ceilings.
Custom codecs own their allocations; decoded values have ordinary Rust
ownership. Buffer byte bounds do not cap arbitrary user heap usage.

## Storage, admission, and scheduling

### Immutable storage and accounting

Body clones/slices and encoded header records retain backing allocations by
reference counting. Small views may keep large allocations alive. Retention
does not carry a transport byte charge or keep a connection owner alive.

Send-queue accounting measures complete serialized frame bytes, including
headers, not Vec capacity or allocation metadata. Two queued sends of one
shared body each count their full wire length. An unbatched frame remains
charged until fully consumed or removed; partial writes do not release its
whole-frame occupancy early. Surviving aliases retain storage, not queue credit.

Zero `send_queue_bytes` disables that limit. When outgoing frame-size policy
is disabled, the native empty-queue exception permits one larger frame.
The exact admission rules live in `ConnectionHandle::send_authorized` in
[connection/mod.rs](../../../../crates/libs/furpc/src/connection/mod.rs).
Driver output retirement lives in
[driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs).

### Bounded admission

Channel clones share logical operation/correlation and reconnect-waiter
admission; cloning is not a fresh pool. Physical connections retain their own
frame/send-queue state. Readiness reservations bind the prepared generation
and cannot migrate onto a replacement.

Server connection and handshake admission occurs before accepting another
stream. Peer-acquisition/reconnect waiters are separate from service readiness.
Furpc imposes no handler-concurrency or service-readiness waiter quota.
Consumers supply those bounds explicitly.

Receive admission is staged:

1. Validate fixed header and configured frame size.
2. Retain/decode the bounded native header area.
3. Classify and authorize the destination.
4. Retain the body, discard with bounded integrity checking, or retire.

No body-proportional allocation precedes the retain decision.
No application delivery, correlation settlement, or control side effect
precedes complete-frame verification. These gates are private protocol state,
not public heap-budget capabilities. See [Framing](Framing.md).

### Ordering and overflow

Writes preserve a frame's byte order and committed suffix; a later frame cannot
interleave. Scalar/vectored output uses the same cursor/commitment contract.
Work quanta bound cooperative preparation and driver turns, not total
application CPU or message construction.

Capacity failures are explicit resource errors or an explicitly permitted,
diagnosed discard. They do not become empty successful messages or arbitrary
application ACKs. Fabric provides no generic HTTP/2-style per-stream flow
control, and an arriving large frame can cause TCP head-of-line blocking.

## Replication flow-control boundary

The replication framework in fube-data owns session validation, operation
queues, ACK/RequestAck dispatch, acknowledgment scheduling, and retransmission.
It does not delegate those policies to generic RPC or transport queue credit.

ACK/RequestAck dispatch must progress independently of data-queue capacity.
Data handlers make a bounded domain admission decision and return promptly;
they do not retain a live transport invocation until application or quorum
completion.

An explicit queue-full drop cannot advance acknowledgment for unadmitted
operations. Later processing and required semantic sends have domain-owned
shutdown. All actual sends still obey native frame/queue policy and original
Peer authority.

The byte adapter lives in
[fube-data transport](../../../../crates/libs/fube-data/src/transport/connection.rs);
native packet/envelope construction lives in
[wire.rs](../../../../crates/libs/fube-data/src/wire.rs).

## Peers and queue limits

A domain can compose initiating Channels and an accepting server, but their
lifetimes and physical queue counters remain independent. There is no shared
ResourceBudget/AggregateLimits API. A process-wide governor belongs above
furpc.

Peer is a generation-bound send capability, not a public queue reservation
or connection owner. Channel readiness reserves a managed request/result
slot; raw sends admit when their native operation is polled. Neither reserves
backing allocations or guarantees that actual frame/queue checks will pass.
