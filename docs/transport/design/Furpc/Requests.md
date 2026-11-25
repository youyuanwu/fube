# furpc: Request envelopes and RPC

`Request<B>` carries a body, editable ordered native headers, optional local
deadline, and optional protected received context. `Response<B>` carries a
body, headers, and read-only native provenance/settlement. Their definitions
and parts APIs live in
[request.rs](../../../../crates/libs/furpc/src/request.rs) and
[response.rs](../../../../crates/libs/furpc/src/response.rs).

## Request envelope

The body type reflects the current boundary:

| Boundary | Body |
| --- | --- |
| Typed outbound call/send | The encoder's actual protocol value. |
| Client byte service or ingress/router | Shared encoded `Body`. |
| Typed handler | The decoder's actual result value. |

`Request::new` creates outbound data with empty headers and no received context.
Actor, Action, and extension records use generic header insertion. There are no
HTTP method, path, status, or version semantics.

`map` changes only the body. `into_parts`/`from_parts` support fallible adaptation
while preserving headers, local deadline, and private context. Header collection
edits preserve ordered/unknown records unless explicitly removed; body or header
edits cannot transfer authority.

The request-local deadline may narrow an explicit client deadline. It is not
a wire Timeout header or the trusted ingress deadline. Byte-service callers
must supply an absolute deadline; missing deadlines fail rather than selecting
an unbounded wait.

Only core ingress creates `RequestContext`. It exposes the original Source,
absolute invocation deadline, and an optional independently authorized Peer.
Its private link to live invocation state is nonowning. Retaining context or
parts cannot prolong admission, recreate a ticket, or keep the scope alive.

Before publishing editable request metadata, ingress captures original
classification, binding/route, correlation, response/send policies, body
allowances, and deadline. Adapter validation uses that snapshot. Edited headers
cannot reauthorize traffic, retarget another admitted binding, change exchange
kind, or renew the invocation.

## RPC calls and responses

`RpcClient<T>` owns a byte-facing Tower service, not a socket implementation,
stored codec, or correlation table. Channel is the reconnecting built-in
transport; an explicit Peer adapter or consumer service can supply another
byte boundary. The canonical call implementation is in
[client.rs](../../../../crates/libs/furpc/src/client.rs).

Each call consumes an encoder and decoder for the actual request/reply types.
It borrows the exact transport mutably, polls readiness, and calls that same
prepared instance. Concurrent callers clone a cloneable client before
readiness; the adapter never silently clones a reservation or retries a call.
Generic transports need not be `Clone`, `Sync`, or `'static`.

The call sequence is:

1. Select the earlier request-local/explicit absolute deadline and check it.
2. Encode the actual request with its current headers and output limit.
3. Await byte-service readiness and response under that same deadline.
4. Check settlement, bound response input, and decode the complete body with
   its current headers.

Construction and unpolled calls are inert. Encoding failure precedes transport
submission and creates no native correlation entry. Managed transport performs
ID assignment, send policy, admission, and native Fault processing before
successful application decoding.

`ClientCodecLimits` narrows encoded request bytes and response-input bytes.
Defaults permit native body capacity; zero permits an empty body. These are
not heap quotas or a replacement for full-frame/send-queue policy.
See [Codecs](Codecs.md).

Native responses preserve their actual settlement time through body adaptation.
An on-time settled response does not become late merely because decoding is
observed later. Synthetic services have no native settlement and use service
result observation instead. Middleware that holds a result pending can itself
cause service-wait timeout.

Dropping a generic call abandons its service future. It cannot certify native
retraction or remote cancellation. Explicit `Peer::request` returns a byte
operation with an Attempt for callers needing commitment observation.

## One-way client and Peer service adapters

`OneWayClient<T>` adapts a typed request over a byte service returning
`WriteReceipt`. It consumes only an encoder, applies an encoded-body limit,
and preserves one absolute deadline through encoding, readiness, submission,
and local-write observation. It creates no RPC entry, response decoder, or
implicit semantic acknowledgment.

Success is the original local-write receipt, including its settlement time.
Dropping the future has the same abandonment/commitment distinction as an RPC
future. Send implementation and Rustdoc examples live in
[client.rs](../../../../crates/libs/furpc/src/client.rs).

The [Peer service adapters](../../../../crates/libs/furpc/src/peer.rs) are:

| Adapter | Result | Generation behavior |
| --- | --- | --- |
| `PeerRequestService` | Native `Response<Body>`. | Submit through the captured Peer only. |
| `PeerSendService` | Native `WriteReceipt`. | Assemble and send through the captured Peer only. |

Both require the request deadline and preserve native policy/errors. Readiness
does not reserve native operation capacity; admission occurs in the underlying
operation. They add no queue, semaphore, worker, retry, or reconnect.
PeerSendService rejects caller-managed correlation fields in either mode.
Neither adapter implicitly selects native notification origination.

SF method profiles, IPC host/PID headers, serving authority, cleanup exemptions,
and ordinary versus reserved consumer admission stay in middleware above these
generic adapters.

## Response envelope

`Response::new` creates synthetic data without native Source or settlement.
Only native completion supplies those private fields. Moving parts or mapping
the body preserves them; rebuilding or editing headers cannot manufacture
provenance, reclassify Faults, or change correlation.

`into_body` discards metadata. Parts-based conversion preserves it while moving
the body, with ordinary Rust ownership and no hidden allocation charge. Shared
byte views retain their backing storage independently.

`Reply<T>` is a different role: typed ingress response data to be encoded and
returned to the runner. It is not a native response receipt or a send grant.
Its definition lives in [reply.rs](../../../../crates/libs/furpc/src/reply.rs).

## Correlation and native IDs

[rpc.rs](../../../../crates/libs/furpc/src/rpc.rs) owns managed envelope
assembly, the process-scoped GUID/index allocator, pending correlation, expiry,
and native Fault adaptation. Correlation is installed before output can commit
and matches the effective RelatesTo on the original physical generation.

The allocator changes GUID namespace before index reuse. Completed IDs need
no lifetime tombstone set; late replies are unmatched and never become
application ingress. Logical Channel identity is not a correlation key.

Managed APIs own MessageId, ExpectsReply, RelatesTo, and notification flags.
Caller-selected raw IDs belong only to raw-only one-way composition. A private
single-completion ingress ticket authorizes a local reply attempt, not
exactly-once remote processing or general ingress deduplication.

Native transport Faults are resolved before successful body decoding.
`RemoteFault` retains numeric/header codes, raw body, native message code units,
and any Fault-body decode error. Domain errors remain consumer schemas.

## Notification and reply authority

`Peer::notify` is an explicit generation-bound native operation with a
NotificationPolicy and acknowledgment lifetime. Typed callers can compose
RpcClient over a consumer byte service delegating to that operation.
RpcClient does not infer notification kind from edited flags.

Notification handlers use the same ingress service and `server::unary` adapter
as ordinary replying handlers. Only the original runner ticket supplies reply
or acknowledgment correlation; neither context nor codec exposes that grant.
See [Native notifications](Actors.md#native-notifications).

Concrete client behavior is exercised in
[client tests](../../../../crates/libs/furpc/tests/client.rs),
[one-way tests](../../../../crates/libs/furpc/tests/transport/one_way_client.rs),
and [transport tests](../../../../crates/libs/furpc/tests/transport.rs).
