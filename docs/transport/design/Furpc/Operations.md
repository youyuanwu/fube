# furpc: Operations, deadlines, and errors

Native operations distinguish admission, irreversible commitment, complete
local writing, correlated response, and domain completion.
The public operation/receipt types live in
[operation.rs](../../../../crates/libs/furpc/src/operation.rs);
the synchronized send/commit state lives in
[connection/mod.rs](../../../../crates/libs/furpc/src/connection/mod.rs) and
[driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs).

## Submission and completion

Constructing a native send/request/notification moves its input into an inert
Operation. First poll performs submission. Byte-service calls may consume a
readiness reservation acquired by that same service instance, not acquire a
second logical request slot.

For clear unbatched output, the first positive socket write commits the frame
through the same synchronized gate used by cancellation. Before commitment,
cancel/expiry can retract it. After commitment, the driver owns its suffix
until complete writing or irreversible connection retirement.

`WriteReceipt` means all frame bytes were accepted locally by the socket.
It does not prove remote receipt, execution, persistence, or semantic ACK.
RPC and notification operations additionally await their correlated response;
domain errors/completion remain above native settlement.

## Deadlines and receipts

Message operations carry one caller-owned absolute deadline. Typed adaptation
preserves the earlier request-local/explicit deadline through encoding,
readiness, admission, writing, and reply observation. No phase grants a fresh
budget.

Native result settlement is recorded once. An on-time settled response or
write remains on time when observed later. Synthetic service results have
no native receipt and use service-result observation. Middleware cannot
fabricate native timing or hide its own wait behind a later decode.

Establishment, invocation, peer-ready, partial-frame/discard, committed-write,
and shutdown-drain timers are independent policies. In particular, operation
expiry after commitment does not authorize truncating a frame and continuing
the stream; the driver's write watchdog/retirement still governs that suffix.

Reconnection changes eligibility only for later work. It does not reset a
failed attempt's deadline, settle it differently, or replay even confirmed
precommit-retracted work.

## Observation and cancellation

`Operation::attempt` supplies a nonowning commitment observer.
`Attempt::cancel_uncommitted` returns confirmed retraction, committed, or
already settled. A cancellation command merely queued for later handling is
not proof of retraction; the shared commit gate decides.

AttemptDisposition distinguishes uncommitted, retracted, committed, locally
written, and retired state with commitment history. Its status does not
retain the result payload or running scope.

Dropping a native operation requests precommit retraction and abandons its
result. Dropping after commitment is not remote cancellation or rollback.
The driver retains committed output and its queue occupancy independently.

Typed RpcClient/OneWayClient calls are ordinary borrowing async futures over
arbitrary byte services. They expose no native Attempt and cannot certify
retraction merely by dropping the wrapped future. Callers requiring that
observation use Peer native operations and separate body adaptation.

## Errors

The actual variants, source retention, and conversions are defined in
[error.rs](../../../../crates/libs/furpc/src/error.rs), not by an additional
error taxonomy in these documents.

| Error group | Contract |
| --- | --- |
| InvalidInput / Unsupported / UnsupportedSchema | Reject invalid local use or unavailable capabilities/schemas explicitly. |
| Protocol / NativeHeader | Preserve malformed native framing, controls, or recognized-header causes. |
| ResourceLimit | Report configured capacity/size or allocation failure without implying malformed wire data. |
| Timeout / Cancelled / Closed | Describe the affected phase/lifetime without promising remote nonexecution or physical cleanup. |
| Codec | Retain encode/decode phase and invalid-payload/resource source. |
| Transport / Handler | Retain non-furpc service/I/O causes and phase; passing through an existing furpc error preserves its category. |
| RemoteFault | Retain native transport Fault information before successful-body decoding. |
| Internal | Report an invariant violation rather than continuing with a fabricated value. |

Domain-specific failures remain in consumer protocols. There is no automatic
wire Fault, successful ACK, empty response, retry, or security fallback for
an arbitrary Rust error.

An operation/handler/individual connection error is not automatically a serving
failure. Serving result and drain behavior belong to [Lifecycle](Lifecycle.md).
An error or cancelled wait is never a universal task-retirement receipt.

## Diagnostics

Registry/session diagnostics record phase and error category with bounded
emission. They distinguish deliberate discard from successful delivery and do
not convert unmatched replies into ingress. Available native Fault fields
remain inspectable without arbitrary payload logging.

Cancellation, delayed-observation, partial-write, and timeout examples live in
[transport tests](../../../../crates/libs/furpc/tests/transport.rs) and
[one-way client tests](../../../../crates/libs/furpc/tests/transport/one_way_client.rs).
