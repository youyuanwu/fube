# furpc: Operations, deadlines, and errors

**Status:** proposed; see the [design summary](README.md).

Defines native submission, commitment, write receipts, cancellation,
operation deadlines, and structured errors. Generic RPC uses ordinary async
service calls above these native operations. Lifecycle completion is
specified separately in [Lifecycle.md](Lifecycle.md).

## Operation contract

### Submission and completion

Conceptually, sending progresses through:

```text
caller-owned -> admitted -> committed -> locally written
                                         |
                               correlated reply, for RPC
                                         |
                               domain result / semantic ACK
```

Admission, local write, correlated reply, and domain success are different
completion boundaries. One-way send success means complete local transmission,
not peer receipt, application execution, persistence, or replication progress.
For TLS, accepting plaintext into rustls is not local-write completion. The
driver must track the corresponding ordered ciphertext through complete socket
submission before issuing that receipt.

Before commitment, cancellation/expiry can retract work. After commitment,
dropping an observer only abandons its result; the driver owns the remaining
output until completion or irreversible connection failure.

The commitment point must reflect the next irreversible layer. For a clear
unbatched frame, first positive socket write is the relevant boundary. TLS
plaintext consumption or inclusion in nonretractable shared output can commit
earlier. A cancellation receipt may report `Committed` even before socket
visibility; it must never falsely report `Retracted`.

The existing channel's first-positive-write cancellation contract must not be
silently mapped to a weaker result. Preserve it for equivalent clear paths;
adapters must explicitly handle any stronger commitment boundary.

### Deadlines and receipts

Endpoint establishment uses a finite `connect_timeout`, converted once into an
internal absolute monotonic deadline. Resolution, endpoint I/O acquisition,
and native hello share it without resetting at each stage. The server's
`handshake_timeout` similarly bounds native setup after acceptance, not idle
listening. Public establishment methods require no `Instant` argument. Each
initial connect has its own budget; cloning a handle does not establish another
connection. Each demand-driven shared reconnect also has one finite
establishment budget covering readiness, I/O, and native/security setup.
Waiters keep their own unchanged deadlines; timing out one wait on an already
published Channel does not cancel or reset its shared establishment.

Message operations retain one caller-owned absolute deadline across their
encoding, admission, send, and reply stages. A caller composing establishment
with a larger workflow can use Tokio's `timeout_at` around that workflow.
This stops its wait and cancels a wholly unpublished initial Channel; for a
published Channel it abandons that invocation, not other users' reconnect work.
Neither action makes a stream reusable or proves physical retirement.

Connection-open, stalled-write, partial-read, and close-drain timers are
different policies. Do not equate an operation deadline with the watchdog for
finishing committed output. Idle waiting need not have an inactivity timeout.
Absolute frame duration and lack-of-progress detection are distinct options;
their defaults require native-source mapping and workload qualification.

Settle each native local result once. An on-time settled result does not become
late merely because its observer polls later. Channel responses preserve native
settlement metadata through typed decoding; generic middleware may impose its
own service-wait timeout, and synthetic responses use service-result observation
instead of claiming a native settlement. Timeout after commitment carries an
uncertain remote outcome; it is not permission for automatic replay.
Logical reconnection changes only eligibility for later calls, not the
settlement or generation of an already-created native attempt. Even confirmed
precommit retraction is not an instruction for Channel to replay a request.

Expose attempt disposition through an optional receipt, separate from the
operation's domain result. Cancellation changes eligibility through the same
synchronized gate used by the driver to commit output. Sending a cancellation
command into a queue is not proof of retraction.

## Observation and cancellation

```rust,ignore
impl<T> Operation<T> {
    pub fn attempt(&self) -> Attempt;
}

impl Attempt {
    pub fn cancel_uncommitted(&self) -> CancelOutcome;
    pub fn disposition(&self) -> AttemptDisposition;
}
```

`Operation<T>` implements `Future<Output = Result<T>>` for native sends,
`Peer::request` byte RPCs and byte-facing Peer::notify
notification exchanges. Service replies/acknowledgments
remain runner-owned completion work, not public grant operations.
Construction moves the input and captures inert handles/configuration; first
poll starts work. A byte-service call may additionally transfer a count
reservation already acquired by that same instance's `poll_ready`; it creates
no second admission, and dropping it unpolled releases that reservation.
These operations do not borrow a receiver or lifecycle facade for their lifetime.

`RpcClient<T>::call` instead returns an ordinary async future borrowing
`&mut self`, following tonic; OneWayClient<T>::send has the same observation and
drop contract for typed one-way writes. Arbitrary Tower services cannot supply a native
Attempt, so the generic client does not expose `attempt()` or claim confirmed
retraction. Dropping its future drops the transport future; native commitment
still determines whether output can be retracted. Callers requiring explicit
native disposition use Peer::request or Peer::notify and decode the byte
response separately. Typed notifications over an explicit notify byte-service
adapter have these same RpcClient future semantics, not an exposed native Attempt.
Middleware must not manufacture retraction success from dropping
a future or sending a cancellation command.

`CancelOutcome` distinguishes confirmed retraction from commitment and already
settled work. Confirmed retraction means no future transmission can occur for
that attempt, including one whose operation was not yet polled. The driver
commit gate, not delivery of a cancellation command, decides the result.
`AttemptDisposition` records whether work remains uncommitted, was retracted,
committed, locally written, or retired with its commitment history.

Drop is local abandonment, not remote cancellation. After commitment, the
driver retains output and its send-queue occupancy independently of observers. An attempt
observer retains only bounded status, not the result payload or socket owner.

## Errors

Use public structured categories, with sources and native codes preserved:

| Category | Meaning |
| --- | --- |
| `InvalidInput` / `Unsupported` | Invalid local construction, conflicting managed fields, or unavailable capability. |
| `ResourceExhausted` | Temporary operation, byte, registration, connection, or waiter capacity exhaustion. |
| `MessageTooLarge` | Permanent rejection against the selected size policy; retrying unchanged work cannot fix it. |
| `Timeout` / `Cancelled` | Include phase and attempt disposition where applicable, without claiming remote nonexecution. |
| `Codec` | Body encode/decode failure, including source and resource category; normally call-local. |
| `Handler` | Service/readiness/handler/response-contract failure with phase and source; no automatic wire error reply. |
| `Transport` | Connection I/O, native protocol, client transport-service readiness/call, or future security failure with phase and the first causal error. Preserve an existing furpc category when passing it through middleware. |
| `RemoteFault` | Native transport Fault with numeric code and available body details. |
| `Closed` | Scope, connection, or registration retired; preserve an available underlying cause. |

Domain-specific errors remain above these categories. A failure is never
represented by an empty response, successful ACK, or automatic fallback.
Reconnect failures preserve their connector/setup phase and source. Channel
may carry a transient readiness failure into the next call's error, following
[tonic-style reconnect semantics](Connections.md#reconnection-without-request-replay);
this is not operation success or an invented retraction receipt.

Map into existing consumer error types at adapters, not by adding dependencies
from `furpc` back into those consumers. Preserve native numeric codes and
available diagnostic details.

Serving uses `Result<()>`, with the same error categories and scope/phase
context. [Completion results](Lifecycle.md#completion-results) distinguish
fatal serving failures and shutdown-drain timeout from normal completion.
Dropping serving yields no serving result, and neither an error nor external
task cancellation certifies joined cleanup. A per-request or per-connection
error is not automatically a serving failure.

## Diagnostics

Record resource class, connection generation, phase, and attempt disposition
where useful. Diagnostics for intentional native drops are explicit and
rate-limited. Never log private keys, claims tokens, or arbitrary payloads.
