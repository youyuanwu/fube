# furpc: Internal framing contracts

Framing is private. Applications exchange complete Message/Body values, not
frame streams, socket chunks, or partially decoded bodies. One native frame
contains a fixed header, ordered encoded native message headers, and body.
TCP reads/writes and in-memory segments do not define additional frames.

## Boundary and ownership

| Component | Responsibility | Implementation |
| --- | --- | --- |
| Fixed-header codec | Native layout, widths, provider interpretation, and CRC rules. | [frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs) |
| FrameReader | Partial receive offsets, admission gates, integrity, retained body or discard progress. | [reader.rs](../../../../crates/libs/furpc/src/wire/reader.rs) |
| Native header decoder | Ordered records, recognized schemas, effective common properties. | [headers.rs](../../../../crates/libs/furpc/src/wire/headers.rs) |
| PreparedFrame / FrameCursor | Shared output segments, preparation, and exact partial-write offsets. | [writer.rs](../../../../crates/libs/furpc/src/wire/writer.rs) |
| ConnectionDriver | I/O, timers, admission decisions, control transitions, and final verified events. | [driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs) |
| Session | Pending correlation, service admission, and verified message dispatch. | [session.rs](../../../../crates/libs/furpc/src/transport/session.rs) |

These are concrete internal components, not public pluggable framing traits.
The reader/cursor do not own sockets, invocation authority, or domain policy.
The driver is the only physical I/O progress owner.

## Fixed-header codec

`FrameHeader` validates total/header/body length relationships with checked
arithmetic before slicing, conversion, or allocation. Outbound construction
checks native widths; configured frame policy is a separate admission check.
Wire context is local configuration, not peer-selected security policy.

The exact fixed layout, checksum algorithms, zero-checksum semantics, and
provider checks live in
[frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs). CRCs detect
corruption; they do not authenticate a peer. The clear profile rejects
unsupported or inconsistent protection instead of downgrading.

## Incremental receive and admission gates

The reader progresses through fixed header, header admission, native-header
accumulation, body decision, integrity completion, then the next frame.
Driver-owned input retains partial/coalesced bytes without treating each read
as a complete message.

At the fixed-header gate, the driver checks frame-size policy before retaining
the native header area. At the body gate, decoded headers select controls,
pending correlation, or an authorized application destination before
body-proportional storage is allocated.

Each decision is bound to the current frame and state. Duplicate, stale,
wrong-frame, or out-of-order decisions are internal errors, not ignored input.
Private permits express admission transitions, not heap quotas or reusable
application capabilities.

| Body disposition | Contract |
| --- | --- |
| Retain | Assemble the body only after destination/policy admission. |
| Discard | Consume incrementally with bounded scratch, applicable integrity checks, and driver byte/time limits; deliver nothing. |
| Retire | Abandon the stream irreversibly rather than guessing another boundary. |

Header-stage checks remain bounded and synchronous. Body decoding, async
service readiness, and domain processing occur only after verified delivery,
outside the reader/driver.

No correlation settlement, handler invocation, acknowledgment, or native
control transition occurs before complete-frame integrity succeeds.
Discard cannot hide checksum/native-header corruption or turn rejection into
an empty successful Message.

Reader work is cooperative and stage-bounded. Empty header/body stages can
advance without another socket read. Coalesced input remains for the next
frame, and read-ahead does not authorize body retention across a blocked gate.
The implementation's state/event types are in
[reader.rs](../../../../crates/libs/furpc/src/wire/reader.rs).

## EOF and retirement

Buffered bytes and pending decisions are processed before EOF classification.
Only an exact between-frames boundary with no unprocessed input is
framing-clean. Partial fixed headers, native headers, or bodies are truncation;
an earlier admission/resource failure retains its own cause.

A framing-clean EOF still has connection-lifecycle meaning; it is not proof
of application success. Integrity/parser failure makes the stream terminal.
There is no scan-ahead resynchronization or reuse after an abandoned committed
frame.

Driver-owned partial-frame, discard, operation, committed-write, and drain
timers are distinct. A new chunk or stage does not renew an absolute deadline.

## Outbound preparation and partial writes

Preparation consumes a complete immutable message after native managed fields
and send authority are resolved. It checks lengths/policy and computes CRCs
over shared segments without flattening the payload.

`PreparedFrame::prepare` is cooperative work, not I/O or a spawned per-frame
task. Preparation failure/cancellation releases temporary ownership and does
not commit the message. Native send-queue admission remains separate.

FrameCursor exposes bounded borrowed slices and advances exactly by accepted
bytes. Scalar fallback and vectored writing share its offsets. Pending or
Interrupted does not advance; zero write for remaining output is failure.
Reported progress cannot exceed the offered bytes.

Before irreversible output, the driver rechecks cancellation, expiry, terminal
state, authorization, and wire context at its commitment gate. First positive
clear socket write commits the frame. The suffix stays driver-owned until
complete writing or connection retirement; later frames cannot interleave.

Cursor completion alone cannot issue a public result. The driver settles the
complete-local-write receipt and releases frame queue occupancy. That receipt
does not prove remote delivery.

## Security boundary

The implemented path feeds clear socket bytes to native framing. Native TLS
requires a separate wire-phase boundary around that same framing, not a body
codec or ordinary TLS connector at TCP byte zero.

Transmit/receive transitions must preserve unread bytes, pending output, and
exact frame ownership. Protected plaintext acceptance would not be a socket
write receipt; associated ordered ciphertext must finish writing first.
These constraints describe [Security](Security.md), not an implemented
protected transport.

Concrete fixed-header, receive-gate, and write-cursor cases live in
[frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs),
[reader tests](../../../../crates/libs/furpc/src/wire/reader/tests.rs),
[writer tests](../../../../crates/libs/furpc/src/wire/writer/tests.rs), and
[driver tests](../../../../crates/libs/furpc/src/connection/tests.rs).
