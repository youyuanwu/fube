# furpc: Internal framing contracts

**Status:** proposed; see the [design summary](README.md).

Defines private fixed-header, incremental receive/admission, and
segmented-write interfaces. These contracts add no public frame API.
[Messages](Messages.md) and [requests](Requests.md) remain the
application boundaries; [security](Security.md) implementation is deferred.

## Boundary and ownership

```text
application Request<B> / Message
    -> native message headers and complete encoded Body
        -> prepared native frame and write cursor
            -> current security wire phase
                -> Tokio byte I/O
```

For the ordinary native TCP path:

```text
native frame = fixed header[12] || native message headers[H] || body[B]
frame length = 12 + H + B
```

One native frame carries one message. TCP packets, I/O calls, and body-memory
segments do not define additional native frames. The frame reader processes
partial bytes internally but publishes no partially received application
body. Logical streaming across messages belongs to a domain protocol.

Use concrete `pub(crate)` types and synchronous state transitions except for
cooperative outbound preparation. No new public frame trait, frame stream,
per-frame task, or independent I/O owner is required.

| Component | Owns | Does not own |
| --- | --- | --- |
| Fixed-header codec | Checked native layout and checksum interpretation. | Allocation, resource policy, authentication, or actor routing. |
| `FrameReader` | One frame's parse offsets, running CRC, and admitted body assembly or discard progress. | Socket polling, timers, native common-header decoding, or application delivery. |
| Connection driver | Bounded read storage, header decoding/classification, admission decisions, security state, and final message handoff. | Application-body decoding or domain processing. |
| `PreparedFrame` / `FrameCursor` | Encoded fixed header, shared message segments, and exact output offsets. | Socket lifetime, operation authority, scheduling, cancellation arbitration, or receipt settlement. |

Names below pin ownership and transitions, not a required private layout.
Permits express private frame-bound admission decisions, not allocation-budget
quotas or public capabilities. Storage uses ordinary shared ownership; queue
accounting belongs to the driver.

## Fixed-header codec

```rust,ignore
pub(crate) const FIXED_HEADER_LEN: usize = 12;

impl FrameHeader {
    pub(crate) fn decode(
        bytes: &[u8; FIXED_HEADER_LEN], wire: &FrameWireContext,
    ) -> FrameResult<Self>;

    pub(crate) fn encode(&self) -> [u8; FIXED_HEADER_LEN];
    pub(crate) fn frame_len(&self) -> u32;
    pub(crate) fn headers_len(&self) -> u16;
    pub(crate) fn body_len(&self) -> u32;
}
```

`FrameHeader` has private fields. Decode and checked outbound preparation are
its only construction paths; encode cannot serialize an unchecked length
combination. Decoding allocates no heap storage.

The codec preserves the native little-endian layout:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | Total frame length, including the fixed header. |
| 4 | 1 | Security-provider mask. |
| 5 | 1 | Optional fixed-header CRC-8. |
| 6 | 2 | Combined native-message-header length. |
| 8 | 4 | Optional CRC-32 over native message headers plus body. |

Validate `frame_len >= 12` and `headers_len <= frame_len - 12` with checked
arithmetic before deriving body length, converting to `usize`, slicing, or
allocating. Outbound preparation also rejects header/frame lengths beyond
their native u16/u32 widths. A native-representable frame may still fail
configured size or resource admission; that is not malformed framing.

`FrameWireContext` is validated local wire configuration and the expected
connection phase/provider, not a peer-selected policy. The driver owns it.
Snapshot the interpretation when the fixed header is accepted; do not change
it halfway through a frame. Provider inconsistency and unavailable protection
modes fail explicitly without downgrade. The initial implementation supports
only the selected clear-TCP profile.

Preserve native checksum rules: a zero checksum field means no verification
for that field. Header CRC-8 uses polynomial 0x07, initial zero, and all 12
fixed-header bytes with both CRC fields zeroed. Body CRC-32 uses the reflected
0xedb88320 polynomial, initial/final XOR 0xffffffff, over native headers then
body, excluding the fixed header. These are corruption checks, not
authentication. Outbound CRC generation follows the selected native profile;
do not impose extra nonzero-checksum requirements on valid received frames.

## Incremental receive and admission gates

```rust,ignore
pub(crate) enum FrameReadStep {
    NeedInput { max_bytes: usize },
    FixedHeader { frame: FrameKey, header: FrameHeader },
    Headers(HeaderPrefix),
    Complete(VerifiedBody),
    Discarded(DiscardedFrame),
    Yield,
}

pub(crate) enum BodyDecision {
    Retain(BodyPermit),
    Discard(DiscardReason),
}

impl FrameReader {
    pub(crate) fn new() -> Self;

    pub(crate) fn step(
        &mut self, input: &mut ReceiveInput,
        wire: &FrameWireContext, work: WorkBudget,
    ) -> FrameResult<FrameReadStep>;

    pub(crate) fn admit_headers(
        &mut self, frame: FrameKey, permit: HeaderPermit,
    ) -> FrameResult<()>;

    pub(crate) fn decide_body(
        &mut self, frame: FrameKey, decision: BodyDecision,
    ) -> FrameResult<()>;

    pub(crate) fn finish_eof(&mut self, input: &ReceiveInput) -> FrameResult<()>;
}
```

`ReceiveInput` is a consumable view of driver-owned receive
storage. It is not an unbounded byte accumulator. `step` performs no I/O,
waits on no semaphore, and invokes no application callback. A positive finite
`WorkBudget` bounds consumed bytes and transitions per turn; return Yield
when exhausted so the driver can service writes, commands, and timers.

The state machine is:

```text
between frames
    -> fixed-header accumulation and validation
    -> FixedHeader event / wait for header admission
    -> bounded native-header accumulation
    -> Headers event / wait for body decision
    -> retain body OR drain body
    -> final integrity check
    -> Complete OR Discarded
    -> between frames
```

Each event is emitted once. The reader does not cross an admission gate until
the corresponding method succeeds, and the driver must not call `step`
again while that gate awaits its decision. Wrong-state, duplicate, stale,
or wrong-frame decisions are explicit internal contract errors, not ignored
inputs. `FrameKey` is a private connection-local identity; permits and prefix/
completion records are bound to that same frame. They are not reusable for a
later frame or another connection.

### Fixed header and header storage

The reader initially consumes at most the 12 fixed-header bytes. After the
FixedHeader event, the driver checks frame-size policy and prepares native-header
storage before calling `admit_headers`. `HeaderPermit` binds that storage to
the current frame.
The reader cannot allocate or retain bytes proportional to the declared body
length at this stage.

Oversized frames may cause immediate connection retirement. They are not
automatically drained merely because their length is representable. If even
bounded header admission is unavailable, the driver either applies its
explicit bounded wait/backpressure policy or retires the connection; it
does not silently consume body bytes without a decision.

The Headers event transfers an owned `HeaderPrefix`: frame identity,
checked fixed-header information, and shared encoded native-header storage.
The driver decodes it using the fixed internal native serializer and the
scope's validated native-header limits, preserving ordered/unknown records
and validating all recognized duplicate properties. Application-body codecs
cannot replace that decoder or supply its settings.
The frame reader itself does not interpret Actor, Action, MessageId,
RelatesTo, native controls, or domain headers.

### Classification and body disposition

Header classification selects an internal native resource category, destination,
and provisional policy outcome. It is bounded, nonblocking work outside the
wire codec. It must not publish reply authority, settle a pending RPC, release
a security gate, invoke a handler, or perform a domain side effect before
the full frame passes its applicable integrity checks.
For ordinary application traffic, the selected raw binding or optional Actor
dispatcher supplies matching and header/source authorization at this stage.
Raw delivery does not require an ordinary Actor header and cannot bypass native
control/correlation classification. Moving application demultiplexing above
the driver never moves body admission after allocation or permits custom body
decoding before verified delivery.
These are generic native/header/body admission gates. Replication queue
admission runs in its framework service after verified delivery.

The driver then chooses exactly one disposition:

| Disposition | Contract |
| --- | --- |
| Retain | Supply a frame-bound `BodyPermit` after classification and destination admission; assemble the body with ordinary shared ownership. |
| Discard | Supply a bounded diagnostic reason; incrementally consume the remaining frame with bounded scratch and verify applicable CRCs. Do not construct a Body, deliver a message, or emit an ACK. |
| Retire connection | Do not resume the reader. Cancel/close that connection under the normal lifecycle contract; arbitrary unread bytes cannot be resynchronized. |

No automatic body-sized allocation precedes the retain decision. Reservation
failure cannot become an unbounded waiter or a success-shaped empty message.
Discard is permitted only by the chosen protocol/resource policy and has
finite byte/time/work limits enforced by the driver. Exceeding those limits
retires the connection instead of keeping drain open indefinitely.

### Storage, progress, and completion

Accumulate body data as shared segments or adopt input segments where possible.
Small retained slices can keep the full backing allocation alive. Retain the
headers and decoded state while waiting for completion; there is no allocation
ledger or backing-capacity charge.

The running body CRC covers encoded native headers and every body byte
exactly once, including discarded bytes. Segmentation does not change it.
A zero-length header area/body can advance immediately through its required
events without an extra socket read. NeedInput always requests a positive,
stage-bounded amount only when bytes are genuinely missing.

Complete transfers a `VerifiedBody` containing the frame identity and the
complete shared Body. It is not an application `Message` yet. The driver
matches it with that frame's retained decoded headers and provisional
admission record, applies remaining native/security checks, then publishes
the complete Message to control, correlation, a Channel/server ingress service,
or separately configured native special-duplex ingress. Ordinary services do not require a
receiver queue, forwarding task, or second native reader.
Discarded confirms frame consumption/integrity and carries only bounded
diagnostic bookkeeping. Neither event processes the next frame implicitly.

The driver can release header/policy storage after choosing Discard because
the reader already incorporated the encoded headers into its running CRC.
No failed, discarded, or incomplete frame is delivered as a message. Malformed
native headers and bad frame checksums retain their actual failure category;
discard is not a way to turn corruption into a successful route miss.

Coalesced input remains in `ReceiveInput` for the next turn. Do not consume
bytes from the next frame, or from a new security wire phase, as part of the
current frame. Any read-ahead is independently bounded by the driver; stop further
reads while an admission gate is blocked. Kernel/TLS read granularity does
not authorize extra body retention.

After EOF, the driver processes already buffered input and ready events
before calling `finish_eof`. EOF does not bypass an admission gate: resolve
its bounded admission/discard/retirement decision first. A resource failure
or admission timeout keeps that cause rather than being mislabeled truncation.
Calling `finish_eof` with an unresolved gate is an internal contract error.
Success requires an exact between-frames boundary with no unprocessed input;
missing fixed-header, native-header, or body bytes fail as truncated input.
A boundary EOF is only framing-clean; native connection/shutdown policy still
determines its outcome. Security-record truncation remains a security error
even if the last decoded native frame was complete.

## Outbound preparation and partial writes

```rust,ignore
impl PreparedFrame {
    pub(crate) async fn prepare(
        message: Message, wire: FrameWireContext, limits: FrameWriteLimits,
    ) -> FrameResult<Self>;

    pub(crate) fn header(&self) -> &FrameHeader;
    pub(crate) fn encoded_len(&self) -> usize;
    pub(crate) fn into_cursor(self) -> FrameCursor;
}

impl FrameCursor {
    pub(crate) fn remaining(&self) -> usize;
    pub(crate) fn io_slices<'a>(
        &'a self, dst: &mut [std::io::IoSlice<'a>],
    ) -> usize;
    pub(crate) fn advance(&mut self, written: usize) -> FrameResult<()>;
    pub(crate) fn is_complete(&self) -> bool;
}
```

Preparation consumes an immutable Message after native headers, managed
correlation fields, and operation authority have been resolved. The driver
performs send/reply/control queue admission separately; preparation cannot
grant permission to originate a message. It validates native widths, configured frame
limits, and checked combined lengths before producing output.

The result owns a fixed-header array plus shared native-header/body segments,
not a flattened `Vec<u8>`. Calculate CRCs over those segments. Any required
new header storage or segment metadata uses fallible allocation.
Preparation invokes no application codec or callback and cooperatively yields
after bounded work, including within a large segment while computing CRCs.
Its future is driven as owned admitted work; the driver can poll it alongside
other progress without requiring a per-frame task. The async method spawns
no worker and performs no I/O; its future owns and drops temporary
storage on failure or cancellation. Preparation does not commit the
message.

The driver rechecks cancellation, expiry, terminal state, authority, and wire
phase at the synchronized commitment gate before irreversible output. A plan
prepared for an obsolete wire phase cannot be written; rebuild or reject it
while still retractable. Do not edit segments or managed headers after
preparation, since their sizes/checksums are already fixed.

`FrameCursor` owns the plan and all remaining offsets. `io_slices` fills
caller-provided bounded scratch with the next nonempty slices, allocating
nothing and retaining no pointer after the borrow ends. It may expose only
part of the frame when scratch is short. With nonempty scratch and remaining
output, it returns at least one nonempty slice; otherwise an empty slice list
must not be treated as a completed write. Scalar fallback writes the first
nonempty slice. No method owns or polls the socket.

On a positive write to the immediate next layer, the driver advances by
exactly the reported count, including writes crossing segment boundaries.
Reject advancement beyond remaining bytes; the driver also checks that the
count cannot exceed the slices actually offered. Pending/Interrupted do not
advance. A zero-byte write for nonempty output is WriteZero, not completion
or a retry spin. An I/O error preserves the original cause and retires the
connection when output cannot safely continue.

Cursor advancement is not cancellation arbitration. For clear unbatched I/O,
the first positive socket write commits through the same gate as cancellation;
the driver then retains the suffix until complete write or whole-connection
retirement. Dropping an observer cannot drop that suffix or allow another
frame to interleave. Before commitment, dropping a cancelled plan releases
its temporary storage normally.

`is_complete` means all native-frame bytes were accepted by the next layer.
Only the driver can settle the operation's complete-local-write receipt.
For clear TCP, that requires full socket-write acceptance; a flush is not
remote receipt. Future TLS plaintext acceptance is not socket completion:
retain bounded frame-to-ciphertext completion accounting until the associated
ordered ciphertext has been written.

Scheduling and batching stay in the driver/send layer. Select eligible frames
at frame boundaries and preserve one receipt/commitment history per
operation. Nonretractable batching can commit before socket visibility and
must report that stronger boundary rather than falsely claim retraction.

## Errors, security, and retirement

`FrameResult<T>` carries a private structured `FrameError`. Distinguish invalid
native structure/checksum/provider, truncated input, unsupported profile,
local size rejection, resource exhaustion, and internal state misuse.
Native-header decode failures keep their codec/protocol source; I/O failures
and timeouts originate in the driver, not a parser pretending to perform I/O.
Map these causes into the existing public error categories without exposing
private framing types or converting failures into empty messages.

A parser/integrity failure makes that connection's framing state terminal.
Do not scan for a new frame boundary or reuse the remaining socket stream.
The driver preserves the first cause, retires active plans/permits and
provisional ingress, settles affected operations once, and releases owned
resources as it exits. This internal cleanup does not add a public post-drop
join guarantee.
Timers remain driver-owned: partial-frame duration/progress, bounded discard,
operation deadline, committed-write watchdog, and shutdown drain are distinct.
Neither feeding a new chunk nor crossing a framing stage renews a deadline.

The deferred security integration preserves these positions:

```text
clear phase:       socket bytes -> FrameReader
handshake phase:   clear native security messages -> security state machine
protected phase:   socket -> TLS records/decryption -> FrameReader
outbound secured:  PreparedFrame -> TLS protection -> ordered socket output
```

The driver/security state machine, not `FrameReader`, owns phase transitions,
authentication, negotiation verification, and authorization release. Preserve
unread bytes at a transition and feed them to the new phase exactly once.
Keep ciphertext, plaintext, and security bookkeeping explicitly owned and
apply security-phase buffer/queue limits. No frame or application-body codec
implements TLS. Unsupported protection profiles reject explicitly; these
contracts require no TLS dependency or placeholder in the initial crate.

## Required evidence

Use pure codec/state-machine cases and deterministic driver I/O, not live
network timing as the only oracle:

- Native fixed-header/CRC vectors, zero-checksum semantics, provider checks,
  exact u16/u32 arithmetic boundaries, and source-valid frames rejected only
  by configured policy. Large boundary cases must not allocate giant bodies.
- Fragmentation at each fixed/header/body boundary, empty headers/bodies,
  multiple frames in one input, exact leftover bytes, and per-turn work caps.
- Both admission gates, stale/wrong-state decisions, allocation refusal before
  body retention, shared-buffer lifetime, and native header validation.
- Retain versus discard with identical CRC outcomes, corrupted/truncated
  discarded bodies, bounded-drain expiry, and no delivery/ACK on rejection.
- No published authority, RPC settlement, native control transition, or handler
  execution before complete frame verification; EOF at every receive stage.
- Scalar/vectored partial writes across every segment boundary, Pending,
  Interrupted, WriteZero, errors, cursor completion, and one receipt per frame.
- Cancellation/commit races, precommit preparation cancellation, immutable
  plans, committed suffix retention, frame-boundary scheduling, drain/abort,
  and release of temporary/provisional storage on every terminal path.

Future security qualification adds coalesced phase changes, protected-record
truncation, bounded plaintext/ciphertext retention, and ciphertext-backed
write receipts. Those are deferred TLS release gates, not clear-TCP support
claims. Independent native exchanges remain necessary for interoperability.

Native evidence uses the [pinned source baseline](Architecture.md#evidence-compatibility-and-scope):
[`TcpFrameHeader`](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpFrameHeader.h),
its [CRC/length/provider validation](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpFrameHeader.cpp#L47-L105),
[`TcpReceiveBuffer::GetNextMessage`](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpReceiveBuffer.cpp#L144-L193),
and [`Message` body segments](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/Message.h#L143-L166).
Earlier header-stage admission is an intentional bounded Rust adaptation, not
a claim that native handlers receive streaming bodies.
