# furpc: Native serialization and body codecs

Native header/control serialization is fixed by the Fabric protocol.
Application body adaptation is separate and replaceable. The
[codec implementation](../../../../crates/libs/furpc/src/codec.rs) defines
Encoder, Decoder, buffers, errors, whole-body helpers, and independent Fubst
directions; it is the authority for their signatures and bounds.

## Layer boundary

| Layer | Serialization responsibility |
| --- | --- |
| Native framing/messaging | Fixed native headers, connection controls, and transport Fault schemas using `fubst`. |
| Byte services and Router | Exchange or select complete encoded bodies without application decoding. |
| Typed clients and server adapters | Run explicit encoder/decoder values for actual protocol types. |
| Consumers | Choose schemas, validate method-specific headers, and define domain errors. |

Endpoint, connectors, ServerBuilder, and Router do not select body codecs.
A custom body format cannot replace native headers, CRCs, controls,
correlation, or an existing SF operation's specified wire schema.

## Native serialization

`fubst` is the native primitive/schema authority, not a second serializer
forked into furpc. Native header interpretation lives in
[wire/headers.rs](../../../../crates/libs/furpc/src/wire/headers.rs);
ListenInstance lives in
[connection/native.rs](../../../../crates/libs/furpc/src/connection/native.rs),
and transport Fault bodies in
[rpc.rs](../../../../crates/libs/furpc/src/rpc.rs).

Standalone construction and transport admission use native representation and
frame/body bounds. They do not expose a connection-wide serializer factory or
application codec override.

Absent trailing fields default only where their native schema declares a
default. A malformed present field is not an absent field. Unknown suffix
handling respects native object boundaries; opaque records retain encoded
bytes. UTF-16 fidelity does not depend on lossy Rust string conversion.

## Application body codecs

Encoder and Decoder are independent public traits. Each has its actual Item
type and mutably borrowed state. Decoder functions/closures also implement the
decoder contract directly. No coordinating codec trait, factory, or stored
connection-wide codec is needed.

| Operation | Direction states |
| --- | --- |
| `RpcClient::call` | Request encoder and response decoder. |
| `OneWayClient::send` | Request encoder only. |
| `server::unary` | Request decoder and reply encoder. |
| `server::one_way` | Request decoder only. |

Callers construct state as ordinary local setup; actual encode/decode work
runs inside the polled client or live server invocation. Async adapters apply
their own Send/ownership bounds without making the base traits require
Clone, Sync, or Serde. One-way paths impose no unused opposite-direction bounds.

`FubstEncoder<T>` and `FubstDecoder<T>` select native-format bodies independently.
Their explicit `with_limits` settings apply only to that body direction, not
native envelope decoding. The supported Serde mapping belongs to
[fubst](../../../../crates/libs/fubst/src/).
Root unit represents an absent body; an empty struct is a native object and
is not interchangeable with unit.

Ordinary typed boundaries carry DTOs until the actual encoder executes.
Genuine byte-oriented content may have an encoder that emits its bytes
unchanged. Consumer-owned frozen retry bytes are another intentional boundary,
not a reason to pass every DTO through a nominal byte codec.

## Header-aware method adaptation

Both EncodeBuf and DecodeBuf always expose immutable `Headers`:

| Work | Header view |
| --- | --- |
| Client encoding | Current request headers before native managed-field insertion. |
| Client decoding | Current response headers after native settlement. |
| Server decoding | Current admitted request headers after outer middleware edits. |
| Server encoding | Current typed reply headers. |
| Standalone helper | Empty headers. |

`DecodeBuf::with_headers` attaches explicit metadata for direct decoding.
Metadata is not source identity, a live invocation, or reply authority.
Header-dependent schemas must validate required fields explicitly.

The [client adapters](../../../../crates/libs/furpc/src/client.rs) bound input
before decoder work. The
[server adapters](../../../../crates/libs/furpc/src/server.rs) also validate
live original invocation/kind and use its admitted budgets and deadline.
Current header edits cannot replace the original admission snapshot.

## Bounded buffers and failures

EncodeBuf performs fallible byte writes and can append shared body segments
without copying their payload. Encoding finishes before native submission;
failure cannot leave a partial transport frame.

DecodeBuf views exactly one complete body, with checked cursor advancement,
shared `take` views, and fallible copies. Successful adapter/helper decoding
must consume or explicitly skip every byte. There is no incremental
need-more-TCP-input result or hidden message-stream framing.

Byte limits constrain encoded output and input, not arbitrary codec
allocations or decoded values. Zero permits only empty bytes. Invalid limits
and oversized input reject before codec execution. Native full-frame and
send-queue policy still apply separately.

Codecs are synchronous, trusted, cooperative application code, run outside the
socket driver without detached workers. They cannot be preempted universally
or charged to an aggregate transport heap quota.

CodecError distinguishes invalid payload from resource limits and retains the
source. Async adapters wrap it in `Error::Codec` with encode/decode phase.
Application codec failure is normally invocation-local, not a malformed native
connection. It never creates a success-shaped empty body or automatic Fault.

## Public body helpers

`codec::encode` and `codec::decode` reuse the whole-body buffer implementation
without transport submission. They perform no readiness, I/O, task creation,
correlation, deadline renewal, or invocation admission.

These helpers are appropriate when the domain intentionally owns an encoded
snapshot or decoding occurs after a transport invocation has ended. They must
not reconstruct live server authority from retained metadata.

Runtime fault/endpoint reporting encodes once into immutable shared Body
storage. Retry scheduling, frozen schema fields, fresh send identities, and
failure-atomic ownership replacement remain Runtime responsibilities.

Runtime deferred one-way ingress decodes after owned executor admission;
furpc serving has already completed at bounded enqueue. Calling
`server::one_way` there would incorrectly require live invocation authority.
Hosting's opaque lifecycle handoff likewise retains original Entry provenance
for Node's later bounded native decode. Their actual locations are listed in
[Consumer composition](Implementation.md#owned-byte-facing-boundaries).

Buffer, custom-format, header-aware, and complete-consumption examples live in
[client tests](../../../../crates/libs/furpc/tests/client.rs) and
[codec unit tests](../../../../crates/libs/furpc/src/codec/tests.rs).
