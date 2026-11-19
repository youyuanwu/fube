# furpc: Native serialization and body codecs

**Status:** proposed; see the [design summary](README.md).

Separates native header/control serialization from application-body
codecs. `fubst` is the native authority and the default body codec, not
a required application format. See [requests](Requests.md) for typed
adapter bounds and [examples](Examples.md) for a custom codec.

## Layer boundary

| Layer | Serialization contract | Configuration |
| --- | --- | --- |
| Native messaging | Built-in native headers, connection controls, and transport Fault schemas using `fubst`. | Native widths and configured transport policy; no extra constructor ceilings or public native-serializer selection. |
| Service routing | Dispatch byte-facing services without body transformation. | Native route keys and ingress policies; no body format or schema selection. |
| Client transport services | Channel or custom Tower services exchange native byte envelopes without application decoding. Peer is a separate native capability, not a service. | Transport admission, deadlines, and native correlation where applicable; no application schema/codec. |
| Typed RPC adapters | Public application-body `Codec`, with `FubstCodec` as the default native convenience choice. | Codec consumed per client call or constructed in the server method service; explicit client codec limits or admitted native duplex/server budgets, plus codec-local work limits. |

Raw clients and servers need native serialization for headers and connection
setup even when they never decode an application body. That implementation
stays inside the transport. Body codecs stay above it and can differ between
operations on one connection. Endpoint, the server builder, connectors, and the
router do not choose an application-body codec.

## Native serialization

Extend the existing codec rather than forking it into `furpc`. Required work
includes selectable finite limits, missing native value/skip operations, and
the supported schema-evolution behavior. This is the native header/control
serialization authority and the implementation behind the default body codec,
not a mandatory format for all application bodies.

The transport constructs its scope-owned serialization context from validated
native limits in `TransportLimits`. Public callers configure finite resource
policy, not an encoder/decoder factory or native format override.
Standalone `Message::builder` and `HeaderRecord` constructors use the same
fixed implementation under native representation limits; they do not
capture a client's or server's limits. Submission applies the actual scope
context as specified by
[message construction](Messages.md#construction-and-submission-limits).
Application codec selection cannot change connection setup, native header
interpretation, or native control/Fault decoding.

Absent trailing fields default only where a schema declares the native
initialized value. A real scope end is different from a malformed present
field. ListenInstance's trailing nonce and Fault's trailing body flag are
concrete required cases.

Unknown-field handling must respect object/scope boundaries and metadata.
Preserve unknown extension data where forwarding requires lossless
re-emission; read-only domain decoding may explicitly skip supported suffixes.
Unknown required polymorphic variants fail, rather than producing defaults.

Preserve raw UTF-16 where wire fidelity requires it. Conversion to a Rust
`String` is fallible; do not normalize or replace invalid code units silently.
Manual and Serde schemas use the same primitive authority and limits.

## Application body codecs

Follow [tonic's codec separation][tonic-codec]: the RPC adapter owns typed-body
adaptation above byte transport; a user-replaceable codec maps typed values to
and from application-body bytes. Provide public, externally implementable `Codec`,
`Encoder`, and `Decoder` traits. Custom implementations need no Serde traits
or access to crate-private transport state.

The traits below are implemented in `furpc::codec` and re-exported at the crate
root, together with `FubstCodec`, `EncodeBuf`, `DecodeBuf`, and `CodecError`. The
[typed RPC contract](Requests.md#rpc-calls-and-responses) specifies its
typed-client ownership and concurrency requirements.

```rust,ignore
pub trait Codec {
    type Encode;
    type Decode;
    type Encoder: Encoder<Item = Self::Encode>;
    type Decoder: Decoder<Item = Self::Decode>;

    fn encoder(&mut self) -> Self::Encoder;
    fn decoder(&mut self) -> Self::Decoder;
}

pub trait Encoder {
    type Item;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut EncodeBuf<'_>,
    ) -> Result<(), CodecError>;
}

pub trait Decoder {
    type Item;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Self::Item, CodecError>;
}
```

`Encode` is the locally sent type and `Decode` the locally received type.
They need not be equal; client and server codecs reverse these directions.
Use tonic's distinct client/server ownership patterns:

- Client [`Grpc<T>`][tonic-client] wraps transport and takes a codec per RPC.
  Furpc's `RpcClient<T>` likewise wraps a byte-facing Tower service; Channel is
  the sole built-in transport, with custom middleware/services also supported.
  Calls are ordinary async methods using `&mut self`, with no stored codec or
  schema type parameters. The separate native DuplexClient retains a bound
  Peer and consumes a codec per notification.
- Server [`Grpc<C>`][tonic-server] wraps a codec at the method adapter.
  Furpc's [`server::Rpc<C>`](Services.md#typed-rpc-adapters) is constructed
  inside a byte-facing method service and adapts a typed service. Ordinary
  Tower composition creates the codec per invocation; the router merely
  dispatches that outer service, without codec-specific configuration or bounds.

Different body types and codecs can share the current native generation and
its pending table. RpcClient remains usable across Channel reconnection for
later independent calls; failed invocations and their codec state are not
re-encoded or replayed. Encoder/decoder state belongs to the operation/invocation,
never to the socket parser. Client and per-invocation server codecs need Send and owned Send
encode/decode state, not Clone or Sync. Stored outer route services may need
Clone/Sync; those bounds do not apply to codecs or typed services created
inside their futures. One-way method adaptation uses only decoding, without
unused encode-state bounds.
Keep those execution bounds on the corresponding APIs, not every use of the
public codec traits.

`FubstCodec<Out, In>` implements `Default` and is the default format selected by
native domain/convenience code. Server method code supplies it explicitly to
`server::Rpc::new`, just as client calls receive it explicitly. There is no
hidden connection-wide default. Its built-in Serde mapping uses the documented
`fubst` adapters and schema restrictions,
not an assumption that every Serde data model has a native encoding. Users
can implement another codec for their own types and protocol, or use raw
messages without a typed codec at all.

The extension point does not replace the fixed native frame, CRC, header,
control, correlation, or security formats. Native headers and transport-control
bodies still use their native `fubst` schemas regardless of the application
codec. RPC processes native transport Faults before invoking the successful
reply's body decoder. A custom body codec therefore does not remove that native
dependency.
An existing native gateway operation still requires its specified body bytes;
choosing JSON or protobuf does not make that operation interoperable.

Codec selection is trusted local configuration agreed by the application
protocol. Do not invent a mandatory codec-ID header, automatic format sniffing,
or peer-selected plugin loading.

Each decode sees exactly one complete body, with no fixed/native headers or
bytes from the next message. It returns one value, not "need more TCP bytes".
Successful decode consumes the entire body; format-defined extensions must be
explicitly consumed or skipped. Domain streaming across multiple messages is a
separate protocol, not implicit codec framing.

Tonic's gRPC record prefixes, compression, and streaming bodies are not part
of this native wire contract. Reuse the RPC/codec boundary, not another
framing layer or an HTTP body dependency.

## Bounded buffers and failures

`EncodeBuf` supports fallible writes and shared segments without flattening.
Native message representability and any explicitly selected codec restrictions
bound encoded output; there is no additional mandatory construction profile.
Encoding finishes before submission, so failure cannot leave a partial frame.
`write_all` copies bytes into the current output segment; `append_body` adopts
shared visible segments. `remaining` reports the operation's encoded-byte
allowance. `DecodeBuf` supports checked advancement, shared `take` views,
`copy_to_slice`, and fallible `copy_remaining`.

`DecodeBuf` exposes exactly one complete body. Its
`copy_remaining() -> Result<Vec<u8>, CodecError>` consumes and fallibly copies
that range, without attaching a charge to the resulting value. Decoded values
and custom codec allocations are ordinary application-owned Rust data.
Request/response parts preserve metadata and provenance, not hidden allocation
reservations. Zero-copy byte views retain backing storage by reference counting.

Run codec work outside the socket driver, without detached workers. Custom
code is trusted and cooperative; the transport does not universally bound its
CPU work or heap usage. Explicit codec-specific limits remain independent of
native frame/send-queue policy.

Preserve codec failure phase, source, and resource-exhaustion category through
`CodecError`. A malformed application body fails its typed call or handler;
it is not automatically a connection-fatal native Protocol error. Malformed
native headers and controls remain connection-protocol failures. A codec
cannot manufacture reply authority, change routing metadata, or turn a local
decode failure into a successful domain response.

Typed adaptation reports `Error::Codec { phase, source }`, with `phase` equal
to `encode` or `decode`. `CodecError::InvalidPayload` and `ResourceLimit` retain
their original error sources. `FubstCodec::with_limits` optionally selects
codec-local `fubst::Limits`; default operations add no depth/work ceiling.

[tonic-codec]: https://docs.rs/tonic/latest/tonic/codec/trait.Codec.html
[tonic-client]: https://docs.rs/tonic/latest/tonic/client/struct.Grpc.html
[tonic-server]: https://docs.rs/tonic/latest/tonic/server/struct.Grpc.html
