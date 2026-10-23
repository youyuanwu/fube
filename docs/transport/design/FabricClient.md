# Pure Rust FabricClient design

**Status:** proposed; no Rust wire implementation or live interoperability is
claimed by this document.

## 1. Goal and protocol baseline

Build an asynchronous, pure Rust client for the Service Fabric **native
client-to-gateway protocol**, using Tokio for TCP and scheduling and rustls for
TLS. Existing clusters should accept its supported operations without a proxy,
cluster modification, native FabricClient library, or COM dependency.

The protocol baseline is the native source at commit
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][native-revision], as described by
[FabricClient.md](../FabricClient.md) and [Transport.md](../Transport.md).
Those documents describe observed native behavior; this document makes
implementation decisions. Proposed defaults and milestones below are not
claims about native defaults.

The [implementation plan](ImplementationPlan.md) expands the delivery steps and
uses the available onebox as the native interoperability oracle; a complete
native Service Fabric build is not a prerequisite.

Wire compatibility means that a native gateway can authenticate the client,
decode its messages, execute supported operations, and exchange replies and
notifications with it. It does not require identical TLS ciphertext, identical
TCP packetization, or reproduction of the native client's internal classes.
Compatibility must be qualified by runtime release, gateway protocol version,
security profile, and operation coverage.

**Scope is TCP and TLS only.** The TLS profile protects complete Fabric frames.
Platform-specific authentication and alternate message-protection formats are
out of scope. Their implementation details are intentionally excluded.

## 2. Scope and delivery boundary

| Area | Decision |
|---|---|
| Network | Persistent TCP connections to explicit gateway `host:port` endpoints, including bracketed IPv6 addresses. |
| Primary security | Mutual-certificate TLS with whole-frame protection. |
| Additional TLS authentication | Claims tokens over the same TLS transport, after the certificate profile works. |
| Clear TCP | Explicit development/test configuration for unsecured gateways; never an automatic fallback from TLS. |
| Public API | Owned Rust types and `async` operations, not a source- or ABI-compatible COM implementation. |
| Initial operations | Gateway ping and `NameExists`; then typed queries and service resolution. |
| Later operations | Notification sessions and incrementally ported management families, health reporting, and file transfer. |
| Excluded protocols | HTTP management, application remoting, replication, cluster-internal routing/IPC, and a FabricTransport service/listener implementation. |
| Excluded integrations | Native credential stores, local-node configuration discovery, and native client pass-through factories. |

A supported TLS gateway must have a common TLS version, cipher suite, signature
algorithm, certificate policy, and whole-frame protection behavior with the
chosen rustls configuration. An otherwise valid Service Fabric cluster outside
that profile is unsupported, not a reason to weaken validation or switch to
clear TCP.

The first release is a compatible **subset**, not a complete replacement for
every `FabricClient.idl` interface. Track method-level coverage explicitly.
Methods that are not implemented must not appear successful merely because an
equivalent-looking HTTP operation or generic gateway request exists.

### 2.1 Meaning of pure Rust

Client behavior, serialization, transport state machines, and the TLS protocol
implementation are Rust. Production crates must not depend on the native
Service Fabric client, COM, OpenSSL, or an external protocol sidecar.
Test-only native programs may serve as independent interoperability oracles.

The cryptographic provider is a separate dependency decision: rustls does not
imply that every cryptographic primitive is implemented in Rust. Its common
`aws-lc-rs` and `ring` backends include native code. An all-Rust cryptographic
dependency profile must be selected explicitly rather than silently accepting
defaults.

The RustCrypto rustls provider is a candidate for a strictly Rust cryptographic
profile, but its current README explicitly warns against production use.
Therefore **production approval of that profile is a release gate**, not an
assumption. Do not write cryptographic primitives for this port or substitute an
unreviewed backend to satisfy the label. A native cryptographic backend would
be a separately approved relaxation, not the all-Rust profile.
[Sources: [rustls providers][rustls-providers],
[RustCrypto provider status][rustcrypto-provider].]

## 3. Architecture and repository placement

Propose two workspace crates under the existing `crates/libs/*` member pattern:

```text
Application
    |
mssf-client
    public owned models and async APIs
    operation schemas / query result dispatch
    gateway manager / resolution cache / notification sessions
    |
mssf-transport
    Fabric serialization / native headers / message IDs
    TCP frame codec / request correlation / control-message dispatch
    Tokio connection driver / rustls security state machine
    |
Tokio TcpStream + rustls ClientConnection
    |
Native Service Fabric gateway
```

`mssf-transport` owns the common native messaging protocol, not the public
`IFabricTransportClient` API. `mssf-client` adds gateway operations directly.
This preserves the native architecture's sibling relationship between
FabricClient and FabricTransport without implementing application service
paths, logical service `Connect`, or application-header splitting.
[Source: [native client transport adapter][native-tcp-adapter].]

Keep serialization and framing as pure modules inside `mssf-transport`; do not
introduce a separate crate or code-generation tool until reuse warrants it.
Initially use this structure:

```text
crates/libs/mssf-transport/src/
    serialization/       typed primitives, objects, scopes, limits
    headers/             records, known schemas, unknown records
    frame.rs             fixed framing and CRCs
    message.rs           owned header/body representation
    connection.rs        socket, security, buffers, lifecycle
    security.rs          rustls driver and certificate policy
    request_reply.rs     pending requests and duplex routing

crates/libs/mssf-client/src/
    client.rs            cloneable public handle and builder
    gateway.rs           endpoint selection and establishment
    protocol/            versioned operation bodies and envelopes
    naming.rs            naming/property operations
    query.rs             typed queries and paging
    resolution.rs        partition lookup and versioned cache
    notifications.rs     filters, synchronization, acknowledgments
    error.rs             native codes and Rust error context
```

These are proposed paths, not files added by this design. Existing native-backed
crates remain unchanged and are not production dependencies of either crate.
Reuse workspace `tokio`, `tokio-util`, `bytes`, and `tracing`. Add rustls and its
explicit provider only when implementation starts. Pin a mutually compatible,
maintained dependency set and review its actual Cargo feature graph. The rustls
0.23 API references below illustrate the required low-level interface, not a
requirement to freeze that release.

## 4. Public API and operation coverage

Expose a cheap-to-clone `FabricClient` backed by shared client state. Building a
client validates configuration locally; connection is lazy, with an explicit
`connect()` convenience for callers that want early failure. Connections are
owned by the client and may outlive an individual operation.

Illustrative API, not a finalized compilable signature:

```rust
let client = FabricClient::builder()
    .gateways(["gateway.example:19000"])
    .security(Security::X509(credentials))
    .build()?;

client.connect().await?;

let name = NamingUri::parse("fabric:/app/service")?;
let exists = client
    .naming()
    .name_exists(&name, RequestOptions::with_timeout(Duration::from_secs(30)))
    .await?;

client.close().await?;
```

Credential material is supplied explicitly as certificate chains and private
keys in supported encodings. Separate the TCP endpoint from the expected TLS
server identity; an IP endpoint need not be the identity in the certificate.
Security configuration has no implicit insecure default.

Use Rust-owned results, typed partition keys, stable identifiers, and explicit
paging/continuation tokens. Unknown enum values and native error codes must be
representable without `transmute` or lossy conversion. A `NamingUri` must match
native parsing/canonicalization rules; using a generic URL parser and sending its
string is not sufficient.

Every implemented operation needs a coverage entry recording:

| Required evidence | Purpose |
|---|---|
| Public operation and supported options | Prevent a partial method from looking complete. |
| Outer actor/action and optional forwarding metadata | Identify the gateway route. |
| Ordered request/reply schemas and type information | Define actual wire compatibility. |
| Required headers and supported protocol versions | Avoid assumptions based on COM interface suffixes. |
| Remote errors, paging, retry classification | Preserve operation semantics. |
| Native-source references and independent live expectations | Make the mapping reviewable. |
| Live gateway result and tested runtime/security profile | Separate implementation from demonstrated support. |

Start with an in-repository coverage table near the implementation, not a
runtime plugin registry. Ordinary `serde` derives, protobuf schemas, COM
structs, and JSON models are not sources of truth for these entries.

## 5. Native serialization

The custom serializer is the first major interoperability dependency. A correct
12-byte frame codec cannot compensate for an incorrectly encoded header object
or operation body.

Port the native format's observable behavior, not its C++ class hierarchy.
The [message-format specification](../FabricClientMessageFormat.md) describes
the byte-level encoding and representative schemas. Keep frame parsing, raw
header-record boundaries, and TLS handling outside the object serializer;
operation/header schemas call the same serialization primitives.

The implementation uses bounded `serialization::{Encoder, Decoder}` cursors
and explicit scope helpers. `serialization::serde` now adds ordered DTO derives
over those same primitives, alongside manual schemas where native boundaries
or validation demand them. `FabricEncode`/`FabricDecode` were an initial design
direction, not implemented traits. Keep native field order and compatibility
semantics visible rather than infer them from Rust layout.

### 5.1 Required representations

| Value | Encoding requirement |
|---|---|
| Serializable object | Object marker, native object-header layout, optional type information, scopes, object-end marker. |
| Object header | Profiled native size/layout, including padding; size excludes the preceding Object marker. Never serialize a Rust struct's memory image. |
| Integers | Typed metadata and most-significant-7-bit-group-first compression; signed values use the native sign-extension rule, not ZigZag. |
| Zero/empty values | Match empty-value metadata and the distinct no-metadata count rules. |
| Strings | Counted UTF-16 code units in little-endian representation; count is not UTF-8 bytes or Unicode scalar values. |
| GUIDs | Native typed GUID representation, including field byte order; not blindly `Uuid::as_bytes()`. |
| Message IDs | Process GUID plus 32-bit index, with the native primitive-field structure. |
| Enums | Match each native serializer specialization; ordinary enums pass through signed 64-bit encoding. |
| TimeSpan | Signed 64-bit 100 ns ticks, with explicit infinity and overflow handling. |
| Containers | Native count/key/value ordering, metadata and nested-object rules, not a generic map format. |
| Polymorphic objects | Native type information and recognized variant schemas; do not guess a variant from body length. |

The target little-endian ABI's `ObjectHeader` is expected to occupy eight bytes:
four size bytes, one flag byte, and three padding bytes. Qualify that profile
using real onebox messages; source inspection guides implementation but does
not establish runtime compatibility. Emit
deterministic zero padding, but do not reject a valid native object because its
padding bytes are nonzero. Native C++ padding is not a semantic field.
[Sources: [object writer][native-object-writer],
[object-header declaration][native-object-header],
[integer compressor][native-compressor].]

### 5.2 Evolution and malformed input

The decoder must bound every object and scope by its containing range. Implement
the native rules for absent trailing fields, extra scopes, unknown extension
data, nested objects, and null/optional values. An unknown field is not
permission to scan for the next marker byte; strings and binary data can
contain the same bytes as markers.

Raw unknown header records are preserved. Read-only object results may skip
supported trailing values/scopes according to the bounded native rules; the
current manual and Serde decoders do not retain arbitrary object extension
data for lossless re-emission. Unknown required polymorphic result kinds fail
explicitly with Protocol errors, never an empty/default success. Lossless
object-extension storage remains outside the delivered subset.

Internally represent wire strings as UTF-16 code units where exact preservation
is needed. Conversion to a public Rust `String` is fallible; never use lossy
conversion for identifiers or request fields.

Use checked arithmetic for lengths, counts, integer decompression, nesting,
and allocations. Reject truncated scopes, integer groups beyond the supported
width, overflow, object
sizes outside their parent, unsupported required flags, and inconsistent array
metadata. Parsing must make progress or fail without panicking.

### 5.3 Serde is not the wire format

Serde derives describe how a Rust value maps into Serde's data model; a format
backend decides the bytes. Deriving `Serialize`/`Deserialize` and selecting
bincode, postcard, or JSON does **not** produce the native Fabric encoding.

| Approach | Decision |
|---|---|
| Stock Serde derives with an existing general-purpose format | Not wire-compatible; do not use for native messages. |
| Serde derives with the custom Fabric `Serializer`/`Deserializer` | Delivered for documented ordered DTOs; reuses the native codec with explicit adapters and field bridges. |
| Manual Encoder/Decoder schema operations | Retained as an incremental escape hatch; native distinctions use explicit Serde adapters over the same primitive authority. |

The delivered backend handles existing supported primitive metadata, compressed
integers, UTF-16, object extents and ordered single-scope structs. It does not
remove native distinctions absent from a generic Rust model:

- **Container kind:** an empty native string vector is `48`, an empty byte
  array is `c4`, and an empty object array is `c0`. A generic
  `serialize_seq(Some(0))` call does not identify the element type or native
  helper. `ByteArray` and `ObjectArray` select their metadata even when empty.
  Generic sequences and native string-vectors are not inferred;
  `CountedStrings` explicitly represents NamingUri's UInt32 count followed by
  inline strings, including its zero-count form.
- **Native value types:** GUID layout, TimeSpan ticks, and explicit signed
  64-bit enum codes need wire-specific conversions. Ordinary derived enum
  variant indices are not native numeric codes. `NativeEnum` selects a native
  i32 type tag and matching i64 kind from explicit variant renames, then uses
  Serde EnumAccess/VariantAccess for the derived payload. `NumericEnum` separately
  handles strict i64 scalar unit enums; unknown-retaining node enums share
  signed numeric Serialize/Deserialize implementations.
- **Scope and nesting:** inline MessageId fields differ from a nested ActivityId
  object. Native base/derived scopes are not automatically equivalent to
  Serde struct nesting or `flatten`. `Inline` and `Derived` represent these
  distinctions without adding incorrect nested objects.
- **Version preservation:** absent trailing fields, raw unknown scope data,
  and polymorphic type information need explicit decoder/storage behavior.
  Ordinary derives do not automatically preserve that native information.
  `Trailing` distinguishes true scope-end absence from malformed present data;
  `Pointer` distinguishes null metadata from absence. `FixedTag`/`Untagged`
  automatically validate fixed object tags before deriving the fields;
  `Typed`/`NativeType` remain an escape hatch for exceptional raw tag policies.

`to_vec`/`from_slice` encode/decode native values; struct roots include an object,
scalar roots do not, and complete root consumption is checked.
`to_fields`/`from_fields` bridge ordered DTO fields inside an existing native
scope without adding another object. Newtype structs are transparent; use a
named one-field struct for an actual nested wrapper. The shared codec enforces
existing string/count/depth/object bounds; 4 MiB remains per object, not a new
aggregate cap on standalone root object arrays.

Supported ordinary widths are u16, i32/u32, i64/u64, bool and strings. GUID uses
explicit native-order `Guid`; native enum/timestamp/tick scalars use i64 wire
fields. Semantic `NodeTimestamp` maps transparently to signed ticks; numeric
`NodeId` maps directly to its 16-byte array rather than GUID metadata.
Generic tuples/sequences/maps/Option/enums/unit/floats/
chars/other integer widths fail explicitly unless wrapped by a documented
adapter. Pointer/type/scope selection and real-delimiter suffix absence use
the same codec through explicit adapters, not generic Option/enum inference.
Domain defaults and validation belong in conversion code.

Fixed-order wire DTOs must not use positional omission, flattening, defaults or
Serde enum-representation attributes. Canonical numeric variant renames select
codes explicitly for the native enum adapters, never via ordinal/discriminant
inference. Unsupported backend calls fail, but attributes/custom serializers
hidden from the backend cannot universally be detected; wire stability depends
on the documented schema contract and independent byte/native tests. Static
Serde custom errors preserve InvalidInput for encode and Protocol for decode.
Existing accepted redundant integer groups within width and counted-zero
strings/bytes remain accepted; canonical-shortest input is not newly required.

### 5.4 Adoption and qualification

Initial handwritten schemas established bootstrap, header, ping and operation
compatibility. The Serde adoption now covers listen controls/common header
writers, query requests/results, ping/NameExists replies, complete node and
health wire bodies, naming URIs, application/service records and optional
fault details. MessageId remains
inline and Activity nested; query argument order is preserved and QueryAddress
is not added as a body field. Safety checks and health evaluations share derived
enum dispatch, including tag/kind agreement checks. Fixed query-wrapper and
empty naming URI tags no longer need custom deserialization callbacks.
Application/service suffixes use explicit Trailing
fields. Aligned health/event/evaluation/deactivation models deserialize directly
using shared field adapters rather than a parallel set of wire records.
`NamingUri` is a checked semantic type with a private native component schema,
not an arbitrary String. Naming query fields and application/service result
names use it; string naming-call convenience remains supported. JSON query
shapes are unchanged, with invalid names rejected on construction/deserialization.
Strict scalar status/definition enums map their numeric codes directly; shared
`HealthState` retains unknown codes, with list-specific rejection after decoding.
URI redundancy is checked by semantic construction above the generic backend;
node-ID agreement, duplicate parameters and cross-field checks remain validated
record conversions. Helpers remain for actual layout differences (duplicate
fields, nested task IDs, native safety variants, suffixes and service kind).
Query payload
validation still precedes embedded remote errors. Paging tag strictness remains
operation-specific, and partial operation-failure diagnostics retain deliberate
incremental handling so malformed text cannot erase a nonzero error code.

Independent old/new encoded-byte comparisons, malformed/suffix/bound tests and
the same four live onebox scenarios passed for the initial Serde migration on SF11.8.121.4.
Qualification used an owned unpublished runtime and nonroot network-namespace
archive consumer; exact fixture and runtime cleanup were verified. This is
evidence for the exercised subset, not every native schema. No C++ fixture
build, committed capture files, generic derive macro or general-purpose format
is required. See the [public mapping contract](../RustFabricClient.md#41-native-serde-payloads)
and its source links for supported adapters, integration and limitations.
The later shared-adapter and direct-model expansions add offline differential/malformed tests
and rerun independent model-peer scenarios; they do not constitute a fresh
live onebox qualification.

## 6. Frames, headers, and messages

### 6.1 Shared frame codec

The fixed header is encoded field by field:

```text
offset  size  field
0       4     frameLength: u32 little-endian, includes the fixed header
4       1     securityProviderMask
5       1     frameHeaderCRC
6       2     headerLength: u16 little-endian
8       4     frameBodyCRC: u32 little-endian

frame = fixed_header[12] || native_headers[headerLength] || body
```

Validate `frameLength >= 12`, `headerLength <= frameLength - 12`, local limits,
and all arithmetic before reserving the full frame. Support partial fixed
headers, partial frames, multiple frames per read, and EOF in every position.
Return bounded `Bytes` slices for complete frames.

Verify nonzero CRCs using the native algorithms: CRC-8 polynomial `0x07`,
initial zero, over the fixed header with both CRC fields zeroed; reflected
CRC-32 polynomial `0xedb88320`, initial/final XOR `0xffffffff`, over native
headers plus body. Zero means absent. Match native CRC generation for clear
traffic and omit optional CRC generation on protected sends; TLS remains the
integrity mechanism.
[Sources: [frame layout/validation][native-frame],
[receive buffering][native-receive],
[Transport CRC rules](../Transport.md#62-checksums).]

The relevant provider masks are `0x00` for clear TCP, `0x01` for certificate
TLS, and `0x08` for claims-over-TLS. Validate consistency **within each incoming
direction**, not equality with the locally emitted mask: a claims client and
its TLS server can use different local provider values. The mask never selects
a security fallback.

### 6.2 Header model

Each native record is:

```text
u16 id || u16 serialized_object_length || Fabric-serialized object
```

Both prefix integers are little-endian; the length excludes the prefix.
The total header area cannot exceed 65,535 bytes. Preserve record order and
duplicates in the raw representation; a map would lose information. Typed
lookups must follow native visible-record/first-match behavior. Skip deleted
records with ID `0x8000` by their length and safely skip unknown records.

Initially implement actor, action, MessageId, RelatesTo, ExpectsReply,
FabricActivity, Timeout, Fault, HighPriority, ClientProtocolVersion,
SecurityNegotiation, and ServerAuth. Add forwarding, query, identity, and
notification headers with their corresponding operations.

Dispatch headers by protocol context, not ID alone. In particular, the claims
negotiation `ClientAuthHeader(providerType_)` uses `0x8029`; `0x8035` is not its
binding. Its presence does not enable an alternate data-protection format.
[Sources: [header identities](../Transport.md#63-native-message-header-records),
[ClientAuthHeader binding][native-client-auth].]

### 6.3 Message ownership

A message owns immutable native-header bytes and body bytes, with decoded
metadata cached only when useful. FabricClient operation bodies are native
serialized objects, not FabricTransport's application-header/body concatenation.
Do not add an application-header length record or service-path suffix.

Socket writes, TLS records, and frame boundaries remain independent. An
outgoing buffer must live until all its bytes have been consumed by the next
layer. Reusing `Bytes` can avoid body copies; TLS encryption still requires
provider-managed output buffers.

## 7. Tokio and rustls integration

### 7.1 Why not a normal `TlsConnector`

Fabric does not start TLS at TCP byte zero:

```text
During security negotiation, TCP contains:
    clear Fabric frame {
        actor = SecurityContext (35)
        MessageId and negotiation headers
        body = TLS library output
    }

After the transition, TCP contains:
    TLS records {
        plaintext = TcpFrameHeader || native headers || body
    }
```

There is no extra clear Fabric header before secured data records.
`tokio_rustls::TlsConnector::connect(TcpStream)` expects an ordinary TLS byte
stream and therefore cannot implement the initial exchange directly.

Use rustls's low-level `ClientConnection` API from a Tokio connection driver.
This is a byte-routing adapter around rustls, not a custom TLS implementation.
Do not use rustls's convenience `complete_io()` against the raw socket during
Fabric negotiation.
[Sources: [native TLS token production][native-tls-negotiation],
[native transition][native-security-transition],
[rustls ClientConnection][rustls-connection].]

### 7.2 Separate wire phase from authentication readiness

Track independent state rather than one `connected` flag:

| State dimension | Values / purpose |
|---|---|
| Socket | Connecting, open, draining, closed. |
| Receive wire mode | Clear Fabric frames, then raw TLS records. |
| Transmit wire mode | Fabric-wrapped TLS output, final wrapped flight queued, then raw TLS records. |
| TLS | Handshaking, established, failed. |
| Peer trust | Unverified or certificate policy accepted. |
| Negotiation verification | Awaiting first protected message or checked. |
| Server status | Not promised, awaiting promised status, success, denied. |
| Claims | Not applicable, acquiring, queued/submitted, denied. |
| Gateway | Not established, ping pending, ready. |

Every physical connection, including an additional priority connection, completes
its own TLS and transport-control processing. Client-wide state must not mark a
new socket authenticated just because another socket is ready.

### 7.3 Driver algorithm and handoff

1. Create `ClientConnection` with explicit trust, credentials, protocol versions,
   and crypto provider. Do not write application plaintext into it yet.
2. Drain its initial TLS output and wrap it as a clear Fabric security message.
   Attach the first `SecurityNegotiationHeader`, and the claims indication when
   applicable. Preserve partial socket-write offsets.
3. Read complete clear Fabric frames. Validate the security-phase envelope;
   retain negotiation metadata separately and feed only the message body to
   `read_tls()`/`process_new_packets()`. Continue until that body's input is
   consumed, respecting rustls backpressure and errors.
4. Drain output generated by that handshake step into Fabric-framed TLS tokens.
   One token can contain multiple TLS records. Do not impose record or TCP
   packet boundaries on token bodies.
5. When rustls finishes its handshake, drain the **final handshake output under
   the old Fabric-wrapped mode**, even if `is_handshaking()` is already false.
   Queue that complete clear frame ahead of any raw TLS output. No ordinary
   application bytes may have been written into rustls before this drain.
6. Switch receive processing at the completed incoming Fabric-frame boundary.
   Any unconsumed TCP bytes after that boundary belong to the new raw TLS input
   path; they may already include a protected control message from the same read.
   Do not discard them or parse them as another clear Fabric header.
7. Process subsequent TCP bytes through rustls, drain all available plaintext
   into the inner Fabric frame decoder, and dispatch complete inner frames.
   Feed outgoing complete Fabric frames to rustls's plaintext writer, then
   queue its TLS output directly on TCP.
8. Keep reading and flushing rustls-generated protocol output throughout the
   connection lifetime, even with no application sends. Handle alerts,
   `close_notify`, and supported post-handshake records. A fatal TLS error ends
   the connection; any alert is sent only using the current wire phase and a
   bounded close deadline.

The transmit and receive transitions are coordinated but not a single socket
write/read event. A final handshake frame and raw encrypted bytes may be
adjacent in one send queue, provided ordering is preserved. A rustls internal
buffer must never hide an unclassified mixture of handshake and application
output at the transition.

Disable TLS early data and resumption in the first profile. Start live
compatibility with TLS 1.2; enable TLS 1.3 only after its different final-flight
and post-handshake behavior passes the same handoff tests against a native
gateway. The native source alone does not prove deployed TLS 1.3 support.
Never enable obsolete protocols or invent a rustls renegotiation fallback.
[Source: [rustls supported features and exclusions][rustls-features].]

### 7.4 I/O scheduling and cancellation

One task owns a connection's socket, rustls state, frame parsers, and ordered
output queue. Drive readiness with Tokio; maintain explicit offsets for partial
reads/writes and bounded work per poll. Do not split security state across
independently locked reader/writer tasks.

Interleave receive progress, output flushing, timers, and commands fairly.
Do not await a whole large `write_all()` while neglecting incoming authorization
or replies. A bounded `mpsc` carries commands; a reserved control budget prevents
ordinary requests from exhausting all queue space.

Cancellation cannot remove a frame suffix already entering the byte stream.
Likewise, discarding already-generated TLS records would corrupt TLS sequencing.
Once transmission has begun, finish the ordered output or close the connection;
cancel only the caller's wait. Expired requests still wholly outside the
transport encoder can be removed without sending.

## 8. TLS trust and transport controls

### 8.1 Certificate policy

Mutual-certificate mode supplies a client chain and signing key, validates the
server's identity, and still observes gateway authorization. Configure:

| Policy | Behavior |
|---|---|
| CA plus expected identity | Validate certificate path, purpose, validity, and the configured DNS/IP identity using a supported verifier. |
| Explicit certificate allowlist | A separately reviewed verifier for configured exact certificate identities, with documented validity/purpose/revocation rules and TLS proof-of-possession checks. |

The initial deliverable may support only CA plus expected identity. Explicit
pin/allowlist support is a separate compatibility milestone because native
certificate authorization is not equivalent to browser hostname verification.
Do not claim complete native certificate-policy parity.

A custom `ServerCertVerifier` must actually parse/authorize the certificate and
delegate TLS 1.2/1.3 handshake-signature verification to supported cryptographic
verification routines. Returning unconditional success from either certificate
or signature methods is prohibited. Self-signed deployments need an explicitly
supported trust profile, not a disabled verifier.
[Sources: [native security behavior](../Transport.md#75-authentication-is-more-than-handshake-success),
[rustls verifier contract][rustls-verifier].]

Revocation policy must be explicit. Do not promise native-equivalent online
revocation checks merely by loading CA roots. If required revocation evidence
cannot be verified by the selected implementation, reject that configuration.
Never log private keys, tokens, plaintext secret operations, or TLS session keys.

### 8.2 Negotiation metadata

Advertise whole-frame protection in `SecurityNegotiationHeader` using its native
ordered fields:

```text
x509ExtraFramingEnabled_   bool, deprecated compatibility field
framingProtectionEnabled_ bool
listenInstance_           ListenInstance
maxIncomingFrameSize_     native serialized size value
```

Set the compatibility field as the native producer does, set framing protection
true, and advertise the actual finite receive limit. For the initial 64-bit
native profile, encode the size field through the native unsigned 64-bit
serializer. Do not serialize a Rust `usize` according to the build host.

Remember the first peer negotiation header. Add the same local negotiation
header to the **first protected outgoing message**, including when that message
is a claims or listen-instance control. Compare a repeated peer header against
the clear version before trusting its values.

The baseline native receiver tolerates some missing-header cases, and the
comparison does not bind every clear negotiation header. For the initial
strict profile, require the expected protected repetition when an initial peer
header was present. A peer that cannot satisfy this profile is unsupported;
historical exceptions require explicit live native cases and a separate
compatibility decision. Never infer a safe downgrade from unauthenticated
metadata.

Do not interpret a false or absent peer framing flag as permission to switch
formats. Our advertised true flag can enable whole-frame protection at the peer;
prove the resulting mode through protected traffic. Bound the preauthorization
decoder throughout this process.
[Sources: [negotiation schema][native-negotiation-header],
[verification behavior][native-verification],
[send-side repetition][native-send-controls].]

### 8.3 Essential control dispatch

Process these controls before ordinary request/reply routing:

| Message | Required behavior |
|---|---|
| Actor `SecurityContext` (`35`) during negotiation | Feed raw token body to rustls; do not Fabric-deserialize the TLS bytes. |
| Actor `Transport` (`1`), empty action | Decode `ListenInstance[address_, instance_, nonce_]`; update connection/target identity according to the client transport profile. |
| Actor `TransportSendTarget` (`29`), `"ConnectionAuth"` | Read fault status, conditionally decode `[errorMessage_, roleGranted_]` when `HasFaultBody` is set, release the promised-status gate on success, and fail on denial. |
| Actor `TransportSendTarget`, `"ClaimsTokenError"` | Report credential failure, including the decoded message when valid, and close. |
| Actor `TransportSendTarget`, reconnect/close control | Retire the affected connection and notify gateway recovery; respect applicable timeout/fault metadata and bounded draining. |

The ordinary native outbound-only TCP client emits a listen instance with an
empty address, instance zero, and an empty GUID nonce. Emit that client-only
form, with MessageId and HighPriority headers, after security release or in
clear mode as appropriate. Use the same local instance in negotiation metadata.
Do not advertise a fictitious listening endpoint or open a callback listener.
Record the peer's advertised instance/nonce on the receiving connection, and
test any behavior where an independently opened priority connection supersedes
an older connection. Listen-instance suppression is not enabled by default.

Do not wait unconditionally for `ConnectionAuth`: the native wait depends on a
server promise in `ServerAuthHeader`. Conversely, when it is promised, ordinary
queued traffic must not bypass that gate. Roles conveyed in an authenticated
status do not cause this implementation to remove its own resource limits.
[Sources: [control production][native-control-production],
[control consumption][native-control-consumption],
[client-only instance construction][native-local-instance],
[outbound nonce initialization][native-outbound-nonce],
[transport instance defaults][native-transport-defaults],
[server-status promise][native-server-status].]

### 8.4 Claims over TLS

Claims reuse TLS; they are not another transport or a replacement for server
certificate validation. Add them after mutual-certificate interoperability:

1. Send the claims provider indication in the first qualifying clear
   negotiation message through `ClientAuthHeader` (`0x8029`).
2. Authenticate the TLS server and process its status promise/metadata.
3. Acquire a token from a caller-supplied asynchronous provider with a deadline.
   Treat clear retrieval metadata as untrusted hints: compare issuer, audience,
   tenant, and destinations with local policy; never contact an arbitrary URL
   or disclose a token merely because a header requested it.
4. After the initial promised-status gate, make the protected
   `ClaimsMessage[claims_]`, actor `Transport`, action `"Claims"`, the first
   released message, ahead of listen-instance or gateway requests.
5. Follow the native release ordering rather than require a universal second
   status message. Initial connection success can precede token acceptance.
   Allow only gateway establishment behind the submitted token until a valid
   ping completes; handle a later denial/status/error as a connection fault.
6. Reacquire tokens for a new session as required by expiration policy. Do not
   treat TLS resumption, if added later, as renewal of claims authorization.

The gateway pauses incoming processing while its claims handler validates the
token. A successful gateway ping behind that token is a readiness check, not
permission for every management operation. Later requests still undergo
operation-level authorization.
[Sources: [claims ordering/release][native-control-production],
[claims acceptance and status][native-claims-completion],
[claims receive gate][native-claims-receive].]

## 9. Gateway sessions and protocol version

The gateway manager owns the configured endpoint list, current gateway
description, connection generation, authentication-failure state, and waiting
operations. Validate addresses locally and use one shared establishment attempt
for concurrent callers rather than a reconnect storm.

```text
Idle -> Selecting -> TCP connecting
     -> TLS/transport controls, if configured
     -> Gateway PingRequest -> GatewayReady

retryable fault -> retire generation -> backoff -> Selecting
authentication/configuration fault -> BlockedUntilConfigurationChanges
close -> Closing -> Closed
```

For clear TCP, omit TLS stages but retain transport controls and gateway ping.
Never infer that an unsecured connection is acceptable from a TLS failure.

Gateway ping uses actor `NamingGateway` (`5`), action `"PingRequest"`, activity,
protocol version, remaining timeout, MessageId, ExpectsReply, and an empty
body. Correlate by `RelatesTo`; the baseline native reply factory also uses the
`"PingRequest"` action, despite the existence of a `PingReply` constant.
Decode the version-appropriate `PingReplyMessageBody`/`GatewayDescription`.
[Source: [gateway establishment specification](../FabricClient.md#42-establishing-a-usable-gateway).]

Initially advertise protocol **1.1**, which supports the ping reply body but
does not claim chunked-file-upload support. The baseline gateway accepts major
1 with a nonnegative minor. Native current minor 2 advertises the upload
capability; move to 1.2 only with the corresponding implementation and evidence.
Do not use COM interface versions or TLS versions as this header.
[Source: [native gateway protocol version][native-protocol-version].]

Model a target as potentially having normal- and high-priority TCP connections.
Read-only bootstrap needs normal priority; add the distinct high-priority path
with notification support. Each connection authenticates independently and is
associated with the current gateway generation. A high-priority connection is
not inferred ready from the normal connection's ping.

Use bounded jittered backoff for retryable connection failures, constrained by
each waiting operation's deadline. Cache terminal credential/trust failures
until explicit credential/configuration replacement. Credential replacement
creates a new generation and new TLS sessions; it does not mutate a live
rustls connection's trust assumptions.

Session-expiration/reconnect controls retire sockets and trigger establishment,
not TLS renegotiation. Keep notification synchronization separate from basic
gateway readiness. A connection event must not imply its cache is synchronized.

## 10. Request/reply, errors, deadlines, and retries

### 10.1 Correlation and routing

Allocate request IDs from a random process GUID and monotonic 32-bit index.
Rotate the GUID before index reuse and never collide with a pending request.
Keep activity IDs separate from request IDs.

Register a pending entry **before exposing bytes to the writer**:

```text
MessageId -> {
    connection/client generation,
    expected operation and accepted reply schemas,
    absolute deadline,
    submission/delivery state,
    oneshot completion
}
```

Each pending request completes exactly once locally. On disconnect, timeout,
cancellation, or close, remove its entry and release permits/buffer reservations.
Late/duplicate/unknown replies cannot complete a newer request; record bounded,
rate-limited diagnostics rather than retaining an unbounded tombstone list.

After transport controls, route the `UncorrelatedReply` marker to the duplex
notification path **before** treating a message as an ordinary pending reply.
Then route ordinary `RelatesTo` messages to their request. An unexpected reverse
message must not masquerade as a reply or be silently acknowledged.
[Source: [native duplex dispatch][native-duplex].]

### 10.2 Error model

Keep distinct error categories:

| Category | Examples and retained context |
|---|---|
| Local input/configuration | Invalid URI, unsupported trust profile, unsupported operation. |
| Wire/schema | Invalid length/CRC, missing required header, unknown required result type. |
| TCP/TLS/authentication | I/O error, certificate denial, claims denial, peer status failure. |
| Local resource/deadline | Queue budget exhausted, timeout, cancellation, closed client. |
| Native transport fault | Fault header, including its native numeric code and body-presence semantics. |
| Gateway/operation failure | `"ClientOperationFailure"` body `[value_, message_]`. |
| Operation-result failure | Query processing error, property-batch/item errors, or other operation-specific status. |

Retain the raw native numeric code and meaningful remote text, with operation,
endpoint, phase, and activity context. Do not convert every failure into a
timeout, `false`, empty collection, or a generic I/O error.

Validate expected success envelopes and decode each operation's status even
when framing and correlation succeeded. A transport fault is not always a
`ClientOperationFailure`, and that envelope is not a FabricTransport service
error header.

### 10.3 End-to-end deadline

Use one Tokio monotonic deadline for queue admission, gateway establishment,
credential acquisition, serialization scheduling, sending, and waiting for the
reply. Before submission, encode remaining duration in the native Timeout
header with checked 100 ns conversion. Never restart the caller's timeout on
gateway failover.

Represent an infinite deadline explicitly and serialize it as the verified
native sentinel; do not approximate it with overflowing `Duration` arithmetic.
Finite request deadlines also bound connection attempts made on their behalf.
An optional connection-maintenance task has its own cancellation and bounded
attempt timeout.

Timeout/cancellation ends the local wait, not remote execution. Report whether
the request was definitely not submitted or **may have executed**, using
conservative delivery state. A local socket write is not a remote acknowledgment.

### 10.4 Replay policy

Gateway reconnection does not automatically replay submitted operations.
Never replay mutations after ambiguous delivery without operation-specific
evidence and explicit caller policy. A fresh MessageId does not provide
deduplication.

Unsubmitted requests can wait for another connection within their original
deadline. A small allowlist of read-only operations can later support bounded
retries; each retry needs a new correlation ID, the same overall deadline, and
schema-specific treatment of continuation/version state. Unknown operations
default to no replay.

## 11. Operation layers

### 11.1 First end-to-end operation: NameExists

Use `NameExists` as the first semantic interoperability proof:

```text
request headers:
    Actor = NamingGateway (5)
    Action = "NameExistsRequest"
    FabricActivity, ClientProtocolVersion, Timeout
    MessageId = R, ExpectsReply = true

request body:
    serialized NamingUri / Uri directly
    [type_, scheme_, authority_, hostType_, host_, port_,
     path_, query_, fragment_, pathSegments_]

success reply:
    RelatesTo = R
    NameExistsReplyMessageBody[nameExists_, userServiceState_]

operation failure:
    RelatesTo = R
    Action = "ClientOperationFailure"
    body = [value_, message_]
```

Do not insert an extra URI wrapper/string object, omit the second success field,
or return `false` when the operation failed.
[Source: [worked native NameExists exchange](../FabricClient.md#6-worked-example-nameexists).]

### 11.2 Queries and management

External `QueryRequest` uses `[queryArgs_, queryName_]`, not the similarly named
internal query body. Port the exact argument-key and map schemas. Typed query
support includes result kind, polymorphic payload, query-processing error, and
paging metadata. Implement a concrete node-list query first, then add result
families with independent onebox expectations; a generic untyped query endpoint
is not complete query support.

For forwarded management operations, set outer actor `NamingGateway` and action
`"ForwardMessage"` while retaining original actor/action in `ForwardMessageHeader`.
The body remains the operation's native body schema. Additional routing envelopes
are added only when the corresponding operation requires them.
[Sources: [queries](../FabricClient.md#71-queries),
[forwarding](../FabricClient.md#72-forwarded-management-operations).]

Health reporting and file transfer require their own batching, delivery,
acknowledgment, and retry designs before implementation. They are not ordinary
universal request/reply wrappers. No milestone should claim those families
complete from codec coverage alone.

### 11.3 Resolution

Initially implement uncached `ResolveServicePartition` with the native partition
key, previous-result/version metadata, and `ServiceResolutionMessageBody`
schema. Return owned partition information, endpoint locations/roles, and
versions. Application endpoint traffic remains outside FabricClient.

Then add a bounded cache keyed by canonical service identity and partition
selection. Updates use native version/generation comparison, not arrival time.
Handle deletion, partition changes, endpoint-role changes, stale responses, and
gateway replacement explicitly. Callers can force a refresh; cache misses never
become an empty successful resolution.

Do not claim support for the older resolution-change callback API by
implementing notification filters alone; it is a distinct public behavior.
[Source: [native resolution behavior](../FabricClient.md#73-service-resolution-and-older-change-callbacks).]

### 11.4 Notifications

Notification support requires all of session establishment, client identity,
filters, version/deletion synchronization, pushed pages, and correlated replies.
It cannot be implemented as an arbitrary callback on every unsolicited frame.

Maintain client ID and desired filters across reconnects; maintain gateway
generation and synchronization state separately. Reconnect using
`NotificationClientConnectionRequest[clientId_, generation_, clientVersions_,
filters_]`, complete synchronization, and only then present the stream as
synchronized. Process `ClientIdentityHeader` and the native action/body variants.

For a pushed notification with MessageId `N`, send the correct
`ServiceNotificationReply` with `RelatesTo(N)` and processing status through
the high-priority path associated with the notification's original target and
gateway generation. Never route an old session's acknowledgment to whichever
gateway happens to be current. Update the versioned cache and admit the event
into a bounded delivery queue before acknowledging successful processing. The
acknowledgment does not wait for arbitrary user application work.

If admission fails, do not acknowledge success and silently drop the event.
Mark the stream desynchronized, surface a lag/resynchronization event, retire
the session if necessary, and synchronize again using the last committed
versions. Commit version advancement consistently with event admission so
reconnect cannot skip an event that was never delivered into the local stream.
Duplicate pages are handled idempotently using native notification identity and
version semantics.

No application callback or slow subscriber runs on the connection driver.
[Sources: [notification protocol](../FabricClient.md#74-notification-sessions-and-reverse-traffic),
[native duplex marker handling][native-duplex].]

## 12. Resource limits and lifecycle

Apply local bounds even where the native client trusts an authenticated gateway
or removes a limit for an admin role. Proposed initial values:

| Resource | Proposed bound / behavior |
|---|---|
| Preauthorization and first protected Fabric frame | 128 KiB, matching the baseline conservative frame limit. |
| Authorized incoming/outgoing plaintext frame | 16 MiB local cap; outgoing also obeys a nonzero peer-advertised frame cap. |
| Native header area | 65,535 bytes, required by wire field width. |
| Object nesting | 64 levels. |
| Container count | At most 1,000,000 elements, additionally bounded by enclosing bytes and decoded-memory budget. |
| Concurrent application requests | 1,024 per client; control traffic has reserved capacity. |
| Outgoing admission budget | 64 MiB per client across all connections, plus explicitly bounded TLS/control buffers. |
| Active decoded-result allocations | 64 MiB per client, excluding completed results whose ownership has transferred to the caller. |
| Notification delivery | Both 256 events and 8 MiB, whichever is reached first. |
| TLS negotiation input | 1 MiB aggregate token bodies, at most 128 handshake input messages, plus the per-frame bound. |
| Connection attempt | 10 seconds through TLS, controls, and ping, also bounded by the caller's remaining deadline. |

These are safety defaults, not a universal Service Fabric message-size contract.
Reject oversize outgoing operations before writing any part of their frame.
The active outgoing ceiling is `min(local_cap, nonzero_peer_cap)`, a deliberate
stricter policy than native limit replacement. An unauthenticated advertised
cap can constrain sending but never raise local bounds.

Account for decoded allocations as well as wire bytes: collection metadata and
Rust object overhead can greatly exceed serialized size. Limit TLS input/output
buffers, partial receive frames, queued plaintext, and decoded replies; a
bounded command channel alone does not bound connection memory.

Do only bounded envelope/control parsing on the connection task. Large result
decoding or encoding must be incremental or use bounded CPU-work admission,
not block all transport progress or spawn an unbounded task per message.

Local queue admission either waits within the request deadline or returns a
typed resource error according to the selected request policy. Keep control
progress available without promising that control bytes can bypass a partially
written frame on one TCP stream.

Explicit `close()` stops admission, cancels connection establishment, retires
notification sessions, completes pending requests, and drains already-started
output only within a bounded deadline. Use TLS `close_notify` when applicable;
do not invent a FabricTransport service `Disconnect` exchange. On last-handle
drop, cancellation is nonblocking and tasks release resources without requiring
an async destructor.

Use socket keepalive where configured. Initial gateway ping is not an invented
periodic heartbeat protocol. Detect failure through the socket, TLS, deadlines,
and documented session controls.

Expose phase-specific diagnostics and bounded counters for connection attempts,
TLS failures, negotiated version/suite, queue bytes, in-flight requests, expired
requests, unknown replies, and notification resynchronizations. Correlate through
activity IDs and connection generations, without recording token bodies,
credentials, or arbitrary operation payloads.

## 13. Interoperability and correctness evidence

### 13.1 Independent codec oracle

The current native runtime oracle is onebox, not an assumed native Service
Fabric source build. Exercise both directions: the real gateway decodes Rust
requests and Rust decodes its replies, with operation results checked against
independently established test state. Rust round trips alone are insufficient
because a symmetric bug can pass both encoding and decoding.

Use the pinned source and message-format specification to guide implementation,
not to manufacture an expected-byte oracle. Do not commit handwritten hex
packets or normalized response snapshots as correctness fixtures. The real
onebox must accept Rust requests and return independently verifiable results.
An existing native client using prebuilt libraries/tools can supply optional
result comparisons, but is not a prerequisite or replacement for onebox.

Cover signed/unsigned integer boundaries, zero/empty values,
UTF-16 non-BMP data, GUID byte order, enum values, TimeSpan sentinels, object
padding/scopes/extensions, arrays/maps, all bootstrap headers/controls, ping,
NameExists success/failure, and every later operation's request/reply variants.
Offline unit/model/property tests cover internal consistency, bounds, and
failure handling; they do not prove these values have the correct native
interpretation. Record runtime/profile, independently established state, and
actually executed cases with each live result. Cases not exercised against
onebox remain unqualified rather than being certified by a byte snapshot.
Raw traffic may be inspected locally for debugging, but is not a committed
golden-test artifact or a substitute for live qualification.

### 13.2 Stream and state-machine tests

Use a deterministic Tokio test peer and in-memory streams to exercise:

| Area | Required cases |
|---|---|
| Framing | Every split point of a valid frame, byte-at-a-time input, concatenation, truncated EOF, invalid lengths and CRCs. |
| TLS adapter | Multi-record tokens, partial socket writes, final clear handshake frame immediately followed by raw TLS bytes, both in the same read, and no application data before the final-flight drain. |
| TLS progress | Handshake failure/alert, stalled peer, output backpressure, idle protocol output, close_notify, supported post-handshake records. |
| Negotiation | First protected repetition, mismatch, missing-header policy, peer limits, and different allowed provider masks by direction. |
| Authorization | Promised/absent status, failure with/without fault body, claims ordering, initial success followed by token denial. |
| Correlation | Concurrent requests, reverse notifications, out-of-order replies, duplicates, late replies, cancellation/disconnect races, and ID rollover. |
| Recovery | Gateway change, stale-generation completions, credential replacement, priority connection failure, and no ambiguous mutation replay. |
| Notifications | Synchronization, duplicate pages, deletion, filter re-registration, slow consumers, bounded-queue overflow, and loss/reconnect before acknowledgment. |
| Shutdown | Close/drop during connect, TLS, blocked send, active request, and notification synchronization; no stranded tasks or permits. |

Add property tests and fuzz targets for the serializer, header/frame decoder,
and state-transition input sequences. Assert no panic, overflow, unbounded
allocation, infinite loop, or dispatch before required trust/readiness gates.
Use Tokio's controlled time for timeout/backoff tests rather than sleeps.

### 13.3 Live compatibility matrix

For each claimed runtime release, run against a native gateway and record:

| Profile | Minimum proof |
|---|---|
| Explicit clear TCP | Ping, NameExists true/false/error, concurrent correlation and reconnect. |
| Certificate TLS 1.2, whole frames | Mutual authentication, CA/identity policy, protected controls, all supported operations. |
| Certificate allowlist | Accepted configured identity and rejection of an unlisted identity, with real handshake signatures verified. |
| Claims over TLS | Valid/invalid/expired token, metadata policy, role restrictions, and session re-establishment. |
| TLS 1.3, if enabled | Native support plus final-flight/post-handshake interoperation, not only a Rust-to-Rust test. |
| Notifications, if claimed | High-priority connection, registration, update/delete, gateway failover and resynchronization. |

Negative trust tests include wrong identity, untrusted issuer, expired/not-yet-valid
certificate, absent/wrong client certificate, malformed chain, and unauthorized
role. Add size-limit and queued-request pressure cases to each enabled profile.

Use the real native onebox gateway for each claimed profile/operation. Compare
public results with independently established state through available management
tools and, where available, a prebuilt native FabricClient. HTTP state checks do
not independently prove native encoding or map HTTP statuses to native error
codes. A Rust mock gateway is not sufficient.
Use only isolated disposable test resources for mutation coverage. Synthetic
test state and live evidence reports must exclude real keys, tokens, and
sensitive operation bodies.

An unsecured onebox qualifies only clear TCP. If no secured native gateway is
available, live TLS qualification remains blocked while offline driver and
clear-TCP work can proceed. Do not claim native TLS compatibility from
Rust-to-Rust TLS tests or silently reconfigure a shared onebox.

## 14. Implementation sequence and exit gates

| Phase | Deliverable | Exit gate |
|---|---|---|
| 0: feasibility | Dependency/provider review, observed onebox endpoint/runtime/security profile, ABI/serializer source analysis; begin a small TLS spike as soon as the secured environment and framing exist. | Onebox test contract is explicit. Native TLS handoff/control proof is required before a TLS compatibility claim, not before clear-TCP development. |
| 1: codec | Bounded serializer, frames, headers, IDs, control schemas. | Internal consistency, fragmentation, malformed input, and allocation bounds pass; native correctness is qualified through live operations, not golden bytes. |
| 2: first client | Gateway manager, ping, request table, NameExists, explicit clear TCP and separately qualified certificate TLS. | Existing/nonexistent names match independent onebox state; errors, concurrent replies, cancellation and reconnect preserve delivery semantics. |
| 3: useful reads | Typed node query with paging, uncached partition resolution, then bounded version-aware cache. | Live onebox results agree with independently known state, including claimed stale/error/paging cases. |
| 4: sessions | Claims profile, priority connections, notification filters, acknowledgments, reconnect synchronization. | Authentication denial and slow-consumer/failover cases work without false readiness or silent event loss. |
| 5: management breadth | Add one method family at a time; separately design health reporting and file transfer. | Every shipped method has the coverage evidence from section 4; protocol 1.2 enabled only with upload support. |
| 6: release | Compatibility matrix, dependency/security review, public API/coverage docs, stability policy. | Claims are limited to tested runtime/security/operation combinations; all-Rust crypto profile has explicit production approval. |

Phases describe dependency order, not permission to release a partially
authenticated or unbounded transport. The implementation plan permits offline
and clear-TCP work while a secured environment is unavailable, but keeps a small
TLS spike early once possible because it can invalidate assumptions before API
breadth grows.

## 15. Decisions, risks, and open gates

| Risk or decision | Resolution / required evidence |
|---|---|
| Native serialization is not a standardized Rust format | Use source analysis for implementation and real onebox exchanges for compatibility; unexercised edge cases remain unqualified. |
| rustls cannot be attached directly to TCP at byte zero | Custom low-level byte-routing driver with explicit final-flight ordering. |
| Cryptographic provider may conflict with a strict all-Rust requirement | Explicit dependency review; experimental RustCrypto status blocks an unqualified production claim. |
| Older/deployed TLS policy may have no common secure configuration | Identify supported runtime profiles; return unsupported/incompatible security, never weaken TLS to connect. |
| Native trust policies exceed the initial verifier | Publish supported policies, reject unsupported requirements, add exact allowlist behavior only with review and tests. |
| Partial API port could be mistaken for full FabricClient | Method-level coverage and capability-appropriate protocol version; no HTTP/native fallback. |
| Gateway reconnect can lose an executed operation's reply | Conservative delivery outcome and no automatic ambiguous mutation replay. |
| Authenticated peers can still exhaust resources | Independent finite bounds on wire, decoded data, queues, and concurrency. |
| Notifications can become stale after failover or overflow | Generation/version synchronization and explicit desynchronized/lag state. |

Remaining implementation-time gates are selection/approval of the crypto
provider, qualification of the profiled ABI against available native messages,
initial deployed runtime releases and their TLS policies, and the first
supported certificate/revocation policy. A native source build is optional.
These gates do not expand scope beyond TCP and whole-frame TLS.

Do not claim interoperability from this design alone. The decisive first
vertical slice is **native gateway + rustls certificate authentication +
transport controls + ping + correctly serialized NameExists**, with both
positive and negative results.

## References

Native links are pinned to the same source revision as the two companion
specifications. Rustls API links are versioned examples; provider documentation
was consulted while writing this proposal and must be rechecked when selecting
dependencies.

[native-revision]: https://github.com/microsoft/service-fabric/commit/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[native-tcp-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L21-L249
[native-object-writer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L320-L425
[native-object-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializableStream.h#L228-L246
[native-compressor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/ByteCompressor.h#L10-L161
[native-frame]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.cpp#L14-L118
[native-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpReceiveBuffer.cpp#L18-L241
[native-client-auth]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ClientAuthHeader.h#L10-L23
[native-tls-negotiation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L176-L332
[native-security-transition]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1964-L2138
[native-negotiation-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityNegotiationHeader.h#L10-L35
[native-verification]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L542-L679
[native-send-controls]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SendBuffer.cpp#L134-L278
[native-control-production]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1391-L1499
[native-control-consumption]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendTarget.cpp#L584-L697
[native-local-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L206-L239
[native-outbound-nonce]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L118-L128
[native-transport-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpDatagramTransport.h#L249-L275
[native-server-status]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L192-L252
[native-claims-completion]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L108-L172
[native-claims-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L334-L375
[native-protocol-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientProtocolVersionHeader.h#L10-L82
[native-duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L36-L77
[rustls-connection]: https://docs.rs/rustls/0.23.35/rustls/client/struct.ClientConnection.html
[rustls-verifier]: https://docs.rs/rustls/0.23.35/rustls/client/danger/trait.ServerCertVerifier.html
[rustls-features]: https://docs.rs/rustls/0.23.35/rustls/manual/_04_features/index.html
[rustls-providers]: https://github.com/rustls/rustls#cryptography-providers
[rustcrypto-provider]: https://github.com/RustCrypto/rustls-rustcrypto
