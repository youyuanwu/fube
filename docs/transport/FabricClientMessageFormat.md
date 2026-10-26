# FabricClient native message format

## 1. Answer, scope, and evidence

FabricClient uses **Service Fabric's native binary messaging format** over TCP.
Ordinary headers and operation bodies use the custom
`FabricSerializableStream` serializer: typed values, positional fields, nested
objects, scopes, compressed integers, and counted UTF-16 strings. This is not
protobuf, JSON, MessagePack, NDR, or serialization of COM calls.

There are three distinct formats to implement:

```text
TCP/TLS representation
    Fabric frame: fixed header + native header records + body
        Fabric serialization inside header objects and ordinary operation bodies
```

This specification describes the source at commit
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][revision] in
`microsoft/service-fabric`. It complements the
[FabricClient behavior specification](FabricClient.md), the
[shared transport specification](Transport.md), and the
[Rust client design](design/FabricClient.md).

Scope is **TCP and whole-frame TLS**, including claims carried over TLS.
Platform-specific authentication and alternate protection formats are excluded.
The common encoding is described in detail; this is not an exhaustive schema
catalog for every management, health, or file-transfer operation.

Hexadecimal examples are **source-derived constructions, not packet captures**.
Integer payloads were checked by compiling the actual native
`ByteCompressor.h` in an isolated compatibility harness. Object/frame lengths
were calculated independently. The complete native serializer, a native client,
and a live gateway were not executed for these examples. Native object padding
and ABI assumptions still require independent fixture qualification for a port.

## 2. From API arguments to network bytes

`FabricClient.idl` defines local interfaces. The TCP path does not transmit
interface IDs, vtable slots, pointers, public `Reserved` fields, or async callback
objects. It selects native actor/action headers and an operation-specific body.

For ordinary requests, `ClientServerRequestMessage::CreateTcpMessage` creates a
`Transport::Message` from `body_->GetTcpMessageBody()`, or an empty message if
there is no body. `Message::AddBody` calls `WriteSerializable` on that selected
native object. The client connection and request/reply layers add version,
timeout, reply expectation, and correlation information.

```text
public arguments
    -> native operation/body type
    -> Fabric-serialized body
    -> Action + Actor + activity + gateway/request metadata
    -> 12-byte frame + native headers + body
    -> TLS, if configured
    -> TCP
```

The selected TCP body need not be the wrapper class used at the API boundary.
For example, `NamingUriMessageBody` returns its contained `NamingUri` directly.
[Sources: [request construction][request-message],
[body serialization][message-body], [URI body adapter][uri-adapter].]

## 3. TCP and TLS representation

### 3.1 Fixed frame header

For clear TCP, and for clear frames carrying TLS negotiation tokens:

```text
Frame = TcpFrameHeader[12] || NativeHeaders[H] || Body[B]
F = 12 + H + B
```

All fixed-width integers in this header are little-endian:

| Offset | Width | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | `frameLength` | Entire frame length `F`, including the 12 fixed bytes. |
| 4 | 1 | `securityProviderMask` | Local sender's provider mask, not a serializer type. |
| 5 | 1 | `frameHeaderCRC` | Optional CRC-8 of the fixed header. |
| 6 | 2 | `headerLength` | `H`, the total native-header area size. |
| 8 | 4 | `frameBodyCRC` | Optional CRC-32 of native headers plus body. |

```text
F >= 12
H <= F - 12
native_headers = frame[12 .. 12 + H]
body           = frame[12 + H .. F]
```

There is no frame magic, COM method ordinal, body-schema ID, separate body-length
field, or protocol-version field in these 12 bytes. A decoder needs connection
state and native headers to interpret the body. TCP reads, writes, and packets
do not delimit frames.

The relevant masks are `0x00` for clear TCP, `0x01` for certificate TLS, and
`0x08` for claims-over-TLS. The receive path checks consistency with the first
frame **in that direction**. It does not require the remote sender's mask to
equal the local mask; client and server provider settings can differ in claims
mode. A mask is not a request to change local trust or security configuration.
[Sources: [frame declaration][frame-header], [validation][frame-validation],
[receive state][receive-buffer], [provider mapping][provider-map].]

### 3.2 CRCs

If nonzero, `frameHeaderCRC` is CRC-8 with polynomial `0x07`, initial zero,
over a copy of all 12 fixed bytes with **both CRC fields zeroed**.

If nonzero, `frameBodyCRC` is reflected CRC-32 with polynomial `0xedb88320`,
initial value `0xffffffff` and final XOR `0xffffffff`, over:

```text
NativeHeaders || Body
```

Zero means that check is absent. Neither CRC authenticates a peer. Protected
sends do not use the optional clear-message CRC generation path.
[Sources: [header CRC][frame-validation], [CRC algorithms][crc-algorithms],
[CRC tables][crc-tables], [send assembly][send-buffer],
[body verification][receive-buffer].]

### 3.3 TLS changes the outer representation, not the inner encoding

Initially, TLS output is carried as the **raw body** of clear Fabric negotiation
messages with actor `SecurityContext` (`35`). Those body bytes are not
Fabric-serialized objects:

```text
TCP during negotiation:
    clear Fabric frame {
        native headers: SecurityContext actor, MessageId, negotiation metadata
        body: TLS output/token bytes
    }
```

After transition to whole-frame protection:

```text
TCP:
    TLS records

Recovered TLS plaintext:
    TcpFrameHeader[12] || NativeHeaders || Body
```

There is no additional clear Fabric header preceding each protected data frame.
The inner `frameLength` measures plaintext, not TLS ciphertext. A TLS record may
contain a frame fragment or multiple frames, and a Fabric negotiation token may
contain multiple TLS records.

The final clear negotiation frame and the first raw TLS bytes may be adjacent
in one TCP read. Switch parsers at the state transition and preserve trailing
bytes. A stock TLS client attached directly to the TCP socket does not implement
this handshake-token envelope.
[Sources: [TLS token production][tls-negotiation],
[connection transition][security-transition],
[decryption before framing][receive-buffer].]

## 4. Native message-header records

### 4.1 Record layout

The `H`-byte area is consecutive records:

```text
u16le header_id
u16le object_length
byte  serialized_object[object_length]
```

Both prefix values are raw integers, **not** Fabric-serialized integers.
`object_length` excludes the four-byte prefix and includes the serialized
object's leading Object marker. The ordinary producer relationship is:

```text
record_length = 4 + object_length
object_length = 1 + serialized_object.ObjectHeader.size
H             = sum(record_length)
H             <= 65535
```

A record with ID `0x8000` is a deleted record: skip its declared payload rather
than dispatch it. Deleted bytes can remain until compaction. The native iterator
rejects a zero `object_length`; unknown nondeleted IDs can be skipped by length.
Do not look for an ObjectEnd byte to discover a record boundary.
[Sources: [record writer][header-writer],
[record iteration/deletion][header-iteration].]

### 4.2 Ordered records are not a unique-key map

Record order and duplicates exist on the native representation. Two access
behaviors must be distinguished:

| Access | Baseline behavior |
|---|---|
| `TryReadFirst<T>` | First visible record matching that header ID. |
| Incoming common properties | The scan assigns each encountered common value; the last visible matching record supplies the effective property. |

The common-property scan covers Action, Actor, MessageId, RelatesTo, Retry,
ExpectsReply, Idempotent, HighPriority, Fault, and UncorrelatedReply. It
deserializes each encountered common occurrence; a malformed later occurrence
is not made harmless by an earlier valid one.

For example, visible `RelatesTo(R1), RelatesTo(R2)` gives `R1` through typed
first-match access but `R2` through the native effective `Message.RelatesTo`
used by reply matching. This is source-defined behavior, not a claim that
ordinary gateways emit conflicting duplicates. A producer should emit unique
ordinary routing/correlation headers; a compatible decoder must not collapse
all native access semantics into one arbitrary map policy.
[Sources: [first-match lookup][typed-lookup],
[incoming construction][incoming-headers],
[effective common scan][common-scan],
[reply consumption][request-table].]

## 5. Fabric serialization grammar

### 5.1 Macro-defined objects

For a normal object whose fields use `FABRIC_FIELDS_*`:

```text
00                          Object marker
ObjectHeader:
    u32le size
    u8 flags
    padding[3]              supported default-alignment ABI
optional type information
zero or more scopes:
    1f                      ScopeBegin
    typed positional fields
    2f                      ScopeEnd
3f                          ObjectEnd
```

The declared native `ObjectHeader` fields are `ULONG size` and a one-byte flags
enum. The writer copies `sizeof(ObjectHeader)`, not a packed five-byte encoding.
On the little-endian, default-alignment ABI used by the examples, that is
**eight bytes**: size at offset 0, flags at 4, and three padding bytes.

`size` starts at the first size byte and includes the complete header, type
information if present, scopes, nested bytes, and ObjectEnd. It excludes the
leading `00`. Thus an object occupying `N` bytes stores `size = N - 1`.
The decoder checks the consumed object extent against this size.

Padding is not a semantic field. The examples zero it for determinism; do not
expect every native producer to initialize it identically. A port must encode
fields explicitly rather than copying the memory of a Rust struct.
[Sources: [object-header declaration][object-header],
[object writer][object-writer], [object reader][object-reader].]

### 5.2 Scopes, inheritance, and primitive wrappers

`FABRIC_FIELDS_02(a, b)` calls the base writer and then emits a scope containing
`a`, followed by `b`. There are no field names, field numbers, or per-field
length tags. A serialized base class and derived class can contribute separate
scopes within the same outer object; they do not automatically create an extra
outer object for the base.

`FABRIC_PRIMITIVE_FIELDS_*` is different: it writes its members directly into
the containing field sequence, **without** introducing an Object marker or a
scope. Examples include `Guid`, `TimeSpan`, `MessageId`, and `ErrorCode`.

A field whose type derives from `FabricSerializable` normally invokes
`WriteSerializable` and therefore introduces a nested object. Do not infer
nesting from the number of C++ members alone.

The base `FabricSerializable` writer itself emits no scope or fields. The
serializer delegates content to the type's writer, so specialized manual
writers still need their own schemas; the grammar above describes the normal
macro-defined path, not permission to rewrite every body into one scope.
[Sources: [field macros][field-macros],
[primitive macros][primitive-macros],
[type dispatch][type-dispatch], [empty base writer][serializable-base].]

### 5.3 Object flags and type information

| Flag | Meaning |
|---|---|
| `0x00` | No optional object-header flags. |
| `0x01` | `ContainsTypeInformation`. |
| `0x02` | `ContainsExtensionData`, associated with external/no-copy buffers. |

With `0x01`, the bytes immediately after the eight-byte header are:

```text
compressed unsigned u32 count, WITHOUT its own type metadata
type_information[count]      opaque bytes defined by this object's type
```

The length must be nonzero. The polymorphic pointer reader also applies
`FabricTypeInformation::LengthMax`, 100 MiB at this source revision; this is a
source bound, not a sensible allocation budget for a new client.

Type information is not a universal textual class name, protobuf type URL, or
ordinary serialized enum. For example, a query result wrapper publishes the
raw bytes of its native `QueryResultHelpers::Enum`; on the profiled ABI that is
a four-byte little-endian discriminator. `Node = 1` therefore has type-info
bytes `01 00 00 00`, preceded by count `04`, not `09 01`.
[Sources: [type-info writing/reading][object-writer],
[pointer activation][object-pointer-reader],
[type-info limit][type-info], [query type-info producer/activator][query-activation],
[query type IDs][query-type-ids].]

## 6. Type metadata bytes

At a value boundary, a metadata byte generally consists of a base type and
optional bits:

```text
0x80  Array
0x40  EmptyValueBit
0x0f  BaseTypeMask
0x30  BoolFalseFlag, a special boolean encoding component
```

The source constants are:

| Base / special byte | Meaning |
|---|---|
| `00` | Object |
| `01` | Pointer |
| `02` | Bool / BoolTrue base |
| `03` | Char |
| `04` | UChar |
| `05` | Short, signed 16-bit |
| `06` | UShort, unsigned 16-bit |
| `07` | Int32 |
| `08` | UInt32 |
| `09` | Int64 |
| `0a` | UInt64 |
| `0b` | Double |
| `0c` | GUID |
| `0d` | WString base; ordinary strings use its Array form |
| `8e` | ByteArrayNoCopy, not the ordinary byte-array representation |
| `1f` | ScopeBegin |
| `2f` | ScopeEnd |
| `3f` | ObjectEnd |

Markers are structural tokens at value boundaries, not byte delimiters to
search for inside arbitrary payloads. Apply the complete marker and boolean
rules before treating a byte as a generic base type with flags.
[Sources: [metadata constants][metadata-types],
[metadata bit operations][metadata-operations].]

### 6.1 Scalar booleans and zero values

The normal native scalar boolean writer emits **one byte, no payload**:

```text
true  = 42                  Bool | EmptyValueBit
false = 72                  Bool | BoolFalseFlag | EmptyValueBit
```

Here "empty" means the value is fully represented by metadata; it does not mean
false. In particular, `02 01` is not the normal native writer's representation
of `true`.

Zero compressed scalars also omit a payload:

| Value | Complete field bytes |
|---|---|
| Int32 zero | `47` |
| UInt32 zero | `48` |
| Int64 zero | `49` |
| UInt64 zero | `4a` |
| Char zero | `43` |
| UChar zero | `44` |
| All-zero GUID | `4c` |
| Double zero | `4b` |

Nonzero Char/UChar fields contain their metadata plus one raw byte. A nonzero
Double contains `0b` plus eight raw IEEE-754 bytes in the profiled ABI's
little-endian layout. The native zero test is `value == 0`, so its writer also
collapses negative floating-point zero into empty zero.
[Sources: [empty-value/boolean tests][empty-tests],
[scalar readers/writers][blittable-scalars].]

## 7. Integer compression

### 7.1 Payload algorithm

For compressed integers, metadata identifies signedness and width. The payload
then encodes seven-bit groups **most significant first**:

```text
each byte: continuation_bit[7] | seven_payload_bits[6:0]
continuation_bit = 1 if another byte follows
```

This is not little-endian LEB128 and not ZigZag.

For unsigned values, concatenate the payload groups. For signed values,
initialize the accumulator using the **first** payload byte's bit `0x40`:

```text
accumulator = -1 if signed and first_byte has 0x40, else 0
for each payload byte:
    accumulator = (accumulator << 7) | (byte & 0x7f)
```

This is mathematical pseudocode. A Rust decoder must implement explicit,
width-checked arithmetic rather than inherit native signed-shift behavior.

The writer retains the leading group needed to preserve the sign. Consequently
signed positive 64 is `80 40`, not unsigned `40`, and signed -65 is `ff 3f`,
not `3f`.

The source maximum payload lengths are `ceil(width / 7)`: 3 bytes for 16-bit,
5 for 32-bit, and 10 for 64-bit types. The native reader bounds the number of
groups; a new implementation should additionally validate numeric overflow.
Do not claim that the native reader requires the shortest possible encoding
just because the writer produces one.
[Sources: [compressor and decoder][compressor],
[typed integer scalars][compressed-scalars].]

### 7.2 Payload examples

These columns exclude type metadata. Zero uses no payload in a scalar:

| Integer | Signed payload | Unsigned payload |
|---:|---|---|
| -8193 | `ff bf 7f` | Not applicable |
| -8192 | `c0 00` | Not applicable |
| -65 | `ff 3f` | Not applicable |
| -64 | `40` | Not applicable |
| -1 | `7f` | Not applicable |
| 0 | Omitted in scalar | Omitted in scalar |
| 1 | `01` | `01` |
| 63 | `3f` | `3f` |
| 64 | `80 40` | `40` |
| 127 | `80 7f` | `7f` |
| 128 | `81 00` | `81 00` |
| 8191 | `bf 7f` | `bf 7f` |
| 8192 | `80 c0 00` | `c0 00` |
| 16384 | `81 80 00` | `81 80 00` |

Complete typed fields include their metadata:

```text
Int32(64)    = 07 80 40
UInt32(64)   = 08 40
Int64(-65)   = 09 ff 3f
UInt64(128)  = 0a 81 00
```

Ordinary enum helpers cast to signed 64-bit and use the Int64 representation,
regardless of the C++ enum's normal in-memory width. For example,
`Actor::NamingGateway = 5` is the field `09 05`.
Explicit `FABRIC_SERIALIZE_AS_*` specializations can override representation;
trace the actual field type rather than assuming every custom type is an enum.
[Source: [enum and explicit conversion helpers][enum-helpers].]

### 7.3 Counts and array elements are different from scalars

`WriteUInt32WithNoMetaData` writes unsigned compressed bytes without a type byte.
It must write `00` for zero because there is no metadata to indicate emptiness.

Likewise, compressed integer arrays have **one** array metadata byte and one
count; individual elements have neither their own type metadata nor empty bits.
An array's zero element occupies payload `00`.

```text
UInt32 array [0, 1, 128]:
    88 03 00 01 81 00
    ^^ ^^ ----------------
    |  |  three compressed element payloads
    |  count = 3, without metadata
    UInt32 | Array

Int32 array [0, -1, 64]:
    87 03 00 7f 80 40
```

[Sources: [no-metadata count][count-writer],
[compressed arrays][compressed-arrays].]

## 8. Strings, GUIDs, and common primitive wrappers

### 8.1 UTF-16 strings

`std::wstring` uses the serializer's WString **array** path:

```text
nonempty: 8d || unsigned_count_without_metadata || UTF16LE[count]
empty:    cd
```

The count is two-byte code units, not bytes, Unicode scalar values, or a
null-terminated length. There is no byte-order mark or terminating NUL.

```text
"A"       = 8d 01 41 00
"Hi"      = 8d 02 48 00 69 00
U+1F600   = 8d 02 3d d8 00 de
empty     = cd
```

The inspected Linux build uses `-fshort-wchar`, giving the two-byte `WCHAR`
representation required by this profile. Preserve code units when lossless
round-tripping is necessary; never silently use lossy UTF-8 conversion for
identifiers.
[Sources: [wstring helper][string-and-vector-helpers],
[string primitive][primitive-methods], [array writer][blittable-arrays],
[character build option][short-wchar].]

### 8.2 GUIDs

A nonzero GUID is `0c` plus 16 raw native GUID bytes. The first three components
have little-endian integer representation; the final eight bytes retain order:

```text
text GUID:
    00112233-4455-6677-8899-aabbccddeeff

typed field:
    0c 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff
```

An all-zero GUID is just `4c`. `Common::Guid` is a primitive wrapper around the
GUID, not another serialized object. A Rust UUID's conventional network-order
byte array cannot be copied unchanged without matching this layout.
[Sources: [GUID primitive wrapper][guid-wrapper],
[GUID writer][primitive-methods], [raw scalar encoding][blittable-scalars].]

### 8.3 TimeSpan, MessageId, ActivityId, and ErrorCode

| Native type | Field representation |
|---|---|
| `TimeSpan` | One Int64 field of 100 ns ticks; no nested object. |
| `MessageId` | GUID field, then UInt32 index, directly in the containing scope. |
| `ActivityId` | Nested object with one scope containing GUID, then UInt64 index. |
| `ErrorCode` | Its numeric enum value only, through primitive fields; the local message string is not included by this helper. |

For a 30-second timeout, ticks are `300000000`:

```text
TimeSpan(30 seconds) field = 09 81 8f 86 c6 00
```

`TimeSpan::MaxValue` is signed Int64 maximum, not a public millisecond value of
`0xffffffff`. Public timeout conversion and this native encoding are separate
steps.

`ClientOperationFailureMessageBody` explicitly serializes an error enum and
message string, so it is **not** equivalent to the primitive `ErrorCode` helper.
Similarly, `FabricActivityHeader(ActivityId)` nests an ActivityId object, whereas
`MessageIdHeader(MessageId)` places its GUID/index primitive fields directly
inside the header object's scope.
[Sources: [TimeSpan fields][timespan], [TimeSpan constants][timespan-constants],
[MessageId fields][message-id], [ActivityId fields][activity-id],
[ErrorCode fields][error-code], [activity header][activity-header],
[message ID header][message-id-header].]

## 9. Arrays, vectors, maps, and pointers

### 9.1 Native array encodings

| Kind | Nonempty encoding | Empty encoding |
|---|---|---|
| Byte array | `84 count raw_bytes` | `c4` |
| Bool array | `82 count raw_bool_bytes` | `c2` |
| Char array | `83 count raw_char_bytes` | `c3` |
| GUID array | `8c count raw_GUIDs` | `cc` |
| Double array | `8b count raw_doubles` | `cb` |
| Compressed integer array | `(base \| 80) count compressed_elements` | `base \| c0` |
| Serializable-object array | `80 count Object0 Object1 ...` | `c0` |
| Serializer pointer array | `81 count Pointer0 Pointer1 ...` | `c1` |

Every `count` in this table is the no-metadata unsigned compressed count.
Raw arrays do not repeat scalar type metadata or scalar empty-value compression
for their elements. A bool array is therefore not a sequence of `42`/`72`;
it contains raw one-byte bool values in this ABI.

Every serializable-array element has its own Object marker/header/scopes.
Pointer-array elements have their own null/non-null pointer marker.
[Sources: [blittable arrays][blittable-arrays],
[compressed arrays][compressed-arrays], [object arrays][object-arrays],
[pointer arrays][pointer-arrays].]

### 9.2 C++ containers choose helpers, not one universal array shape

`std::vector<T>` normally uses `ArrayUtility<T>`. Primitive specializations select
the corresponding native array encoding; a `DEFINE_USER_ARRAY_UTILITY(T)`
specialization selects an object array. A type explicitly declared blittable
can instead use its raw bytes through a byte array.

Important exceptions:

```text
vector<wstring>:
    typed UInt32 length
    string field 0
    string field 1
    ...

vector<shared_ptr<T>> / vector<unique_ptr<T>>:
    typed UInt32 length
    pointer field 0
    pointer field 1
    ...
```

Those lengths **do** have UInt32 metadata. In particular, an empty
`vector<wstring>` is `48`, not `cd`, `c0`, or a count byte `00`. This matters for
`Uri.pathSegments_`.

For example:

```text
vector<wstring> ["A", ""]:
    08 02 8d 01 41 00 cd
```

Trace each actual specialization. "It is a vector" does not determine its wire
shape by itself.
[Sources: [array utilities][array-helpers],
[string/vector helpers][string-and-vector-helpers],
[smart-pointer vectors][pointer-helpers].]

### 9.3 Maps

The map helper builds a vector of `ArrayPair<TKey, TValue>`. For the registered
user-map types, that becomes:

```text
ObjectArray(count):
    Object ArrayPair {
        Scope { key; value; }
    }
    ...
```

The pair's key and value use their own type-specific helpers. Map iteration
determines output order; do not replace it with arbitrary hash iteration when
constructing canonical producer fixtures.

For a `map<wstring,wstring>`, each key and value is a WString field. A
`QueryArgumentMap` is different: its map key is a `QueryArgumentMapKey` object
with a string field, so there is another object around each key. Its ordering
comparison is case-insensitive.

An empty registered map writes the empty object array `c0` in the containing
scope; it does not acquire an extra generic map Object wrapper unless its
declaring field type, such as `QueryArgumentMap`, supplies one.
[Sources: [map helper][map-helpers],
[ArrayPair and registrations][pair-helper],
[query argument map schema][query-arguments].]

### 9.4 Pointers and polymorphism

At the serializer level:

```text
null pointer:     41
nonnull pointer:  01 || serialized Object
```

No address, COM pointer, reference count, or object-identity reference is sent.
The object can contain type information used by its activator. A shared pointer
to a nonserializable value may introduce a `PointerWrapper<T>` object containing
one field; it is not necessarily a bare primitive after `01`.

The native smart-pointer vector helper above is not the same container layout
as `WritePointerArray`, despite both containing pointer values.
[Sources: [pointer writer/reader][pointer-values],
[smart-pointer and wrapper helpers][pointer-helpers],
[PointerWrapper definition][pair-helper].]

### 9.5 No-copy extension buffers are not ordinary byte arrays

The serializer defines `ByteArrayNoCopy = 8e`, with counts/indices referencing
external buffers, and object flag `0x02` for extension data. Those bytes cannot
be parsed as `84 count inline_bytes`.

The inspected user-mode `Message::BodyBuffers::PrepareForSend` rejects extension
buffers, and incoming body deserialization supplies an empty extension-buffer
collection. Therefore this document does **not** define an additional external
buffer transport for ordinary FabricClient messages. Unknown no-copy fields or
objects requiring such buffers are not safely skippable as ordinary extensions;
the native unknown-data path can reject them.
[Sources: [no-copy methods][no-copy],
[body send preparation][body-prepare],
[body receive construction][body-receive],
[unknown-data handling][unknown-data].]

## 10. Version tolerance and unknown data

Scopes support append-style version evolution:

1. Fields are read in declared order. Encountering ScopeEnd/ObjectEnd returns
   the scope-end status rather than fabricating a value. Fields absent at the
   tail retain their initialized/current values according to the declaring
   type's read path; they are not universally replaced by zero.
2. After known fields, `ReadEndType` walks the remaining typed values to the
   end of that scope, retaining unknown bytes for that scope.
3. Additional unknown scopes can be retained. Re-serialization appends stored
   unknown fields/scopes through `WriteEndType` and
   `WriteUnknownDataInExtraScopes`.
4. Nested unknown objects can be skipped using their bounded object size when
   they do not require unsupported extension buffers.

This is not arbitrary field reordering: positional fields still require the
correct schema. There are no per-field protobuf tags allowing a reader to find
field 7 independently.

Do not scan for byte `2f` or `3f`. An integer payload, GUID, string, or raw byte
array can contain those bytes. Unknown-field walking must understand enough
type grammar to advance correctly, including counts, compressed elements,
pointer/object nesting, and object extents.

A port should preserve unknown data when it re-emits a received object.
Read-only result decoding can skip it according to these rules. Unknown required
polymorphic result types must remain unsupported/explicitly diagnosed, not
silently become empty successful results.
[Sources: [scope reader/writer][scope-methods],
[object reader][object-reader],
[unknown fields/objects][unknown-data],
[unknown scope writing][extra-scopes].]

## 11. FabricClient header schemas

`Object{...}` below means the complete serialized header object. `inline`
means a primitive wrapper does not introduce another object. Fields are ordered
as shown; records themselves do not have a globally mandated order.

| ID | Header | Object's ordered fields |
|---|---|---|
| `8001` | Actor | `Int64(actor enum)` |
| `8002` | Action | `WString(action)` |
| `8003` | MessageId | inline `GUID(process GUID), UInt32(index)` |
| `8004` | RelatesTo | inline `GUID(request GUID), UInt32(request index)` |
| `8005` | ExpectsReply | `Bool(expectsReply)` |
| `8007` | Fault | `Int64(error enum), Bool(hasFaultBody)` |
| `8009` | HighPriority | `Bool(highPriority)` |
| `8020` | FabricActivity | `Object ActivityId{GUID, UInt64(index)}` |
| `8023` | Timeout | inline `Int64(TimeSpan ticks)` |
| `8025` | ClientProtocolVersion | `Int64(major), Int64(minor)` |
| `8028` | ForwardMessage | `Int64(original actor), WString(original action)` |
| `8029` | ClientAuth, in claims negotiation | `Int64(provider enum)`; Claims ordinal is 4. |
| `803e` | UncorrelatedReply | `Bool(isUncorrelatedReply)`; producer defaults true. |
| `8041` | ServerAuth | `Bool(willSendConnectionAuthStatus), Pointer<ClaimsRetrievalMetadata>` |
| `8048` | SecurityNegotiation | `Bool(extra-framing compatibility), Bool(framing protection), Object ListenInstance, UInt64(max incoming frame size)` in the profiled 64-bit ABI. |

`ClaimsRetrievalMetadata` contains `WString(type_)`, then
`map<WString,WString>(metadata_)`. `ListenInstance` contains `WString(address_)`,
`UInt64(instance_)`, and GUID nonce, in one object scope.

The claims negotiation `ClientAuthHeader` is actually bound to `8029`; the
separate enum member `CustomClientAuth = 8035` is not its binding. Context matters
when interpreting header IDs.

Additional operations require additional headers. This table is not an
allowlist, and it does not establish that header `804c` always means an
application-header split. FabricClient has no generic FabricTransport
application-header/body split.
[Sources: [header IDs][header-ids], [actor/action schemas][actor-header],
[action fields][action-header], [MessageId][message-id-header],
[RelatesTo][relates-to-header], [booleans][expects-header],
[fault schema][fault-header], [priority schema][priority-header],
[activity schema][activity-header], [timeout schema][timeout-header],
[version schema][protocol-version], [forwarding schema][forward-header],
[claims indication][client-auth-header],
[duplex marker][uncorrelated-header], [server metadata][server-auth-header],
[claims metadata schema][claims-metadata],
[negotiation schema][negotiation-header], [listen instance][listen-instance].]

### 11.1 Small complete header examples

These are complete records, with zero object padding:

```text
Actor = NamingGateway (5):
    01 80                         header ID 8001
    0e 00                         serialized object length = 14
    00                            Object
    0d 00 00 00                   ObjectHeader.size = 13
    00 00 00 00                   flags = 0, three padding bytes
    1f 09 05 2f 3f                scope, Int64(5), end scope/object

ExpectsReply = true:
    05 80 0d 00
    00 0c 00 00 00 00 00 00 00 1f 42 2f 3f

ClientProtocolVersion = 1.2:
    25 80 10 00
    00 0f 00 00 00 00 00 00 00 1f 09 01 09 02 2f 3f
```

The Actor record occupies 18 bytes, the boolean record 17 bytes, and the version
record 20 bytes. Their object size values, serialized object lengths, and total
record sizes are different quantities.

## 12. Representative message bodies and envelopes

### 12.1 Gateway ping

```text
Actor: NamingGateway (5)
Action: "PingRequest"
Headers: FabricActivity, ClientProtocolVersion, Timeout,
         ExpectsReply(true), MessageId(R)
Body: empty, NOT an empty serialized object
```

At the baseline revision the native current version is major 1/minor 2. Minor
1 introduced the ping body and minor 2 chunked-upload capability. The Rust
design's initial choice to advertise 1.1 is a capability decision, not a
different encoding of this header.

The ping reply is correlated by `RelatesTo(R)`. The native factory uses
`"PingRequest"` for that reply too; do not require the literal `"PingReply"`.
When supported by protocol version, its body is:

```text
Object PingReplyMessageBody {
    Scope {
        Object GatewayDescription {
            Scope {
                WString(address_)
                Object NodeInstance {
                    Scope {
                        ByteArray(NodeId)
                        UInt64(instanceId_)
                    }
                }
                WString(nodeName_)
            }
        }
    }
}
```

NodeId is a special raw-byte-array type, not two compressed integers. Its
profiled 16-byte payload is the native `LargeInteger` layout:
`u64le high || u64le low`. Thus its field starts `84 10`, followed by those
16 bytes. This is different from blindly writing a normal little-endian
128-bit integer, which would put the low half first.
[Sources: [ping factory][ping-factory], [ping body][ping-body],
[gateway description][gateway-description], [node instance][node-instance],
[NodeId byte-array specialization][node-id],
[LargeInteger layout][large-integer], [byte-array helper][enum-helpers].]

### 12.2 NameExists

```text
Actor: NamingGateway (5)
Action: "NameExistsRequest"
Headers: gateway/request metadata and MessageId(R)

Body: Object NamingUri / Uri {
    Scope {
        Int64(type_)
        WString(scheme_)
        WString(authority_)
        Int64(hostType_)
        WString(host_)
        Int32(port_)
        WString(path_)
        WString(query_)
        WString(fragment_)
        vector<WString>(pathSegments_)
    }
}
```

There is no additional `NamingUriMessageBody` object around this URI and no
single string field containing the entire name. Native URI parsing and
canonicalization determine these values; wire encoding is not a substitute
for reproducing those naming rules.

The success body is:

```text
Object NameExistsReplyMessageBody {
    Scope { Bool(nameExists_); Int64(userServiceState_ enum); }
}
```

For example, `nameExists = true`, `userServiceState = Created (4)` is the
15-byte serialized body:

```text
00 0e 00 00 00 00 00 00 00 1f 42 09 04 2f 3f
```

This is a body example, not an entire reply: the native reply must still have
its frame and correlation headers. The ordinary naming success action is in
the `NameOperationReply` family.
[Sources: [URI adapter][uri-adapter], [URI fields][uri-fields],
[NameExists reply][nameexists-reply],
[state values][user-service-state],
[naming reply factory][naming-replies].]

### 12.3 Operation failures and transport faults

Gateway/operation failures can use:

```text
Action: "ClientOperationFailure"
RelatesTo: R
Body: Object ClientOperationFailureMessageBody {
    Scope { Int64(error enum); WString(message); }
}
```

This class deliberately keeps error value and text as separate fields for
compatibility. It is not the primitive ErrorCode format, a raw HRESULT body,
or FabricTransport's service-error header.

The request/reply layer can also consume a `FaultHeader` before interpreting an
operation-specific body. `hasFaultBody` is a decoded boolean field, not an
inference from whether the frame body happens to be nonempty.
[Sources: [failure body][failure-body],
[fault header][fault-header], [reply fault consumption][request-table].]

### 12.4 External queries

```text
Actor: NamingGateway (5)
Action: "QueryRequest"
Body: Object QueryRequestMessageBody {
    Scope {
        Object QueryArgumentMap {
            Scope {
                ObjectArray of ArrayPair {
                    Scope {
                        Object QueryArgumentMapKey {
                            Scope { WString(key_); }
                        }
                        WString(value)
                    }
                }
            }
        }
        WString(queryName_)
    }
}
```

The external field order is arguments, then query name. The similarly named
internal body instead has query enum, arguments, and query type; it is not
interchangeable.

The common query result schema is:

```text
Object QueryResult {
    Scope {
        Int64(resultKind_)
        Pointer<polymorphic QueryResultListWrapperBase>(resultWrapper_)
        inline ErrorCode(queryProcessingError_)    // numeric enum only
        Pointer<PagingStatus>(pagingStatus_)
    }
}
```

Use the wrapper's type information to select the exact item/list schema.
Do not equate the result-shape enum with the item-type discriminator or assume
all query payloads are strings. Framing success and even request/reply success
do not remove the need to check `queryProcessingError_`.
[Sources: [query request schemas][query-request],
[argument map][query-arguments], [query result fields][query-result],
[query activator][query-activation], [ErrorCode helper][error-code].]

### 12.5 Forwarded management and reverse notifications

Forwarded management requests retain their operation body but change the outer
actor/action to NamingGateway / `"ForwardMessage"`. `ForwardMessageHeader`
contains the original actor and action. Do not infer body schema solely from
the outer action when a forwarding envelope is present.

Reverse notification exchanges use `UncorrelatedReplyHeader` to distinguish a
gateway push from an ordinary reply. The push has its own MessageId `N`, and
the acknowledgment uses `RelatesTo(N)`. Dispatch this marker before ordinary
pending-request correlation. Notification bodies have naming/session schemas,
not opaque FabricTransport application payloads.
[Sources: [forwarding details](FabricClient.md#72-forwarded-management-operations),
[notification details](FabricClient.md#74-notification-sessions-and-reverse-traffic),
[duplex dispatch][duplex].]

### 12.6 Transport control bodies

These remain separate from gateway operation bodies:

| Control | Actor/action | Ordered body fields |
|---|---|---|
| Listen instance | Transport (`1`), empty action | Object: `WString(address), UInt64(instance), GUID(nonce)`. |
| Connection authorization | TransportSendTarget (`29`), `"ConnectionAuth"` | Object: `WString(errorMessage), Int64(roleGranted enum)` when status says a fault body exists. |
| Claims token | Transport (`1`), `"Claims"` | Object: `WString(claims)`. |
| TLS negotiation | SecurityContext (`35`) | Raw TLS token bytes, not an Object. |

Authorization also uses Fault/ServerAuth metadata and connection state; a
decoder cannot decide readiness merely from a successfully decoded control
body. The first protected outgoing message repeats negotiation metadata for
the native comparison path.
[Sources: [listen body][listen-instance], [authorization body][connection-auth],
[claims body][claims-message], [control production][control-production],
[negotiation comparison][negotiation-verification].]

## 13. Complete source-derived PingRequest byte example

This is one constructed **clear-TCP message**, after any required transport
setup. It is not a complete connection transcript or a captured native packet.

Inputs:

```text
Actor                       5
Action                      "PingRequest"
ClientProtocolVersion       1.2
Timeout                     30 seconds = 300000000 ticks
MessageId process GUID      00112233-4455-6677-8899-aabbccddeeff
MessageId index             1
ActivityId GUID             same illustrative GUID
ActivityId index            0
ExpectsReply                true
Body                        empty
Provider mask               00
CRCs                        zero: optional checks absent
Object padding              zero, canonical choice for this example
```

The same GUID is deliberately reused for two different header types; their
representations still differ as described in section 8.3.

| Absolute byte offset | Record | Record bytes |
|---:|---|---:|
| 12 | Action | 40 |
| 52 | Actor | 18 |
| 70 | FabricActivity | 46 |
| 116 | ClientProtocolVersion | 20 |
| 136 | Timeout | 22 |
| 158 | ExpectsReply | 17 |
| 175 | MessageId | 35 |

`H = 198 = 0x00c6`, `B = 0`, `F = 210 = 0x000000d2`.
The left column below is a hexadecimal byte offset, not part of the message:

```text
0000  d2 00 00 00 00 00 c6 00 00 00 00 00 02 80 24 00
0010  00 23 00 00 00 00 00 00 00 1f 8d 0b 50 00 69 00
0020  6e 00 67 00 52 00 65 00 71 00 75 00 65 00 73 00
0030  74 00 2f 3f 01 80 0e 00 00 0d 00 00 00 00 00 00
0040  00 1f 09 05 2f 3f 20 80 2a 00 00 29 00 00 00 00
0050  00 00 00 1f 00 1d 00 00 00 00 00 00 00 1f 0c 33
0060  22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff 4a
0070  2f 3f 2f 3f 25 80 10 00 00 0f 00 00 00 00 00 00
0080  00 1f 09 01 09 02 2f 3f 23 80 12 00 00 11 00 00
0090  00 00 00 00 00 1f 09 81 8f 86 c6 00 2f 3f 05 80
00a0  0d 00 00 0c 00 00 00 00 00 00 00 1f 42 2f 3f 03
00b0  80 1f 00 00 1e 00 00 00 00 00 00 00 1f 0c 33 22
00c0  11 00 55 44 77 66 88 99 aa bb cc dd ee ff 08 01
00d0  2f 3f
```

The native clear sender normally enables header CRC generation; zero CRCs here
are an explicitly chosen accepted representation, not a claim about its defaults.
For TLS, the recovered plaintext uses the same frame/header/body format, with
the secured connection's provider mask and security state, before encryption.
No byte in this example supplies a real client identity or authentication.

## 14. Decoder and interoperability requirements

| Requirement | Why |
|---|---|
| Separate TCP, TLS, frame, record, object, and scope parsers | Each layer has different boundaries and state. |
| Validate all enclosing lengths before allocation/slicing | `u32` frame length is not permission to allocate gigabytes. |
| Bound object depth, counts, decoded memory, and compressed widths | Small wire inputs can expand into large object graphs or containers. |
| Match metadata, actual helper specialization, and field order | A correct primitive codec cannot fix a wrong schema. |
| Preserve UTF-16 code units and GUID field byte order | Default Rust string/UUID representations differ. |
| Keep header order/deletion and native lookup distinctions | Unique-key maps lose information needed for exact behavior. |
| Treat unknown data structurally, not by marker search | Marker byte values can occur in payloads. |
| Distinguish bodyless messages from empty objects | Ping and raw TLS token bodies are not ordinary serialized request objects. |
| Check fault/operation/result error layers separately | A valid frame is not necessarily a successful operation. |
| Use native producer and consumer fixtures | A Rust encoder/decoder round trip can share the same mistake. |

Required fixture cases include boolean true/false, signed sign-boundary values,
scalar zero versus array/count zero, empty string versus empty string vector,
UTF-16 surrogate pairs, GUID layout, primitive versus nested IDs, map pair/key
objects, unknown trailing fields/scopes, polymorphic query results, duplicate/
deleted headers, and every enabled operation's success/failure shape.

Qualify the complete Ping example and other constructions against an
independent full native serializer/consumer before treating them as production
golden vectors. Then exercise a native gateway with partial/coalesced TCP input,
TLS handoff, authorization, correlation, and supported operation semantics.
Source-derived compatibility at one commit is not a stable public wire-protocol
guarantee across every released runtime.

## References

All native links are pinned to the inspected revision. File paths are relative
to the Service Fabric source checkout.

[revision]: https://github.com/microsoft/service-fabric/commit/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[request-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerRequestMessage.h#L55-L108
[message-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Message.h#L290-L327
[uri-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingUriMessageBody.h
[frame-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.h#L10-L41
[frame-validation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.cpp#L14-L118
[receive-buffer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpReceiveBuffer.cpp#L18-L241
[provider-map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecurityProvider.cpp#L15-L112
[crc-algorithms]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/crc.h
[crc-tables]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/crc.cpp#L7-L92
[send-buffer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendBuffer.cpp#L88-L278
[tls-negotiation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L176-L332
[security-transition]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1964-L2138
[header-writer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L163-L186
[header-iteration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L524-L567
[typed-lookup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.h#L492-L503
[incoming-headers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L22-L30
[common-scan]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L765-L905
[request-table]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L73
[object-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializableStream.h#L228-L246
[object-writer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L320-L448
[object-reader]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L450-L728
[object-pointer-reader]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L530-L651
[field-macros]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L264-L336
[primitive-macros]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4310-L4338
[type-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4827-L4875
[serializable-base]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializable.cpp#L118-L134
[type-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricTypeInformation.h
[query-activation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryResultListWrapperBase.cpp#L18-L34
[query-type-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryResultHelpersEnum.h#L26-L42
[metadata-types]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializationTypes.h#L12-L51
[metadata-operations]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializationTypes.cpp
[empty-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L11-L53
[blittable-scalars]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1920-L1984
[compressor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/ByteCompressor.h#L10-L161
[compressed-scalars]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L2101-L2176
[enum-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L44-L133
[count-writer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1554-L1574
[compressed-arrays]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L2179-L2342
[string-and-vector-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4877-L5033
[primitive-methods]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1087-L1139
[blittable-arrays]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1986-L2099
[short-wchar]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/CMakeLists.txt#L170-L175
[guid-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Guid.h
[timespan]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/TimeSpan.h#L135-L159
[timespan-constants]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/TimeSpan.cpp#L10-L14
[message-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageId.h#L48-L55
[activity-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ActivityId.h#L69-L74
[error-code]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ErrorCode.h#L80-L88
[array-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L135-L246
[map-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4428-L4471
[pair-helper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L5141-L5184
[pointer-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4580-L4777
[pointer-values]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1141-L1205
[object-arrays]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L730-L877
[pointer-arrays]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1208-L1374
[no-copy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1376-L1516
[body-prepare]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Message.cpp#L574-L597
[body-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Message.cpp#L338-L388
[unknown-data]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1631-L1918
[scope-methods]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L170-L318
[extra-scopes]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L2380-L2422
[header-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaderId.h#L21-L155
[actor-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ActorHeader.h
[action-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ActionHeader.h
[message-id-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageIdHeader.h
[relates-to-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RelatesToHeader.h
[expects-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ExpectsReplyHeader.h
[fault-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/FaultHeader.h
[priority-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/HighPriorityHeader.h
[activity-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/FabricActivityHeader.h
[timeout-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TimeoutHeader.h
[protocol-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientProtocolVersionHeader.h#L10-L82
[forward-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ForwardMessageHeader.h
[client-auth-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ClientAuthHeader.h
[uncorrelated-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/UncorrelatedReplyHeader.h
[server-auth-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ServerAuthHeader.h
[claims-metadata]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/ClaimsRetrievalMetadata.h#L65-L79
[negotiation-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityNegotiationHeader.h
[listen-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ListenInstance.h
[ping-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingTcpMessage.h#L91-L99
[ping-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/PingReplyMessageBody.h
[gateway-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/GatewayDescription.h
[node-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/federation/NodeInstance.h#L41-L46
[node-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/federation/NodeId.h#L248-L253
[large-integer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/LargeInteger.h#L189-L197
[uri-fields]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Uri.h#L87-L120
[nameexists-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NameExistsReplyMessageBody.h
[user-service-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/UserServiceState.h
[naming-replies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/NamingMessage.h#L108-L127
[failure-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientOperationFailureMessageBody.h
[query-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/Query/QueryRequestMessageBody.h
[query-arguments]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryArgumentMap.h#L73-L105
[query-result]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryResult.h#L140-L189
[duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L36-L77
[connection-auth]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.h#L156-L171
[claims-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ClaimsMessage.h
[control-production]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1391-L1499
[negotiation-verification]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L542-L679
