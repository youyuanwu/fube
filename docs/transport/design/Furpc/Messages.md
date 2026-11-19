# furpc: Raw messages and native headers

**Status:** proposed; see the [design summary](README.md).

Defines immutable native messages, shared bodies, header semantics, and
raw sends. [Requests](Requests.md) add typed envelopes;
[resources](Resources.md) distinguish storage ownership from queue accounting.

`Message` belongs to raw native messaging, above private framing and below
typed RPC/body codecs and services. Raw protocols construct it directly;
typed handlers normally use `Request<T>` and `Reply<T>`. It is neither a frame
nor a second application-request format.

## Native headers and authority

Message headers are part of the public raw messaging API, not an entirely
private `furpc` representation. Separate their visibility from authority to
modify or submit them:

| Category | Public access and ownership |
| --- | --- |
| Actor, Action, Activity, Timeout, and domain extension headers | Inspect raw received values, edit service-facing request metadata, and append outgoing records through generic header APIs. |
| MessageId, RelatesTo, ExpectsReply, and special duplex flags | Inspectable on messages; managed RPC, reply, and duplex APIs own their outgoing values. See the [raw-only ID contract](Requests.md#correlation-and-native-ids). |
| Fixed frame lengths, CRCs, provider masks, and connection/security control envelopes | Internal encoding and processing; not user-mutable application headers. |

Raw Message and direct special-duplex ReceivedMessage expose immutable header views.
The service-facing `Request<B>` instead exposes an editable header collection,
with trusted ingress context kept separate. Provide ordered raw-record iteration,
typed native lookups, and outgoing message/header builders. Users can add domain
extension records and preserve unknown headers without editing a frame buffer
or implementing a body codec. Native header values use the native schemas;
pluggable body codecs do not replace their encoding.

Actor and Action are native headers, not special fields on the message
builder. Their wire schemas and typed helpers belong to native messaging;
their application meanings and handler selection belong to consumers and the
optional router. Generic header insertion does not select a handler.
The ordinary Actor header is optional: absence is not a framing error and
`first_actor` returns None without synthesizing a record. Native effective
lookup may still report its Empty/default-zero value. Only optional Actor
dispatch uses that value as a delivery key; explicit raw ingress can use
ReplicationActor or another domain header instead. Reserved native control
classification remains internal and cannot be bypassed by selecting raw ingress.

Builder mutation and successful submission are separate boundaries. Managed
submission rejects conflicting user-supplied transport-owned fields rather
than silently overwriting them. A raw record or a copied RelatesTo value never
mints a consuming reply grant or changes source provenance. A restricted
domain facade need not expose the full raw header-building API.

Keep the raw ordered record sequence, including duplicate and unknown records.
Deleted records are skipped for semantic lookup; preserved wire storage may
retain their bytes. Do not replace headers with a unique-key map.

Provide explicitly different operations for:

- First visible typed occurrence, matching native first-match lookup.
- Effective received common properties, matching native ordered scanning.
- Iteration over preserved raw records.

Every recognized common occurrence must decode, even when a later occurrence
overwrites its effective value. Cover Retry and Idempotent as well as Actor,
Action, MessageId, RelatesTo, ExpectsReply, Fault, HighPriority, and special
reply state. A malformed occurrence cannot be hidden behind a valid neighbor.

Received immutable messages have stable common-property views. Outgoing
builders produce new messages; request header edits invalidate/recompute
derived metadata without changing shared raw messages.
They do not emulate C++ mutation-cache side effects accidentally.
Byte-preserving forwarding and explicit header replacement are distinct
operations; neither silently compacts duplicates.

The native header-area bound remains mandatory. Additional record/work limits
are resource policy, not invented native format rules.

## Raw messages, headers, and sending

```rust,ignore
pub mod header {
    use crate::{ActorId, HeaderId, HeaderRecord, Result};

    pub const ACTOR: HeaderId;
    pub const ACTION: HeaderId;

    pub fn actor(value: ActorId) -> Result<HeaderRecord>;
    pub fn action(value: &str) -> Result<HeaderRecord>;
}

impl HeaderRecord {
    pub fn native<T: serde::Serialize>(
        id: HeaderId, value: &T,
    ) -> Result<Self>;
    pub fn from_encoded(
        id: HeaderId, encoded_native_object: Vec<u8>,
    ) -> Result<Self>;
}

impl Peer {
    pub fn send(&self, message: Message, deadline: Instant)
        -> Operation<WriteReceipt>;
}

impl MessageBuilder {
    pub fn append_header(self, header: HeaderRecord) -> Self;
    pub fn body(self, body: Body) -> Self;
    pub fn build(self) -> Result<Message>;
}

impl Message {
    pub fn builder() -> MessageBuilder;
    pub fn headers(&self) -> &Headers;
    pub fn body(&self) -> &Body;
    pub fn into_body(self) -> Body;
}

impl Headers {
    pub fn append(&mut self, header: HeaderRecord) -> Result<()>;
    pub fn remove_all(&mut self, id: HeaderId) -> Result<usize>;
    pub fn records(&self) -> impl Iterator<Item = HeaderRef<'_>>;
    pub fn first_raw(&self, id: HeaderId) -> Option<HeaderRef<'_>>;
    pub fn first_actor(&self) -> Option<ActorId>;
    pub fn first_action(&self) -> Option<&[u16]>;
    pub fn effective(&self) -> &CommonHeaders;
}
```

`Message::builder` and the header constructors are standalone data-construction
APIs. They need no client, server, received context, or active runtime and
grant no admission, send permission, reply authority, or connection lifetime.
Raw handlers build responses directly; the runner still owns reply completion.

The public `header` namespace exposes native header-ID constants and optional
typed constructors, similar to named HTTP header constants rather than
request-builder fields. `ACTOR` and `ACTION` use source-defined native IDs.
`header::actor` and `header::action` bind those IDs to the correct native
object schemas using the fixed serializer; callers need not encode those
schemas themselves. They return ordinary records for `append_header`, not
another envelope type. Additional common-header helpers follow this pattern,
not new per-header setters on message/request/reply envelopes.

`HeaderRecord::native` uses the fixed internal serializer for supported native
Serde schemas. `HeaderRecord::from_encoded` accepts manually encoded native
schema bytes; it does not certify an unknown schema or authorize managed
fields. Both preserve error categories. Application-body codecs do not change
their native format.

`first_actor` and `first_action` are typed first-visible views, not dispatch
operations. Action is exposed as native UTF-16 units without lossy conversion
or normalization; conversion to Rust text is fallible. `header::action`
encodes Rust text without normalization. Preserving an existing encoded
header is distinct from reconstructing it from text. Effective-property
lookup and validation of every recognized duplicate remain unchanged.

### Construction and submission limits

Standalone construction enforces native widths: 65,535 combined header bytes
(including four bytes per record), nonzero record payloads, and a u32 total
frame length including the 12-byte fixed header. There is no separate
construction profile or `furpc::construction` module, retained-capacity ceiling,
segment-count ceiling, native-depth cap, or declared-field/element work quota.
`Body::from_bytes`, `Body::try_from(Vec<u8>)`, and `Body::from_segments(Vec<Body>)`
are fallible; the segmented constructor does not copy payload bytes. Body
slices share the parent's segment table and backing storage.

The initial implementation explicitly reports incomplete MessageId/RelatesTo
schemas as unsupported rather than synthesizing their native constructor-time
process IDs. Supporting those generated defaults is deferred.

Standalone construction does not capture a facade's `TransportLimits`.
The combined encoded header area must fit the native 16-bit length, including
record overhead. Native-representable nesting and collections are not rejected
by additional constructor-only resource limits.

`native` bounds encoding output by the native header-area capacity.
`from_encoded` checks native lengths, not spare backing capacity.
Builder methods retain the first construction error and
stop accumulating header data after failure; `build` returns that error.
Known record sizes/counts are checked before growing builder storage.

Successful construction validates local native structure,
not eligibility on any particular connection. Send/reply admission applies
the actual scope's native-header/resource limits and recognized-field checks,
then rechecks the complete envelope after adding transport-managed headers.
It also checks mode, terminal state, authority, and deadline before commitment.
These checks use the scope's validated internal context, never the standalone
constructor settings as a substitute.

Consequently, a locally valid message may fail submission under a stricter
connection policy or lack room for managed headers. This deferred policy check
is explicit; construction never reserves transport capacity. Shared storage
follows ordinary reference-counted ownership, independently of
[send-queue accounting](Resources.md#immutable-storage-and-accounting).

`append_header` appends in order. Any replacement API must explicitly name its
occurrence/removal behavior; no map-like `set` silently erases native
duplicates. Common typed views distinguish first occurrence from effective
received values. `HeaderRef` exposes borrowed ID/value views, never a mutable
record. Owned forwarding shares its immutable backing bytes.

`Headers::append` adds one record at the end; `remove_all` removes every visible
occurrence of that ID and returns the count, preserving all other records'
relative order. Removing an absent ID returns zero. Explicit replacement is
`remove_all(id)` followed by `append(record)`; these are separate fallible
operations, not an atomic replacement. Neither inserts native defaults.

These mutations require exclusive access to the collection, available through
`Request::headers_mut` or public `RequestParts::headers`, not through an
immutable raw message. Each operation validates affected native records and
checks native widths before changing the collection.
Failure leaves that operation's collection unchanged.
Recompute derived common properties from the current ordered sequence; do not
keep stale first-visible/effective caches or mask a malformed occurrence.
Unchanged encoded bytes stay shared. Removed records disappear from the new
collection without accumulating hidden tombstones; older aliases keep their
own storage alive. Editing an admitted request preserves its original-scope
[validation boundary](Requests.md#request-envelope), not an allocation ledger.

Header collection mutation never modifies trusted context or reruns native
classification. Managed fields remain subject to the selected outgoing API's
submission restrictions; editing a received MessageId, RelatesTo, or flags
cannot change its original correlation, exchange kind, or reply grant.
Construction does not invent Actor/Action values. Application/domain helpers
may construct an agreed envelope above this API; transport submission still
checks the actual headers and policy.

`Body` accepts owned bytes and shared segments; its public borrowed segment
iterator cannot outlive the backing storage. Message clones share that storage.

Normal send is lazy and capacity exhaustion is explicit. Its success receipt
means a complete local write only. No destination argument is required because
the handle already selects the connection. For semantic ACKs, the service
binding must explicitly grant a bound Peer through RequestContext::peer;
special-duplex delivery can independently grant ReceivedMessage::peer.
That handle preserves its captured outbound policy on every send, RPC, and
notification operation, including through clones and typed adapters. It cannot
forge an ordinary RPC reply by supplying RelatesTo.
Ordinary correlated responses are IngressResponse data completed through the
runner's private grant. Direct special-duplex acknowledgment uses ReplyGrant.

In managed mode, caller-supplied transport correlation fields reject before
transmission, including preserved records that conflict with managed
submission. In raw-only mode, protocol-owned IDs follow that mode's explicit
contract. Native connection-control messages cannot be injected through the
application Peer capability.
