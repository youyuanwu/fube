# furpc: Raw messages and native headers

`Message` is an immutable native envelope with ordered headers and a shared
segmented `Body`. It is data, not a frame, connection owner, or send/reply grant.
Public construction lives in
[message.rs](../../../../crates/libs/furpc/src/message.rs); shared backing
storage lives in [body.rs](../../../../crates/libs/furpc/src/body.rs).

## Native headers and authority

Native headers are public messaging data. `header::actor`, `header::action`,
and generic `HeaderRecord` constructors produce records for ordinary ordered
insertion. Actor/Action are not special envelope fields and do not select a
handler merely by being constructed.

The [header API](../../../../crates/libs/furpc/src/header.rs) distinguishes:

| View | Meaning |
| --- | --- |
| Raw ordered records | Preserve duplicate and unknown encoded records. |
| First-visible lookup | Inspect the first matching visible occurrence. |
| Effective common properties | Interpret native ordered scanning, including later effective values. |

Every recognized occurrence is validated; a valid neighbor cannot hide a
malformed duplicate. Unknown records remain opaque. Native UTF-16 values are
available without lossy conversion, normalization, or case folding.
The [native decoder](../../../../crates/libs/furpc/src/wire/headers.rs) owns
recognized schemas, trailing defaults, and effective-property interpretation.

An ordinary Actor header is optional. Its absence is observable separately from
native effective Empty/zero semantics. Raw ingress can interpret a domain header
such as ReplicationActor without a synthetic Actor record.

Managed correlation fields are inspectable, but their outgoing values belong
to managed RPC, notification, and runner reply paths. Copying MessageId,
RelatesTo, or flags never creates reply authority or source provenance.
Connection-control actors and framing fields remain transport-owned.

## Raw messages, headers, and sending

Standalone builders require no runtime, endpoint, or received context.
`MessageBuilder` retains the first construction error and returns it from
`build`. `HeaderRecord::native` uses the fixed native serializer;
`from_encoded` accepts encoded native object bytes without certifying an
unknown domain schema.

Raw messages expose immutable headers. Request and response envelopes instead
allow collection edits through the same bounded header operations. Appending
preserves order; `remove_all` explicitly removes every visible occurrence of
an ID. Replacement is an explicit removal followed by append, not an atomic
map-style overwrite.

Each individual collection edit either succeeds or leaves that operation's
collection unchanged. Derived first/effective properties reflect the new
sequence. Shared raw aliases remain unchanged, and removed records do not
accumulate hidden collection tombstones.

Bodies accept owned bytes, shared slices, and segmented composition. Clones
and slices retain their backing allocations; segmented construction does not
flatten payloads. Storage lifetime is independent of send-queue credit.

`Peer::send` returns a lazy native operation. Submission uses the Peer's captured
generation and policy; success means complete local writing, not remote
delivery or semantic acknowledgment. The implementation is split between
[peer.rs](../../../../crates/libs/furpc/src/peer.rs) and
[operation.rs](../../../../crates/libs/furpc/src/operation.rs).

## Construction and submission limits

Construction enforces native representation: combined header-area width,
nonzero record payloads, total frame width, and checked arithmetic. It does
not reserve transport capacity or impose a separate heap, segment-count,
nesting, or work profile. Native schema validation is not authorization.

Submission validates the actual complete envelope against the selected
connection's policy, including transport-managed fields and their additional
size. A constructible message can therefore fail a stricter frame policy,
queue admission, managed-field rule, deadline, or send restriction.

Managed sends reject caller-supplied correlation fields instead of overwriting
them. Raw-only one-way sends retain caller-owned IDs but cannot originate RPC,
notifications, or correlated replies. `PeerSendService` deliberately rejects
managed correlation records even in raw-only mode.

Response construction remains ordinary data construction. Only the ingress
runner's original private ticket can turn returned reply data into an
authorized correlated write. See [Services](Services.md).

Construction behavior is exercised in
[messages.rs](../../../../crates/libs/furpc/tests/messages.rs) and the
[message unit tests](../../../../crates/libs/furpc/src/message/tests.rs).
