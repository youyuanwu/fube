# Native transport: SF source gaps and Rust profile limits

## 1. Scope and conclusion

`fube-transport` is a **bounded implementation of selected Service Fabric native
TCP protocol mechanisms**, not a complete, source-faithful port of the native
Transport stack. Supported encoding can match SF while admission, failure,
lifetime, and configuration policies remain different. Some differences reject
otherwise valid SF traffic.

This comparison describes **main** at
[`2b9e8436bc120b63a5dd01bcda6ba0bc3c45b2fb`][rust-revision].
Rust source links outside section 7 follow the current file locations, including
the extraction of the unchanged serializer into `fubst`; the comparison remains
bound to that baseline. This extraction does not address the gaps below. Line
references should be rechecked after later changes. The SF source was inspected
locally in `../reference/service-fabric` at
[`51a7703a1f83749a9234d3ceb4296d8622e42cca`][sf-revision].
Native links below are pinned to that revision.

The unmerged V1 checkpoint at
`286f675ea5f8e21758b11ccf2618259806a9d3b9` is historical comparison material
only. Its mitigations are separated in section 7: **they are not available on
this main baseline**.

The existing [operational channel profile](NativeChannel.md),
[FabricClient message format](FabricClientMessageFormat.md), and
[FabricTransport specification](Transport.md) provide additional context.
Those documents also contain evidence pinned to an earlier SF revision; this
comparison does not assume the two native revisions are byte-identical.
Source inspection alone does not establish complete native interoperability,
performance equivalence, or deployed qualification.

## 2. Distinct implementation layers

```text
Gateway / accepted TCP / activated-host IPC adapters
    -> Endpoint / accept_unsecured_tcp / native_ipc setup
        -> Channel (GatewayChannel)
            -> bounded channel admission, correlation, actor ingress
                -> crate-private raw_session::engine
                    -> public raw_session / message / frame
                        -> fubst serialization / Serde codec
```

`Channel` is an alias for the existing `GatewayChannel`, not an independently
ported `TcpDatagramTransport`. `Endpoint::unsecured_tcp` accepts numeric unicast
destinations with nonzero ports, including non-loopback addresses.
`Endpoint::local_ip` can select an outgoing interface; `connect_until` shares a
caller-owned absolute deadline. There is no built-in hostname-resolution API
on Endpoint.

`accept_unsecured_tcp` establishes an already accepted socket using the same
Channel driver. The caller owns listening and domain admission; this is not a
full native listener/target service. Both TCP paths are unencrypted and
unauthenticated. Activated-host IPC retains separate loopback-only connect and
accept checks plus its host/PID envelope policy.

The crate-private `raw_session::engine` owns established socket/task progress,
admitted write jobs, common envelope/control processing, first terminal cause,
disposal, and joined shutdown. Channel retains correlation, ID history,
consuming reply grants, session policy, and actor authorization above that
engine, sharing a coordinator for atomic initial-write eligibility. Engine
extraction is not a native graceful-close or recovery implementation.

References: [crate scope][rust-lib], [Endpoint][rust-endpoint],
[accepted TCP][rust-accepted], [IPC setup][rust-ipc],
[channel backend][rust-channel], [engine ownership][rust-engine],
[engine receive/controls][rust-controls].

## 3. Why the Rust implementation is bounded

The existing channel deliberately establishes finite count and retained-byte
reservations before accepting work, retains charges across queued and
completed-undelivered operations, and fails explicitly when admission is
exhausted. Plain TCP does not authenticate peers; non-loopback connectivity
does not supply TLS or native security negotiation. Unsupported session behavior
terminates rather than attempting an unimplemented recovery.

These are documented Rust host/profile choices, **not native SF defaults**.
The retention model is described in
[NativeChannel.md](NativeChannel.md#finite-retention-and-ordering).
The retained-byte ceiling is a conservative reservation allowance, not eager
allocation or a process-RSS limit; OS buffers, runtime internals, and arbitrary
caller allocations are outside that accounting.

SF is not unbounded either. It has frame/header field widths, configurable
message and queue limits, connection throttling, timeouts, and memory controls.
Some differences are intentional ordinary-Channel admission policies, not
wire-format defects. However, main's framing and serializer ceilings are fixed
in the shared implementation; increasing Channel admission settings cannot
bypass them. The configurable native opt-ins described in section 7 exist only
in the unmerged checkpoint. The compatibility gap is rejecting native-permitted
traffic or changing native behavior without the corresponding profile
qualification, not the existence of resource protection itself.

## 4. Limits and their native counterparts

| Area | Ordinary Rust profile | Inspected SF behavior / consequence |
| --- | --- | --- |
| Total frame bytes | Default and maximum configurable Channel envelope: **4 MiB**. | SF uses configurable frame limits and a **32-bit hard wire envelope**. Peer checking is disabled by default. A Rust Channel cannot admit larger valid native frames. |
| Header bytes | **65,535 bytes**. | Matches the native `USHORT` combined-header envelope; this is a native constraint, not an invented gap. |
| Header records | Shared encode/decode and Channel envelope validation cap records at **4,096**. | SF bounds the serialized header area, without this additional record-count ceiling. |
| Serialization | Fixed ceilings: **32,768 UTF-16 units**, **4,096 collection elements**, **depth 32**, **4 MiB object size**. | Native array counts and object extents use `ULONG`; its object stack is dynamic. The inspected code does not impose these Rust ceilings. Valid native schemas can exceed them. |
| Retained channel work | Defaults: **8 ordinary operations**, **4 reply operations**, **8 ingress slots**; receive and close waiters are separately bounded. | These are Rust admission classes, not SF transport defaults. Native transport has different controls, including a default **64 MiB send queue**. |
| Aggregate reservation | Default **512 MiB** retained-slot allowance. | Not equivalent to the native send-queue byte limit or global memory throttle. Comparing these values as the same resource would be incorrect. |
| Lifetime ID history | Default **4,096 tracked IDs**, including local hello; exhaustion retires the channel. | Native RequestTable removes completed requests. It does not impose this lifetime-history quota. |
| Unmatched replies | More than **1,024 consecutive unmatched replies** terminates the channel. | Native RequestTable returns false for unmatched/late replies without this counter. |
| Actor registrations | Maximum **32**, fixed at composition time. | Native Demuxer supports registration/unregistration in its actor map without that fixed registration-count restriction. |
| Establishment | Endpoint defaults to **30 seconds**, configurable. | Native `ConnectionOpenTimeout` defaults to **60 seconds**. |
| Channel/raw frame watchdog | Channel uses **30 seconds**; public raw limits permit no more than 30s. Started read/write budgets are absolute rather than reset on each positive byte. | Native stuck-send detection defaults to **300 seconds** and tracks submitted/completed sends. Main has no progress-reset writer opt-in. |

Rust references: [Channel limits and validation][rust-channel-limits],
[serialization defaults][rust-serialization], [message record policy][rust-message],
[lifetime history][rust-history], [unmatched replies][rust-receive],
[actor registration][rust-demux], [raw limits][rust-raw-limits],
[raw reader][rust-raw-reader], [raw writer][rust-raw-writer].

SF references: [transport configuration][sf-config],
[frame hard limit][sf-frame-header], [connection frame limits][sf-frame-limits],
[header byte limit][sf-header-limit], [duplicate header support][sf-headers],
[request table][sf-request-table], [dynamic demux registration][sf-demux],
[serializer interface and object stack][sf-serializer-interface],
[native array counts][sf-array-counts], [send completion][sf-send-completion],
[send submission][sf-send-submission].

### Message size is not total frame size

SF Naming's default `MaxMessageSize` is also **4 MiB**. That does not make the
Rust 4 MiB total-frame ceiling source-equivalent. Native
`TransportSecurity::GetInternalMaxFrameSize` adds the fixed frame header, the
maximum header area, and a body-overhead allowance to a nonzero configured
message size.

Other native contexts differ: IPC defaults to **16 MiB**, replication's
`MaxReplicationMessageSize` defaults to **50 MiB**, and peer frame checking
defaults disabled. The 32-bit hard envelope is not a promise that near-4-GiB
messages are practical or authorized.

References: [Naming setting][sf-naming-config], [frame overhead][sf-frame-overhead],
[IPC and peer settings][sf-config], [replication setting][sf-replication-config],
[native peer initialization][sf-peer-init].

## 5. Behavioral differences beyond numeric limits

### Common-header duplicates

Native `MessageHeaders::Add` permits repeated header types.
`UpdateCommonHeaders` validates and scans the records in order, overwriting
cached Action, Actor, MessageId, RelatesTo, and other common values as it goes.

The ordinary Rust Channel instead rejects repeated Actor, Action, and MessageId
records, among other owned envelope fields. Incoming RelatesTo has already been
adapted to validate every occurrence and use the final value; ExpectsReply only
has a narrowly allowed second identical occurrence. Those exceptions do not
implement the full native repeated-header behavior.

This is a stricter Rust envelope policy with a compatibility consequence, not a
different native header serialization rule. References:
[Rust envelope validation][rust-envelope], [native Add][sf-headers],
[native common-header scan][sf-common-headers].

Last-occurrence behavior describes the native inbound scan, not every header
mutation. Native `Remove` clears the corresponding cached common property
instead of recomputing it from surviving duplicates; Rust `Message::properties`
rescans its current header vector. References:
[native removal][sf-header-remove], [Rust properties][rust-properties].

### Missing trailing-field defaults

Native field readers return a non-error scope-end status for an absent trailing
field, leaving the destination's initialized value unchanged. The native field
macros accept that result. Existing Rust common-header and ordinary Serde
decoding instead request every declared field and reject the delimiter as
incorrect field metadata.

Concrete source-derived examples are a `ListenInstance` object containing
address and instance but omitting its trailing nonce, or a `FaultHeader`
containing its error code but omitting `hasFaultBody`. Native initialized values
provide the zero nonce and false body flag; Rust requires the missing fields.
This is a version-tolerant supported-schema gap, not a requirement to make
arbitrary Serde fields optional.

The difference affects shared decoding and Channel, including accepted TCP and
IPC hello decoding. Explicit Serde trailing adapters exist, but these common
schemas do not use them. The historical V1 schema-specific defaults did not
repair shared hello decoding either; see section 7.

References: [Rust common-header decoding][rust-header-read],
[Rust ListenInstance decoding][rust-listen-instance],
[Serde field access][rust-serde-fields], [hello control decoding][rust-control-instance],
[accepted TCP hello][rust-accepted], [IPC hello][rust-ipc],
[native field readers][sf-field-readers], [native field macros][sf-field-macros],
[native ListenInstance schema][sf-listen-instance],
[native GUID initialization][sf-guid-default],
[native FaultHeader schema][sf-fault-header].

### Unknown suffixes on existing common headers

Missing serializer operations can also restrict forward-compatible skipping,
even when the application never needs the unknown field. For example, append a
native `UChar` field (metadata `0x04` plus one payload byte) after the known
Action string, before the object's scope end, with corrected lengths/checksums.
Native decodes Action and consumes/preserves the unknown suffix. The Rust
default `skip_value` lacks that case and rejects it as unsupported trailing
value metadata.

This affects shared message parsing and Channel. `Header::read` constructs
`Decoder::new`; main has no selectable native unknown-data decoder. Even the
historical V1 header-count opt-in did not change that common-header decoding
path. See section 7 for its separate schema-specific mitigation.

References: [Rust header reader][rust-header-read],
[Rust properties][rust-properties], [default unknown-field skip][rust-skip],
[native Action decoding][sf-common-headers],
[native unknown-field processing][sf-unknown-fields].

### Missing Retry and Idempotent validation

Native treats Retry (`0x8006`) and Idempotent (`0x8008`) as common headers and
deserializes every occurrence during message construction. Rust omits both from
its common-property match, preserving them as opaque header records.

An otherwise valid frame with a Retry record containing the nonempty value
`[0xff]` passes Rust's record parsing without decoding that value. Native Retry
deserialization fails, marks the message invalid, and aborts the connection.
The corresponding malformed-value distinction exists for Idempotent.

This is a shared-parser difference. Channel is affected where its supplied
composition permits these records; an adapter can reject them independently.
Main has no header-profile opt-in adding native validation. Missing cached
semantic properties are an additional API limitation, distinct from
malformed-message acceptance. The historical V1 header-count opt-in did not
restore this validation either.

References: [Rust record parsing][rust-message],
[Rust common properties][rust-properties],
[native header IDs][sf-header-ids], [native common validation][sf-common-headers],
[native invalid-message abort][sf-invalid-message].

### Correlated fault-body details

Rust decodes the Fault header's `has_body` flag, but ordinary Channel reply
completion ignores it and substitutes the fixed text `"native transport Fault"`.
Native RequestTable attempts to decode `RejectFaultBody` when the flag is set;
if that body contains an error message, it overwrites the returned error with
the body's error details.

A correctly correlated, policy-admitted reply with a nonzero Fault code,
`hasFaultBody=true`, and a valid explanatory body therefore loses diagnostic
text in Rust. If the body's error value differs from the header's, the returned
code can differ too.

The gap is in **Channel RPC completion**, not shared raw byte preservation.
No existing Channel opt-in restores this native fault-body processing.

References: [Rust Fault properties][rust-properties],
[Rust fault completion][rust-fault-completion],
[native request completion][sf-request-table],
[native RejectFaultBody][sf-reject-fault-body].

### Unknown ingress and late replies

Native Demuxer logs and drops messages for unknown actors. Rust actor selection
returns a Protocol error, which propagates out of the channel receive loop.
The unmatched-reply cutoff similarly changes a drop into terminal failure.
Unsupported role/security/reconnect controls also terminate the Rust Channel.
Common-control handling now lives in the private engine; actor selection and
ordinary-reply correlation remain Channel policy.

These fail-closed profile decisions must not be described as directly ported
native behavior. References: [Rust demux][rust-demux],
[Rust receive policy][rust-receive], [engine controls][rust-controls],
[native actor drop][sf-actor-drop],
[native unmatched reply][sf-request-table].

### ID generation and retention

Native MessageId uses a process GUID and a process-wide counter converted to a
32-bit index. The Rust Channel retains used IDs for the connection lifetime and
rejects generated-counter exhaustion rather than wrapping.

The history guards against transport-attempt collisions, but it also creates a
finite connection lifetime absent from native RequestTable semantics. Increasing
the history limit only delays that difference; it does not remove it.
References: [native MessageId][sf-message-id], [Rust ID submission][rust-submit],
[Rust history][rust-history].

### Graceful close versus abort

The ordinary Channel close path invokes abort, clearing accepted-but-unstarted
queued work and stopping receive. Its joined-suffix guarantee applies to a frame
that has already started. Native `TcpConnection::Close` distinguishes graceful
close from abort: queued/pending sends can drain before send-side shutdown and
the close-drain timeout.

For example, frame A has started and accepted frame B remains queued when close
is requested while the peer remains responsive. Rust completes A's started
suffix but removes B; native graceful close can send both, subject to expiration
and its drain policy.

This is an intentional ordinary Channel lifecycle-profile difference, including
IPC adaptations, not evidence of lost buffer ownership. The private engine's
first terminal transition clears queued jobs and retires Channel policy; its
joined writer preserves an already-started suffix unless the owner explicitly
disposes the entire socket. Neither raw joining nor that engine is a complete
native graceful-close state machine. The separate historical V1 draining
implementation is not available on main. Local write completion does not prove
remote application execution.

References: [Rust terminal transition][rust-terminal],
[Rust abort/disposal][rust-abort], [Rust close][rust-close],
[engine joined lifetime][rust-engine-drive], [native close/drain][sf-close],
[native send completion][sf-send-completion].

## 6. Unsupported coverage and performance differences

The channel remains Linux-only and uses clear connections, including supported
non-loopback TCP. It does not implement the full native security negotiation,
TLS/claims, session renewal, reconnect, failover, or multi-target/pool service.
Actor36 application-service `IFabricTransport` is not supplied by this channel.
Accepting an externally supplied TCP socket does not provide a native listener
manager, name resolver, pooled targets, or security. The historical V1 backend
supplies some of that separate machinery, but it is not on main and does not
establish complete native equivalence.

Native IPC configures keepalive and reconnects on connection faults. The Rust
IPC/channel profile has no automatic reconnect or native IPC recovery policy.
References: [Rust channel scope][rust-channel],
[clear-frame restriction][rust-frame-validation],
[native IPC setup and reconnect][sf-ipc].

Duplex ingress support is not notification-origination support. Native
`BeginNotification` adds `UncorrelatedReplyHeader` and uses request machinery to
send a notification and await its correlated acknowledgment. Rust can receive
and reply to special duplex ingress, but RPC, raw/reserved sends, and consuming
reply submissions reject originating an `uncorrelated_reply` message. An
accepted server-side Channel therefore cannot originate that native request.

This is an intentional Channel authority/API boundary, including accepted TCP
and IPC. Shared encoding can represent the headers, but an owner would still
need notification correlation and lifetime machinery. A separately authorized
capability would be needed; removing existing raw-send guards is not an
equivalent implementation.
References: [Rust submission guards][rust-submission-guards],
[Rust duplex policy][rust-duplex], [native BeginNotification][sf-notification],
[native duplex handling][sf-duplex].

The low-level Rust serializer supports selected native scalar/object/array
operations, not all `FabricSerializableStream` methods. For example, it has no
dedicated BoolArray, Int32Array, or GuidArray API. The Serde layer further
requires explicit positional adapters: bare sequences, maps, Option, enums,
and several scalar shapes are unsupported as ordinary Serde mappings.
Main's low-level/Serde readers skip supported unknown tails rather than
retaining them; they lack the checkpoint's `object_with_unknown` and
`unknown_scopes` preservation APIs. A missing serializer API does not imply that
every supported transport message requires it.
References: [Rust serializer][rust-serialization],
[object/scope reader][rust-object-reader], [Serde contract][rust-serde],
[native serializer methods][sf-serializer-interface].

Main assembles contiguous frame buffers: `Message::to_frame` clones the body,
`Frame::encode` appends headers/body into another buffer, and the raw writer
writes that encoded frame. It has neither the checkpoint's `SegmentedFrame`
borrowing API nor native multi-message batching and Linux `writev`. The
checkpoint avoids body flattening but still does not establish native send-path
performance equivalence. References: [Rust message conversion][rust-message],
[Rust frame encoding][rust-frame], [Rust writer][rust-raw-writer],
[native batch preparation][sf-send-buffer], [native writev][sf-writev].

## 7. Historical V1 mitigations: unmerged, not on main

The separate `feature/sf-v1-class-port` checkpoint
`286f675ea5f8e21758b11ccf2618259806a9d3b9` contains
`crates/libs/fube-data/src/v1` and changes to shared transport that are **absent
from the main baseline**. They are selected opt-ins, not fixes already delivered
to main or a global replacement of Channel behavior.

In that checkpoint, V1 peers bypass GatewayChannel: their transport facade,
listener/target/queue machinery, and managed ownership are in `fube-data/src/v1`,
using selected shared framing and byte-I/O APIs.

| Checkpoint-only addition | V1 consumer in that checkpoint | Boundary |
| --- | --- | --- |
| Configurable serializer limits, native-width unsigned decoding, native object handling, raw UTF-16, unknown-scope preservation | `v1/wire.rs`; schemas such as ReplicationFromHeader and InduceFaultMessageBody | Applied by the V1 `Wire` implementation. Ordinary `Encoder::new`, `Decoder::new`, and Serde entry points keep their defaults. |
| `FrameLimit` / `SizedLimits` | TCP send preparation, receive loop, and writer setup | Larger/native envelopes do not widen `ChannelLimits` validation. |
| `HeaderProfile::Native` | TCP hello/send header assembly and incoming frame parsing | Removes the extra record-count restriction for that path; does not change ordinary Channel envelope validation or select native serialization for every header value. |
| `prepare_hello` / `decode_hello` and watchdog-free receive | V1 connection creation and receive/control processing | The V1 owner queues hello and governs connection/receive policy; ordinary Channel establishment remains distinct. |
| Progress-reset segmented writer | V1 queued-frame sender | V1 uses native-derived 60s open / 300s send defaults, not the legacy raw watchdog profile. |
| Linux resolver, interface, listener, and expiration helpers | V1 listener/dial/admission paths | Preserve selected native PAL/error/clock behavior using `libc`; do not supply the entire native platform or configuration service. |
| Queue draining, socket shutdown, and close timers | V1 peer connection lifecycle | Separate from Channel close; does not add native graceful draining to ordinary Channel. |

Even there, common-header decoding still constructs the default Decoder, and
hello decoding uses shared `ListenInstance::decode`. Its header-count profile
does not repair common-header unknown suffixes, missing defaults, or
Retry/Idempotent validation. The V1 `trailing` helper defaults selected
replication fields, not every native schema.

Checkpoint source references below are **commit-scoped**, not paths available
on main. The base is `crates/libs/fube-data/src/v1/`:

| Evidence at `286f675` | Source range |
| --- | --- |
| Facade/peer construction | `replication_transport.rs:167-215` |
| Codec policy and unknown-scope preservation | `wire.rs:12-49` |
| Selected trailing-field defaults | `wire.rs:174-184` |
| Native-derived timeout defaults / hello queue | `replication_transport_tcp.rs:43-103,312-333` |
| Send preparation / native expiration | `replication_transport_tcp.rs:1070-1180` |
| Receive admission / message transfer | `replication_transport_tcp.rs:1758-1849` |
| Segmented send / shutdown and close timers | `replication_transport_tcp.rs:1885-1976,2023-2068` |
| Failed-hello/TargetDown handling | `replication_transport_tcp.rs:2185-2197` |

Shared checkpoint-only evidence is in
`crates/libs/fube-transport/src/serialization.rs:149-160,428-438,804-832`,
`raw_session.rs:299-304,621-706`, and `connection.rs:39-323`.
These can be read locally without switching branches or assuming the checkpoint
has been published:

```bash
git show 286f675ea5f8e21758b11ccf2618259806a9d3b9:crates/libs/fube-data/src/v1/wire.rs
git show 286f675ea5f8e21758b11ccf2618259806a9d3b9:docs/runtime/v1-port/p19.md
```

The recorded P19 status is **unfinished/unaccepted**, with its historical
regression timeout unresolved and workflow archived-incomplete. The P19 status
document exists at that checkpoint, not on main. None of these mitigations is
phase acceptance, a timeout-risk waiver, or complete native equivalence.

## 8. Implications for a source-faithful port

Keep native format widths, decoding rules, header occurrence semantics,
configuration defaults, and state transitions distinct from host admission.
Finite memory/operation protection still belongs in an explicit owner/profile
policy, with clear resource errors and preserved accepted-operation lifetimes.

Do not silently disable protection or merely raise constants to claim fidelity.
Header scanning/mutation/validation, schema defaults and unknown suffixes,
fault-body completion, graceful close, terminal/drop decisions, ID lifetime,
notification capabilities, recovery, serializer coverage, and batching need
their own native-source mappings and documented adaptations. Missing platform
or security features must remain explicit unsupported scope until implemented
and qualified.

This document records main's gaps and separate historical mitigations; it does
not authorize resuming the archived V1 workflow, changing production defaults,
or exposing unsecured networking.

[rust-revision]: https://github.com/youyuanwu/fube/tree/2b9e8436bc120b63a5dd01bcda6ba0bc3c45b2fb
[sf-revision]: https://github.com/microsoft/service-fabric/tree/51a7703a1f83749a9234d3ceb4296d8622e42cca
[sf-config]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TransportConfig.h#L30-L157
[sf-frame-header]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpFrameHeader.h#L19-L38
[sf-frame-limits]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L387-L479
[sf-header-limit]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageHeaders.h#L337-L340
[sf-headers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageHeaders.h#L86-L91
[sf-request-table]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/RequestTable.cpp#L17-L70
[sf-demux]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/DemuxerT.h#L136-L185
[sf-serializer-interface]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/shared/serialization/inc/FabricSerializableStream.h#L26-L289
[sf-array-counts]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1986-L2100
[sf-send-completion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L1783-L1818
[sf-send-submission]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L2599-L2622
[sf-naming-config]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/ServiceModel/ServiceModelConfig.h#L69-L74
[sf-frame-overhead]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TransportSecurity.cpp#L233-L260
[sf-replication-config]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/ServiceModel/reliability/replication/REConfigMacros.h#L136-L147
[sf-peer-init]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L200-L203
[sf-common-headers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageHeaders.cpp#L765-L859
[sf-header-remove]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageHeaders.cpp#L679-L738
[sf-field-readers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1941-L1963
[sf-field-macros]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Common/FabricSerializationHelper.h#L256-L380
[sf-listen-instance]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/ListenInstance.h#L10-L42
[sf-guid-default]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Common/Guid.h#L28
[sf-fault-header]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/FaultHeader.h#L13-L28
[sf-unknown-fields]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1631-L1755
[sf-header-ids]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageHeaderId.h#L24-L32
[sf-invalid-message]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L1370-L1374
[sf-reject-fault-body]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/RejectFaultBody.h#L28-L38
[sf-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L639-L747
[sf-notification]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/requestreply.cpp#L263-L271
[sf-duplex]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L75
[sf-actor-drop]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/DemuxerT.h#L253-L272
[sf-message-id]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/MessageId.cpp#L10-L17
[sf-ipc]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/IpcClient.cpp#L63-L161
[sf-send-buffer]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpSendBuffer.cpp#L306-L350
[sf-writev]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L2381-L2422
[rust-lib]: ../../crates/libs/fube-transport/src/lib.rs#L1-L29
[rust-endpoint]: ../../crates/libs/fube-transport/src/endpoint.rs#L18-L107
[rust-accepted]: ../../crates/libs/fube-transport/src/accepted_channel.rs#L18-L95
[rust-ipc]: ../../crates/libs/fube-transport/src/native_ipc.rs#L24-L116
[rust-channel]: ../../crates/libs/fube-transport/src/native_channel.rs#L1-L47
[rust-engine]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L1-L223
[rust-controls]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L307-L332
[rust-control-instance]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L376-L392
[rust-channel-limits]: ../../crates/libs/fube-transport/src/native_channel.rs#L80-L142
[rust-serialization]: ../../crates/libs/fubst/src/lib.rs#L11-L101
[rust-message]: ../../crates/libs/fube-transport/src/message.rs#L148-L206
[rust-header-read]: ../../crates/libs/fube-transport/src/message.rs#L116-L121
[rust-listen-instance]: ../../crates/libs/fube-transport/src/message.rs#L259-L275
[rust-properties]: ../../crates/libs/fube-transport/src/message.rs#L208-L241
[rust-serde-fields]: ../../crates/libs/fubst/src/serde.rs#L1103-L1127
[rust-object-reader]: ../../crates/libs/fubst/src/lib.rs#L515-L576
[rust-skip]: ../../crates/libs/fubst/src/lib.rs#L578-L614
[rust-history]: ../../crates/libs/fube-transport/src/native_channel.rs#L456-L465
[rust-submit]: ../../crates/libs/fube-transport/src/native_channel.rs#L1067-L1111
[rust-envelope]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L394-L448
[rust-receive]: ../../crates/libs/fube-transport/src/native_channel.rs#L1341-L1485
[rust-fault-completion]: ../../crates/libs/fube-transport/src/native_channel.rs#L1457-L1462
[rust-terminal]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L142-L160
[rust-abort]: ../../crates/libs/fube-transport/src/native_channel.rs#L725-L757
[rust-close]: ../../crates/libs/fube-transport/src/native_channel.rs#L1223-L1229
[rust-engine-drive]: ../../crates/libs/fube-transport/src/raw_session/engine.rs#L343-L369
[rust-submission-guards]: ../../crates/libs/fube-transport/src/native_channel.rs#L1021-L1052
[rust-demux]: ../../crates/libs/fube-transport/src/native_demux.rs#L42-L94
[rust-duplex]: ../../crates/libs/fube-transport/src/native_demux.rs#L97-L107
[rust-raw-limits]: ../../crates/libs/fube-transport/src/raw_session.rs#L43-L60
[rust-raw-reader]: ../../crates/libs/fube-transport/src/raw_session.rs#L74-L125
[rust-raw-writer]: ../../crates/libs/fube-transport/src/raw_session.rs#L235-L289
[rust-frame]: ../../crates/libs/fube-transport/src/frame.rs#L13-L37
[rust-frame-validation]: ../../crates/libs/fube-transport/src/frame.rs#L60-L80
[rust-serde]: ../../crates/libs/fubst/src/serde.rs#L1-L25
