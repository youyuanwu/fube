# RA source-baseline codec oracle

This is an explicit producer/consumer gate, not a running native FM/RA test.
Native revision: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
See [the catalog](../../docs/cluster/NativeRaProtocol.md) for all contracts.

## What actually runs

The producer compiles the **unmodified** native `FabricStream.cpp`,
`FabricSerializable.cpp`, `FabricSerializableStream.cpp`,
`FabricSerializationTypes.cpp`, `ByteCompressor.h` and
`Common/FabricSerializationHelper.h`. It mechanically extracts field macros
and member declarations for 75 schema types/237 members, including the selected
Add, inherited DeleteReplica, reply, ReplicaUp/Reply and service-type
notification request/reply bodies, NodeUp/Ack's complete schema closure and the
six-field Federation PToP header value, node controls, status, complete load graph
and generation-handshake reply/update bodies.
The native helper macros generate their actual Write/Read methods. Nested
fields are not inferred from Rust or encoded as handwritten wire bytes.

`native/ktl.h` is a test-only allocator/container/raw seekable byte-storage
support layer, **not KTL/PAL**. It contains no Fabric metadata, compression,
scope or wire decoding algorithm. Schema adapters omit unrelated production
methods/dependencies and expose fields for fixture construction/comparison.
`scaling.inc` supplies fixture polymorphic activation with pinned kind tags.
The actual stream and field macros retain native encoding/decoding behavior.
Clang MS extensions/delayed templates and a two-byte WCHAR profile are explicit.
The fixture avoids system-width wchar string functions under `-fshort-wchar`.
Native map helpers necessarily copy strings through `char_traits`. The platform
adapter provides two-byte `wmemcpy`/`wmemmove`/`wmemset` raw-memory operations
instead of the host libc's four-byte wchar ABI. These are storage adapters,
not serialization algorithms; native field macros and streams are unmodified.

This is **source-baseline native-stream conformance with schema adapters**,
not the complete production C++ RA classes, KTL/PAL qualification, a native
handler trace test, or interoperability with the SF 11.8.121.4 qualification
binaries. Neither runtime nor Federation compatibility follows from it.

The C++ producer independently constructs expected values and emits 116 inputs.
The existing 36 cases and their contracts are retained:
default/rich AddInstance, default/error AddInstanceReply, default/rich Remove
(force false/true), default/rich removal reply, explicit-empty/rich ReplicaUp
(both nonempty lists, all last/fromFMM combinations), FM/FMM generation,
actor/action/priority headers. The seven notification cases add empty/two-info
requests, empty/one-info partial replies, both literal actions and priority=false.
The notification factories target normal FM=11 for the request without
GenerationHeader and RA=12 for the reply with the actual FM generation;
both use false priority. The Add/removal replies share a native body type.
Rust decodes and checks every field, then encodes
independently constructed expected values; the actual native decoder compares
all fields, not just bytes. Native object padding need not be canonical.
Nondefault cases cover GUID/raw NodeId, high-bit instances, negative LSN/epochs,
UTF-16/surrogates, enums/errors, all service collections and null/Invalid/both
derived scaling variants. Rust rejects every proper prefix of rich body
fixtures including both rich force bodies and the rich/partial notification
bodies; native rejects 31 truncated bodies, including NodeUp, Ack, PToP
and the distinct DeleteReplica/Reply fixtures.
Non-polymorphic Delete inheritance invokes base rich traversal and native
base/derived scopes, not a flattened or nested replacement. ReplicaUp's empty
native constructor leaves scalar booleans uninitialized; the empty fixture
explicitly initializes them, and is not labeled a native-default guarantee.
ReplicaInfo's rich fixture initializes its intermediate role explicitly.

Six AddPrimary cases retain the original 45 and add default/rich request and
reply bodies plus both literal actions. The native body types remain
ReplicaMessageBody and ReplicaReplyMessageBody, so schema counts stay
59 types/199 fields at that stage. Rust checks production AddPrimary factories/decoders
against native-produced action/actor/priority/generation bytes, including
FM/FMM reply-owner asymmetry and proper-prefix rejection. Header/schema support
does not enable FMM execution or claim full native AddPrimary handler parity.

### Normal DeleteReplica/Reply extension

Seven cases retain all prior 51 inputs:
`default-delete-replica`, `rich-delete-replica`,
`default-delete-replica-reply`, `rich-delete-replica-reply-0`,
`rich-delete-replica-reply--2147017398`, `action-DeleteReplica` and
`action-DeleteReplicaReply`. The rich request explicitly selects force=false,
volatile stateful service and 1/1 sizes, Primary/Ready/up replica fields. Rich
reply fixtures exercise None/None/Dropped/down fields and success/StaleRequest
primitive error values. The dropped replica is freshly initialized as in native
`ReplicaDescription.cpp:93–101`: invalid package version/LSNs and empty endpoints,
with only identity, Dropped state and down flag changed. Other nondefault fields remain synthetic schema stress
values, not proof of an executable FM lifecycle or a captured runtime reply.
Existing RemoveInstance force=false/true fixtures and every original adapter
remain unchanged.

Native [DeleteReplicaMessageBody.h:10–55](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/DeleteReplicaMessageBody.h#L10-L55)
inherits ReplicaMessageBody (`fudesc_`, `replica_`, `service_`) and adds only
`isForce_`. The request alias preserves those native base/derived scopes.
The reply alias retains ReplicaReplyMessageBody (`fudesc_`, `replica_`,
`errorCode_`). No new schema type was invented in that extension: its counts
remained 59 types/199 fields.
Replica role and state values come from the pinned common `ReplicaRole.h`
and `ReplicaStates.h`, not declaration-order guesses in Rust.

[RSMessage.cpp:86–87,288–299](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RSMessage.cpp#L86-L87)
and `RSMessage.h:260–267` pin literal actions, destination actor RA=12 for
DeleteReplica and FM=11 for DeleteReplicaReply, with high priority=true.
`fm/SendMessageAction.cpp:28–47` adds the request's actual FM generation/owner
header. `ra/Communication.FMTransport.h:45–63,118–135` and
`ra/Infrastructure.MessageUtility.h:15–23` do not add a reply generation;
`FederationWrapper.cpp:162–195` rewrites its destination actor to FMM=10 when
sent to that owner. These native source files are included in the manifest's
input hashes.

Rust exercises the production `delete_replica_message`/`decode_delete_replica`
and `delete_replica_reply_message`/`decode_delete_replica_reply` with
native-produced actor/action/priority/generation bytes, both generation-owner
flags and both reply owners. Factory comparisons verify actor/action/priority
and generation presence; decoding verifies the generation's fields.
Cross-action decoding rejects the shared-schema RemoveInstance and AddPrimary
actions. Rust rejects every proper prefix of all five new bodies; the actual
native decoder executes two additional truncated-body rejections. This is
wire/schema evidence only, not original handler execution, native FM
interoperability, or authorization of force deletion, recreation,
reconfiguration, FMM execution or recovery.

### Stage-two node control/load and endpoint codecs

All original 58 cases, 59 types/199 fields, 62 files and eleven native
truncation rejections remain preserved. This extension adds **43 inputs**:
initialized empty and rich roots for all thirteen actions; their thirteen
literal action headers; two signed high-bit/FMM activation request values;
legacy-only and modern-only ReportLoad roots. Stage-two totals were **101 cases,
73 types/233 fields, 105 bundle files and 26 native truncation rejections**.
Every proper prefix of every new body fixture is rejected by Rust.

The new roots are NodeActivateRequest/Reply, NodeDeactivateRequest/Reply,
NodeUpdateServiceRequest/Reply, ServiceTypeEnabled/Disabled and both replies,
ReplicaEndpointUpdated/Reply and ReportLoad. All thirteen pinned RSMessage
factory defaults are **priority=false**, including endpoint messages.
Incoming FM→RA requests and type-status/endpoint replies have the sender's
actual GenerationHeader. RA→FM node replies, type-status, endpoint and load
sends do not invent one. Native-supported node-reply/endpoint owners retain
FM=11 and FMM=10; status/load outbound paths remain ordinary FM only.
Rust exercises each production decoder and factory against native-produced
literal action/actor/priority/generation fixtures. Its separately constructed
bodies are encoded, compared with production-factory output and re-decoded by
the actual native stream. No private runtime/data-engine type is used.

Both distinct native activation request classes and both reply classes are
extracted, despite their shared Rust wire shapes. Empty fixtures explicitly
initialize unspecified native scalar members; they are not claims about native
empty-constructor guarantees. Nondefault cases exercise negative signed
sequences, high-bit uint64 service/status sequences, multiple full service types
and complete nondefault ServiceDescription (including scaling policies).
Endpoint fixtures retain complete native ReplicaMessageBody/ReplicaReplyMessageBody.

The load closure mechanically retains:

* ReportLoadMessageBody: legacy reports, modern descriptions, primitive StopwatchTime.
* ReportLoadInfo: FailoverUnitId, role, LoadMetric vector; **not isStateful_**.
* LoadMetric: name, uint32 value.
* LoadMetricStats: name, last value, count, both doubles, primitive last-report time.
* LoadOrMoveCostDescription: primitive Guid, service, stateful, primary and secondary
  vectors and native `map<NodeId,vector<LoadMetricStats>>`; **not isReset_**.

The C++ fixture uses the actual native map/ArrayPair helper, not a handwritten
encoding. Each rich modern report has one map entry with raw 16-byte NodeId and
two stats values; both report vectors and primary/secondary vectors are nonempty.
The singleton comparator adapter limitation described below remains explicit:
map ordering/duplicate semantics are not qualified. Modern-only production may
emit empty legacy reports without deleting the complete legacy schema.

Codec evidence does not claim native handler/full FM parity. Bounded execution
is separate execution work: **one FT per real Node**, multi-FT RA model/store
coordination and ordinary FM production only. **Live endpoint publication is
deferred to stage 5**: pinned EndpointPublishState derives its publication
timeout from reconfiguration duration and asserts pending publication requires
active reconfiguration. Encoding or acknowledging an endpoint body cannot
substitute for those state-machine preconditions.

### Generation-handshake-only codecs

Fifteen inputs retain all prior 101 cases and increase the current wire gate to
**116 cases, 75 types/237 fields, 120 bundle files and 31 native truncation
rejections**. This is generation-handshake only, not LFUM upload, complete stage
three, rebuild completion or native runtime interoperability. Historical bundle
receipts are unchanged; a new source-bound development receipt does not relabel
old development receipts as clean-head production.

The four literal action fixtures are GenerationProposal, GenerationProposalReply,
GenerationUpdate and GenerationUpdateReject. Source-extracted RSMessage factory
definitions are validated and recorded in the manifest: all default to
priority=true; proposal/update target RA=12 and reply/reject use FM=11.
GenerationProposal has exactly zero body bytes, not a serialized empty object.
Its FM/FMM owner flag remains in GenerationHeader. Native
`fm/RebuildContext.cpp:85–88` uses BroadcastRequest and
`ReliabilitySubsystem.cpp:318–386` preprocesses it and replies through the
original request context. Therefore the reply does **not** go through
SendToFMM and keeps Actor FM=11 even for an FMM proposal. It has no
GenerationHeader. Correlation IDs and the consuming reply context belong to
the transport composition, not the body factory.

The native reply body is `(GenerationNumber proposedGeneration_,
GenerationNumber currentGeneration_, ErrorCode error_)`. ErrorCode retains the
native primitive LONG64 schema, not a nested object or diagnostic string.
Default, rich success/two nonzero errors and a tied signed-generation/high-bit
NodeId fixture run in both directions. The high owner fixture deliberately has
the lower high limb but the larger low limb; a separate reject fixture compares
equal-high owners differing only in low limb. Rust exercises production
`older_than` on the decoded values. Actual original-native comparator execution
remains the separate 64-pair helper gate below; wire adapters do not pretend to
execute native generation algorithms.

GenerationUpdate contains exactly one Epoch object, whose wire field order is
**configuration, data loss**, opposite its native constructor arguments.
Initialized default and distinct `INT64_MIN+17`/`INT64_MAX-29` fields are checked.
The ordinary-FM body is mandatory; the native FMM bodyless branch is rejected,
including an attempted full-body FMM update. It is not a successful stub.
GenerationUpdateReject directly serializes GenerationNumber, without a wrapper
or GenerationHeader. Its selected destination is ordinary FM=11; native
`GenerationStateManager.cpp:272–282` uses SendToFMM/Actor=10 for the excluded
FMM branch. Three rejection-body fixtures cover default and high/tied owners.

Every new nonempty fixture rejects all proper prefixes in Rust, and native
executes five additional truncated-body rejections. Production decoder tests
reject malformed/missing/duplicate headers, cross-actions, wrong actor,
unexpected reply/reject generation headers, bodyless FM update, wrong body
shape and trailing bytes. Priority is parsed as valid native factory metadata,
not treated as proof of negotiation success. Receive-generation progress is
not send-generation advancement: LFUMUpload/Reply and pending-upload completion
remain separate, unselected work.

### Bootstrap and source identity header extension

Nine cases add fresh/rich NodeUp, fresh/rich NodeUpAck, both literal actions,
RS actor=13, direct PToP and rich PToP. The fresh fixtures explicitly initialize
empty inventories, `anyReplicaFound=false`, the static native default fabric
version `1.0.960.0:__default__:0`, actual node metadata and activation sequence 7.
They are not assertions about uninitialized native constructor fields.

The rich NodeUp has both fault-domain URI objects, all three nonempty node maps,
both package inventory entries and `anyReplicaFound=true`. The rich Ack has two
upgrades with every application/package upgrade, removal, resource-governance,
container-image and network field populated; activation is false and the sequence
is negative. Native field equality checks the complete content in both directions.
Rust also invokes the production bounded NodeUp/Ack decoders and factories with
native-produced actor/action/priority/generation header values: fresh profiles
are accepted, rich unsupported profiles are rejected **after retaining all
decoded fields**. Every proper prefix of all four bootstrap bodies and rich
PToP is rejected by Rust. This catches omitted/reordered fields, collapsed Uri
or version objects, false empty inventories and incorrect collection scopes.

The actual native map helper serializes `std::map` through an object array of
`ArrayPair` objects, each with its own key/value scope. Native maps and sets
remain `std::map`/`std::set`, not handwritten pair codecs or replacement bytes.
String vectors (including sets via the native helper) instead use a UInt32
count followed by strings. Maps and sets use **one entry** per rich collection:
this exercises their nonempty wire scopes without invoking the host's
four-byte-wchar comparator under `-fshort-wchar`. The structured package-key
adapter throws if comparison is attempted; no map/set ordering, duplicate-key
semantics or native identifier-comparison qualification is claimed here.

PToP values exercise distinct source/destination NodeInstances with high-bit
incarnations, actor/ring fields and both exact-instance boolean values.
`MessageHeaderId.h`'s `PToP=0x8013` and `PToPActor.h`'s `Direct=0` are extracted
and required by the consumer manifest contract. The Rust PToP type is an
explicit **schema-only adapter**, not execution of the cluster tool's private
Link implementation or its deployment/session admission. Factory source files
and native constants are included in native input hashes. None of these cases
executes native NodeUp handlers, an FM/RA runtime, a deployed SF release or a
Federation transport; those remain separate evidence classes.

## Explicit production

Requires Linux x86-64, Python 3.11+, Rust workspace toolchain, Clang with MS
extensions, and the clean pinned native source. No full SF build/installation.
Compile the Rust example with the current source identity; the producer
rejects an unbound/stale executable:

```sh
# Supply exclusive task-owned RAM target/scratch roots and a fresh bundle path.
# Measure fit first; keep the task's existing build/evidence/security caps.
: "${CARGO_TARGET_DIR:?explicit owned Cargo target root required}"
: "${TMPDIR:?explicit owned scratch root required}"
: "${RA_BUNDLE:?fresh owned bundle path required}"
FUBE_RA_SOURCE_DIGEST=$(python3 -c 'from tests.ra_protocol.producer import source_digest; print(source_digest()[0])') \
  cargo build --locked -p fube-ra --example ra_conformance
python3 -m tests.ra_protocol.producer \
  --native /path/to/clean/service-fabric \
  --out "$RA_BUNDLE" --cxx /path/to/clang++ \
  --rust "$CARGO_TARGET_DIR/debug/examples/ra_conformance"
```

Producer compilation is intentional, never a consumer fallback. It records
compiler/flags, native input hashes, generated schema, repository HEAD and
source-input hashes, and SHA256 for the complete prebuilt inventory.
Source identity includes the RA/store/transport Rust sources, manifests/lockfile
and producer/consumer/schema/platform inputs. HEAD identifies the source base;
source_digest also binds dirty tracked/new work during development.

## Compiler-free acceptance

Transfer the completed bundle and independently expected HEAD/source digest.
No native source, SF install, compiler, Rust/Cargo or package installation:

```sh
env PATH=/nonexistent /usr/bin/python3 -m tests.ra_protocol.consumer \
  --bundle /absolute/bundle --out /absolute/new/rust-output \
  --expected-head COMMIT --expected-source SOURCE_DIGEST
```

The bundle has exactly 116 `native/*.bin`, `schema.h`, `native-oracle`,
`ra-conformance` and `manifest.json` (120 files total). No other files are
accepted. The consumer verifies exact coverage/type/member counts, regular files, hashes,
provenance and both compiled source identities **before** execution. Missing,
modified, wrong-source and symlink inputs fail; it imports no producer and
cannot generate replacement inputs. The manifest is integrity/provenance
metadata, not a cryptographic signature or hostile-producer trust system.
Offline validator tests deliberately use model bytes and make no native
production/execution claim:

```sh
python3 -m unittest tests.offline.cluster.test_ra_protocol_inputs -v
python3 -m unittest tests.ra_protocol.test_stage2 -v
python3 -m unittest tests.ra_protocol.test_generation -v
cargo test --locked -p fube-ra
```

Current-opening production must use a fresh bundle bound to the current
implementation HEAD/source digest. `build/ra-native-final-v2` was produced for
previous work and is not this slice's production. The producer's HEAD names the
commit base and source digest also binds dirty sources during development;
final delivery produces again at the clean reviewed implementation head.

## Separate actual-native helper evidence

The helper producer is **not part of, and does not change**, the 116-case,
75-type/237-field wire bundle or its compiler-free consumer. It compiles
unmodified `pal/wc16.c`, `RetryableErrorState.cpp` and
`RetryableErrorStateThresholdCollection.cpp`. Identifier, `LargeInteger`,
`NodeId`, `GenerationNumber`, `CheckGenerationHeader` and `GetGenerationState`
methods are mechanically extracted verbatim from the same clean pinned source.
Generated native source stays in the output directory and is never committed.

```sh
mkdir -p build/ra-helper-tmp
export TMPDIR="$PWD/build/ra-helper-tmp"
FUBE_RA_SOURCE_DIGEST=$(python3 -c 'from tests.ra_protocol.producer import source_digest; print(source_digest()[0])') \
  cargo build --locked -p fube-ra --example ra_helper_conformance
python3 -m tests.ra_protocol.helper_producer \
  --native /path/to/clean/service-fabric \
  --out "$PWD/build/ra-helper-evidence" --cxx /path/to/clang \
  --rust "$PWD/target/debug/examples/ra_helper_conformance"
```

Lightweight guard tests: `python3 -m unittest tests.ra_protocol.test_helper_producer`.

For the locally unpacked Clang 18 toolchain, set
`LD_LIBRARY_PATH="$PWD/build/ra-producer-tools/usr/lib/x86_64-linux-gnu"` and
use `--cxx "$PWD/build/ra-producer-tools/usr/lib/llvm-18/bin/clang"`.
`--rust` is optional: omission produces **native-only** evidence and an empty
`comparisons` object, not a Rust conformance pass. It never implicitly builds
Rust. The explicit Rust executable must report the current source identity.
Helper producer/adapters and the Rust example are included in source identity.
The output manifest records HEAD, current source digest/input hashes, native
input/method hashes, compiler version, exact compile/run argv/cwd, environment,
adapter disclosures and every generated/prebuilt file hash.

With `--rust`, both programs independently construct cases and compare exact
`trace.txt` and `upper-units.bin` hashes. Coverage includes all 65,536 UTF-16
units under native startup **C locale**, 196 string-name pairs, 144 structured
identifier pairs (number priority, ad-hoc package omission, signed ordering,
embedded NUL and non-transitive malformed raw fields), and five retry families:
Open, registration-at-Open, Close, Delete and registration-at-Drop.
Policies cover live signed/zero/negative threshold updates, equality precedence,
lowered drop, last-before-drop, current-threshold clear, shared registration
restart/error and a deterministic mixed-property read. Native raw action values
are retained separately in `native/raw-actions.txt`; Restart/Error are mapped to
None **only for comparison with the enabled Rust handler-consumed action**.
The Rust side calls production `native_upper_unit`, `compare_service_types`,
`NativeConfig` and `RetryPolicy`, not local algorithm replicas.

Generation coverage compares 64 signed-generation/unsigned-owner pairs using
actual native owner and generation methods versus Rust `older_than`. Eight
normal-FM stale/equal/new generation headers run the actual native
`CheckGenerationHeader` method and the Rust Executor's notification-reply
dispatch. UTF-16 storage/type shells, mutable config transport fields, no-op
locking/tracing and raw-action projection are explicit adapters. This is
helper/selected-dispatch evidence, **not** complete original handler execution,
a native RA/Hosting/Federation runtime, FMM support, config-loader qualification,
arbitrary process locales, or wire compatibility beyond the separate wire gate.
