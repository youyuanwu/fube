# RA source-baseline codec oracle

This is an explicit producer/consumer gate, not a running native FM/RA test.
Native revision: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
See [the catalog](../../docs/cluster/NativeRaProtocol.md) for all contracts.

## What actually runs

The producer compiles the **unmodified** native `FabricStream.cpp`,
`FabricSerializable.cpp`, `FabricSerializableStream.cpp`,
`FabricSerializationTypes.cpp`, `ByteCompressor.h` and
`Common/FabricSerializationHelper.h`. It mechanically extracts field macros
and member declarations for 77 schema types/240 members, including the selected
Add, inherited DeleteReplica, reply, ReplicaUp/Reply and service-type
notification request/reply bodies, NodeUp/Ack's complete schema closure and the
six-field Federation PToP header value, node controls, status, complete load graph
and generation-handshake reply/update bodies, RAReplicaMessageBody and nested
ReplicaDeactivationInfo.
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

The C++ producer independently constructs expected values and emits 179 inputs.
Configured failover adds default/rich ChangeConfiguration and legacy
ChangeConfigurationReply bodies plus both literal action headers.
The request reuses ConfigurationMessageBody; the reply adds the two-field
FailoverUnitReplyMessageBody. Both directions use source-extracted actors and
priority. The deprecated reply has codec coverage but remains explicitly
rejected by RA, never a reconfiguration-completion gate.
Bounded three-member Primary failover adds a three-element non-force Deactivate
body, independently checked in both native/Rust directions and for truncation.
Healthy three-member admission adds three-element-vector cases for
DoReconfiguration, Activate and DoReconfigurationReply. A separately initialized
third descriptor is appended to the rich native/Rust bodies; these are synthetic
codec fixtures, not a native handler trace or live membership proof.
The maintained gate covers 85 types/257 fields, 183 bundle files and 55 actual
native truncation rejections, retaining the prior 173-case failover gate,
172-case three-member gate
and all prior 169 LFUM cases.
Live-session LFUM adds empty/rich/deleted-only metadata bodies, a bodyless
correlated reply and both action headers. Native LFUMMessageBody extraction
preserves generation/NodeDescription/FailoverUnitInfo-vector/AnyReplicaFound
order; the request has no GenerationHeader and the reply echoes it.
The LFUM gate covered 84 types/255 fields, 173 bundle files and 49 actual
native truncation rejections. All prior 163 cases and 47 rejections remain.
The empty LFUM fixture explicitly initializes AnyReplicaFound=false; the native
body's empty constructor does not initialize that scalar.
Healthy planned Primary transfer adds six default/rich/action cases for
GetLSN/Reply. GetLSN reuses ReplicaMessageBody; GetLSNReply inherits
ReplicaReplyMessageBody and adds nested deactivation information in its derived
scope. Native extraction now covers 83 types/251 fields and 167 bundle files;
native executes 47 truncation rejections. All prior 157 cases and 45
truncation rejections remain retained. Rust verifies peer actor/priority,
generation, exact action factories and every proper body prefix.
Healthy configured-Secondary retirement adds six default/rich/action cases for
Deactivate/Reply. Its inherited configuration is followed by a derived scope
containing `isForce_` and the nested deactivation information. This adds one
native schema type and two fields, for 82 types/250 fields and 161 bundle files.
Rust verifies exact peer factories/generation, complete decoding and proper
prefix rejection; native executes 45 truncation rejections. All prior
151 cases and 43 truncation rejections remain retained.
The same-Primary configuration extension adds twelve independent cases for
DoReconfiguration/Reply and Activate/Reply: four default/rich body pairs and
four literal action headers. Four native schema types add eight fields
(81 types/248 fields in total). The inherited ConfigurationMessageBody keeps
native unit/replica-vector/service order, with derived phase-0 duration or
deactivation information in its own scope. Default phase-0 duration remains
TimeSpan::MaxValue; configuration replies contain the full replica vector.
Production Rust factories/decoders verify both reply owners, exact priority/
generation and cross-action rejection. Proper-prefix checks cover every new
body; that extension increased native rejection coverage to 43 truncations. This is source
stream/schema-adapter evidence, not native handler execution or FMM admission.
All previous 139 inputs and 39 truncation rejections remain retained.
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
bodies; native rejects 39 truncated bodies, including NodeUp, Ack, PToP
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

The bounded original opened/opening/never-created Idle destination deletion also reuses these
inherited DeleteReplica/Reply bodies and factories; it adds no wire schema or
fixture shape. Separate RA/store and real-process FM/Node/RAP tests qualify
Idle deletion, held disposal, independent pending-remove, lost reply/replay and
late-creation tombstones. The missing-FT reply uses native constructor-default
FT target/minimum (zero) rather than copying the FM Add's 1/1 description.
Its source-bound applied-Delete absence sidecar is a separate trusted-local DTO,
not a new native schema or a fabricated runtime disposal receipt. Commit/model
tests and actual child/Channel acceptance cover those semantics. They do not execute the native C++ deletion handler
or expand the codec oracle's interoperability claims.
Registered in-flight Open uses existing RAP cancellation and Close retries;
returned controls remain owned for replicator-before-service Abort. Its frozen
FM admission requires genuine disposal proof, not never-created absence.
Held callback/provider/service and real child-adapter tests are separate from
these unchanged 139 wire cases and 77 schema types/240 fields.
Pre-registration Opening instead uses the original created FT's 1/1 description
and native immediate Close without registration/RAP. Its distinct source-bound
pre-Open cancellation receipt waits for exact late lookup/acquisition drain;
it is neither a runtime disposal receipt nor zero-history missing-FT absence.
That trusted-local DTO and real-process/child tests add no wire cases or schemas.
Known pre-Open registration failure similarly reuses closed Delete and its
created 1/1 FT description. Applied original failure/lookup/creation and Delete
lineage has a distinct drained-ownership local receipt; the failed Add error
is preserved. Controlled real Hosting/process/Channel acceptance uses the
existing native dynamic registration Drop=1 threshold, not a new wire schema
or a changed production default. Native C++ handler execution remains separate.
Terminal runtime Open failure uses the same native closed Delete shape after
RAP's actual Abort/final release. A separate local failed-Open receipt binds the
Applied failure and Delete to the exact original runtime and usage acquisition;
it is not a successful-Close, registration-failure or absence receipt.
Controlled process/Channel acceptance holds real provider/service disposal and
uses the existing dynamic Open Drop=1 threshold for qualification only.
These source-derived lifecycle tests add no wire cases or schema types.

Opened peer Idle AppHostDown now reuses the existing ReplicaUp dropped list,
native ReplicaDown stage, processed-list ACK and closed DeleteReplicaReply.
Source-bound original FM Add/node ownership gates, separate loss/ACK commits
and the unchanged 30-second retry are lifecycle evidence, not new schemas.
Actual two-child destination app termination, lost ACK/metadata reply replay
and healthy Primary writes do not establish native C++ handler execution,
runtime disposal or replacement permission. The oracle remains 139 cases.
The same lifecycle path now covers AppHostDown during normal RAP Close/Drop
started from the original opened Idle. Committed opened-origin lineage and
late-Close rejection keep partial cleanup separate from disposal; actual held
provider/service process-loss acceptance and intent/loss/ACK commit barriers
remain independent from these unchanged codec cases and native-handler evidence.
Successful normal Close of the original opened peer Idle now uses those same
report/ACK/metadata shapes without HostDown. Its Applied completion and actual
original-runtime disposal/usage proof are separate from FM acknowledgment;
held provider/service disposal, a real lost-ACK retry and metadata replay add
lifecycle evidence, not wire cases, schema types, reuse authority or native
C++ handler execution.

Normal Drop of that original opened Idle now uses OnDropped/ReplicaDropped
stage with the same canonical ReplicaUp dropped entry, processed-list ACK and
closed metadata Delete shapes. Non-dropped ACKs cannot clear this stage, even
before the first report. Real disposal and committed original ownership release
remain independent from FM acknowledgment and Primary pending-remove; no
replacement is enabled. Intent/completion/ACK commit tests and controlled real
provider/service disposal, lost-ACK retry and metadata replay add lifecycle
evidence only. The oracle remains 139 cases, not native C++ handler execution.
For pending Drop, matching HostDown supersedes it with ReplicaDown stage;
successful Drop committing first instead retains ReplicaDropped and original
disposal proof. Both races, stale RAP replies, failed/unknown commits and real
provider/service process loss with a lost-ACK retry add no wire cases or reuse
authority. Initial-Open and Abort/ForceAbort loss remain gated.

Normal non-force Abort of the original opened peer Idle now uses the same
ReplicaDropped-stage/canonical ReplicaUp shapes and closed metadata ACK path.
RAP Abort uses existing `flags=4`; shared original-open admission preserves
native non-force behavior and rejects local-only/initial-Open/unowned lifetimes.
Actual provider/service Abort disposal and exact original Applied completion/
usage release remain independent of metadata acknowledgment and Primary removal.
Both-list dropped ACKs, commit barriers, real held-disposal/lost-ACK retry and
metadata replay add lifecycle evidence only: 139 codec cases remain unchanged.
HostDown during Abort, ForceAbort reporting and replacement remain gated.

The original-session destination Delete matrix now covers normal/force,
opening/closing/closed/failed/absent/lost targets, Delete-error ForceAbort and
native deleted-FT stage-None suppression. The force flag and all native replies
reuse existing codecs. Distinct local positive process-loss evidence joins
metadata acknowledgment as `host_lost`, never graceful disposal or reuse.
Applied cleanup histories surviving later app exit, exact original ownership,
model/store commit barriers and actual held-disposal/force/no-runtime/lost-reply
acceptance add behavioral evidence only. Unproved cleanup/Node-owner loss stays
explicitly incomplete/unknown. No new schema types or cases are claimed:
the oracle remains 139 bidirectional native-stream/schema-adapter cases, not
native RA handler execution or FM/Federation interoperability.

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

Fifteen inputs retain all prior 101 cases and increased the then-current wire gate to
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

### First peer creation codecs

Eight inputs retain all preceding 116 cases, 75 types/237 fields, 120 bundle
files and 31 native truncation rejections. The new totals are **124 cases,
77 types/240 fields, 128 bundle files and 34 native truncation rejections**:

* `default-create-replica`, `rich-create-replica`, `initial-create-replica`;
* `default-create-replica-reply`, `rich-create-replica-reply-0`,
  `rich-create-replica-reply--2147017398`;
* `action-CreateReplica`, `action-CreateReplicaReply`.

The mechanically extracted RAReplicaMessageBody inherits ReplicaMessageBody
and adds one nested ReplicaDeactivationInfo in its next native scope. The info
preserves Epoch (configuration, data loss) then signed catchup LSN. Native
default construction is zero invalid Epoch/LSN=-1; rich cases exercise distinct
signed fields, and the initial case carries CC epoch/0. Nondefault bases retain
the complete service graph and explicitly select volatile, Idle, InBuild/up
1/1 fields without altering any previous Add/Delete/notification fixture.
The reply retains complete ReplicaReplyMessageBody, success/StaleRequest
primitive errors, real endpoint string fields, and Idle **InBuild=1**, not
Ready. These are independently constructed schema values, not captured runtime
endpoints or proof of actual RAP invocation.

Pinned RSMessage.cpp:115–116 selects Actor RA=12/priority=true for **both**
actions. The producer validates their source initializers and records
`peer_factories` in the manifest, just as generation factories are source-bound.
ReconfigurationAgent.cpp:3453–3468,3504–3528 and
Infrastructure.SendMessageActions.h:53–88 route both actions through the same
send action. GenerationStateManager.cpp:23–27 unconditionally adds that RA's
per-owner send GenerationHeader. FederationWrapper.cpp:199–230 preserves it;
MessageHandler.cpp:237–244,815–822 uses the presence-asserting native reader.
Missing peer generation must not become an invented zero/default generation.
Unlike FM replies, these semantic peer replies target RA and carry generation.

Rust exercises both production factories and decoders against native-produced
actor/action/priority/generation values, including both owner flags, with
independently constructed bodies returned to the native stream. Every proper
prefix of all six new nonempty bodies is rejected. Native executes three
additional truncated-body rejections. Header tests reject wrong/cross actions,
actors FM/FMM, missing/malformed/duplicate generation or factory headers, and
flattened/nested/absent deactivation scope shapes. The schema's normal
forward-compatible base-reader behavior is retained; action checks distinguish
shared-schema actions.

This is the first peer codec increment only: no AddReplica, BuildIdleReplica,
copy, quorum, serving, GetLSN, Deactivate or Activate. Native successful RAP Open
updates endpoints and sends CreateReplicaReply; the full native primary handler
immediately continues into build, which is deliberately outside this slice.
Model/real-session execution and runtime receipts are separate from this
source-baseline native-stream/schema-adapter gate. No production native handler
trace or Federation/binary interoperability is claimed.

### FM AddReplica/Reply codecs

Seven cases extend the preceding 124 inputs to 131, retaining 77 types/240 fields
and producing 135 bundle files:
`default-add-replica`, `rich-add-replica`, `default-add-replica-reply`,
`rich-add-replica-reply-0`, `rich-add-replica-reply--2147017398`,
`action-AddReplica` and `action-AddReplicaReply`.
Both actions reuse native ReplicaMessageBody/ReplicaReplyMessageBody. The rich
request selects a volatile 1/1 Idle/InBuild; the successful reply is Ready/Idle
with no service endpoint, while the error reply remains InBuild. Actual native
streams reject two additional truncated bodies, for 36 measured rejections.

Source extraction verifies exact native FM-to-RA and RA-to-FM factory names,
literal actions, actor RA=12/FM=11 and high priority=true. The producer binds
that `replica_factories` metadata into the manifest; the compiler-free consumer
requires every new case and exact metadata before executing either binary.
Rust checks both public factories/decoders against independently produced
native fields and envelope values, with FM/FMM generation preservation on
requests and FM/FMM reply-owner codec support without a mandatory reply
GenerationHeader. Every proper prefix is rejected, and cross-action decoding
cannot reinterpret a peer CreateReplicaReply as FM completion.

This is schema-adapter/native-stream evidence, not native handler execution.
The bounded native-free FM/RA/Hosting/RAP build, lost-reply replay and removal
race acceptance is separately described in
[MinimalFmCluster.md](../../docs/cluster/MinimalFmCluster.md).
Active membership, reconfiguration, recovery and deployed native
FM/Federation interoperability remain gated.

### FM RemoveReplica/Reply codecs

Seven cases extend the preceding 131 inputs to **138**, retaining
**77 types/240 fields**, with **142 bundle files** and **38 measured native
truncation rejections**:
`default-remove-replica`, `rich-remove-replica`,
`default-remove-replica-reply`, `rich-remove-replica-reply-0`,
`rich-remove-replica-reply--2147017398`, `action-RemoveReplica` and
`action-RemoveReplicaReply`.
RemoveReplica uses bare ReplicaMessageBody, not the inherited DeleteReplica
body used by RemoveInstance/DeleteReplica. Its rich request is Idle/InBuild;
the reply has the Primary's InDrop wire state Dropped=3 while retaining the
remote identity, endpoints and up flag. It is not a remote deletion receipt.

The producer mechanically verifies both native RemoveReplica factory definitions
and binds them in `replica_factories`; the consumer requires exact metadata and
every case before execution. Rust checks public factories/decoders against
native-produced body/envelope values: request actor RA=12, true priority and
actual GenerationHeader; reply actor FM=11 (FMM=10 codec owner only), true
priority and no mandatory generation. Every proper prefix is rejected;
cross-action decoders cannot confuse RemoveReplica with AddReplica,
RemoveInstance or DeleteReplica. Both actual native truncated-body rejections
are measured. This remains source-baseline native-stream/schema-adapter evidence,
not native handler traces or deployed native FM/Federation interoperability.
The additional `in-create-remove-replica-reply` case independently constructs
native `ReplicaReplyMessageBody` by echoing the original Idle/InBuild descriptor,
with both endpoints empty and success. This is native
`ReconfigurationAgent.cpp:2092-2106`'s reply form, not the post-Open RAP
Dropped=3 reply. It increases the gate to **139 cases, 143 bundle files and
39 measured native truncation rejections**, retaining 77 types/240 fields.
Rust reconstructs and verifies the exact echo, factory/envelope and every proper
prefix; the native decoder compares every field and rejects an additional
truncation. Existing receipt guards require the new case before execution.
Native-free FM/RA/RAP execution, lost-reply replay and Primary/destination
lifetime checks are separate acceptance tests. The same exact original-descriptor
echo is used by native absent-after-AbortAddIdleReplica cleanup
(`ReconfigurationAgent.cpp:2070-2080,2898-2910`). Original failed-creation cleanup
therefore adds no action/schema/fixture: the gate stays 139 cases and 39 native
truncation rejections. RA/FM tests separately qualify the original failure,
pending-remove, lost reply/replay and late-success fences; codec evidence does
not execute that native handler. Original InCreate cancellation and failed
creation cleanup use no RAP; down/arbitrary-not-found cleanup, replacement and
active configuration remain gated.

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

The bundle has exactly 124 `native/*.bin`, `schema.h`, `native-oracle`,
`ra-conformance` and `manifest.json` (128 files total). No other files are
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
python3 -m unittest tests.ra_protocol.test_peer -v
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
