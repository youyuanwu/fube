# Minimal native-free FM cluster

**Profile:** one foreground in-memory FM, two separate RA+Hosting processes,
and one fixed stateless application profile. Explicit placement only; no
automatic recovery, reconnect, failover or native FM/Federation parity.

The explicit `--stateful-singleton` variant now enables fresh FM-authorized
volatile 1/1 Primary creation, using the separate `hosting-local-app` package.
Each of the two configured nodes can host one independent singleton partition;
this does not form a multi-replica set. Stateless remains default.

The [launcher guide](../../crates/tools/fube-cluster/README.md) defines invocation,
bounded JSON commands, independent application contact and cleanup receipts.
It uses the production local Node/RA/Hosting/runtime, memory store and common
transport engine, not simulated application activation.

## Run

Build both binaries with the workspace toolchain:

```sh
cargo build --locked -p fube-cluster -p cluster-stateless-app
target/debug/fube-cluster --root /absolute/private/profile-root \
  --app "$PWD/target/debug/cluster-stateless-app"
```

On constrained hosts, explicitly relocate **both** Cargo output and the profile
root/package/runtime/scratch paths. A different Cargo target alone does not move
application materialization. The profile root's existing parent must be owned
and writable; the root is private (0700).

After `ready`, send commands on stdin:

```json
{"op":"place-a","action":"create","node":1}
{"op":"observe-a","action":"status"}
{"op":"remove-a","action":"delete","node":1}
{"op":"finish","action":"quit"}
```

Create/delete initially return pending. Inspect with a **new** status operation
ID until semantic completion. An identical ID/content retry returns its retained
result; conflicting reuse rejects. Status retries return their original snapshot.
There are 64 retained operation identities, no eviction, and one creation
at a time per node. After verified deletion, a new create on that same live node
gets strictly increasing replica and RA-instance IDs. The public stateless
instance ID is the replica ID. Retired operation retries preserve their original
lifetime/endpoint, not a successor's observation. EOF still requests shutdown
after identity capacity is full.

Independently connect to the reported numeric-loopback endpoint, send exactly
`identity\n`, and read a bounded JSON line. It includes runtime-origin node
ID/incarnation/PID and service/partition/replica/incarnation plus app PID.
Contact is a separate observation, not implied by the FM's endpoint field.

## Authority and completion

The native-derived fixed-profile exchanges are NodeUp/Ack, final empty
ReplicaUp/Reply, AddInstance/Reply, RemoveInstance/Reply and
ServiceTypeNotification/Reply. Full native body schemas, recipient actors,
priority, PToP NodeInstance identity and FM generation are retained. The one-use
original-child control capability admits one established Channel; it is trusted
local admission, not hostile-network authentication or lifecycle success.
Endpoint owns dialing; FM/node clients use established Channels, and native
Hosting IPC remains a distinct setup.

The FM retains desired versus observed state. Actual Running requires the
eligible RA semantic reply after real RAP Open; Absent requires matched
Remove completion after disposal. Registration or a transport write cannot
provide either. Unexpected reports and unsupported obligations remain staged/
pending, not successful no-ops.

Delete completion additionally requires the original Node's positive instance
retirement receipt: original-runtime disposal-before-Close-reply provenance,
Applied exact Close command, exact original Hosting acquisition release and
drained predecessor replica work. A Remove reply alone leaves delete pending.
Recreate rejects until both inputs match the same lifetime; old receipts,
replies and reports cannot retire a successor. Receipt files are bounded local
ownership observations, not new native messages or network authentication.

The original app host, factory, package/runtime root and closed RA/runtime
tombstone stay resident through cycles. The old service object, partition/report
authority and joined listener are disposed before reuse. This is service-instance
recreation, not app-process recycling. An unproved pending Open or uncertain
cleanup remains nonreusable. Original FM/node processes do not restart.

Loss of the FM link makes observation unknown; the app may remain serving.
Original node and app owners remain responsible for cleanup through independent
owner control. No replacement is authorized by link or node PID disappearance.

## Node-wide coordination (stage 2)

Both fixed profiles accept `activate` and `deactivate` with a node and operation
ID. NodeUpAck seeds the actual activation sequence. Native node requests retain
normal-FM generation and sequence/intent checks; activation replies acknowledge
intent, not serving replicas. Deactivation replies follow all required serialized
close jobs and committed closure. Older requests cannot change intent; a repeated
completed deactivation can replay its reply without another Close/acquisition.
A newer opposite intent cancels the old monitor. Operator status distinguishes
pending node work and reports superseded/lost work as unknown.

The native-derived close monitor uses a dynamically readable 900-second deadline
and 15-second check interval. Its one-time termination pass deduplicates original
host registrations; fenced/private commits cannot authorize termination or a
successful reply. Actual HostDown/closure still completes the barrier. Forced
exit supplies no graceful provider/service disposal proof. Closed volatile data
is not reopened/adopted by activation, and FM loss remains nonreusable.

`Cluster::update_service(index, service)` sends native NodeUpdateServiceRequest
within the same fixed identity/profile. Each RA group waits for all entity jobs;
only a failed commit for a job that actually updated the description suppresses
the node reply. That reply does not wait for RAP's separate description reply.
The original session sends ProxyUpdateServiceDescription and retries its pending
obligation; matched RAP completion clears only that obligation. The bounded FM
resends node intent/service work at the native default five-second scan interval,
retaining the original sequence/body and accepting stale/duplicate replies safely.
Dynamic FM scan configuration is not enabled.

Hosting Enabled/Disabled status has separate native staleness/coalescing ownership,
disabled priority, sorted type bodies and a fresh message sequence on each
captured retry. The owned adapter honors the dynamic five-second minimum start
interval and 30-second retry; only the current sequence clears the corresponding
pending kind. Hourly cleanup retains ten-minute event history independently of
unacknowledged pending status. The older prepared-registration profile's local
diagnostic projection does not gain FM authority.

Actual public partition `report_load` reaches the original actor-12 IPC inbox,
RA and ordinary FM as ReportLoad. Complete legacy/modern schemas are retained;
the deployed profile admits modern reports for its current owned service/FT.
Reporting is best-effort, with no per-report semantic ACK or persistence claim.
Service updates may change declared metrics, not widen topology or ownership.

Multi-FT closure/update barriers, definite/unknown commits, pending creation,
stale intent and capacity are tested in the generic RA executor. Real acceptance
keeps **one FT per Node** and exercises updated metrics/public load, held provider
and service disposal, forced original-host termination, cancellation and an
independently serving sibling. Endpoint codecs/helper are included, but live
endpoint publication remains gated until stage-5 reconfiguration. Codec/model/
native-free results are not complete C++ handler traces, native FM interoperability,
general multi-FT deployment or complete P2/P4/M2 parity.

## Ordinary-FM generation handshake (stage-3 increment)

The ordinary-FM proposal/reply and update/reject paths are enabled on the
original bounded session. Proposal is a consuming correlated request, not a
one-way notification; all four native factories are high priority. Proposal
and Update carry GenerationHeader, while Reply and Reject do not. Preprocessing
retains the highest proposal even before owner readiness; a non-open RA returns
the native RANotReadyForUse error after recording it. FMM and a local FM-service
FT remain gated, including the latter's epoch/ForceAbort dependency.

Proposed, receive and send generations are separate session-only values,
initially seeded from fresh NodeUpAck. An Update matching the proposal changes
receive and records that upload is required, with no successful Update reply.
An unmatched Update sends the native proposed-generation rejection; messages
below receive are ignored. The generation/publication lock also protects
locked-entity admission. Already-running commits retain their original
operation/fence; definite failure and uncertainty cannot publish private state
or fabricate an uploaded inventory. No snapshot job, LFUMUpload or upload ACK
is implemented yet, so the handshake never advances send generation or grants
new serving/restart authority.

Programmatic `Cluster::begin_generation_handshake()` opts the two original,
admitted nodes into a bounded FM round only when lifecycle/control requests
are not pending. It is not a new CLI command and does not change default
startup. The FM retries proposals and pending updates, restarting negotiation
above a higher proposal or qualifying rejection. Retry timing uses native
five-/ten-second defaults; dynamic FM rebuild configuration and Federation
broadcast membership are not enabled. The round intentionally remains
`awaiting_upload`, `complete=false`, with mutations/service updates gated.
Existing replicas are not reopened or relabeled as recovered; independent
owner shutdown and real application access remain available.

Only generation-control messages bypass the deployed envelope's frozen launch
generation. Other live action families remain restricted to that original
generation until LFUM completion is implemented; the generic RA model does
apply current receive-generation admission. This is not a fully rebuilt FM or
native Federation interoperability. Focused model/store tests cover stale and
duplicate traffic, reordered proposals, definite/unknown commits and queued
entity admission. Real nodes exercise correlated replies, rejection,
renegotiation, unchanged send generation and independent sibling access.
Bidirectional native codecs and native ordering/admission helpers are separate
from the still-required complete C++ handler traces and interoperability gates.

## Peer-created Idle replica (stage-4 increment)

The explicit `--stateful-peer-idle` profile uses the same volatile
`hosting-local-app` package, one shared partition, two original nodes and
distinct replica identities. Only node 1 can be FM-created as the initial
Primary; node 2 is reserved for peer-authorized Idle creation. Existing
stateless and independent-singleton profiles are unchanged. Each node still
hosts at most one FT.

After the original Primary is open, programmatic
`Cluster::create_idle_replica()` authorizes that Primary RA to issue native
CreateReplica on its separately bound original peer session. It does not
forge a Primary source at FM or implement FM AddReplica. Native body
inheritance, deactivation information, generation and PToP NodeInstance
headers remain intact. The original loopback peer connection is bounded and
non-reconnectable; neither a packet write nor peer loss completes creation.

Destination RA records the original sender and commits the new lifetime before
registration or RAP Open. Its CreateReplicaReply waits for the Open-completion
commit and returns the actual replication endpoint. The replica remains
Idle/InBuild, with no service endpoint or read/write access. Repeated creation
while opening performs no second Open/acquisition; an identical retry after
Open replays the native response. Stale epochs, instances and generations
cannot replace that lifetime. Original disposal/Hosting usage and app-process
cleanup retain their existing independent ownership.
The deployment assigns both actual package versions `1.0:1.0:1`; physical
package/code incarnations, runtime IDs and acquisitions remain independently
minted on each node. This does not bypass Hosting's operational version-mismatch
gate or invent a deactivation-if-unused outcome. The RA model separately
preserves requested versus selected versions for its native-derived scenarios.
Terminal peer creation failures preserve the native error response and retain
their still-gated FM dropped-report obligation; they cannot become successful
remote Open observations or silently discard cleanup/reporting responsibility.

The Primary retains a bounded remote-Open observation. An exact current peer
reply is not proof of copy, build, active membership, quorum or serving
readiness. Primary Close invalidates the observation and fences late replies.
Fresh-start/fixed-generation admission is required; LFUM rebuild, peer
recreation/adoption, persisted state, ActiveSecondary promotion, AddReplica
and reconfiguration remain gated. The next increment must connect real RAP
build and destination application copy/replication ACKs before any
AddReplicaReply or active-member claim.

These are native-derived model/store and native-free real-process contracts,
not full C++ handler traces or native Federation interoperability.

## Initial stateful FM control

Run the same executable with `--app /absolute/hosting-local-app
--stateful-singleton`, then use `create`, `status`, `delete` and `quit`.
`AddPrimary`/`AddPrimaryReply` preserve native action/actor/priority, shared
ReplicaMessageBody/ReplicaReplyMessageBody and request-generation/reply-owner
asymmetry. The node uses `Profile::VolatileStatefulFmPrimary` and a distinct RA
network command, not redirection to trusted-local Primary authorization.

Native source pin `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` is authoritative:
`ReconfigurationAgent.cpp:1731-1847,4313-4342` supplies processor/precheck and
Ready-duplicate behavior; `FailoverUnit.cpp:5337-5350` sets initial Primary
deactivation info to `(CurrentConfigurationEpoch, 0)`.
`fm/FailoverUnitMessageProcessor.cpp:46-62` completes an expected creating/up
Primary on the matching node incarnation. `fm/PendingTask.cpp:165-175`,
`fm/Replica.cpp:661-667` and `common/FailoverConfig.h:78-82` define pending
Primary retry with default 10/60-second per-replica intervals. This fixed
profile retains those native predicates/defaults, not dynamic FM configuration.

RA registration/Open and the final Primary/Ready reply remain commit-gated.
Unknown store completion holds the original reply/duplicate until resolution.
Ready duplicate requests reply from the existing FT without adopting the
request epoch or acquiring another use. Closed/stale requests retain dropped
reporting obligations through direct ReplicaUp delivery; live replacement and
unproved reopen paths fail admission.
FM does not turn a transport write into readiness. An independent application
client verifies one public admitted/applied/acknowledged write and readback.

Disposal-gated fresh recreation and native force Delete handling are described
below. Reconfiguration, FMM, copy/build, multi-replica movement and recovery
remain gated. Owned local shutdown joins resource/listener disposal,
exact usage and physical cleanup while retaining stateful reporting escrow.
Cleanup receipts do not imply native terminal-report acknowledgment.

### Normal singleton deletion

After the original FM Primary creation completes, `delete` sends
`DeleteReplica` with the native DeleteReplicaMessageBody and `force=false`.
The separate Node/RA command retains network authority; it is neither local
Close authorization nor a stateless RemoveInstance redirect. Local stateful
profiles and the stateless profile do not gain this command.

Pinned `ReconfigurationAgent.cpp:777-835,2239-2242` supplies DeleteLocalReplica:
normal deletion rejects an older current epoch and requires the local ReplicaId,
not equality with the FM's possibly older replica instance.
`ReconfigurationAgent.cpp:671-707` sends DeleteReplicaReply after successful
Delete close. `FailoverUnit.cpp:5472-5544` drops volatile state and deliberately
does not schedule a ReplicaDown/ReplicaDropped report for normal Delete.
The reply uses a fresh native dropped description (None roles, invalid package
and LSNs, no endpoints), not the former Primary/Ready description.
`fm/PendingTask.cpp:32-46` and `fm/FailoverUnitMessageProcessor.cpp:224-255`
supply the deleting-FT action and successful deleted-replica processing.

RA marks deletion and finishes Close only behind their respective commits.
Real RAP disposal joins the provider, service and application/replication
listeners. FM joins the matching successful reply with the same original-node
disposal/Applied-Close/exact-H2-release receipt used by the stateless profile.
A missing reply or proof leaves deletion pending; lost replies retry under the
same native 10/60-second action predicates without another use or lifetime.
Failed/unknown commits cannot publish deletion completion or release.

The deleted tombstone, app host, factory and package remain resident under
their original owners. A later explicit `create` can authorize a fresh lifetime
only after this verified deletion. Duplicate operator operations retain their original outcome.
Normal deletion does not acknowledge unrelated terminal reporting escrow.

### Unexpected singleton app-host loss

The FM singleton profile now reports original app-host loss, including finite
lease fencing, over real `ReplicaUp`/`ReplicaUpReply` delivery. Graceful Close,
normal Drop and the remaining singleton terminal origins are covered below.
Local Idle and local-Primary profiles do
not gain FM reporting authority.

Pinned `FailoverUnit.cpp:5472-5544` and `FMMessageState.cpp:148-176` distinguish
the state from its payload: AppHostDown calls OnReplicaDown(false, instance),
retaining **ReplicaDown stage**, but the closed volatile FT has a fresh dropped
description in ReplicaUp's **dropped list** (`FailoverUnit.cpp:4015-4050,5806-5856`).
Publication and the exact original H2 release remain behind the actual Close
commit. A store veto/unknown outcome cannot release usage or publish the report.

FM processes that exact original report as lost volatile state, not operator
deletion. `ReplicaUpdateTask.cpp:68-93,302-337` returns it in the **processed
list** of ReplicaUpReply, not an echo of the incoming dropped list.
`ReconfigurationAgent.cpp:3325-3408` and `FailoverUnit.cpp:5734-5774` then clear
only the eligible down-stage obligation. The canonical processed reply also
authorizes native Delete of the already closed FT, producing DeleteReplicaReply;
a dropped-list reply instead uses closed ForceDelete without that reply.
This metadata-only follow-up does not enable force deletion of a live replica.

The owned stateful adapter requests its first report immediately and retries
unacknowledged reports at the pinned default 30-second FMMessageRetryInterval
(`common/FailoverConfig.h:398-410`), retaining the original lifetime and captured
work. This fixed single-FT scheduler does not add dynamic FM configuration,
native jitter or a general multi-FT batching scheduler. Wrong incarnation,
up-state, epoch or session replies cannot clear this bounded original-profile
obligation. Failed/unknown acknowledgment commits retain the original state,
retry owner and queued work; duplicates acquire no second use.

Status exposes `observed: "lost"` with no endpoint, while desired placement
remains separate. `loss_reported` records FM processing and
`native_replica_deleted` records the later native metadata reply.
Neither means original-runtime disposal, an `absent` observation, a retirement
receipt, recovered data or permission to recreate. FM can retry normal
DeleteReplica for the same lost closed lifetime until that metadata reply,
without completing an operator deletion lacking its independent disposal proof.
The original Activation still owns physical cleanup.

Real-process/channel tests cover app crash and lease expiry, public acknowledged
write/readback before loss, a genuinely lost acknowledgment and the 30-second
retry, stale/duplicate replies and an independently serving sibling. Hard exit
does not manufacture provider/service destructor receipts. These are native-free
composition tests with selected trusted premises, not deployed native handler
or Federation-interoperability qualification.

### Graceful singleton Close reporting

While the original FM session remains live, the existing local Node `close`
with `CloseMode::Close` now reports the original FM-created volatile singleton.
This is not a new FM command or CLI operation, and local-only profiles remain
unchanged. Normal operator `delete` still uses DeleteReplica/Reply without
scheduling a terminal report.

Pinned `FailoverUnit.cpp:5472-5544` calls OnReplicaDown for ordinary Close,
just as for AppHostDown; Drop/Abort instead call OnDropped. Thus graceful
volatile Close retains **ReplicaDown stage**, not ReplicaDropped, while sending
the same canonical dropped ReplicaUp entry. Successful real RAP disposal and
the actual Close commit precede report scheduling and exact original H2 release.
Held provider/service disposal and failed/unknown commits cannot publish
completion, release usage or start reporting.

The existing immediate/30-second retry, processed-list ReplicaUpReply and
closed-FT DeleteReplicaReply path then converge without reopening or acquiring
another use. A dropped-list ACK instead performs closed ForceDelete with no
semantic deletion reply. Stale/duplicate ACKs preserve original ownership.
Graceful disposal proof remains an independently joined original-runtime
observation; metadata ACK processing neither creates nor replaces it.
FM still records the dropped volatile lifetime as `lost`, not `absent`,
recovered or reusable, even when Node separately has positive disposal proof.
An unsolicited original disposal sidecar is validated and ignored for
retirement, not treated as a link failure; malformed/cross-owner proofs still
reject. Neither proof arrival order can recover the dropped volatile data.

Real-process/channel tests hold provider and service disposal separately,
verify acknowledged public write/readback before Close, independently observe
both listeners retire while the original host remains alive, lose a valid ACK
and exercise the native-default retry, and verify unchanged original disposal
lineage and independently serving sibling data. RA tests cover both ACK lists
and actual store veto/unknown resolution. Normal Drop and the remaining
singleton lifecycle paths are covered below; reconfiguration and recovery remain gated. These tests
do not establish full native handler or native FM/Federation interoperability.

### Normal singleton Drop reporting

The existing local Node `close` with `CloseMode::Drop` now reports normal Drop
of the original FM-created volatile singleton while its original FM session
remains live. This is not a new FM message or CLI operation. Local-only profiles
remain unchanged, and normal operator Delete still schedules no terminal report.

Pinned `FailoverUnit.cpp:5472-5544` calls OnDropped for Drop, unlike the
OnReplicaDown used by ordinary Close and AppHostDown.
`FMMessageState.cpp:99-113` therefore selects **ReplicaDropped stage**,
not ReplicaDown. Both stages send the current `ReplicaUp` action with a canonical
dropped entry; no legacy ReplicaDropped wire action is introduced.
Real RAP cleanup and the actual Close commit precede report scheduling and exact
original H2 release. Held disposal and failed/unknown commits cannot publish
completion or release ownership.

`FailoverUnit.cpp:5734-5753` requires a dropped replica description to acknowledge
ReplicaDropped stage. A delayed non-dropped down/Ready description cannot clear
it. The existing original-profile admission, immediate/30-second retry and
processed-list ACK/closed metadata DeleteReplicaReply path are reused.
A dropped-list ACK instead performs closed ForceDelete without that reply.
Metadata acknowledgment cannot create or replace independently joined disposal
proof, supply recovered data, or make FM's `lost` lifetime absent/reusable.
Valid unsolicited original disposal sidecars remain validated and ignored for
retirement; cross-owner/malformed proofs reject.

Native `CanCloseLocalReplica` and StartClose's closed-FT return
(`FailoverUnit.cpp:4781-4842,4916-4942`) are preserved: a normal Drop cannot
supersede an already-closing Close, and a Drop of an already-closed Down lifetime
does not relabel its original report. Abort/ForceAbort and closed-AddPrimary
handling are covered below; reconfiguration and recovery remain gated.

Model tests cover both ACK lists, stage-eligible acknowledgment before the first
retry, non-dropped/stale replies, duplicate completion and actual store
veto/unknown resolution. Real-process/channel tests separately hold provider and
service disposal, check public access fencing and application/replication listener
retirement, exercise an actual lost-ACK retry, and preserve exact original
disposal/accounting lineage with unaffected sibling data.
These are bounded native-free compositions, not full native handler execution,
native stateful deployment or FM/Federation interoperability qualification.
Owner shutdown without a live FM session retains unacknowledged reporting;
physical cleanup does not synthesize its semantic ACK.

### Singleton lifecycle completion and fresh recreation

The bounded volatile 1/1 FM profile now supports the remaining stage-one
terminal origins. Native `ReplicaCloseMode.h:113-133,179-198` distinguishes
non-force Abort from ForceAbort: both send RAP Abort (`flags=4`), but only
ForceAbort has force-close admission. Both use OnDropped/ReplicaDropped stage
after successful disposal and the actual Close commit
(`FailoverUnit.cpp:4781-4842,5472-5544`). Closed/closing Down lifetimes are not
relabeled. Local-only profiles still gain no FM reporting authority.

Native force `DeleteReplica` is different: DeleteLocalReplica chooses
Obliterate for an open FT (`ReconfigurationAgent.cpp:777-835`).
The committed action terminates the exact original app host; matching HostDown
then finishes closure and schedules ReplicaDropped-stage reporting
(`FailoverUnit.cpp:4951-4957,5047-5070,5472-5544`). It does not send RAP Close
or manufacture a graceful DeleteReplicaReply/disposal receipt. Process cleanup
can precede exact H2 release, and the logical owner remains retained until RA
commits that release. Subsequent processed-list acknowledgment can delete closed
metadata and send DeleteReplicaReply, but FM still records lost volatile data
without reuse authority. Force is supported on the admitted native command;
the operator CLI's `delete` remains normal `force=false`, with no new force/reset
escape.

Missing/closed DeleteReplica tombstones preserve native
UpdateInstanceForClosedReplica ordering: advance a higher epoch and higher
ReplicaId/instance before the command's own guards
(`FailoverUnit.cpp:366-390`, `ReconfigurationAgent.cpp:786-829`). Normal stale
epochs reject; force bypasses that epoch guard, never the ReplicaId guard.
Closed force requests use normal Delete completion without killing the host.
These metadata-only replies cannot prove disposal of a service object.
Delete-error escalation to ForceAbort retains native ReplicaDropped stage but
suppresses payload delivery for an already locally deleted FT. A later normal
Delete retry completes closed metadata; no gated report effect can strand its
independently proved disposal/accounting.

For closed AddPrimary, the native handler sends a canonical dropped ReplicaUp
directly; an older requested instance additionally receives StaleRequest with
the requested replica description and existing FT description. This does not
change the existing report stage or allocate another use
(`ReconfigurationAgent.cpp:1782-1821`, `FailoverUnit.cpp:4720-4742`).
Locally deleted tombstones suppress dropped reporting
(`FailoverUnit.cpp:4015-4038`). These non-creating requests remain processable
on the original FM session even after app-host retirement.

After normal operator deletion joins its semantic reply with independently
proved original disposal/accounting, a new explicit `create` allocates higher
ReplicaId and instance values. Node refuses unproved predecessor reuse.
Identical retransmits share the original admitted create while its commit is
pending/unknown; conflicting payloads cannot reuse that approval or acquire
another use.
Original per-replica reporting must drain before reuse; unrelated node-wide
type-notification obligations stay independently owned across lifetimes.
Committed RA creation resets previous configuration, report state and initial
deactivation to `(current configuration epoch, 0)`, and invokes real RAP
Open(New). The original node, app host, factory and package remain resident;
the new service/provider has fresh empty volatile data, not recovered history.
Old native frames, receipts and completions cannot retire or resurrect the
successor. Lost lifetimes remain nonreusable even after metadata completion or
a genuine unsolicited disposal proof.

Focused model tests cover canonical reports, both ACK lists, closed/stale
requests, tombstone/force guards and actual commit veto/unknown resolution.
Real channels hold Abort/ForceAbort provider/service disposal, verify public
access/listener fencing and lost-ACK retry, and terminate the original host for
force Delete without destructor proof. Foreground and library tests exercise
fresh empty data, new acknowledged writes, exact new acquisitions, stale frames
on the same original session and unaffected sibling data across recreation.
Live replacement/adoption, description changes, persisted state, FMM, peer
control and recovery remain gated. No full native handler trace or native
FM/Federation interoperability is established by these compositions.

## Shutdown and restart safety

Always reuse the **same profile root** for the ownership domain. Its exclusive
lock and dirty authority marker precede startup. A new invocation is admitted
only for an empty initial root or a matching verified clean predecessor handoff.
Original node/app cleanup receipts, reaping/group drainage, and independent
physical/root observations precede that handoff.

Owner loss or missing proof is an explicit incomplete outcome and leaves the
interlock dirty, even if an independent observer later sees a dead app.
There is no force/reset escape. Do not remove the marker or use a new root to
bypass uncertain old authority. Small interlock/evidence files remain while
owned runtime/package trees are removed.

This fixed trusted app creates no descendant processes. Original process
capabilities and existing supervision are preserved; this is not a general
escaped-descendant adoption or durable crash-recovery mechanism.

## Evidence and limits

- [Executable qualification](../../crates/tools/fube-cluster/tests/foreground.rs)
  exercises two complete lifetimes per node with independent identity contact,
  unchanged original process start identities and sibling contact.
  [Composed failure tests](../../crates/tools/fube-cluster/src/tests.rs)
  cover duplicates, stale generation, link uncertainty, sibling isolation,
  serialized old/cross-owner cleanup receipts, deletion joins, partial startup,
  owner loss, physical cleanup and clean/dirty relaunch. Original-session replay
  also exercises stale requests, replies, reports and ACKs across recreation.
- [Native source-stream oracle](../../tests/ra_protocol/README.md) separately
  describes the maintained 59-type/199-field codec-adapter evidence, including
  distinct AddPrimary and DeleteReplica actions. Source schema
  and codec execution are not deployed native handler/interoperability evidence.
- Existing runtime/client/data profiles and required native regressions remain
  separate; no unrun native qualification follows from native-free success.
- [Deferred decisions](../mvp-deferred/minimal-fm-cluster.md) preserve excluded
  stateful/PLB/FMM/HA/recovery/upgrades/autoscale/Naming/HM/provisioning/parity work.
  [Reusable-lifecycle decisions](../mvp-deferred/reusable-stateless-lifecycle.md)
  distinguish retained hosts from optional host/package recycling.
