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
recreation/adoption, persisted state, ActiveSecondary promotion
and reconfiguration remain gated.

After observing `status()["peer_idle"]["state"] == "open"`, the separate
programmatic `Cluster::build_idle_replica()` submits a trusted local build
authorization. Primary RA publishes its frozen build obligation before sending
`ReplicatorBuildIdleReplica` through the original Hosting/runtime session.
The existing RAP binds the native invocation before calling
`IPrimaryReplicator::build_replica`; the request uses the native empty-runtime,
default-service control shape and one exact opened remote descriptor.
No new FM or peer network action is enabled.

The separate `Cluster::add_idle_replica()` API now lets FM own the whole original
Idle build, rather than requiring manual create/build calls. It sends native
AddReplica to the Primary's existing FT (metadata F, not a creating command),
with the exact original epoch, service, normal FM generation and remote
incarnation. Primary RA publishes its frozen FM request before CreateReplica;
successful peer Open publishes the actual endpoint and build obligation before
RAP dispatch. Matching RAP build success marks the Primary-side remote Idle
Ready and emits native AddReplicaReply. This is native
`ReconfigurationAgent.cpp:1856-2042,2851-2939,4027-4091` and
`FailoverUnit.cpp:427-505`'s ordinary N/I build branch, not activation.

The destination remains Idle/InBuild, unconfigured, with denied access and no
service endpoint. FM updates only its matching up/InBuild original Idle after
receiving AddReplicaReply; it does not mark node2 as a serving Primary. Poll
`peer_idle.fm.state`/`ready` (also `idle_build` in ledger status), not the separate
diagnostic child `peer_idle.built`/`ready` fields. The method returns pending
until that semantic reply is consumed. FM automatically retries the frozen
request on the native 10/60-second action cadence. Pending duplicates resend
CreateReplica or RAP build as appropriate; Ready duplicates resend the same
reply without another Open/build. A lost FM reply cannot turn child telemetry
into FM success.

Peer creation failure returns an AddReplicaReply error. FM preserves that failed
outcome, marks the original replica to-be-dropped and freezes pending-remove;
the bounded original observation is retained for error replay and cleanup instead
of authorizing another lifetime. RAP build errors remain diagnostics/pending
and retry through existing nonterminal cleanup, not failed FM completion.
Broader replacement, configuration repair and unowned/down cleanup remain
gated. Manual create/build cannot adopt or mutate an FM-owned Idle.
Local explicit removal cancels the FM add obligation and fences queued/late
success; cancellation does not fabricate RemoveReplicaReply. Primary Close
also fences late build and queued FM success. Original session loss leaves FM
unknown, not permission to reconnect or recover.

The destination application owns a joined copy/replication pump using public
`IStateReplicator`/`IOperationStream`/`IOperation` contracts. It applies the
complete copy (including values and the retry-result ledger) before consuming
continuing replication; every delivered operation is applied before its
explicit ACK. Copy EOS does not stop the replication pump. Original shutdown
joins the pump and retains the existing provider/service disposal boundaries.
Application pump errors are logged and fence the data owner; failed application
apply cannot ACK.

The build API returns `pending`, not build success. Poll
`status()["peer_idle"]["built"]`; only the exact original semantic RAP reply
can publish that observation. Pending retries reuse the frozen request, completed
retries do not rebuild, and Primary Close fences late completions. A native
build error remains visible in `peer_idle.build.result/detail`. Explicit retry
resends the frozen build request, not a fresh remote lifetime. RAP first removes
partial build resources with `BuildRetryCleanup`, suppressing build completion;
only a subsequent retry can build the same incarnation. Failed cleanup remains
an obligation, not successful copy or permission to replace a peer.

Separate `Cluster::remove_idle_replica()` abandons the opened original Idle
before build, during build, or after successful/failed build. Primary RA publishes
its frozen removal obligation before sending `ReplicatorRemoveIdleReplica` over
the original Hosting/runtime session. Native RA InDrop maps to `state=3`
(Dropped) in that request's remote descriptor; this does not change the
destination's actual role or lifecycle. The request retains the original
replica ID/incarnation/endpoint, empty runtime, default service and zero flags.
This local API does not admit removal before peer Open, issue FM RemoveReplica
or authorize arbitrary remote replacement.

The existing RAP issues native `ReplicaRemoval::Idle` authority before calling
`IPrimaryReplicator::remove_replica`. If build is still running, RAP cancels it
and suppresses completion until a later removal retry can drain/clean up.
Removing an unbuilt remote records a tombstone without inventing a user callback.
Already-removed retries succeed idempotently; the exact SF
`FABRIC_E_REPLICA_DOES_NOT_EXIST` result is normalized to successful absence,
but uncertain completion, other errors and stale invocation authority cannot
become success.
Retry cleanup is nonterminal; explicit Idle removal is terminal and suppresses
delayed Build for that incarnation. Removal does not change an active
configuration or its quorum.

The removal API returns `pending`, not removal success. Repeat pending calls
to retry the original cancellation/cleanup obligation; no autonomous peer retry
or reconnect is introduced. Poll `peer_idle.state` (`removing` or `removed`) and
`peer_idle.removal.result/detail`. Only a matching semantic RAP success publishes
`removed`; native errors remain visible and retryable. Once removal intent is
published, RA rejects new builds and ignores late build completions. Primary
Close fences pending removal replies. The session-only original observation is
retained as a tombstone, not reusable remote authority.

This follows the pinned native
`ra/ReconfigurationAgent.cpp:2050-2138,4094-4150`,
`ra/FailoverUnitProxy.cpp:960-1060` and
`ra/FailoverUnitProxy.ReplicatorBuildIdleReplicaAsyncOperation.cpp:119-180,317-353`
under `Reliability/Failover`: failed-build retry cleanup is different from an
explicit removal, and successful build does not automatically request removal.
Native `Replication/ReplicaManager.cpp:1783-1831` removes the Primary-side Idle
replication session, not the destination service/data. The destination remains
opened with its original registration, usage and copied state; normal shutdown
still joins and disposes that original owner. No FM RemoveReplicaReply or
destination DeleteReplica completion is inferred.

The separate programmatic `Cluster::remove_idle_replica_via_fm()` API now sends
native FM RemoveReplica for the original FM-owned pending/failed creation or opened Idle,
including a manual unbuilt/copied Idle or an FM-owned pending/failed/successful build. It freezes
the original FT/service/remote incarnation and retries on native 10/60-second
cadence. After Open, RA records FM removal ownership and InDrop intent before RAP dispatch.
After Open, ordinary RAP errors remain pending; successful removal emits RemoveReplicaReply,
and completed duplicate requests replay that reply without another user removal.
Only the matching original Primary/FT/remote success clears FM pending-remove.
Poll `peer_idle.fm_removal` (also root `idle_removal`), not diagnostic
`peer_idle.state=removed`, to determine FM completion.

Removal cancels pending FM add, fences late build and queued AddReplicaReply,
and retains terminal same-incarnation tombstones. A lost RemoveReplicaReply
leaves FM pending even after diagnostic RAP success; frozen retry obtains replay.
Unresolved original-session loss leaves unknown/pending-remove, never recovery.
Local removal cannot take over an FM-owned removal. The destination lifetime,
data, registration and usage remain intact until separate deletion or normal shutdown; FM completion
does not imply remote deletion or active configuration change. Native
down/arbitrary-not-found cleanup and replacement remain gated
([RA:2044-2140,4094-4149](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L2044-L2140),
[FM:201-214](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnitMessageProcessor.cpp#L201-L214)).

For the original pending FM creation, RA follows native
`ReconfigurationAgent.cpp:2092-2106`'s InCreate branch and
`FailoverUnit.cpp:360-364`: retire creation bookkeeping and reply immediately,
**without RAP removal or InDrop**. The success reply echoes FM's exact
InBuild=1/empty-endpoint descriptor; the post-Open RAP path still replies with
Dropped=3 and its actual endpoint. FM accepts the exact echo for the original
pending incarnation and reports `completion=creation_cancelled`, separately
from `completion=replicator_removed`. Both require the actual RemoveReplicaReply
to clear pending-remove; reply loss still requires frozen retry/replay.

InCreate means no accepted CreateReplicaReply at the Primary, not proof the
destination never opened. The bounded profile retains a session-only
incarnation tombstone, ignores late CreateReplicaReply success/error and build
completion, and cannot restart the cancelled FM add. An already dispatched
CreateReplica can still Open the destination before or after FM cancellation;
that lifetime remains owned, Idle/non-serving and normally shut down, not
silently deleted. If the Primary accepted Open first, ordinary RAP removal
applies instead. Removal before any original pending FM Add, manual pre-Open
removal and arbitrary unknown/replacement remain gated.

After an original CreateReplicaReply error, native
`ReconfigurationAgent.cpp:2898-2910` calls AbortAddIdleReplica
(`FailoverUnit.cpp:348-352`) and replies to FM with the failure. The bounded RA
retains the original failed observation as a session-only tombstone, not live
creation authority; late success cannot start build or replace it. FM follows
`FailoverUnitMessageProcessor.cpp:64-89`: keep the Add error, mark
`to_be_dropped=true` and create the original pending-remove obligation.
Its frozen RemoveReplica uses the original FM Add descriptor, not a possibly
different failed reply's endpoint/state/version. The native absent-replica branch
(`ReconfigurationAgent.cpp:2070-2080`) echoes that exact descriptor without
InDrop or RAP, and duplicates replay it.

Only the matching semantic success clears pending-remove and reports
`completion=failed_creation_retired`. The FM add remains failed, including after
cleanup; a lost reply remains pending and uses normal 10/60-second retries.
`remove_idle_replica_via_fm()` explicitly resends the already-owned obligation.
`cleanup_required=false` describes completed Primary/FM bookkeeping only:
`to_be_dropped` and `destination_cleanup_required` remain true, with
`destination_deleted=false`. No destination disposal, active membership or
replacement is inferred. Arbitrary unknown replicas and later incarnations
still reject before dispatch.

### Unexpected opened destination app-host loss

The original peer-created Idle now reports actual AppHostDown observed while
its Open has completed (RA Ready, still Idle/InBuild). The RA retains this
specific origin; local-only Idle, opening/failed creation and arbitrary closed
peer lifetimes do not gain terminal-report authority.

Normal RAP Close can now retain that original open origin through Closing.
Only a Close started from the committed opened Idle with its runtime/usage
records this lineage; a Close during initial Open does not qualify.
Native `ReconfigurationAgent.cpp:1144-1181`,
`FailoverUnit.cpp:4831-4840,5422-5424,4948-5014` allow AppHostDown to supersede
pending Close and make volatile host loss terminal rather than retrying it.
If HostDown wins, the same loss report/ACK/metadata path applies. Late
ReplicaCloseReply is stale once the FT is closed
(`ReconfigurationAgent.cpp:3903-3924`), never successful-Close disposal proof.
If Close commits first, later HostDown is stale and does not reclassify it.

Native `FailoverUnit.cpp:5472-5544` calls OnReplicaDown for AppHostDown, even
though a volatile FT closes as dropped. RA therefore retains **ReplicaDown
stage** and sends the canonical description in ReplicaUp's **dropped list**.
The host-loss commit must apply before releasing the exact original Hosting
usage or publishing report work. The original node adapter sends immediately
and retains the unchanged native 30-second retry while unacknowledged.

FM admits only the original destination on its live node session, with the
frozen original FM Add and exact partition/service/epoch/replica lineage.
This also permits an opened destination's report before FM receives AddReplicaReply;
the report never supplies serving or build success. Native
`fm/ReplicaUpdateTask.cpp:68-93,302-337` returns the entry in the **processed
list** of ReplicaUpReply. The committed ACK follows native closed Delete and
DeleteReplicaReply, without RAP Close. A dropped-list ACK instead follows
closed ForceDelete without that reply. FM can retry normal metadata Delete
using the original Idle descriptor on its existing 10/60-second cadence.

Status records `state=lost`, `destination_lost=true`, `ready=false`, with
`native_replica_deleted` tracking the separate metadata reply. Historical Add
replies/errors remain retained, but late success cannot restore readiness or
restart creation. Physical cleanup is still owned independently:
`destination_cleanup_required` remains true, with no retirement/disposal,
absence, cancellation or failed-Open receipt and no reuse/replacement.
Primary pending-remove is unchanged until its own RemoveReplicaReply; the
existing explicit FM removal API can also retire the Primary session after
the destination loss. No automatic reconfiguration/recovery is introduced.

Real two-child acceptance kills the exact destination app while both original
RA/FM sessions and the healthy Primary remain alive. It holds Primary removal,
rejects a stale ACK, waits the actual 30-second report retry, loses/replays the
metadata Delete reply, then verifies independent Primary removal and writes.
Model/store tests cover pending/acknowledged Add, unrelated reports, independent
owner loss and separate failed/unknown loss/ACK commits. These are source-derived
and native-free process/Channel results, not native C++ handler execution or
FM/Federation interoperability.
Separate controlled owned-Node/production-adapter/process/Channel acceptance
holds provider or service disposal during an actual normal RAP Close, then
kills the original app. Provider disposal can block before the service Close
callback begins. No completed disposal is inferred from partially completed
callbacks, HostDown's exact usage release or metadata deletion. Stale ACK,
metadata reply replay, late Create/Add and independent Primary removal/writes
retain their existing barriers. Model/store tests cover Close versus HostDown
winning, late Close replies, and separate Close-origin/loss/ACK commit failures
and uncertainty. This adds no child-node Close-control feature or wire schema.
Opening loss, other pending-close modes, broader down/arbitrary-unknown cleanup
and replacement remain gated.

### Deleting the original opened destination Idle

`Cluster::delete_idle_replica_via_fm()` independently authorizes normal
`DeleteReplica(force=false)` for the already-opened original destination after
FM abandonment has been requested. It reuses stateful Delete/RAP Close,
canonical dropped DeleteReplicaReply and original disposal/accounting proof;
it is not another Primary-side RemoveReplica operation.
Native `fm/PendingTask.cpp:124-157` schedules pending-remove at the Primary
and to-be-dropped deletion at the destination independently, without a
RemoveReplicaReply-before-Delete barrier. `FailoverUnitMessageProcessor.cpp:201-255`
clears pending-remove only on RemoveReplicaReply, but processes successful
DeleteReplicaReply as dropped/deleted. `Replica.cpp:300-306` then clears
to-be-dropped. The original failed Add outcome is retained, not converted to Ready.

The bounded ledger freezes the original FT/service/target incarnation and
retries Delete every 10 seconds, backing off to 60 seconds after the first
minute. Only the matching original destination's canonical dropped reply sets
`peer_idle.fm_deletion.destination_deleted` (also root `idle_deletion`).
The separately validated original-runtime disposal/Applied-Close/exact-H2-release
receipt sets `disposed`. Both are required for `state=deleted`/`complete=true`;
arrival order is irrelevant, and a lost semantic reply remains pending despite
physical disposal. Delete completion does not clear pending-remove or authorize
a fresh lifetime. The destination app host, factory, package and tombstone remain
resident with `reusable=false`; normal shutdown still owns their cleanup.
Destination-session loss leaves unresolved deletion unknown; source-session
loss does not supply or undo destination completion.

RA admits only this already-opened peer-created Idle and its original deletion
retries. Native DeleteLocalReplica epoch/ReplicaId checks still apply; an older
FM replica instance does not block deletion of the matching local ReplicaId.
Delete intent and Close completion remain behind their own commits. The reply
uses native None/None/Dropped/down metadata, invalid version/LSNs and no endpoints.
Normal deletion schedules no ReplicaDown/ReplicaDropped report. Same-incarnation
CreateReplica after deletion is ignored without Open, build or usage acquisition.
Force, arbitrary unknown/replacement,
active membership and recovery remain gated.

### Deleting the original registered destination during Open

The same API also admits the original Idle while RAP Open is in flight, once
the committed RA state owns its runtime registration and original usage lease.
Pre-registration Opening uses the separate cancellation proof below.
Normal Delete commits deleted/Closing
intent and sends Drop-flagged RAP Close without waiting for Primary Remove.
Native `FailoverUnit.cpp:4916-5028` clears Open-pending and retains Close-pending;
`ReconfigurationAgent.cpp:3782-3828` ignores late Open replies.
`ReconfigurationAgentProxy.cpp:451-568` cancels incompatible work and relies on
RA Close retries after the previous operation drains, rather than acknowledging
cancellation as cleanup. The existing Node retry cadence remains 15 seconds.

The runtime retains a control returned by a cancelled service Open solely for
cleanup, aborting that replicator before the service through their existing
interfaces. It does not open the control, publish endpoints or grant access.
Delete's canonical reply and original runtime disposal/Applied-Close/exact
usage-release proof still join independently. Admission freezes this branch
as requiring disposal: neither retry nor the unanswered Add may downgrade it
to `never_created`. Primary pending-remove needs its own RemoveReplicaReply;
the destination tombstone grants no replacement.

Real application-process/Channel tests hold registered service Open after real
provider/replicator construction, observe Close cancellation, then independently
hold provider or service disposal. They verify retained usage and no early
reply/proof, native Close retry, lost Delete reply/replay, late Create/Open
fencing, no build and a writable original Primary. Public Cluster acceptance
uses both actual child adapters and the production retirement publisher.
Definite/unknown commits retain their original publication barriers.
This adds no native wire shape or native C++ handler qualification.

### Deleting the original destination during pre-registration Opening

The original unanswered FM Add may also delete an Idle whose CreateReplica has
committed Opening but has not adopted runtime registration/usage.
Native `FailoverUnit.cpp:4980-5028` marks deleted, clears Open-pending and
immediately finishes Close for InCreate without ServiceTypeRegistration.
No RAP Open/Close or user-service disposal is invented. The reply retains the
created FT's original 1/1 description, not the missing-FT constructor's zero
target/minimum, and uses the same canonical dropped replica metadata.
Late acquired lookup results are consumed as stale and release their exact
registration/usage ownership without authorizing RuntimeOpen.

The original Node captures the actual creation token and applied Delete
command/ticket lineage. A distinct `pre-open-cancellation.json` sidecar is
published only from its live original owner, unfenced deleted tombstone and
fully drained commands/effects/lookups/acquisitions, with no runtime route or
disposal witness. FM joins it with DeleteReplicaReply in either order:
`completion=pre_open_cancelled`, `pre_open_cancelled=true`, `disposed=false`,
`never_created=false`, `created_lifetime=false`, `reusable=false`.
The original FT/lookup history is not relabeled as zero-history absence.
Native metadata success alone cannot certify that a late acquisition drained.
Primary pending-remove still requires its own RemoveReplicaReply.

Admission freezes the observed target branch. A registered lifetime requires
disposal; observed pre-registration Opening cannot downgrade to missing-FT
absence on retry. An unobserved original creation permits the corresponding
source-bound proof for the actual outcome. If registration wins the race,
genuine disposal remains required. The known pre-Open registration failure
branch below is separate; other failed/unknown lifetimes, force,
arbitrary targets, replacement and recovery remain gated.

Real-process/Channel acceptance holds the original FT before lookup adoption,
then holds an actual acquired lookup after the native Delete reply. It proves
no early local proof, exact late release, lost-reply retry/replay, stable
tombstone/lineage and no callback/build resurrection. Public Cluster acceptance
holds original factory registration, drives both real child adapters and the
production cancellation publisher, then releases registration after completion.
The Primary remains writable. Definite/unknown commits preserve their barriers.
The new receipt is trusted-local metadata, not a native wire schema or evidence
of native C++ handler execution/interoperability.

### Deleting the original destination after pre-Open registration failure

Normal FM Delete also handles the original Idle after definitive registration
failure, while preserving the original failed Add outcome. Native
`FailoverUnit.cpp:5212-5235,5610-5657` closes failed creation; the ordinary
closed Delete path (`ReconfigurationAgent.cpp:777-834`,
`FailoverUnit.cpp:4916-4940`) then marks the retained FT deleted and replies
without RAP Close. The created FT keeps its 1/1 description. Only failure
before runtime/usage adoption records this bounded origin; a later registration
error after runtime loss does not qualify.

The original Node retains the applied registration command/ticket, original
creation token, issued lookup and native error. After applied Delete it joins
that lineage with its separate Delete command/ticket and waits for unfenced,
drained lookup/usage/command/effect ownership, no runtime route and the live
original app/node. `failed-registration-deletion.json` is a distinct local
receipt, not a Close/disposal or zero-history absence receipt.
FM requires the original failed Add and freezes its error at deletion admission;
only the matching receipt plus DeleteReplicaReply reports
`completion=failed_registration`, `failed_registration=true`,
`disposed=false`, `never_created=false`, `pre_open_cancelled=false`.
`created_lifetime=false` and `reusable=false` remain explicit.
The failed Add/error survives; Primary pending-remove needs its own reply.
The committed Delete also retires the matching failed-create report placeholder
without claiming runtime cleanup or inventing a report acknowledgment.

Real-process/Channel acceptance holds actual factory registration and uses the
existing native dynamic registration Drop threshold of one for qualification;
production defaults are unchanged. The real Hosting lookup fails, the actual
peer error is forwarded as AddReplicaReply, then lost Delete reply/replay and
late factory registration/Create preserve the tombstone, failure and healthy
Primary writes. Model/store tests cover late acquired offers, definite/unknown
Delete commits, mismatched lineage/error and independent owner loss. This is
controlled owned-Node/production-adapter acceptance, not a new child-node
policy-control feature or native C++ handler/interoperability qualification.
Unknown/missing failure lineage, other absent/down
branches and replacement remain gated.

### Deleting the original destination after terminal runtime Open failure

The same API admits the original Idle after a definitive runtime Open failure.
Native `FailoverUnit.cpp:5212-5235` treats terminal Open failure as already
closed by RAP; `StartCloseLocalReplica` at `4916-4940` then marks closed
metadata deleted without another RAP Close. The existing Rust runtime awaits
replicator-before-service Abort and final resource release before its error
ReplicaOpenReply. A failed reply alone is not destination cleanup.

The original Node captures that exact eligible reply's runtime/host, original
creation token and paired lookup/acquisition. Only the Applied terminal Open
completion and exact usage release validate the failure; a separate Applied
Delete and drained original local work permit `failed-open-deletion.json`.
No successful Close command, pre-Open failure, cancellation or missing-FT
absence is fabricated. A nonterminal failure, host crash, fenced/unknown
outcome or missing original ownership cannot supply this proof.

FM freezes the original failed Add error and requires this distinct receipt
plus the canonical DeleteReplicaReply, in either order. Completion reports
`completion=failed_open`, `failed_open=true`, `disposed=true`, with all
absence/pre-Open-cancellation/registration-failure flags false.
The failed Add/error survives; Primary pending-remove still requires its own
RemoveReplicaReply. Deleted metadata suppresses only the matching failed-create
report placeholder. No replacement or reuse is authorized.

Controlled owned-Node/production-adapter/process/Channel acceptance fails the
initial Idle service role callback inside real RAP Open, after actual data
replicator construction/Open. Independent provider and service disposal holds
prevent early error/proof publication. The existing dynamic Open Drop=1
threshold selects the terminal native branch for qualification; production
defaults are unchanged and no child policy-control feature is added.
Lost Delete reply/replay, late Create/Add, retained error and healthy Primary
writes are exercised. Model/store tests cover separate failure/Delete commits,
definite failure, uncertainty, foreign lineage/proof conflicts and owner loss.
This adds no native wire schema or C++ handler/interoperability qualification.
Opening/other-close-mode host-loss and other failed/unknown absent/down cleanup
remain gated.

### Deleting the original destination before CreateReplica

For an original unanswered FM Add whose destination has no FT, the same
`delete_idle_replica_via_fm()` API now authorizes normal deletion after FM
abandonment intent, without waiting for Primary RemoveReplicaReply.
This follows `ra/ReconfigurationAgent.cpp:777-834,4279-4293`,
`ra/FailoverUnit.cpp:49-73,366-391` and
`common/FailoverUnitDescription.cpp:11-29`: TryCreateFailoverUnit constructs a closed FT from
ID/consistency/service, then UpdateInstanceForClosedReplica installs the
original replica identity/current epoch before normal Delete.
Its committed reply uses that native FT description, with previous epoch,
target and minimum left at their constructor defaults (zero), and a canonical
None/None/Dropped/down replica with invalid package/LSNs and no endpoints.
It is not an echo of FM's original InBuild/1/1 descriptor, a RAP operation or a
service disposal.

The original Node retains the actual applied Delete command and RA-operation
identities. `replica_absence()` publishes that distinct trusted-local proof only
while the committed deleted tombstone has zero Open/Close tokens, no runtime,
lookup or usage history, no RAP route/disposal witness and fully drained local
work. The source-bound `absence.json` sidecar is independent of native messages
and checked against the original node/session/generation, process/profile and
frozen request. FM must join it with the matching canonical DeleteReplicaReply;
either arrival order is valid. `peer_idle.fm_deletion` / root `idle_deletion`
then reports `completion=never_created`, `never_created=true`, `disposed=false`
and `state=deleted`/`complete=true`. There is no forged Close/disposal/H2-release
lineage. `created_lifetime=false`, no retirement receipt and `reusable=false`
remain explicit even though native replica metadata is deleted.

Late same-incarnation CreateReplica follows the native closed/equal-instance
branch (`ReconfigurationAgent.cpp:2283-2323`) without opening; the locally
deleted FT suppresses its dropped report
(`FailoverUnit.cpp:4015-4037,4720-4738`). Retries replay the original Delete reply
and retain the original absence lineage. Definite/unknown commits cannot
publish it early. Destination-owner loss leaves unresolved cleanup unknown;
Primary pending-remove still requires its own RemoveReplicaReply.
Only the original unanswered FM Add permits absent cleanup. An already-opened
original or registered in-flight Open still uses genuine disposal proof;
pre-registration Opening uses the distinct cancellation proof above.
Other failed or unknown absent lifetimes, force, arbitrary targets and later incarnations remain
gated. No fresh lifetime, restart recovery or active membership is authorized.

Controlled real application-process/Channel acceptance holds the original
CreateReplica, loses the first actual DeleteReplicaReply, publishes/validates
absence and retries for identical semantic replay before delivering late
creation. The target performs zero create/Open/Close/provider-acquisition
callbacks and retains zero usage, while the Primary can acknowledge/read its
next write. A second public Cluster API test delays only original peer binding:
both actual child processes run the production adapter and absence publisher;
Delete completes even while the queued peer request holds back the independent
Primary reply. Binding the original peer then delivers late creation without
resurrection and allows FM's own pending-remove reply to complete. These are
native-free process tests, not native C++ handler traces.

Before explicit deletion, creation/build deliberately retains Idle/InBuild metadata, no service endpoint,
denied read/write access and no active membership or quorum change even after
copy completes. Manual build sends no AddReplicaReply; only the distinct
FM-owned flow above emits that reply. The real-process acceptance writes
before build, observes nonempty copied values and retry results on the actual
destination, then proves a second write is applied/ACKed there while the peer
remains unconfigured and non-serving. `write-more\n` and `state\n` are bounded
sample controls for that observation, not RA authority or recovery APIs.
Additional real-process acceptance removes the Idle before and after real
nonempty copy, proves the Primary can still acknowledge/read a subsequent
write, and verifies that the destination remains opened with unchanged copied
data rather than being deleted or recreated. Controlled RAP/model cases cover
failed builds, in-flight cancellation/retry, removal failures, stale replies,
absent-replica normalization and delayed-build tombstones.
FM-owned actual-process acceptance additionally drops the first real
AddReplicaReply, proves FM stays pending, retries the same AddReplica to receive
the cached Ready-Idle result, and checks ongoing replication without another
destination Open. A separate removal race rejects late success while preserving
the destination service/data and a writable original Primary.
FM removal acceptance drops an actual RemoveReplicaReply, proves pending-remove
survives child removal telemetry, retries the original request for replay and
rejects delayed AddReplicaReply resurrection. Both unbuilt and copied manual
Idles also qualify FM removal without disposing the destination.
Controlled peer-channel tests retain the actual original CreateReplica or its
real Open reply across cancellation, replay the FM reply and deliver late Open
through the original session. They prove no RAP build, one intact destination
Open and continued Primary writes/reads in both orderings.
The failed-creation variant injects a correlated native error on the original
peer channel, consumes the actual forwarded AddReplicaReply at FM, drops the
first removal reply and retries it. Delivering the held original CreateReplica
after cleanup produces a real late Open that cannot trigger build or Ready.
Both original application processes remain owned and the Primary stays writable.
This is controlled wire-error/native-free process evidence, not execution of a
native C++ Open-failure handler or proof of destination disposal.
Separate destination-deletion acceptance holds actual provider and service
disposal independently, proving no early reply/release while the Primary can
still write. It loses the first real DeleteReplicaReply, supplies original
disposal proof, retries for the identical semantic reply and completes Delete
while FM still awaits RemoveReplicaReply. Delayed original CreateReplica and
AddReplica cannot reopen/rebuild or acquire another use. An additional public
Cluster API acceptance deletes an Idle containing real nonempty copied data,
joins the actual child-published retirement receipt and preserves Primary
writes/readback. These are native-free application-process/Channel tests, not
native C++ deletion-handler execution.

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
