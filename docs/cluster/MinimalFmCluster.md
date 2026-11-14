# Minimal native-free FM cluster

**Profile:** one foreground in-memory FM, two separate RA+Hosting processes,
and one fixed stateless application profile. Explicit placement only; no
automatic recovery, reconnect, failover or native FM/Federation parity.
An opt-in [healthy three-replica profile](#healthy-three-replica-service) runs
three original RA+Hosting processes for one volatile partition at target/minimum
3/2. The default and existing two-node profiles are unchanged.

The explicit `--stateful-singleton` variant now enables fresh FM-authorized
volatile 1/1 Primary creation, using the separate `hosting-local-app` package.
Each of the two configured nodes can host one independent singleton partition;
this does not form a multi-replica set. Stateless remains default.

The [launcher guide](../../crates/tools/fube-cluster/README.md) defines invocation,
bounded JSON commands, independent application contact and cleanup receipts.
It uses the production local Node/RA/Hosting/runtime, memory store and common
transport engine, not simulated application activation.

FM/peer decoding, topology/capability admission and command dispatch live in
[`fube-node::control::ingress`](../../crates/libs/fube-node/src/control/ingress.rs).
Both this launcher and the standalone external-FM runner use that boundary;
neither implements its own RA reconfiguration state machine. Launcher-specific
parent control, host-replacement receipts and process/Channel ownership remain
outside it. Backpressure retains the original incoming frame and unacknowledged
handoff rather than closing the FM connection.

## Immutable configured initial membership

The configured volatile profile is selected with
`--stateful --target-replica-set-size N --min-replica-set-size M`, or
`ServiceProfile::Stateful { target: N, minimum: M }`. It creates one partition
on N original nodes, validates `1 <= M <= N <= 64` before acquiring a deployment,
and freezes both settings for that deployment. The 64-member ceiling is a Rust
resource bound, not an ordinary-service restriction from native SF. Existing
bounded serialization, metadata, command and output limits remain in force.

The existing node1 `create` command opens the Primary, then the foreground
controller builds each original Secondary with AddReplica/CreateReplica and
real RAP copy before one unchanged-Primary DoReconfiguration. Successful
Ready/Idle replies are not active membership. All exact peer Activate replies,
their applied RA commits and final RAP EndReconfiguration/FM reply must complete
before the retained create result becomes complete. A local `observed: running`
Primary is not that service-level completion. Settings are present in every
initial Open/build body, remain unchanged through admission and do not cause
a service update-version increment. Only configuration epochs advance.

Source correspondence at `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:

- `ServiceModel/reliability/failover/ServiceDescription.cpp:239-258` validates
  positive stateful minimum and minimum no greater than target.
- `fm/PlacementTask.cpp:43-117` computes placement difference from desired
  target separately from existing/in-build membership.
- `fm/ReconfigurationTask.cpp:377-418` admits ready Idles using the service's
  configured minimum and removes excess members relative to target; admitting
  members is not a service-description resize.
- `ra/FailoverUnit.cpp:5962-5973` carries configured service sizes into the FT
  description; `fm/FailoverUnitConfiguration.h:64-68` derives read/write quorums
  from configuration membership rather than treating minimum as a quorum override.

RA and Node accept validated initial configured sizes and generic ready P/P +
I/S vectors. The existing per-peer activation accounting, exact owner/endpoint
checks, separate PC/CC projection and commit barriers are reused. The configured
adapter rejects incoming size changes and keeps unsupported lifecycle operations
explicitly rejected. Original per-peer Channels and process cleanup receipts
remain deployment/session/node bound; clean handoff now accounts for variable
child counts without relaxing physical-owner cleanup.

Acceptance covers `1/1`, `2/1`, `3/2`, `4/2`, `4/3`, `5/3` and `4/4`, real copy
and application replication, blocked initial writes below minimum, lost final
configuration-reply replay and clean original-owner shutdown. Four-member
quorum checks distinguish configured minimum from actual majority: `4/2` cannot
acknowledge with only two live members, while `4/4` can still acknowledge with
three live members in its unchanged four-member configuration. RA tests pause
each activation commit and reject malformed membership/ownership. Foreground
acceptance builds four replicas from one create and relaunches only after clean
handoff. These checks do not qualify every size up to 64.
The RA model also exercises all 63 remote activation commits at target/minimum
64/32; effect reservations scale with bound peer count before persistence.
A native-shaped target-changing request that the legacy two-member retirement
path could accept is rejected by the immutable profile's actual ingress.

This initial slice deliberately waits for all original build destinations and
admits them together. The failover extension below adds loss-driven promotion.
Incremental/partial admission, resize, generic planned transfer,
healthy Secondary retirement, whole-node/FM restart and persisted recovery remain
unsupported for this new profile. The older named profiles remain separate
qualification fixtures with their existing behavior; their bounded lifecycle
support is not implicitly enabled for arbitrary configured sizes.

## Immutable configured Primary failover

After completed initial admission, a matched native terminal report for the
Primary's app-host can start no-data-loss failover for the configured
profile. The original FM, Nodes/RAs, stores and peer Channels remain owned.
FM-link disappearance is not proof of Primary failure. No new service Open,
host adoption, private data-engine access or size update is substituted for
the normal RAP callbacks.

FM keeps PC from the last committed membership and removes the failed Primary
from CC. Other unavailable Secondaries can leave CC only while its count is
above the configured minimum, matching `fm/ReconfigurationTask.cpp:325-344`.
Desired target/minimum and service update version remain unchanged. Available
members must cover PC's read quorum before this no-data-loss-only adapter
starts promotion; it does not use minimum as a read/write-quorum override.

RA retains one GetLSN result/history per exact replica incarnation. Phase 1
checks PC and CC read quorums separately, waits for up replicas for the native
dynamic 30-second progress-query duration, and never converts timer expiry into
quorum success. Candidate selection filters older deactivation primary epochs
before comparing last LSN, then considers first-LSN catch-up capability.
Different survivor LSNs no longer reject an otherwise valid failover.

If another member is preferable, RA commits ChangeConfiguration with initialized
LSNs **only for that selected member**, reverts its local promotion intent and
does not call RAP to promote itself. FM fences the exact configuration/source,
advances the Primary epoch and sends DoReconfiguration to the new candidate,
which performs its own GetLSN round. Exact duplicate reports replay the current
round without another epoch bump. Candidate-report retention is bounded by the
configured node count.

The native FM still emits ChangeConfigurationReply, but pinned RA marks that
action Deprecated. The Rust adapter explicitly rejects/logs it and exposes
`deprecated_change_configuration_reply.state = rejected`; no RA state command
or retry-completion gate depends on it. New GetLSN/DoReconfiguration traffic,
not this legacy reply, advances the redirected transition.

Catch-up uses the existing runtime projection of separate PC/CC configurations.
Every required up Secondary's metadata-only Deactivate reply and Activate reply
is accounted independently, after its applied RA commit. Retries target only
outstanding members. Final RAP EndReconfiguration and the committed native FM
reply precede publishing the new Primary endpoint. A lost final reply replays
completion without another Open or promotion.

Native correspondence at the pinned revision:

- `ra/FailoverUnit.cpp:1929-2202`: eligible progress, deactivation-epoch filtering,
  highest-LSN selection and first-LSN catch-up capability.
- `ra/FailoverUnit.cpp:2878-2926,2968-3055`: ChangeConfiguration versus local
  catch-up, separate PC/CC read quorum and progress-query waiting.
- `ra/FailoverUnit.cpp:3123-3248,4153-4188`: phase-3/4 member accounting and
  selected-candidate-only LSNs in ChangeConfiguration.
- `ra/FailoverUnit.cpp:3543-3575`: revert candidate roles without rolling back
  the FT epoch fence.
- `fm/FailoverUnitMessageProcessor.cpp:529-660`: candidate change and new epoch.
- `ra/Communication.MessageMetadataTable.cpp:46` and
  `ra/MessageHandler.cpp:161-165`: explicit deprecated-reply rejection.
- `common/FailoverConfig.h:633`: dynamic progress-query wait default.

Real-process acceptance covers `2/1`, `4/2`, `4/3`, `4/4`, `5/3` and an
insufficient-PC-quorum `5/1` case. It verifies nonempty data continuity,
unavailable Secondaries, final-reply loss/replay, unchanged settings, original
owners/one Open and cleanup. `4/4` can complete reconfiguration after Primary
loss but cannot acknowledge a new write below minimum; retaining a down CC
member at `4/3` does not replace the configuration-derived quorum.
Separate process acceptance injects a native candidate-change FM input to
verify new-candidate routing and deprecated-reply rejection without reopening
survivors; it is not evidence that real differing runtime histories triggered
selection. RA models cover unequal/older-history progress, catch-up capability,
redirection/rejoining, read-quorum blocking, expiry and every survivor commit,
including 64/32. The 64 bound is not 64-process qualification.

This adds no resize, automatic placement/restart, whole-node/FM recovery,
data-loss exchange or persisted recovery. Another configured-profile failover
requires the explicit repair below; losses/supersession during a transition
remain gated.

## Immutable configured Primary repair

`Cluster::repair_primary_replica()` now extends the existing named-profile
repair path to immutable configured sizes. After completed no-data-loss failover,
the failed Primary's native loss/deletion must be processed, all original Node/RA
sessions must remain connected and every other member must still be running.
Only one missing member is admitted; failover with additional unavailable
Secondaries does not authorize partial or multi-loss repair.

The same physical predecessor boundary described in
[third-replica repair](#post-failover-third-replica-repair) is retained: exact
Closed/deleted RA ownership, original process exit/reaping/group drainage,
report/command/lookup/acquisition drainage and old Hosting-root removal precede
a new host gate, app process and runtime registration. FM validates the original
Node's exact replacement receipt and independently observes old PID/root
absence. The successor receives a replica ID/incarnation above every retained
identity. A redirected Primary epoch is accepted without weakening partition,
source, service, data-loss epoch or newer-incarnation fences.

The current Primary owns native AddReplica/CreateReplica and real RAP
Open/BuildIdleReplica, copying nonempty state into the fresh volatile Idle.
Target/minimum and service update version remain unchanged. After semantic
Ready/Idle completion, same-Primary DoReconfiguration retains every survivor
in PC and admits exactly one fresh I/S to CC. Each required S/S Deactivate
reply commits independently before activation; retries address only outstanding
members. A 2/1 repair has no remote PC Secondary to deactivate. Every CC
Secondary's Activate reply and final RAP EndReconfiguration/RA commit precede
FM repair completion. Poll `repair.ready`, not `host_ready` or `built`.

Completed repair authorizes another matched current-Primary failover using the
latest membership/history. Candidate-redirection retention is scoped to the
current failover round, so stale reports cannot consume a later round's budget
or replay its legacy reply. Existing predecessor reports, Delete requests/replies,
configuration requests and replacement controls remain fenced across cycles.
Resize and automatic placement/restart remain deferred.

| Pinned SF source | Configured repair responsibility |
|---|---|
| `fm/FailoverUnit.cpp:718-739`; `fm/Replica.cpp:555-585` | Reuse a dropped placement with a fresh identity and reset build metadata, independently of target/minimum. |
| `ra/ReconfigurationAgent.cpp:2244-2324` | A closed local lifetime creates a fresh Idle only for a newer incarnation; it does not recover the lost volatile runtime. |
| `ra/FailoverUnit.cpp:3091-3248` | Distinct PC/CC obligations and per-member phase-3/4 completion; skip no required survivor or commit. |
| `ra/ReconfigurationAgent.cpp:2626-2644,2766-2775` | Refresh committed deactivation history for subsequent failover rounds. |

RA and FM models cover configured repair through 64/32, preserving sizes and
rejecting changed owners, resize, stale/partial replies and survivor loss.
RA holds each admission, Deactivate, Activate and final completion commit.
Real-process acceptance runs three failover/repair cycles at 2/1, 4/2, 4/4 and
5/3, promoting a rebuilt replica and replacing one placement twice. It covers
nonempty recopy/continuing writes, lost build/final replies, old traffic,
unchanged original Node/survivor owners and quorum blocking after repair.
A 4/3 interrupted-second-repair case independently joins cleanup before FM
admits the new host. The native-input-injected candidate-routing test also
repairs after redirection and fails over to the rebuilt replica; it remains
routing/repair evidence, not live differing-history selection qualification.

This reuses existing wire contracts and adds no schema/action. The maintained
native-stream gate remains 179 cases/55 native truncation rejections, not full
native C++ handler/Federation interoperability. Sixty-four is a model/resource
bound, not 64-process qualification. Multi-loss or interrupted transition recovery,
node/FM restart, data-loss exchange and persisted
recovery remain unsupported.

## Standalone Secondary app-host loss repair

`Cluster::repair_secondary_replica(node)` explicitly repairs one matched
ActiveSecondary app-host loss while preserving the original healthy Primary.
The one-based destination selects an original live Node/RA session, not a new
placement. Initial admission or the preceding repair must have completed.
The old replica's loss report and native closed metadata deletion must be
processed, with every other member and original Node/peer session still owned.
FM-link disappearance alone cannot authorize replacement.

The pinned FM removes a dropped/down Secondary only while CC plus ready Idles
exceeds minimum. With no spare placement in this topology, `minimum == target`
returns an explicit blocked-at-minimum error. No size/service-version update,
forced quorum reduction or fabricated ready Idle substitutes for that rule.
Multiple losses reject new repair; insufficient live quorum cannot acknowledge
a write. This is explicit placement/replacement authorization, not automatic PLB.

First, same-Primary DoReconfiguration preserves PC from the last committed
membership and removes only the failed S/N from CC. Target/minimum and
data-loss/Primary epochs remain unchanged; only the configuration counter
advances. Real RAP catch-up precedes metadata-only S/S Deactivate, then
RAP-backed Activate for every required live survivor, each behind its applied RA commit.
Dropped members do not owe phase-3 replies. A two-member service can finish
removal with no remote Secondary, but still requires final RAP/FM completion.
The healthy Primary keeps its endpoint, runtime, acquisitions and single Open.

Only the exact successful removal reply permits physical host replacement.
The same predecessor exit/reaping/group-drain, reporting/command/lookup/usage
drain and Hosting-root removal fences used by Primary repair apply. A fresh
host gate, runtime registration and globally newer replica incarnation are
created on the original Node. Native AddReplica/CreateReplica and real
BuildIdleReplica copy into an empty volatile application. Existing RAP may
first remove the old custom-replicator resource; normal FM Add retries then
continue the fresh build, without a private data-engine hook.

Same-Primary admission retains every survivor in PC and adds exactly the fresh
I/S to CC. Secondary-only removal/admission retains existing deactivation
history, rather than inventing a new Primary catch-up boundary. Final RAP,
per-Secondary activation and RA/FM completion restore target membership.
Poll `repair.ready`: `removed`, `host_ready` and `built` are intermediate states.
The current repair and retained predecessor histories fence late reports,
Delete traffic and stale configuration/build replies across repeated cycles.

A new loss during removal or admission makes repair unknown. A late successful
reply cannot advance it or start an unproved replacement. The bounded
[Primary-loss supersession](#primary-loss-during-secondary-removal) below can
replace removal before host replacement; other overlapping recovery stays gated.
Known in-flight report epochs remain admitted as terminal loss,
not relabeled as an FM-link failure. Existing data-plane quorum/access checks
remain independent of this uncertain control-plane result.

| Pinned SF source | Secondary-loss responsibility |
|---|---|
| `fm/ReconfigurationTask.cpp:354-412` | Remove dropped/down Secondaries subject to minimum and ready-Idle accounting; admit built Idles under the unchanged Primary. |
| `fm/FailoverUnit.cpp:718-739`; `fm/Replica.cpp:555-585` | Reuse a dropped placement only with a fresh identity and reset build metadata. |
| `ra/ReconfigurationAgent.cpp:2244-2324` | Create a newer Idle from closed metadata; do not recover the lost volatile runtime. |
| `ra/FailoverUnit.cpp:736-810,2596-2646,3091-3248` | Retain same-Primary deactivation history, skip dropped phase-3 members, and account for required live phase-3/4 completion. |

RA/FM models cover metadata removal and fresh admission through 64/32,
including unchanged history, commit holds/veto, exact survivor/incarnation
fences, stale replies and minimum rejection. Real-process qualification runs
two Secondary replacements at 2/1, 4/2, 4/3 and 5/3, then Primary failover to
the rebuilt replica and repair of the old Primary. It loses/replays removal,
build and final admission replies, preserves nonempty data and original owners,
and verifies continuing acknowledged writes and independently joined cleanup.
Separate 4/4 minimum, 4/2 and 5/3 quorum-loss, and further-loss-during-removal
cases reject unsafe progress without weakening write quorum.

No new native schema/action is introduced: the source-stream gate remains
179 cases/55 native truncation rejections. This is source-derived model/process
and native-stream/schema-adapter evidence, not full original C++ handler or
Federation interoperability. Multi-loss repair, spare-node placement, automatic
restart, broader interrupted-transition recovery, resize and node/FM restart remain
deferred. The 64 bound is model/resource qualification, not 64-process testing.

## Primary loss during Secondary removal

If the current Primary app host is lost while an explicit Secondary removal
still awaits its final FM reply, `drive()` can supersede that removal with
no-data-loss Primary failover. The original Secondary loss/deletion must already
be processed, no replacement host may have been requested/admitted, and every
other original Node/RA session and surviving app host must remain live.
FM-link loss, another Secondary loss, completed removal, and interrupted
copy/readmission do not authorize this path.

SF distinguishes this from failover out of stable membership. It preserves
the removal's original PC epoch/roles and CC member set, increments the
current epoch by `0x1_0000_0001`, and swaps the failed P/P to a down P/S
while promoting one live S/P. The previously removed S/N stays out of CC;
the failed Primary is not also removed from CC. Target/minimum, service
version and data-loss epoch remain unchanged. FM checks separate PC/CC read
quorums, then RA independently obtains real per-member progress/history and
uses the existing history-first selection, catch-up, per-survivor
Deactivate/Activate and final RAP/FM completion before publishing readiness.

Survivors may not yet have received removal, may be deactivated, may have a
pending RAP activation, or may have completed locally without FM receiving
the final reply. Each admits only the matching newer failover/query epochs
and exact retained owners. Secondary-only history is retained until the new
Primary catch-up establishes its own deactivation boundary. Old peer messages,
RAP completions, removal replies and retries cannot finish the new transition
or start the old replacement. `repair.state` becomes `superseded` with
`superseded_by` naming the failover epoch; `repair.ready` remains false.
Supersession restores a serving Primary, not target redundancy. The explicit
[sequential restoration](#sequential-restoration-after-supersession) below can
then repair the two retained predecessor placements.

| Pinned SF source | Supersession responsibility |
|---|---|
| `fm/ReconfigurationTask.cpp:40-48,225-259,320-352` | Reconfigure a lost Primary during an existing transition; retain PC and the CC set, independently checking their read quorums. |
| `ra/FailoverUnit.cpp:624-672,1737-1751` | Accept newer GetLSN epochs, query the replicator's real status and retain the incoming PC when starting failover. |
| `ra/FailoverUnit.cpp:1929-2202,2878-2926,2968-3055` | Select using retained history/progress and preserve separate PC/CC quorum obligations. |

RA models cover the four survivor states above at 5/3 and 64/32, commit
holds/veto, stale completions, exact query epochs, and a CC read quorum that
cannot substitute for the larger PC read quorum. FM models additionally cover
4/2, absent quorum at 3/2, FM-link loss, another Secondary loss, host-request
and completed-removal fences. Real-process 5/3 cases lose removal delivery,
interrupt phase 3 with deactivated survivors, and hold the final FM reply.
All retain nonempty data, acknowledge a later write, preserve surviving
app/runtime/Node ownership and single Open, reject stale replies, avoid
replacement and independently join shutdown cleanup. A 3/2 process case
cannot promote its lone survivor, even after the late removal reply arrives.
These are source-derived model/process results, not native handler or
Federation interoperability evidence. No native schema/action is added;
general interrupted recovery, general multi-loss repair, resize and node/FM restart
remain deferred.

## Sequential restoration after supersession

After completed Primary-loss supersession, the two exact processed predecessor
losses can be repaired explicitly, one at a time, on their original live
Node/RA sessions. Above minimum, `repair_secondary_replica(node)` first selects
the old Primary, now a down CC Secondary, then the previously removed Secondary.
At minimum, select the already-removed placement first to build a Ready Idle.
The opposite order and concurrent repair are rejected. This is not a general
multiple-loss placement/recovery API.

For the qualified 5/3 case, CC starts with four members, three live. Native
same-Primary removal drops the failed old Primary to reach three; fresh-host
copy and committed I/S admission restore four live members. Only after that
cycle completes can the already-removed placement be rebuilt and admitted,
restoring five live members. Target five, minimum three, service version and
the new Primary/data-loss epochs never change.

For 4/3, completed supersession instead leaves three CC members, two live.
`repair_secondary_replica(2)` rebuilds the already-removed Secondary with a
fresh host and real copy while CC stays unchanged. Only its matching successful
AddReplicaReply authorizes a same-Primary reconfiguration that retains the
three-member PC, removes the down S/N and admits the fresh I/S together. CC
still has three members, now all live; there is no intermediate two-member CC.
After that cycle completes, `repair_secondary_replica(1)` rebuilds the old
Primary's placement and admits the fourth member. This uses the two original
lost placements, not a new spare node, and does not change target/minimum.

Minimum constrains configured CC size, not the number of Ready survivors.
Two live members in the three-member CC can acknowledge writes with minimum
three when they have the actual write quorum. Ready-Idle-first restoration
preserves this behavior; it never lowers minimum or substitutes a copied Idle
for committed membership. SF's independent PC/CC write-quorum checks during
same-Primary reconfiguration remain in force
(`ra/FailoverUnitProxy.cpp:1399-1455`).

Every replacement retains predecessor exit/reaping/group/root cleanup,
report/lookup/usage drainage, a fresh Hosting gate/runtime and newer replica
identity. Closed Primary metadata from an interrupted removal is eligible only
with its exact terminal loss/deletion and a newer Primary's history; an old
in-flight stage is not converted into successful completion. CreateReplica,
RAP BuildIdleReplica and real copy remain the sole construction path.

Each cycle admits one fresh replica, not an entire target set. Ordinary
admission grows membership by one; combined removal/admission keeps its size.
It preserves every surviving owner and the completed configuration as PC.
Secondary-only removal/admission retain the serving Primary's deactivation history.
`repair.ready` means the selected placement completed its cycle;
`repair.target_restored` becomes true only at full target, and
`repair.remaining_lost` lists the remaining one-based placements. Final replies
cannot relabel an unrepaired placement Running. The serving Primary keeps its
endpoint/runtime and original survivors are neither reopened nor moved.
Another loss, including loss of a newly built incarnation, fences restoration
as unknown; late replies cannot resume it or authorize overlapping failover.

This follows pinned `fm/ReconfigurationTask.cpp:354-412` (minimum/Ready-Idle
accounting and one-member admission), `fm/Replica.cpp:555-585` (new identities),
`ra/ReconfigurationAgent.cpp:2244-2324` (newer CreateReplica from closed metadata)
and `ra/FailoverUnit.cpp:736-810,2596-2646,3091-3248` (same-Primary history and
per-live-survivor phase obligations). RA models restore partial/full membership
at 4/3, 5/3, 64/32 and 64/63 across all four preceding survivor stages, with
commit holds and exact-owner/history rejection. FM models additionally cover
4/2 and Ready-Idle-first restoration at 4/3, 6/5 and 64/63, unprocessed deletion,
wrong order, concurrent calls and another loss before/during/after repair,
including loss of the fresh incarnation.

Real 5/3 and 4/3 processes restore both placements after a held final removal
reply and after a deterministic phase-3 interruption. They lose/retry build and
admission replies (plus separate removal replies at 5/3), copy nonempty data,
fence predecessor traffic, retain surviving owners and single Open, acknowledge
continued writes, then fail over to a rebuilt replica and repair that later
Primary loss. The 4/3 cases also acknowledge a write while the fresh Idle's
build reply is held, then recopy that continued data into the old Primary's
fresh replacement. Shutdown independently joins cleanup.
Native-stream qualification remains 179 cases; this is not
native C++ handler/Federation interoperability, generic interrupted-copy recovery,
automatic restart, resize or durable/node/FM recovery.

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
or fabricate an uploaded inventory. The live-session LFUM increment below now
completes inventory upload without granting new serving/restart authority.

Programmatic `Cluster::begin_generation_handshake()` opts the two original,
admitted nodes into a bounded FM round only when lifecycle/control requests
are not pending. It is not a new CLI command and does not change default
startup. The FM retries proposals and pending updates, restarting negotiation
above a higher proposal or qualifying rejection. Retry timing uses native
five-/ten-second defaults; dynamic FM rebuild configuration and Federation
broadcast membership are not enabled. The round remains `awaiting_upload`,
`complete=false`, with mutations/service updates gated until both original
nodes' LFUM inventories are processed.
Existing replicas are not reopened or relabeled as recovered; independent
owner shutdown and real application access remain available.

Generation-control messages and correlated LFUM replies have their own native
admission; ordinary traffic uses the current receive-generation lower bound.
This is not a fully rebuilt FM or native Federation interoperability.
Focused model/store tests cover stale and duplicate traffic, reordered
proposals, definite/unknown commits and queued entity admission. Real nodes
exercise correlated replies, rejection, renegotiation and independent sibling access.
Bidirectional native codecs and native ordering/admission helpers are separate
from the still-required complete C++ handler traces and interoperability gates.

### Live-session LFUM upload

`Cluster::begin_generation_handshake()` now continues through ordinary-FM
LFUMUpload/Reply on the same admitted stateless or independent volatile
singleton sessions, a stable fully admitted three-member 3/2 service, or the
original healthy 3/2 same-Primary Secondary admission after every participating
RA has committed its configuration request.
LFUM is the local failover-unit map: this uploads replica
metadata, not application data, a database backup or a new runtime lifetime.
Unconfigured Idles, in-flight builds, other interrupted reconfigurations,
swap/repair inventories, failed RAP configuration states and restart reconciliation
remain explicitly gated.

Pinned SF revision `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` defines the boundaries:

| Source | Preserved contract |
|---|---|
| `ra/GenerationStateManager.cpp:83-98,359-455` | Each accepted update captures the ordinary-FM entity set and AnyReplicaFound, then queues fresh reads behind each entity's admitted work. Aggregate only after all jobs finish; no private or uncertain commit is sampled. |
| `ra/FailoverUnit.cpp:3999-4134`; `ra/Replica.cpp:217-230`; `ra/ReconfigurationAgent.cpp:4345-4357` | Omit locally deleted replicas, retain canonical dropped descriptions for closed replicas, clear reported LSNs and preserve local-map AnyReplicaFound independently of the emitted list. Singleton intermediate epoch/role remain invalid/None. |
| `ra/FailoverUnit.cpp:3376-3424,3999-4125` | Completed membership clears previous/intermediate epochs and roles. A stable replicated inventory includes local and remote descriptions, clears every LSN and sets report-from-Primary only for the local current Primary. |
| `ra/FailoverUnit.cpp:937-1085,1737-1745,2596-2623,2703-2757` | Initial same-Primary admission retains the committed PC epoch/roles. Catch-up has invalid IC; Primary phase 4 sets IC=CC and IC roles to current roles. Secondary Activate clears IC while retaining PC until its RAP completion. |
| `common/LFUMMessageBody.h:18-61`; `common/RSMessage.cpp:181-182` | Body order is generation, full NodeDescription, FailoverUnitInfo vector, AnyReplicaFound. High-priority FM request has no GenerationHeader; the high-priority RA reply is bodyless and carries the echoed generation. |
| `common/RALFUMUploadOperation.cpp:14-39`; `common/FailoverConfig.h:30` | A consuming correlated request owns the frozen upload. Timeout retains that body for a new request after the native 30-second retry interval; node drive and owner shutdown remain independent. |
| `fm/RebuildContext.cpp:389-486`; `fm/FailoverManager.cpp:2620-2647` | Process inventory before acknowledgment. A duplicate/already-ready or different-generation upload receives its echoed reply without replacing accepted inventory or completing a different round. |
| `ra/GenerationStateManager.cpp:318-337` | Only a reply equal to current receive generation advances send generation. An older in-flight reply cannot finish a superseding round. |

Each accepted `GenerationUpdate`, including a repeat with the same generation,
owns fresh per-entity read jobs and a distinct upload operation. The shared Node
retains each operation's frozen body, correlated request and retry deadline
independently. A semantic reply retires only that operation; an old-generation
reply still ends its own operation but cannot advance send generation. A matching
same-generation reply can advance send generation while sibling uploads remain
owned. `generations.pending_uploads` counts those obligations separately from
the current-generation `upload_pending` readiness flag.

The RA uses its current committed service description, not the historical
reconfiguration request, without widening the Node profile's service-update
admission. Concurrent uploads are bounded by RA `max_outstanding`. Capacity
retains the original generation-update frame without advancing receive; only
its transport handoff is released so the upload replies that free capacity can
still arrive. This is not a semantic ACK or successful RA admission.

For the bounded initial P/P plus I/S admission, the shared RA derives inventory
from the committed phase, not the prospective RAP result:

| Committed phase | Previous epoch/roles | Intermediate epoch/roles |
|---|---|---|
| Primary catch-up | Retained from the admitted request | Invalid/None |
| Primary Activate or final RAP End pending | Retained from the admitted request | Current epoch/current roles |
| Secondary RAP Activate completion pending | Retained from the admitted request | Invalid/None |
| Completed local configuration | Invalid/None | Invalid/None |

Every reported LSN is cleared, and local identity/endpoints remain those of the
committed local replica. A failed final commit reports the preceding committed
phase; an uncertain commit continues to block its queued inventory read.
Only retries/continuations of that exact initial configuration may progress
while upload completion is pending; unrelated new peer control remains gated.
Generation completion neither finishes reconfiguration nor grants or revokes
application access: runtime authority and actual configuration quorum remain
independent. Each upload freezes its own phase metadata even if later local
commits advance the reconfiguration.

The bounded surviving FM reconciles reports against its exact original node,
partition, service, configuration and replica ownership; it does not reconstruct
unknown partitions or elect writers from reports. This FM is a test harness,
not a production FM or a reliability implementation.
`generation_handshake.uploads` retains each accepted inventory. All processed
uploads set `phase=complete`, `complete=true`, allowing ordinary control/service updates in
the new generation. An individual RA keeps its upload pending until its own
matching semantic reply; transport receipt and diagnostic observation files
cannot substitute for that reply. A lost reply replays the same inventory even
after FM has completed the round.

Model/store tests cover multi-entity barriers, failed/unknown private commits,
empty maps versus deleted-only maps, exact/stale/FMM replies and duplicate FM
receipts. Original-process acceptance covers empty/nonempty inventories,
renegotiation, resumed new-generation Add/Delete and continuing nonempty Primary
data on the original app/runtime owners. A shared upload-state/Channel test
overlaps same-generation requests, retains
a further update on capacity, and consumes an actual correlated reply while
that frame is retained. It then loses another request's reply, checks the native
retry deadline and fires that deadline explicitly, preserving its frozen
inventory even after a sibling ACK advanced send generation, without a
wall-clock wait.
Stable 3/2 process acceptance uploads all three members' complete configurations,
withholds the original correlated replies, supersedes the generation and delivers
those stale replies before accepting the new inventories. Original Node/app/runtime
owners, roles, endpoints, data and new-generation control remain intact. Shared
RA tests cover actual final membership commits, definite failure/unknown outcomes,
Primary/Secondary inventory and immutable snapshots after later local changes.
The standalone process tests exercise the same stable replicated upload path.
Additional local-cluster and standalone cases hold the real application
Secondary role callback, leaving Primary Activate and Secondary RAP completion
pending. They roll generation twice, consume stale correlated replies without
advancing send, validate the complete PC/IC inventories, then release the
original callbacks and finish the original configuration. Data, read authority
and original owners are preserved. The local-cluster case additionally loses
both Secondaries and proves that a new write cannot use the old singleton vote.
FM-channel loss during an owned upload leaves it pending
without advancing send generation or shutting down the original serving app.
Native-stream/schema-adapter evidence adds six cases:
169 bidirectional cases, 84 types/255 fields, 173 bundle files and 49 actual
native truncation rejections. Full C++ handler traces/interoperability,
nonempty NodeUp reconciliation, full ReplicaUp batching/final upload, interrupted
peer-configuration upload outside this initial-admission slice, swap/repair inventory, FM/FMM rebuild and durable
restart remain unqualified.

## Healthy three-replica service

`ServiceProfile::StatefulThree` (`--stateful-three` for startup) extends the
native-free harness to three original nodes and one volatile partition.
Only node1 receives AddPrimary. After it is ready, the public
`Cluster::add_idle_replica_on(2)` and `add_idle_replica_on(3)` APIs independently
authorize the two original destinations. Either build order is supported.
Each native AddReplica drives its exact CreateReplica/Reply, RAP Build and
AddReplicaReply, with separately retained retry/error/identity state.
`idle_build.builds` exposes those semantic FM outcomes; aggregate `ready`
requires both successful replies, not node observations or completed socket writes.

After both builds, `activate_idle_replica()` sends one healthy same-Primary
1-to-3 configuration with target/minimum 3/2. Previous membership contains only
the Primary; current membership contains that same Primary and both Secondaries.
The existing runtime projects those quorum obligations separately. RAP catch-up
precedes peer activation; each exact ActivateReply commits independently.
Only after both replies may RA send EndReconfiguration. Its matching successful
RAP completion and commit precede DoReconfigurationReply and FM readiness.
Retries retain the original bodies and resend only pending activation work;
neither peer's result can complete the other's build or activation.

Pinned SF revision `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` supplies the flow:

| Source | Preserved responsibility |
|---|---|
| `ra/ReconfigurationAgent.cpp:1995-2040` | Per-replica Add/build ownership, identity checks and CreateReplica dispatch without changing the current configuration epoch. |
| `ra/FailoverUnit.cpp:2703-2873` | Phase-4 per-remote activation stages; all up/Ready members must activate before EndReconfiguration can grant writes. |
| `ra/FailoverUnit.cpp:4192-4220` | Each Activate carries the complete configuration, not a local-only role setter. |
| `ra/FailoverUnit.cpp:3164-3272,3487-3496` | Final RAP completion and committed stable configuration before FM completion. |

Model/store tests cover interleaved target ownership, stale completions, a
failed/retried build, missing/duplicate activation ACKs, and held, failed or
unknown ACK commits. Original-process tests copy nonempty data to both nodes,
replay lost AddReplicaReply/final DoReconfigurationReply without another Open,
continue replication to both and preserve original app/runtime ownership.
Losing one Secondary still permits Primary-plus-survivor writes at quorum two;
losing both cannot acknowledge a new write using the old singleton vote.
These loss checks do not elect, shrink membership or authorize replacement.
All three original node/app owners supply independent cleanup proof.

The same native schema codecs now have three additional three-element-vector
cases for DoReconfiguration, Activate and DoReconfigurationReply: 172
bidirectional cases, 84 types/255 fields, 176 bundle files and 52 actual native
truncation rejections. These are native-stream/schema-adapter and native-free
process results, not full original-handler or FM/Federation interoperability.
Three-member planned transfer, retirement/replacement, superseding configurations,
generation/peer inventories, node/FM-loss failover, data loss, reconnect and restart remain
explicitly gated. A failed native build remains failed/to-be-dropped with
cleanup required; this profile does not invent successful repair or replacement.
Original-owner shutdown still disposes all owned resources.
Stdin retains its existing create/status/quit interface;
the public Cluster APIs authorize build and configuration.

### Three-replica Primary app-host failover

The completed original 3/2 profile now supports one loss-driven Primary failover
without changing the data-loss epoch. Its **original Primary app host** must
actually exit while the original Node/RA and both configured Secondaries remain
available. RA's committed canonical dropped ReplicaUp report is reconciled
against the exact original FM ownership before FM starts reconfiguration.
A broken FM link, diagnostic observation, cleanup sidecar or historical Ready
label cannot supply that loss report. The normal processed ReplicaUp ACK and
closed metadata Delete remain separate from disposal or replacement.

FM selects original node2 as the bounded candidate and increments the Primary
configuration epoch. PC contains all three original members; the old Primary is
Dropped/down and removed from CC. CC contains node2 Primary and node3 Secondary.
Desired target/minimum remains 3/2. All original peers are bound before entity
work; each peer has its own original listener/Channel, so the two survivors
exchange native reliability traffic directly without reconnect/adoption.

The candidate starts phase 1, **not** healthy-swap phase-0 demotion of a dead
writer. It obtains real local and peer RAP UpdateEpochAndGetStatus/GetLSN
results. The peer commits its new epoch before querying RAP and returns its
actual progress/deactivation history, never a descriptor/default LSN. This
profile's original process acceptance used both survivors' initialized,
matching tails and compatible original history. Missing progress, a changed
data-loss epoch or catch-up progress inconsistent with phase 1 cannot grant
promotion. The configured-failover extension above subsequently generalizes RA
history selection and ChangeConfiguration; broader history repair and
DatalossReport remain gated.

Real promotion/catch-up retains separate PC/CC quorum obligations. Because PC
and CC differ, phase 3 cannot use the equal-membership skip optimization:
node3 receives S/S Deactivate, commits the configuration and new deactivation
boundary, and returns the native fresh identity-only DeactivateReply. This is
**metadata deactivation**, not service Close, runtime disposal or proof that
the Secondary was dropped. Its actual service/replicator stays resident.
The candidate then sends Activate, waits for node3's real RAP configuration
completion and committed ActivateReply, and finishes RAP EndReconfiguration.
Only the final matching DoReconfigurationReply publishes the promoted endpoint.
An endpoint notification/ACK does not complete failover.

Pinned SF revision `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` defines these decisions:

| Source | Preserved responsibility |
|---|---|
| `fm/ReconfigurationTask.cpp:175-355`; `fm/FailoverUnit.cpp:878-895,1087-1105` | Available-member candidate admission, retain PC, increment Primary epoch and remove the down old Primary from CC without manufacturing singleton quorum. |
| `ra/ReconfigurationState.cpp:76-119`; `ra/FailoverUnit.cpp:2204-2299,3013-3055` | Failover starts GetLSN; obtain local/remote progress and both configuration read-quorum evidence before selecting an eligible Primary. |
| `ra/FailoverUnit.cpp:624-703,1993-2157` | GetLSN epoch commit, real RAP status, deactivation-history filtering and progress/catch-up-capability eligibility. |
| `ra/FailoverUnit.cpp:736-818,2596-2637,3091-3124` | Actual catch-up result establishes the new boundary; unequal PC/CC requires phase-3 deactivation. |
| `ra/ReconfigurationAgent.cpp:2590-2682`; `ra/FailoverUnit.cpp:929-958` | S/S Deactivate commits metadata/history and replies without RAP Close; S/N retirement is a different branch. |
| `ra/FailoverUnit.cpp:2703-2873,3164-3272,3487-3496` | Peer activation, final RAP completion and committed stable configuration before FM completion/publication. |

Model/store tests hold candidate admission, progress-query/deactivation and
final completion commits; reject false progress/history, stale identities,
missing/partial replies and unsupported data-loss changes. Definite/unknown
S/S deactivation commits emit no early reply or invented Close. FM models reject
channel-loss-only promotion and insufficient survivors, retain the native
10/60-second retry and ignore repeated old loss reports after completion.
Original-process acceptance kills the actual original Primary app host,
retains nonempty data, loses/replays the final reply, checks the old listener
is gone, and verifies new-Primary writes replicated on the original node3
provider. Both survivors retain one Open and their original app/runtime owners.
Insufficient survivors prevent failover; loss of the remaining Secondary after
promotion cannot acknowledge a new admitted write or restore the old writer.
All original cleanup owners remain independently verified.

After completion, `configuration.kind=primary_failover`, `ready=true` means the
two surviving configured members have completed this flow, not that a replacement
third replica exists. The explicit rebuild below is separate. Repeat/aborted failover, whole-node/FM loss,
restart/reconnect, arbitrary candidates and mismatched-history recovery remain
unsupported. Native-stream evidence adds one three-element non-force Deactivate
case: 173 bidirectional cases, 84 types/255 fields, 177 bundle files and 53 native
truncation rejections. Neither these source-derived models/processes nor the
schema adapter execute complete original C++ handlers or native Federation/FM.

### Post-failover third-replica repair

`Cluster::repair_primary_replica()` explicitly restores one third replica after
the above completed failover and old Primary's processed loss/metadata deletion.
`drive()` owns subsequent build and configuration work. Poll `repair.ready`;
`host_ready` means only fresh host ownership, and `built` means only successful
native AddReplicaReply. Neither authorizes active membership. The earlier
`configuration.ready` may still describe the two surviving failover members
until the separately owned repair configuration starts.

Original node1's Node/RA, in-memory store, generation and peer Channels remain
alive. Node independently requires actual original app-process exit, committed
Closed/deleted metadata, drained reporting/commands/lookups and exact acquisition
release. It explicitly reaps/drains the original process owner and terminates
its Hosting activation before replacing it. The original host gate remains
revoked; a new gate, host identity, app process, Hosting root and runtime
registration are created. The launcher checks the exact original-node receipt
and independent old PID/root absence. A loss-report ACK, dropped metadata or
FM disconnect alone cannot supply this physical ownership boundary.

FM allocates a strictly newer replica ID/incarnation above the original identities.
New node2 Primary owns native AddReplica, peer CreateReplica/Reply, real RAP
Open/BuildIdleReplica and AddReplicaReply. Node1's fresh volatile application
starts empty and recopies nonempty state from node2; no old runtime/data lifetime
is adopted and no persisted Existing Open is substituted. Copy completion is
Ready/Idle, not ActiveSecondary. Both original survivors retain their runtime
and acquisitions without another Open.

After successful build, unchanged-Primary DoReconfiguration retains the two
survivors as PC and adds the fresh I/S to CC. Desired target/minimum remains
3/2, service update version is unchanged and only the configuration counter
advances; no data-loss epoch is introduced. Actual RAP catch-up establishes the
new deactivation boundary. Since PC differs from CC, node3's S/S metadata
Deactivate and its committed identity-only reply precede activation. No RAP
Close or disposal is involved in this S/S exchange. Both Secondary Activate
replies commit independently before final RAP EndReconfiguration and the
matching native DoReconfigurationReply complete repair.

| Pinned SF source | Preserved responsibility |
|---|---|
| `fm/FailoverUnit.cpp:718-739`; `fm/Replica.cpp:555-585` | Reuse a dropped placement only with a new replica identity/incarnation and reset build/endpoint metadata. The fixed profile uses a deterministic monotonic allocator, not native wall-clock allocation. |
| `ra/FailoverUnit.cpp:5487-5523`; `ra/ReconfigurationAgent.cpp:2244-2324` | A lost volatile replica is dropped, not recovered from local state. A newer incarnation creates a fresh local Idle after the old lifetime is closed. |
| `ra/ReconfigurationAgent.cpp:1855-2038`; `ra/FailoverUnit.cpp:315-363` | The current Primary owns peer creation, real build and native semantic completion; unchanged current epoch during Idle build. |
| `ra/FailoverUnit.cpp:736-818,2596-2637,3091-3124` | Real catch-up and separate PC/CC obligations; the native phase-3 skip currently requires equal memberships. |
| `ra/ReconfigurationAgent.cpp:2590-2682`; `ra/FailoverUnit.cpp:929-958,2703-2873,3164-3272` | S/S Deactivate changes metadata/history without Close, followed by activation and final runtime/RA/FM completion. |

Store/model tests hold repair admission, deactivation/activation and final
completion commits, reject missing loss/deletion, reused/foreign host lineage,
wrong-source/stale/partial replies and survivor loss, and retain frozen retries.
Real three-node process acceptance builds nonempty state, kills the original
Primary app, promotes node2, writes more data, replaces node1's host and recopies,
loses/replays final completion and verifies new replica ownership. Old reports,
Delete replies and native Delete requests cannot affect the successor. Losing
node3 after repair still permits acknowledged writes on node2 plus rebuilt
node1; losing both Secondaries blocks acknowledgment rather than restoring
singleton quorum. Interrupted repair shutdown verifies both app-owner cleanups.

This is explicit repair in the original live three-node session, not an
automatic placement policy, whole-node/FM failure, arbitrary app restart,
reconnect, history repair or data-loss recovery.
Survivor loss during repair leaves it unknown; it never manufactures a successful
build or reduces quorum. Existing native message schemas/factories are reused,
including the three-element Deactivate fixture: the maintained codec gate remains
173 cases/53 native truncation rejections. These are native-source-derived
store/process and native-stream/schema-adapter results, not full original C++
handler execution or deployed FM/Federation interoperability.

### Repeated live-session Primary failover and repair

After repair completes, another matched loss of the **current** Primary app
can start the same no-data-loss failover protocol. FM takes PC from the latest
completed three-member configuration, not the original launch roles. Both
current Secondaries must remain available. This fixed profile deterministically
chooses the lowest placement among those ready candidates; it does not implement
PLB ranking. RA now applies the history/LSN selection and ChangeConfiguration
rules described above; repeated-cycle process qualification still uses matched
survivor progress/history.
Target/minimum stays 3/2; only the configuration/Primary epoch advances.

Call `repair_primary_replica()` after each completed failover and processed
predecessor deletion. It is idempotent within that cycle. A new cycle cannot
start until the preceding repair's final semantic completion. The current
Primary owns the new build, and the dropped placement receives a replica
ID/incarnation greater than every retained placement identity. The same live
Node/RA/store/peer Channels can host successive fresh app owners, including
replacing a previously repaired host. Every replacement independently reaps and
drains its exact predecessor, removes its Hosting root and installs a new gate;
no former runtime is adopted or reopened.

S/S Deactivate and Activate commit the latest deactivation history, so a later
GetLSN reply cannot accidentally report the first failover's boundary.
Completed RA transitions may accept a newer configuration, while old epochs are
fenced before routing to the new transition's peer. Exact completed current-cycle
requests still replay their final reply. FM retains predecessor report/reply fences;
Node retains old Delete lineage. Owned replacement commands and receipts are
keyed by predecessor replica/incarnation. Shutdown validates each historical
replacement and the final owner, including a replacement not yet admitted by FM.
These are retained in-memory live-session histories, not durable restart state.

| Pinned SF source | Repeated-cycle responsibility |
|---|---|
| `fm/ReconfigurationTask.cpp:176-320` | Re-evaluate the current Primary and current-configuration candidates under quorum obligations, rather than a one-time original-node transition. |
| `fm/FailoverUnit.cpp:718-739`; `fm/Replica.cpp:555-585` | Reuse dropped placement with a fresh identity greater than prior/current replica identities. |
| `ra/FailoverUnit.cpp:1355-1445` | Replay completed same-epoch work, or initialize a subsequent reconfiguration from its new epoch/roles. |
| `ra/ReconfigurationAgent.cpp:2626-2644,2766-2775` | Refresh deactivation information on Deactivate/Activate, including already-open surviving replicas. |
| `ra/infrastructure.FailoverUnitEntityTraits.cpp:222-267` | Reject stale epochs/incarnations before executing current transition handlers. |

Real-process acceptance runs three complete failover/repair cycles, preserving
acknowledged writes, promoting a rebuilt replica and replacing node1's host
twice. The second repair loses/replays build and final configuration replies.
Old reports, Delete requests/replies, failover requests and owned replacement
commands cannot affect successors. Node process identities stay unchanged and
each current app owner opens once. Losing a Secondary followed by the current
Primary prevents another promotion; separate interrupted-second-repair coverage
verifies shutdown before the new host receipt reaches FM.

Overlapping/interrupted reconfiguration recovery, survivor loss during repair,
whole-node/FM failure, reconnect, persisted recovery and data loss remain gated.
This reuses existing native codecs; source-stream/schema-adapter evidence is
still not complete original C++ handler execution or deployed interoperability.

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
down/arbitrary-not-found cleanup and broader replacement remain gated
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

Normal RAP Close or Drop can now retain that original open origin through Closing.
Only Close/Drop started from the committed opened Idle with its runtime/usage
records this lineage; closure during initial Open does not qualify.
Native `ReconfigurationAgent.cpp:1144-1181`,
`FailoverUnit.cpp:4831-4840,5422-5424,4948-5014` allow AppHostDown to supersede
pending Close/Drop and make volatile host loss terminal rather than retrying it.
If HostDown wins, the same loss report/ACK/metadata path applies. Late
ReplicaCloseReply is stale once the FT is closed
(`ReconfigurationAgent.cpp:3903-3924`), never successful-Close disposal proof.
If successful Close/Drop commits first, later HostDown is stale and does not
reclassify it. Successful Drop retains ReplicaDropped stage and its independently
certified disposal proof; HostDown winning instead retains ReplicaDown stage,
never a successful Drop or disposal receipt.

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
Without explicit Delete, `destination_cleanup_required` remains true, with no retirement/disposal,
absence, cancellation or failed-Open receipt and no reuse/replacement.
Primary pending-remove is unchanged until its own RemoveReplicaReply; the
existing explicit FM removal API can also retire the Primary session after
the destination loss. No automatic reconfiguration/recovery is introduced.

Real two-child acceptance kills the exact destination app while both original
RA/FM sessions and the healthy Primary remain alive. It holds Primary removal,
rejects a stale ACK, loses/replays the
metadata Delete reply, then verifies independent Primary removal and writes.
Model/store tests cover pending/acknowledged Add, unrelated reports, independent
owner loss and separate failed/unknown loss/ACK commits. These are source-derived
and native-free process/Channel results, not native C++ handler execution or
FM/Federation interoperability.
Separate controlled owned-Node/production-adapter/process/Channel acceptance
holds provider or service disposal during an actual normal RAP Close or Drop, then
kills the original app. Provider disposal can block before the service Close
callback begins. No completed disposal is inferred from partially completed
callbacks, HostDown's exact usage release or metadata deletion. Stale ACK,
metadata reply replay, late Create/Add and independent Primary removal/writes
retain their existing barriers. Model/store tests cover Close versus HostDown
winning for each mode, late Close replies, and separate opened-origin/loss/ACK
commit failures and uncertainty. Controlled Close/Drop process-loss acceptance
also checks the native 30-second deadline and explicitly fires the retained
lost-ACK retry with both original Nodes/peer/FM links progressing. This adds no
child-node Close-control feature or wire schema.
Opening loss, Abort/ForceAbort loss, broader down/arbitrary-unknown cleanup
and replacement outside the explicit fresh-Idle slice remain gated.

### Graceful opened destination Close reporting

Successful normal Close of the original opened peer Idle now reports to FM,
instead of retaining the unsupported StatefulClosePending placeholder.
Native `FailoverUnit.cpp:5472-5511` calls OnReplicaDown for ordinary Close;
the volatile FT therefore retains **ReplicaDown stage** while its canonical
description travels in ReplicaUp's **dropped list**, just as for host loss.

Only a matching successful RAP Close of the committed opened-origin normal
Close records this terminal origin. The completion commit precedes original
registration/usage release and report work; initial-Open cancellation, failed
Close, ForceAbort and arbitrary closed lifetimes do not gain this authority.
Normal Drop/Abort have their distinct stage semantics described below.
If HostDown wins first, late Close replies remain stale and provide no
disposal proof. If successful Close wins, later HostDown cannot reclassify it.

The original runtime awaits real provider/service disposal before replying.
The existing Node `instance_disposal()` proof still requires that exact Applied
Close completion, original runtime/host and paired lookup/acquisition release,
plus drained local work. It can be observed before FM acknowledgment and stays
unchanged through the processed-list ACK and closed metadata Delete/reply;
metadata handling issues no second RAP Close or usage release.
The original app host, runtime and factory stay resident.

FM still records the closed volatile lifetime as `lost`, retains historical
Add results and clears current Idle readiness. Genuine disposal evidence is
separate from that native report acknowledgment: a validated retirement sidecar
does not authorize retirement/reuse in this terminal-report flow. Explicit
operator deletion is not implicitly created or completed; an explicit Delete
must join its original metadata reply and retained cleanup fact. Primary pending-remove needs its own
RemoveReplicaReply, and no replacement/recovery is authorized.
The existing publisher may persist the original Close proof after metadata
deletion; that does not change those FM authority boundaries.

Controlled owned-Node/production-adapter/process/Channel acceptance holds real
provider or service disposal, releases it without killing the app, and verifies
the exact original proof before ACK and after metadata reply replay. It checks
stale ACKs, the native 30-second deadline and explicitly fired retry after losing
an ACK, late traffic and independent Primary removal/writes. Both original nodes
continue to progress during that retry. Model/store tests cover separate
completion/ACK commit failure and uncertainty, excluded Close origins and late
HostDown. No child-node Close-control feature, wire schema or native C++
handler/interoperability qualification is added.

### Normal opened destination Drop reporting

The existing local Node `close` with `CloseMode::Drop` now reports the original
successfully opened peer Idle. This adds no FM message or child-node control
operation. The committed opened-origin Close/Drop intent and matching successful
RAP completion are required; initial-Open cancellation, failed Close/Drop,
ForceAbort and arbitrary closed lifetimes remain excluded.

Pinned `FailoverUnit.cpp:5472-5544` calls OnDropped for Drop, rather than Close's
OnReplicaDown. `FMMessageState.cpp:99-113` selects **ReplicaDropped stage**,
while the canonical volatile dropped entry still travels in ReplicaUp's
**dropped list**, not the legacy ReplicaDropped wire action.
Real provider/service cleanup and the completion commit precede reporting and
exact original registration/usage release. Held cleanup and definite/unknown
commit outcomes cannot publish success.

Native `FailoverUnit.cpp:5734-5753` requires a dropped description to acknowledge
this stage, even before the first report retry. A delayed non-dropped down/Ready
description cannot clear it. The existing immediate/30-second reporting,
processed-list ACK and closed metadata Delete/reply path are reused; a
dropped-list ACK instead performs closed ForceDelete without that reply.
Normal Drop cannot supersede an already-closing Close, and a later Close/Drop
does not relabel an already-closed lifetime's original report.
Matching HostDown during that committed opened-origin Drop now supersedes it
through AppHostDown/ReplicaDown, not OnDropped, as described above. Late RAP
completion cannot certify disposal after loss; successful Drop winning first
keeps its ReplicaDropped stage and independently certified original disposal.

Original-runtime disposal/accounting proof remains independently observable
before ACK and unchanged after metadata replay. The replication listener is
retired while the original app host/runtime/factory remain resident. FM records
`lost`, not reusable state; neither a metadata ACK nor valid disposal sidecar
authorizes replacement, implicitly completes operator deletion or clears Primary
pending-remove. Model/store tests cover intent/completion/ACK commit barriers,
both ACK lists, stale/late traffic and excluded origins. Controlled original
Node/process/Channel tests hold provider/service disposal separately, exercise
an actual lost-ACK retry and metadata reply replay, and retain healthy Primary
writes and independent removal. These add lifecycle evidence, not native C++
handler execution or FM/Federation interoperability qualification.

### Normal opened destination Abort reporting

The existing local Node `close` with `CloseMode::Abort` now supports the original
successfully opened peer Idle. Pinned `ReplicaCloseMode.h` distinguishes Abort
from ForceAbort: both use RAP Abort (`flags=4`), but only ForceAbort implies
force-close admission. Native `FailoverUnit.cpp:4781-4842,5472-5544` preserves
that distinction and calls OnDropped after successful Abort cleanup.

Node and RA share original opened-peer admission: the live opened Idle must
own its runtime/usage, or retain that original open lineage through closing or
closed state. Local-only Idles, initial Open and arbitrary unowned closed
lifetimes remain rejected. Non-force Abort cannot supersede an already-closing
operation, even if its runtime registration has disappeared. Closed Abort is
a no-op and cannot relabel a prior Down/Dropped report or certify disposal.

Committed opened-origin Abort intent and matching successful RAP completion
enable **ReplicaDropped stage**, not ReplicaDown. Real replicator/service Abort
and final release precede that reply, Applied completion, exact original usage
release and report work. The existing original `instance_disposal()` proof is
independently observable before ACK and unchanged through metadata replay.
The replication listener retires while the original app host/runtime/factory
stay resident. No successful service Close callback is fabricated.

The existing canonical ReplicaUp dropped entry, immediate/30-second retry,
dropped-description ACK requirement and processed-list ACK/closed metadata
Delete/reply path apply. A dropped-list ACK instead performs closed ForceDelete
without that reply. FM still records lost volatile state, not reuse authority;
disposal and metadata ACK do not clear Primary pending-remove, implicitly create
operator deletion or authorize replacement.

Model/store tests cover non-force admission, unavailable registration, both ACK
lists before/after reporting, stale/late traffic and intent/completion/ACK commit
failure and uncertainty. Controlled original Node/process/Channel acceptance
holds provider/service disposal separately, verifies actual Abort rather than
Close, loses/retries an ACK at the unchanged native cadence and replays metadata
replies with stable original proof and independent Primary removal/writes.
Initial-Open cleanup, failed/ForceAbort reporting, HostDown during Abort and
broader replacement remain gated. No wire schema, child-node control feature or native
C++ handler/FM-Federation interoperability qualification is added.

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

This opened-runtime branch retains the original peer-created Idle and deletion
lineage. Native DeleteLocalReplica epoch/ReplicaId checks still apply; an older
FM replica instance does not block deletion of the matching local ReplicaId.
Delete intent and Close completion remain behind their own commits. The reply
uses native None/None/Dropped/down metadata, invalid version/LSNs and no endpoints.
Normal deletion schedules no ReplicaDown/ReplicaDropped report. Same-incarnation
CreateReplica after deletion is ignored without Open, build or usage acquisition.
Force for this original lifetime is supported by the explicit API below.
Foreign identities, broader replacement, active membership and recovery remain gated.

### Complete original-session destination Delete contract

The bounded volatile peer-Idle profile now handles normal and force Delete for
the original opening, closing, closed, failed, absent or lost destination.
`delete_idle_replica_via_fm()` remains normal. Explicit
`delete_idle_replica_via_fm_with_force(true)` authorizes the existing native
force flag; it adds no child policy-control or implicit replacement command.
Original FM abandonment remains required, independently of its RemoveReplicaReply.

Pinned `ReconfigurationAgent.cpp:777-834` chooses normal Delete for a closed FT,
even for a force request; an open force request selects Obliterate.
`FailoverUnit.cpp:4781-4842,4916-5070` preserves native close admission:
normal Delete waits/retries while another Close/Drop/Abort owns cleanup, then
deletes closed metadata without another RAP call. Force can supersede pending
closure and terminates only the original acquired host, never an arbitrary PID.
Without an adopted registration, native force finishes closure without killing
the resident factory. Missing-FT force uses the ordinary closed tombstone.

Delete Close errors follow the native Deleting retry policy into ForceAbort.
The next existing Close retry sends RAP Abort (`flags=4`); its actual cleanup,
Applied completion and exact usage release remain required for disposal.
`FMMessageState.cpp:99-113,148-176,336-342` suppresses both reporting stages for
locally deleted metadata. Thus Delete-error ForceAbort, or host loss after
normal Delete marked deleted, leaves no pending dropped/down report.
A subsequent original Delete retry returns the canonical closed reply.
Fresh Obliterate instead reports ReplicaDropped until processed ACK/closed
metadata Delete; it never invents graceful disposal.

The original request descriptor and target incarnation stay frozen. Explicit
normal-to-force escalation retains that descriptor and subsequent retries keep
force=true. No downgrade or stronger cleanup claim is inferred from transport,
an observation snapshot, or a later replica incarnation.

Native metadata acknowledgment still joins an independent original-owner fact:

| `completion` | Required local fact in addition to DeleteReplicaReply |
|---|---|
| `disposed` | Actual successful RAP cleanup, Applied completion and exact original acquisition release, including Delete's Abort fallback. |
| `never_created` | Applied missing-FT Delete with zero Open/Close/runtime/lookup history. |
| `pre_open_cancelled` | Original creation and Applied cancellation, committed deleted metadata, no RAP route and drained late lookup/acquisition ownership; also covers force before registration. |
| `failed_registration` | Original Applied pre-Open failure plus separate Applied Delete and drained ownership. |
| `failed_open` | Original-runtime Abort/final-release failure reply, Applied failure/Delete and exact usage release. |
| `host_lost` | Original owned process positively observed exited, original Applied Delete intent, unfenced closed/deleted metadata and fully drained original usage/lookup/command ownership. |

`host-loss.json` carries the last fact from the still-owned original Node.
It is not a destructor receipt: `host_lost=true`, `disposed=false`, no retirement
or reuse authority. Its creation/Delete operation, FT/service/replica,
deployment/session/generation, original node/app PIDs and runtime root are
validated together. Positive completed cleanup/no-runtime histories are
preferred to host-loss classification, and remain valid if the app exits later;
the original Node, proof lineage and all draining/fencing checks must still hold.
A late issued lookup into a retired, already-deleted host completes explicitly
as native type-not-registered rather than acquiring a new owner or stranding
the original lookup.

Without explicit operator Delete, unsolicited Close/loss reports still do not
retire the placement or authorize reuse. Explicit Delete can now join an
already-closed original's independently retained cleanup proof or host-loss
fact. Metadata alone leaves `complete=false`, `cleanup_state=unproved`;
original Node/session loss leaves `state=unknown`. Neither is converted to
absence, disposal or a recovered empty replica.

All successful outcomes keep Primary pending-remove independent and
`reusable=false`. Package/work roots stay owned until shutdown. Model/store
tests cover normal/force/closed races, Delete-error escalation, force intent/
HostDown/ACK commit failure and uncertainty, strict identity/epoch checks,
proof ordering/conflicts and owner loss. Real original Node/process/Channel
tests cover held provider/service closure, force supersession without releasing
those holds, no-runtime force cancellation/absence, metadata replay, stale
Create, historical proofs after app exit and continued Primary data access.
These complete this original-session Delete subprofile, not persisted services,
active membership, replacement, reconfiguration, recovery or native C++
handler/FM-Federation interoperability qualification.

### Explicit fresh Idle replacement

`Cluster::replace_idle_replica()` explicitly authorizes **one** successor on
the same original destination Node/app session. It requires the original
successful FM Add, matched Primary RemoveReplicaReply, normal non-force
DeleteReplicaReply and independently validated original runtime disposal,
Applied Close and exact usage release. Both original node sessions must remain
admitted, active and live, without loss reports or generation/node/service work.
Deletion alone still leaves `reusable=false`; no timer, report ACK or cleanup
receipt implicitly schedules replacement. Failed/absent creation, host loss,
force deletion, arbitrary peers and a second successor remain gated.

The source baseline remains `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:

| Native source | Preserved behavior |
|---|---|
| `fm/FailoverUnit.cpp:718-739`, `fm/Replica.cpp:555-585` | Reuse the placement only for a new replica identity/incarnation, resetting build/role/endpoint metadata. This fixed profile deterministically allocates equal new IDs above both original replica IDs and the destination instance; it does not reproduce the native wall-clock allocator. |
| `ra/ReconfigurationAgent.cpp:1855-2038,4094-4148`, `ra/FailoverUnit.cpp:315-363` | Successful original Idle removal removes the Primary's old replica entry. A fresh FM Add then starts new CreateReplica/build work; old replies cannot complete the new incarnation. Replacement while removal remains pending is not enabled. |
| `ra/ReconfigurationAgent.cpp:2244-2324` | A closed destination creates a fresh Idle only for a newer incarnation. Open follows durable creation; matching Open completion commits before CreateReplicaReply. Equal deleted traffic is ignored; older closed traffic receives native StaleRequest without reopening. |

The destination Node separately requires its own genuine disposal proof,
drained predecessor commands/acquisitions/reporting and a connected original
runtime with a live owned app. No metadata acknowledgment can bypass that gate.
The old proof remains intact through a failed/unknown creation commit and is
cleared locally only when the committed successor's Open effect runs.
Original node/app PIDs, Hosting package/factory and runtime root stay resident;
this is a new service/replicator lifetime, not app restart or warm adoption.

The existing AddReplica -> CreateReplica/Reply -> RAP build -> AddReplicaReply
path opens and recopies the successor from the healthy Primary. Nonempty copy
and continuing application ACKs are required; the destination remains
Idle/InBuild, non-serving and unconfigured. Native action schemas and retry
cadences are unchanged. Repeated replacement calls retry/observe the same
successor rather than minting another identity.

`status()["idle_predecessor"]` retains the frozen original Add/Remove/Delete
outcomes and disposal proof. Current `idle_build`, `idle_removal` and
`idle_deletion` refer to the successor; old native replies, reports and retained
proof files cannot clear its work or replace its ownership. The successor can
itself be explicitly removed/deleted with a new matching disposal proof.

Model/store tests cover admission barriers, definite/unknown creation commits
and stale FM/peer/RAP completions. A real public Cluster API test covers original
cleanup, distinct successor identity on unchanged process owners, nonempty copy,
continued replication, lost Add reply/replay, stale predecessor Add/Remove/Delete,
retained history and successor cleanup. These are native-source-derived and
native-free process results, not native C++ handler traces, FM/Federation
interoperability, active membership, automatic replacement or recovery.

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
branch below is separate. Force and app-exit races use the contract above;
unproved ownership, foreign targets, replacement and recovery never become
successful absence or disposal.

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
Unknown/missing failure lineage permits metadata handling only, not a fabricated
failure or disposal receipt. Replacement remains gated.

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
Opening/loss/closed Delete and force use the original-session contract above.
Unproved cleanup remains explicit; general unsolicited lifecycle reporting,
replacement and recovery remain separately gated.

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
Foreign targets and later incarnations remain
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

## Same-Primary active configuration (stage-5 increment)

In the existing `--stateful-peer-idle` profile,
`Cluster::activate_idle_replica()` explicitly admits the successfully FM-built
Idle into one two-member configuration. The original Primary stays Primary;
the original destination (or its one supported fresh successor) becomes
ActiveSecondary. This is a separate opt-in programmatic operation, not a new
CLI action, automatic scaling, primary transfer or failover.

The native DoReconfiguration body carries a versioned service-size update from
target/minimum 1/1 to 2/1, the next full configuration epoch in the same primary
epoch, and exact original P/P and I/S descriptions. Both original nodes must
remain admitted, active and healthy. Unanswered/failed Add, pending removal or
deletion, generation upload, node/service work and unknown ownership reject.
Service/package identity, replica IDs/incarnations and Primary endpoints do not
change. Native configuration descriptions contain the local replica as well
as the remote member; quorum counts do not come from a reduced remote-only list.

Pinned SF source remains `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:

| Source | Preserved bounded flow |
|---|---|
| `ra/ReconfigurationAgent.cpp:2142-2238`; `ra/FailoverUnit.cpp:1386-1468,1754-1772,1909-1924` | Existing-Primary `Other` reconfiguration, durable new intent before effects, native service-description update and direct phase-2 catch-up rather than GetLSN or a role setter. |
| `ra/FailoverUnit.cpp:736-818,2596-2637,3127-3162` | Matching successful RAP Catchup reply advances the phase. PC contains only the unchanged local Primary, so phase 3 has no peer deactivation work before phase 4. Errors remain pending; no invented catch-up success. |
| `ra/ReconfigurationAgent.cpp:2685-2849`; `ra/FailoverUnit.cpp:959-1001,2703-2873` | Peer Activate records the exact new configuration, then invokes real RAP UpdateConfiguration on the existing Idle. ActivateReply follows matching role-update completion and its commit. The Primary waits for that exact reply before RAP EndReconfiguration. |
| `ra/FailoverUnit.cpp:3164-3272,3487-3496` | Final matching RAP EndReconfiguration and commit precede DoReconfigurationReply. Stable completion clears previous epoch/roles; neither a transport write nor peer activation alone completes FM work. |

The production Hosting IPC allowlist now admits native
UpdateConfiguration/Reply on lifecycle-enabled original sessions. RAP retains
its native empty-runtime/default-service control shape and flags:
Catchup=2, secondary update=0, EndReconfiguration=1. The existing public runtime
and data replicator perform the actual callbacks, configuration projection and
application-ACK quorum waits. No direct engine configuration, private runtime
state mutation or qualification-only execution hook supplies completion.

Poll `status()["configuration"]` for `state=ready`/`ready=true`; only an exact
ordinary-FM DoReconfigurationReply produces that result. Both placements then
record their active replica roles; `peer_idle.active_secondary` distinguishes
membership from historical Idle build completion. The Secondary has no new
application listener/discovery endpoint, and cannot write as Primary. The
existing Primary application endpoint remains independently contactable.

FM retries its frozen request with the existing 10/60-second action cadence;
the Node retains the existing 15-second pending RAP/peer retry. Identical
completed requests replay the retained semantic result without repeating
Open/build/role callbacks. Stale epochs/incarnations and late RAP completions
cannot advance another phase. Store veto/uncertainty keeps proposed state,
activation ACKs and final FM success private until actual resolution.

Current configuration has two write-quorum members; previous configuration
contains only the original Primary. The real process test first copies nonempty
state, then acknowledges a new write under the active configuration. After
positive original Secondary app-host loss, native down reporting/processed ACK
marks configuration observation unknown without authorizing replacement. A
distinct third write is admitted/applied locally but receives **no ACK** from
the old singleton quorum. Original owners still perform independent shutdown
and cleanup; loss is not graceful disposal or automatic reconfiguration.

Model/store tests cover phase ordering, retryable RAP error, exact identities,
duplicate/stale traffic, separate PC/CC descriptions, creation/activation commit
veto/uncertainty and the final reply commit barrier. Actual original-child tests
cover real ActiveSecondary callbacks, nonempty data/continuing ACKs, lost final
reply/replay without a second Open and loss without quorum fallback.
The maintained native-stream gate adds four action contracts and twelve cases:
151 bidirectional cases, 81 schema types/248 fields, 155 bundle files and
43 actual native truncation rejections, retaining every prior case.

This qualifies the frozen same-Primary admission and terminal observation path;
the bounded healthy retirement/replacement extension is described below.
Outside the planned swap below, arbitrary configuration supersession, primary
transfer, GetLSN, broader Deactivate, endpoint changes/publication and data loss,
FMM, generation upload, reconnect and restart remain gated. Codec/schema
execution and native-free process acceptance are not complete original C++
handler traces or native FM/Federation interoperability.

### Healthy configured-Secondary retirement and replacement

After the original successful 1-to-2 admission,
`Cluster::retire_secondary_replica()` explicitly requests a healthy, unchanged-
Primary 2-to-1 reconfiguration. Both original admitted sessions must remain
healthy; no down-report, missing process or old singleton quorum authorizes it.
The next same-Primary epoch carries P/P and S/N, a versioned target-size change
from 2/1 to 1/1, and both full previous/current configurations.

Pinned SF source `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` defines the flow:

| Source | Preserved contract |
|---|---|
| `ra/FailoverUnit.cpp:736-818,2596-2702` | Real RAP Catchup (flags=2) must satisfy the two-member PC and singleton CC before phase-3 Deactivate is sent. Neither a transport write nor a report ACK supplies catch-up completion. |
| `ra/ReconfigurationAgent.cpp:2620-2665,671-706`; `ra/FailoverUnit.cpp:5472-5540` | Non-force S/N Deactivate commits intent, closes the original volatile destination through real RAP Drop/Close (flags=8), releases its original ownership, then commits the canonical dropped DeactivateReply. Failed Close stays pending. |
| `ra/FailoverUnit.cpp:1192-1222,2953-2960,3164-3272,3376-3415,3487-3496` | Exact peer completion precedes final RAP EndReconfiguration (flags=1). Final committed completion removes the non-CC peer from the Primary RA view and replies with only the surviving current Primary, with previous epoch/role cleared. It does not synthesize a separate Idle RemoveReplica/Reply. |
| `common/DeactivateMessageBody.h:15-49` | Native ConfigurationMessageBody inheritance plus derived `isForce_`, nested deactivation information, exact peer actor/priority and sender generation. Force/restart and other Deactivate branches remain gated. |

Poll `configuration` for `kind=same_primary_retirement` and `ready=true`.
`peer_idle.active_secondary=false`/`state=retired_secondary` distinguishes
retirement from active membership. Dropped reporting and processed-list ACKs
remain native metadata exchanges, not loss-driven membership repair or
replacement permission. In particular, a report-triggered deleted tombstone
and disposal sidecar alone cannot authorize a successor.

Explicit `delete_idle_replica_via_fm()` still joins its exact semantic reply
with the original applied successful Close and independently validated disposal/
usage-release proof. Only then may `replace_idle_replica()` authorize the one
fresh successor. Existing native Add/Create/build recopies nonempty Primary
state under a strictly newer replica ID/incarnation; `activate_idle_replica()`
then performs the real 1-to-2 activation again. Original node, app, package,
factory and runtime owners stay unchanged. `configuration.history` and
`idle_predecessor` preserve the completed predecessor requests/replies and
proofs. Same-phase retries replay retained work/results; stale predecessor
epochs, replies, deletion and reports cannot complete or retire the successor.

Model/store tests cover separate PC/CC descriptions, failed RAP catch-up/Close,
exact identity/endpoints, duplicate/stale messages, commit veto/uncertainty and
Close/final-reply commit barriers. HostDown during retirement retains native
down reporting without a DeactivateReply or disposal claim.
Original-process acceptance exercises lost
retirement completion, explicit deletion/proof gating, singleton writes after
safe retirement, successor nonempty copy, reactivation and new application ACKs
without another Primary Open or process-owner replacement.
The maintained native-stream gate now covers 157 bidirectional cases,
82 types/250 fields, 161 bundle files and 45 actual native truncation rejections.

This enables one healthy original configured-Secondary replacement, not a
second replacement, retirement of an already-replaced Idle, concurrent/new
reconfiguration supersession, automatic failure repair,
endpoint publication, FMM, generation upload, reconnect or durable restart.
The separate planned-Primary-transfer increment below adds its bounded role
handoff and endpoint publication.
Original C++ handler traces and native FM/Federation interoperability remain
unqualified.

### Healthy planned Primary transfer

After completed two-member active admission (including the supported fresh
Secondary), `Cluster::transfer_primary()` explicitly transfers Primary from
original node 1 to node 2. Both original admitted sessions and replicas must be
healthy, with no pending build/removal/deletion, node/service work or generation
upload. The request keeps the service, membership and replica incarnations,
sets P/S and S/P and increments the configuration version by `0x100000001`
without changing data-loss version. The FM withdraws its old endpoint while
transfer is pending; withdrawal alone is not proof of runtime write revocation.

Pinned SF source `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` defines the flow:

| Source | Preserved contract |
|---|---|
| `fm/FailoverUnit.cpp:878-895`; `ra/ReconfigurationState.cpp:76-119`; `ra/FailoverUnit.cpp:1754-1779` | Explicit healthy SwapPrimary starts at the old Primary in phase 0, not direct promotion or a local role setter. Real RAP configuration/catch-up revokes writes, catches up the candidate and changes the old service/replicator to Secondary. |
| `ra/FailoverUnit.cpp:736-782,2926-2950,3497-3540` | Matching successful demotion and its commit precede peer DoReconfiguration with retained non-Max phase-0 duration. A duplicate FM request replays that continuation, never an old-Primary success reply. |
| `ra/FailoverUnit.cpp:628-703,2204-2299`; `ra/ReconfigurationAgent.cpp:2425-2507` | Candidate phase 1 uses real local RAP UpdateEpochAndGetStatus and native peer GetLSN/Reply. GetLSNReply inherits ReplicaReplyMessageBody and adds nested deactivation information. This bounded profile requires initialized matched progress on both original members; other histories stop rather than inventing election, repair or data loss. |
| `ra/FailoverUnit.cpp:736-818,3063-3124`; `common/FailoverConfig.h:387` | Candidate promotion/catch-up uses full two-member PC and CC. The native default PC=CC optimization skips phase-3 Deactivate; it does not reuse the S/N retirement/Close path. |
| `ra/FailoverUnit.cpp:2703-2873,3164-3272,3376-3415,3487-3496` | Peer Activate updates the demoted member without another Open/build. Exact ActivateReply precedes final RAP EndReconfiguration and commit before the new Primary's native DoReconfigurationReply. Stable completion clears previous epoch/roles. |
| `ra/FailoverUnit.cpp:3590-3630,4597-4624,5701-5710`; `ra/EndpointPublishState.cpp:47-145`; `common/FailoverConfig.h:486` | Original-runtime ProxyReplicaEndpointUpdated is ACKed, but only matching active promotion updates the committed endpoint. The existing helper owns the native 30-second early-publication timeout and exact FM ACK. Final reconfiguration completion publishes the new Primary address; an endpoint update/ACK alone does not complete the transfer. |

Poll `configuration` for `kind=planned_primary_transfer` and `ready=true`.
Only the exact new-Primary native reply changes readiness and places the
numeric loopback service endpoint on node 2. Early native
ReplicaEndpointUpdated/Reply records are retained separately in
`configuration.endpoint_update`, without making a pending transfer ready.
`configuration.history` preserves prior admission/retirement records.
`nodes[].retained_work.local_replica.swap` reports the owned phase/error;
the older `local_replica.configuration` remains historical same-Primary work.

The sample retains its original service/provider instances. Promotion stops the
old Secondary stream consumer and starts the same service's Primary listener;
demotion consumes replication on the existing provider without a new copy.
The old listener remains contactable but rejects writes through the real public
partition/replicator authority checks. No engine-specific mutation or
qualification-only execution hook supplies progress, role change or success.

Model/store tests cover failed demotion, both progress results, exact native FM
completion, retained retries, endpoint timeout/publication, definite commit
veto/uncertainty and demotion/endpoint/final-reply commit barriers.
Original-process tests copy nonempty state, lose/replay the final reply, verify
old-writer rejection and new-Primary ACKs continuing on the demoted member,
retain both original owners and one Open each, and confirm new-Primary host
loss cannot reactivate the old writer. Native down reporting makes transfer
observation unknown; it supplies no replacement, election or quorum fallback.

This enables one explicit healthy transfer only. Reverse/second transfer,
concurrent supersession, mismatched progress, aborted-swap recovery, loss-driven
failover, changed membership/endpoints outside this promotion, data loss,
FMM, reconnect and restart remain gated. Native source-stream evidence covers
163 bidirectional cases, 83 types/251 fields, 167 bundle files and 47 actual
native truncation rejections. Original C++ handler traces and native
FM/Federation interoperability remain unqualified.

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
write/readback before loss, a genuinely lost acknowledgment, the native 30-second
deadline and explicitly fired retry, stale/duplicate replies and an independently
serving sibling. Hard exit
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
Delete-error escalation to ForceAbort leaves native message stage None for an
already locally deleted FT; OnDropped does not schedule another report. A later normal
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
