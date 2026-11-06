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
reporting obligations; unsupported replacement/reopen paths fail admission.
FM does not turn a transport write into readiness. An independent application
client verifies one public admitted/applied/acknowledged write and readback.

Stateful force deletion, recreation, reconfiguration, FMM, copy/build,
multi-replica movement and recovery remain gated. Owned local shutdown joins resource/listener disposal,
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
their original owners; **stateful recreation stays rejected** even after
verified deletion. Duplicate operator operations retain their original outcome.
Normal deletion does not acknowledge unrelated terminal reporting escrow.

### Unexpected singleton app-host loss

The FM singleton profile now reports original app-host loss, including finite
lease fencing, over real `ReplicaUp`/`ReplicaUpReply` delivery. Graceful Close
of that same lifetime is covered separately below; Drop/Abort and closed-
AddPrimary reporting remain staged. Local Idle and local-Primary profiles do
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
and actual store veto/unknown resolution. Drop/Abort, closed-AddPrimary
reporting, recreation/reconfiguration and recovery remain gated. These tests
do not establish full native handler or native FM/Federation interoperability.

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
