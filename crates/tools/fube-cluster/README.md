# Bounded native-free FM cluster

This tool owns one in-memory FM and exactly two child processes running the
existing local RA/Hosting/runtime. It supports only the fixed
`ClusterStatelessApp/ClusterService/Code/ClusterStatelessType` package. This is
not native Federation interoperability, full M1, recovery, failover or a general
service host.

An explicit `--stateful-singleton` variant admits the existing
`hosting-local-app` volatile package and one fresh 1/1 Primary lifetime per node.
The two nodes host independent singleton partitions, not two replicas or an HA
configuration. Stateless remains the default.

`--stateful-peer-idle` is a separate fresh-start, fixed-generation opt-in:
node1 alone receives FM AddPrimary; node2 is the bound remote volatile Idle
for the **same partition**, with a distinct replica identity.
After the Primary is ready, `Cluster::add_idle_replica().await` lets FM own the
complete native AddReplica -> CreateReplica/Reply -> RAP build -> AddReplicaReply
sequence. It returns `pending`; poll `status()["peer_idle"]["fm"]` for
`state=ready`/`ready=true`. Only the actual AddReplicaReply completes FM build,
not the separate child `peer_idle.built`/`ready` observations.
The Primary/FM remote description becomes Ready/Idle; the destination still
stays Idle/InBuild, non-serving and unconfigured.
FM retains and retries the original AddReplica every 10 seconds, backing off to
60 seconds after the first minute. Duplicate creation/build requests preserve
the original lifetime; Ready duplicates resend the reply without another build.
Peer creation/Open failure preserves the failed FM outcome and automatically
marks the original replica `to_be_dropped` with a frozen pending-remove obligation;
RAP build errors remain pending and retry through nonterminal cleanup.
Local explicit removal cancels the FM add obligation and fences late success,
without claiming FM RemoveReplicaReply or destination disposal. Original session
loss yields unknown, never recovery or transparent adoption.

`Cluster::remove_idle_replica_via_fm().await` lets FM cancel its original pending
creation, retire failed creation or abandon the original opened Idle through native RemoveReplica/Reply,
whether unbuilt, building, built or failed build.
If the Primary RA has not accepted CreateReplicaReply, native InCreate removal
retires creation bookkeeping and immediately replies with the exact original
FM descriptor (InBuild=1, empty replication endpoint), without RAP removal.
`completion=creation_cancelled` distinguishes this from the post-Open
`completion=replicator_removed` path. If Open wins the race, the existing RAP
cancellation/removal path still applies. Poll `peer_idle.fm_removal` (also root `idle_removal`) for
`state=removed`/`pending_remove=false`; diagnostic `peer_idle.state=removed`
does not complete FM. The frozen request retries on the same 10/60-second
cadence. RA retains ordinary RAP errors for retry and replays successful replies
on duplicate FM requests without another removal. A lost reply leaves FM pending.
Removal cancels any FM add and fences delayed/queued AddReplicaReply and build
success. Unresolved original-owner loss yields unknown, not recovery. Destination
service/data and registration remain intact; no remote deletion, active membership
or automatic removal after successful build is inferred. A late destination Open
is ignored by the Primary and cannot start build; any resulting destination
service remains owned until explicit cleanup or shutdown. Manual pre-Open,
down/arbitrary-unknown cleanup and replacement remain gated.

After an original CreateReplicaReply error, the Primary retains only a failed
observation/tombstone (`creation_aborted=true`), not live creation/build authority.
FM sends the original Add descriptor, never fields from the failed reply.
Native absent-after-abort RemoveReplica echoes that descriptor without RAP;
`completion=failed_creation_retired` clears only pending-remove. The FM add stays
`state=failed` with its original error. `cleanup_required=false` then means
Primary/FM bookkeeping is complete; `to_be_dropped` and
`destination_cleanup_required` remain true, and `destination_deleted` stays false.
The normal 10/60-second retry path sends the frozen removal; calling
`remove_idle_replica_via_fm()` explicitly resends it immediately. Reply loss,
stale success and original-owner loss cannot turn failure into Ready or grant
replacement. Ordinary successful build never schedules removal automatically.

The earlier manual path remains separate: `Cluster::create_idle_replica().await` submits a trusted
local Primary-RA command and returns `pending`. Poll `status()["peer_idle"]`
for the committed CreateReplicaReply observation (`open`, not built/serving).
Retrying this API retains the exact original remote lifetime.
After Open, `Cluster::build_idle_replica().await` explicitly requests actual
Primary-to-Idle copy through RAP. It returns `pending`; poll `peer_idle.built`
for the original runtime's semantic build completion. The joined destination
application pump applies copied values/retry results and continuing replication
before explicit operation ACKs. Pending retries reuse the request; native errors
remain in `peer_idle.build.result/detail`. Retrying a failed build resends the
original request: RAP first performs nonterminal `BuildRetryCleanup`, with no
successful build reply, then a later retry can rebuild that same incarnation.
Close fences late completions. Destination data observations under
`nodes[1].retained_work.app_observation.idle_data` are diagnostics, not authority.
`Cluster::remove_idle_replica().await` explicitly abandons the opened original
Idle, whether build has not started, is pending, succeeded or failed. It returns
`pending`; poll `peer_idle.state` for `removed`. Repeat pending calls to retry
cancellation/cleanup through the original RAP session. An in-flight build is
cancelled first, not acknowledged as removed. Only the matching semantic
`ReplicatorRemoveIdleReplicaReply` completes removal; frame writes do not.
Errors remain visible in `peer_idle.removal.result/detail` and are retryable.
RAP normalizes only SF's replica-does-not-exist error to successful absence.
Removal intent fences late build replies, and terminal tombstones reject delayed
builds/recreation of this incarnation. Completed removal retries are idempotent.
This is not automatic after successful build, not retry cleanup and not remote
DeleteReplica: the destination service, copied data, registration and usage stay
owned until normal shutdown. The removal descriptor's `state=3` is the Primary's
native InDrop wire view; the destination remains Idle/InBuild and non-serving.
Both actual activations use the deployment-wide package version `1.0:1.0:1`;
their physical package sequence, acquisition, runtime and process owners remain
independently generated. Existing profiles keep their generated package versions.

Each child publishes a separate one-use peer listener; only the original
capability-bearing parent stdin command lets node1 dial after both original
children are admitted. CreateReplica and CreateReplicaReply are high-priority
one-way RA messages, with native GenerationHeader and exact PToP NodeInstance
headers on a separate bounded Channel, never an FM relay.
Peer ingress also checks the configured shared partition and exact target
node/replica/incarnation before first-FT admission, for both request and reply.
Node2 runs
`--stateful-lifecycle --data-replicator`; its real Open returns Idle/InBuild,
an actual replication endpoint, no service endpoint and denied read/write access.
Peer receipts and pending commands remain bounded independently of FM work.
The originally bound Idle drains package notification/status work through its
original FM session; that background path grants no FM placement/control authority.
Peer loss does not dispose an application, fabricate a reply or reconnect.
This profile admits CLI `create` on node1, `status`, and `quit` only; it rejects
new FM generations, mutation of the fixed service, and node2 FM creation.
Build completion still leaves Idle/InBuild metadata, no service endpoint and
denied access. There is no quorum addition, serving secondary, failover,
replacement, or recovery. Removal
before peer Open through the local API and remote deletion remain gated; both original owners retain
normal cleanup.

Build both executables, then run:

```sh
fube-cluster --root /absolute/private/profile-root \
  --app /absolute/cluster-stateless-app
```

For initial stateful FM control:

```sh
fube-cluster --root /absolute/private/stateful-profile \
  --app /absolute/hosting-local-app --stateful-singleton
```

Use `create`, `status`, `delete`, `activate`, `deactivate` and `quit` in this variant. `AddPrimary`/`AddPrimaryReply`
use the native shared ReplicaMessageBody/ReplicaReplyMessageBody schemas and
distinct actions; no local Open observation becomes an FM acknowledgment.
FM stays pending until the exact original semantic Primary/Ready reply.
The original creating-primary request retries under native default per-replica
10/60-second minimum/maximum action intervals, without changing identity or
reopening the replica. Dynamic FM configuration is not enabled by this profile.
Independently send `write\n` then `read\n` to the reported application endpoint.
The bounded fixture accepts fixed nonzero public mutations (`write` and the
optional continuing `write-more`), applies each
admitted sequence before awaiting quorum completion, and returns progress/value.
It rejects writes before runtime/engine role, epoch, readiness and access agree.
This is volatile singleton acknowledgment, not durability or replicated copy.

Normal stateful `delete` is admitted only after the original Primary creation
completes. It sends native `DeleteReplica(force=false)` and waits for the
matching `DeleteReplicaReply` plus original Node disposal/accounting proof.
Lost replies retry the same lifetime with native 10/60-second action intervals.
Normal Delete produces no terminal ReplicaDown/ReplicaDropped-report ACK.
The deleted tombstone remains retained. A subsequent explicit `create`, only
after both deletion inputs match, opens fresh empty volatile data under higher
replica/instance identities while retaining the original node/app host.
Live replacement/adoption, promotion, `DoReconfiguration`, copy/build, FMM and
recovery remain explicitly gated. `quit` uses the original
owned local cleanup path, retaining terminal stateful reporting as pending
escrow rather than manufacturing ReplicaUp acknowledgments.

Unexpected original singleton app-host loss (including lease fencing) now sends
non-final ReplicaUp with a canonical dropped entry while RA retains native
ReplicaDown stage. FM replies in the processed list and status becomes `lost`,
with no endpoint; it does not become `absent` or reusable. The stateful report
retry uses the native default 30-second interval, stopping only after committed
semantic processing or independently authorized deletion.

Native ACK processing can delete the already closed FT and send
DeleteReplicaReply; `native_replica_deleted` is metadata completion, **not**
original-runtime disposal or recovered volatile data. Missing native cleanup
replies can trigger normal DeleteReplica retries for that same closed lifetime.
`loss_reported` and native deletion never supply a retirement receipt or permit
recreation. Graceful `CloseMode::Close` of the original FM singleton while its
FM session remains live now uses that same Down-stage/dropped-payload reporting
and ACK path, after real RAP disposal and committed exact usage release.
Positive Node disposal proof remains separate from metadata acknowledgment;
FM still records lost volatile state, not recovery or reusable absence.
Valid unsolicited disposal sidecars cannot retire that loss or break its live
session; malformed/cross-owner proofs still reject.
This enables no new CLI operation or local-only-profile authority.
Normal `CloseMode::Drop` now also reports that original FM singleton, but uses
native **ReplicaDropped stage**, not Down. It sends the same canonical dropped
ReplicaUp entry after committed real RAP cleanup and exact release; only a
dropped description can clear its stage. Existing retry/ACK, metadata deletion
and independent disposal boundaries are unchanged. A late Drop does not relabel
an already-closing/closed Down lifetime.
Abort/ForceAbort now report through ReplicaDropped stage after RAP Abort and
committed exact release. Abort retains non-force close admission. Closed
AddPrimary sends the original canonical dropped report without relabeling the
stage or allocating a use; deleted tombstones suppress that report.
Native force DeleteReplica terminates the exact original host via Obliterate
and reports only after HostDown, without graceful disposal/reuse proof.
The CLI's `delete` remains normal; no force/reset escape is introduced.
See [singleton lifecycle completion and fresh recreation](../../../docs/cluster/MinimalFmCluster.md#singleton-lifecycle-completion-and-fresh-recreation).
See [source semantics and qualification limits](../../../docs/cluster/MinimalFmCluster.md#unexpected-singleton-app-host-loss).

The root is operator-owned and private (0700). Its parent must already exist.
All materialized packages, runtime roots, node controls and scratch stay beneath
it. Only trusted numeric-loopback communication is supported. There is no
arbitrary package or executable input in the FM protocol.
The supplied fixed app executable is bounded to 256 MiB. EOF also requests
orderly shutdown (including after operation-identity capacity is exhausted).

After the `ready` JSON line, stdin accepts one JSON object per line:

```json
{"op":"create-1","action":"create","node":1}
{"op":"inspect-1","action":"status"}
{"op":"delete-1","action":"delete","node":1}
{"op":"shutdown","action":"quit"}
```

Create/delete return `pending` after submission, not serving success. Issue a
new status operation to observe later progress. Retry an identical operation ID
to retrieve its retained outcome; reuse with different content is rejected.
Status operation retries return their original snapshot. At most 64 operation
identities are retained without eviction, with 4096-byte input lines, 64-KiB
output objects and strict
unknown-field rejection. Each node has one current instance lifetime.
In the stateless profile only, after verified deletion, create again with a **new operation ID** on the same
live node; replica and RA-instance IDs both strictly increase. Old completed
operation retries retain their original lifetime/endpoint. Loss or unproved
cleanup does not enable recreation. A reported endpoint is numeric loopback; independently send
exactly `identity\n` and read its bounded JSON/newline identity response.

`activate`/`deactivate` use the same node/operation format and native sequence
contracts. Deactivation waits for committed replica closure, with the native
900-second original-host termination deadline; activation does not reopen lost
volatile data or establish serving. A newer opposite intent supersedes pending
work. Node intent and programmatic `Cluster::update_service` requests retry at
the native default five-second FM scan interval. Status includes current intent,
pending control, type status, service-update version and load receipt presence.
Real service-description/load IPC and node-wide barriers are described in
[stage-two coordination](../../../docs/cluster/MinimalFmCluster.md#node-wide-coordination-stage-2).
Live endpoint publication, general multi-FT deployment and FMM remain gated.

The programmatic `Cluster::begin_generation_handshake()` starts the bounded
ordinary-FM GenerationProposal/Reply and GenerationUpdate/Reject round.
It requires two admitted nodes without pending lifecycle/control requests.
This is opt-in, not a CLI command or a change to default bootstrap. Proposed,
receive and send generations remain distinct; only receive changes on a
matching Update. The round stops at `awaiting_upload`, `complete=false`:
LFUM upload is not implemented, send generation does not advance, and new
mutations/service updates are gated. Existing serving replicas and independent
owned shutdown are not replaced. See the [stage-3 increment and remaining gates](../../../docs/cluster/MinimalFmCluster.md#ordinary-fm-generation-handshake-stage-3-increment).

Native NodeUp/Ack and the final empty ReplicaUp/Reply precede RA construction.
FM learns the actual generated package version from the original node's bounded
private startup metadata, not a lifecycle ACK. Required Add/Remove and selected
report/type exchanges use existing native factories, full schemas, generation
and recipient actors. Native PToP header 0x8013 carries exact sender/recipient
NodeInstance (pinned SF `3988b451`, `Federation/PToPHeader.h`,
`PToPActor.h`, `Transport/MessageHeaderId.h`). No incarnation field is invented
inside NodeUp. A one-use launch capability on the original child's stdin admits
one original Channel/session; it is local trusted admission, not authentication
for an untrusted multi-user network or evidence of readiness. No reconnect is
provided.

Link loss leaves desired authority retained and observation unknown; the app can
still be serving. Stop uses independent original-child control. Nodes retain
their original Activation/app capabilities, perform real disposal/physical
cleanup, close executor/store and publish source-bound receipts. The foreground
reaps its original child/group handles and independently verifies app/runtime
absence. Unsupported/staged effects and semantic obligations are counted,
retained and never acknowledged merely because a write or send pass finished.
Original external-effect escrow is 32 slots; reply receipts, scheduling
completions and semantic pending maps have separate finite bounds.

Deletion joins the exact semantic RemoveInstance/DeleteReplica reply with a bounded local `retirement.json`
from the original Node. That receipt requires original-runtime service disposal,
the exact Applied Close command, original Hosting acquisition release and drained
predecessor replica work. A written frame, Closed snapshot, Remove response,
zero usage count or missing PID is not a substitute. Missing proof keeps delete
pending; old/cross-owner receipts cannot authorize another lifetime. Known
retired native traffic is ignored without acknowledging current work or closing
its healthy original link.

The app host/factory/package/runtime and retained partition tombstones remain
under their original owners. Only the service instance and its listener retire
between cycles; the public stateless instance ID equals the fresh ReplicaId,
not the separate RA incarnation. The FM and nodes never restart for recreation.
Unproved pending-Open cleanup may remain nonreusable. All existing finite
operation/message/escrow limits remain; this is not unlimited-cycle operation.

**Always reuse the same profile root for this ownership domain.** An exclusive
lock and atomically published dirty marker precede startup. Only verified
original cleanup and physical removal publish a clean predecessor handoff,
matching the separate authority marker and retaining both source-bound cleanup
receipts. A pre-activation child may explicitly prove no Activation was acquired;
missing/abruptly lost children cannot supply that receipt.
Owner/node loss or missing proof leaves the marker dirty and refuses relaunch;
there is no force/reset option. A different root is a different ownership/trust
domain, not a supported way around uncertain old authority. Do not remove the
small interlock files to bypass failed cleanup.

## Qualification contract

No nested Cargo invocation, native fixture or implicit build happens in tests.
Build workspace binaries before archiving. The existing nextest configuration
explicitly includes the fixed sample app. Tests resolve binaries at runtime from
the remapped target, and create unique private fixtures under the normal
temporary parent. No custom developer variables are required by the archive
consumer. Optional explicit overrides for local runs are:

```sh
export FUBE_CLUSTER_APP="$CARGO_TARGET_DIR/debug/cluster-stateless-app"
export FUBE_CLUSTER_STATEFUL_APP="$CARGO_TARGET_DIR/debug/hosting-local-app"
export FUBE_CLUSTER_NODE_EXE="$CARGO_TARGET_DIR/debug/fube-cluster"
export FUBE_CLUSTER_TEST_ROOT="/absolute/task-owned/runs"
cargo test --locked -p fube-cluster -- --test-threads=1
```

Missing prebuilt executables/invalid parents are failures, not passing skips.
The same archived group must run after extraction elsewhere, with the original
producer target unavailable. Unit tests
protect exact semantic identities, bootstrap/catalog admission, duplicate and
capacity behavior. Real executable-node tests fault the original Channel and
original OwnedApp capability, checking uncertainty with a live endpoint, stale
generation rejection, phase-eligible terminal completion, semantic report retry
until actual RA stage clearance, sibling isolation, partial-start rollback, lost-node
receipt refusal and clean-vs-dirty relaunch. The CLI test launches the real
foreground executable and both real nodes; its endpoint oracle is captured from
original launch/activation metadata before create. It verifies all actual app
identity fields and monotone lifetimes, rejects a swapped sibling, completes two
lifetimes per node while checking original process start identities and sibling
contact, observes disposal and independently checks final process/runtime
removal. Consumer tests reject serialized stale/cross-owner retirement proofs
and check both Remove/proof arrival orders. Dirty lost-owner fixtures intentionally retain
their failed-handoff domain and evidence. These tests are native-free evidence, not
deployed native FM/Federation qualification.
Stateful tests additionally exercise the real foreground FM/RA processes,
acknowledged public write/readback, dropped AddPrimaryReply with original-lifetime
retry, native retry-interval boundaries, FM normal deletion with both disposal
barriers, a lost DeleteReplicaReply, independent sibling contact and explicitly
requested fresh-data recreation after verified normal deletion. Same-session
predecessor frames and old receipts cannot retire the successor. The
archive includes both fixed app binaries; no consumer-side build fallback exists.
Owned app-crash/lease-loss compositions additionally exercise real FM channels,
native processed-versus-dropped reply semantics, a lost ACK with the actual
30-second retry, metadata-only DeleteReplicaReply and unaffected sibling data.
They retain their selected trusted construction premises rather than claiming
full native handler execution or unexpected-loss CLI/Federation parity.
The same composition covers graceful Close with separately held provider and
service disposal, no premature report/release, original-host survival and both
listener releases, stale/duplicate/lost ACKs and unchanged original disposal
proof. Close uses native ReplicaDown stage. Normal Drop additionally exercises
native ReplicaDropped stage, non-dropped ACK rejection, public access fencing,
both disposal barriers and the actual lost-ACK retry. Model tests preserve
already-closing/closed Down lifetimes and stage-eligible ACKs before the first
retry. Abort/ForceAbort additionally cover both real disposal barriers and
original-host survival; force Delete covers actual original-host termination
without manufactured destructor receipts. Closed AddPrimary resends cross the
same live channel without another acquisition or stage relabeling.
