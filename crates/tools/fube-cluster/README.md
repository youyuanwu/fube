# Bounded native-free FM cluster

This tool owns one in-memory FM and, by default, two child processes running the
existing local RA/Hosting/runtime. It supports only the fixed
`ClusterStatelessApp/ClusterService/Code/ClusterStatelessType` package. This is
not native Federation interoperability, full M1, recovery, failover or a general
service host.

An explicit `--stateful-singleton` variant admits the existing
`hosting-local-app` volatile package and one fresh 1/1 Primary lifetime per node.
The two nodes host independent singleton partitions, not two replicas or an HA
configuration. Stateless remains the default.

`--stateful --target-replica-set-size N --min-replica-set-size M` /
`ServiceProfile::Stateful { target: N, minimum: M }` creates one volatile
partition on N original nodes, with **immutable** settings satisfying
`1 <= M <= N <= 64`. The ceiling is an implementation resource limit, not
an SF replica-set-size rule; existing message, metadata and output budgets
remain enforced. Process qualification covers up to five replicas, not every
size up to that ceiling.

Send the existing `create` command for node1. Driving the cluster automatically
builds the original Secondary destinations, one at a time, then admits all
successful Idles through unchanged-Primary reconfiguration. Target/minimum
are present from the first AddPrimary/CreateReplica, and neither the settings
nor the service update version changes during admission. Minimum and
configuration-derived quorum remain separate: opening the Primary alone
does not bypass native write-access checks.

For N > 1, the create operation remains pending until every exact peer
activation and final RAP/FM configuration completion; `nodes[0].observed`
can already say `running` for the local Primary. Poll `configuration.ready`
or the retained create result, not registration or transport completion.
Status exposes `replica_set: { target, minimum, immutable: true }`.
For N = 1 the normal AddPrimary completion settles creation.

After completed initial membership or explicit repair, a matched Primary app-host
loss can start no-data-loss failover on the original live Node/RA sessions. Available
Secondaries need not all have identical progress. RA collects per-member
GetLSN/history, checks separate PC/CC read quorums, selects using deactivation
history before LSN and can redirect the FM candidate with ChangeConfiguration.
All required surviving Deactivate/Activate commits and final RAP/FM completion
precede endpoint publication. FM-link loss alone cannot authorize promotion.
Configured sizes and service version remain unchanged; current membership may
shrink, respecting minimum when removing additional down Secondaries.

After completed failover and processed predecessor deletion,
`Cluster::repair_primary_replica().await` explicitly restores one missing member
at the unchanged target/minimum. All other members must remain live. Continue
`drive()` and poll `repair.ready`: fresh host ownership and Ready/Idle copy
completion alone are not active membership. Physical predecessor cleanup gates
a fresh host/incarnation; native creation, copy, every surviving S/S Deactivate
and Secondary Activate commit precede final RAP/FM completion. The same live
Node/RA/store/peer Channels support repeated failover/repair cycles.

The configured profile still rejects resize, planned transfer and standalone
Secondary retirement/replacement. Host replacement is explicit, never automatic.
Insufficient read quorum blocks failover without declaring data loss. Additional
loss during an in-flight transition makes its ownership unknown, not successful.
Another configured failover requires completed repair; multi-loss repair,
interrupted/superseding recovery and whole-node/FM restart remain unsupported.
The named profiles below remain
separate qualification paths, including their historical size transitions and
repeated failover/repair.
See [native mapping and acceptance](../../../docs/cluster/MinimalFmCluster.md#immutable-configured-initial-membership).
See [configured failover evidence and limits](../../../docs/cluster/MinimalFmCluster.md#immutable-configured-primary-failover).
See [configured repair evidence and limits](../../../docs/cluster/MinimalFmCluster.md#immutable-configured-primary-repair).

`--stateful-three` / `ServiceProfile::StatefulThree` is a separate, fixed
three-node volatile profile. Create only the original node1 Primary. Then call
`Cluster::add_idle_replica_on(2)` and `add_idle_replica_on(3)` in either order.
Drive until `status()["idle_build"]["ready"]` is true; each entry in `builds`
retains its own native AddReplica request/reply and retry ownership.
`Cluster::activate_idle_replica()` then admits both built Idles in one healthy
1-to-3 DoReconfiguration, with target/minimum 3/2. Poll `configuration.ready`.
The APIs, not additional stdin commands, own build/configuration admission.

Both exact peer Activate replies and the final RAP EndReconfiguration must
complete before the FM configuration is ready. Both destinations copy actual
nonempty data and remain non-writers. A fresh write needs the Primary plus one
Secondary, not both; missing members never restore the historical singleton
quorum. Source-bound process tests also lose/replay build and final configuration
replies, preserve all original owners/one Open each and verify independent
three-child cleanup. The bounded Primary app-host failover and explicit repair
below are supported; planned transfer, general replacement/retirement,
generation upload, data loss and node/runtime adoption remain gated for this profile.
See [source mapping and limits](../../../docs/cluster/MinimalFmCluster.md#healthy-three-replica-service).

Once that original 3/2 configuration is complete, a matching native terminal
report for node1's Primary automatically starts one no-data-loss failover to
node2, provided both original Secondaries remain available. FM-link loss alone
cannot authorize it. Original per-peer listeners/Channels connect the surviving
RAs directly; no new process/runtime/service Open or replica build is substituted.
Actual local/remote GetLSN must return matched initialized progress and compatible
history before promotion/catch-up. Native S/S Deactivate is metadata-only, not
Close or disposal; its committed reply precedes Activate and final RAP/FM
completion. Poll `configuration.kind=primary_failover` and `configuration.ready`.
Only the exact new-Primary completion publishes its service endpoint.

The resulting current membership is node2 Primary plus node3 Secondary, still
with desired target/minimum 3/2 and write quorum two. Original nonempty data
continues on those owners. There is no automatic replacement of node1 or quorum
fallback if another member is lost. Lost final replies replay without another
promotion/Open; repeated old-Primary loss reports cannot undo the new endpoint.
This first-cycle example qualifies Primary **app-host** loss with its RA/Node
alive, not node/FM process failover, restart, mismatched-history repair or
data-loss recovery. See [the SF flow and evidence](../../../docs/cluster/MinimalFmCluster.md#three-replica-primary-app-host-failover).

After completed failover and the old Primary's processed native loss/deletion,
`Cluster::repair_primary_replica().await` explicitly starts one repair on node1.
Continue `drive()` and poll `status()["repair"]["ready"]`. The original Node/RA,
store and peer Channels remain owned; the original dead app must be reaped,
its process group drained, acquisitions released and Hosting directory removed
before a new app host, registration and runtime can be installed. The old IPC
gate is never revived. This is fresh volatile creation/copy, not persisted Open,
runtime adoption or reuse of the lost replica's data/identity.

The new Primary owns native AddReplica -> CreateReplica/Reply -> RAP build ->
AddReplicaReply for a strictly newer node1 replica ID/incarnation. Actual
nonempty copy precedes same-Primary reconfiguration from two to three current
members. Desired target/minimum stays 3/2; service version and data-loss epoch
do not change. Actual catch-up, committed S/S metadata Deactivate, both Activate
replies and final RAP/FM completion restore 1P+2S. Lost replies replay the same
operation. The new Primary and surviving Secondary never reopen; old reports
and Delete traffic cannot retire the fresh owner. After repair one Secondary
can be lost while writes continue; losing both still prevents acknowledgments.
Shutdown verifies old and new app cleanup even if repair is interrupted before
FM admits the new host. This is explicit app-host repair, not automatic
placement or whole-node/FM restart.
See [source mapping and qualification](../../../docs/cluster/MinimalFmCluster.md#post-failover-third-replica-repair).

Once repair completes, another loss of the current Primary app can repeat this
flow. FM uses latest committed membership and the lowest ready Secondary
placement as candidate; both current Secondaries' matched progress/history are
still required. Call `repair_primary_replica()` after each failover to restore
1P+2S at unchanged 3/2. Poll `repair.ready` for that cycle, not an older completed
configuration. Each replacement uses a strictly newer identity and physically
cleans its exact predecessor; the same Node/RA/store/peer sessions remain alive.
Three real-process cycles, repeated lost replies, stale predecessor traffic and
interrupted-second-repair cleanup are qualified. Concurrent repair/failover,
lagging-history selection, data loss and whole-node/FM recovery remain gated.
See [repeated-cycle SF mapping and limits](../../../docs/cluster/MinimalFmCluster.md#repeated-live-session-primary-failover-and-repair).

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

`Cluster::activate_idle_replica().await` is the separate opt-in same-Primary
configuration path after successful FM Idle build. It sends native
DoReconfiguration with a versioned service-size update to target/minimum 2/1.
Real RAP catch-up, peer Activate/Reply, secondary role callbacks and the final
RAP EndReconfiguration precede DoReconfigurationReply. Poll root
`configuration.ready`; neither an Idle build nor peer activation completes FM.
The destination becomes ActiveSecondary, not another Primary or a new
application listener. The original Primary endpoint remains unchanged.
Current two-member and previous singleton quorum obligations stay distinct:
loss of the Secondary cannot acknowledge a fresh write from the old singleton.
Exact original requests retry/replay without repeating Open/build/role work.
Outside the explicit retirement/replacement and planned transfer below,
Idle removal/replacement, node/service mutations, generation upload and
additional configurations remain gated; owner
shutdown remains available. See the [source mapping and acceptance boundaries](../../../docs/cluster/MinimalFmCluster.md#same-primary-active-configuration-stage-5-increment).

`Cluster::retire_secondary_replica().await` explicitly retires the **healthy
original ActiveSecondary**, keeping the same Primary. Native 2-to-1
DoReconfiguration retains the two-member previous quorum until real RAP Catchup
succeeds, then Deactivate drops/closes the original Secondary before replying.
Final RAP EndReconfiguration and committed native FM completion remove the
retired member. Poll `configuration.kind=same_primary_retirement` and
`configuration.ready=true`; `peer_idle.state=retired_secondary` is not active
membership. No synthetic Idle RemoveReplica/Reply completes this path.
Explicit `delete_idle_replica_via_fm()` must still join its semantic reply with
independent original disposal/usage-release proof before
`replace_idle_replica()` can build the one fresh successor.
`activate_idle_replica()` then reactivates that built Idle through real RAP.
`configuration.history`/`idle_predecessor` preserve the predecessor results.
Original processes, factory/runtime owners and Primary endpoint stay unchanged.
Host loss never authorizes retirement or restores singleton quorum; second
replacement, concurrent supersession and restart stay gated.
See the [source mapping and evidence limits](../../../docs/cluster/MinimalFmCluster.md#healthy-configured-secondary-retirement-and-replacement).

`Cluster::transfer_primary().await` explicitly transfers Primary from node 1
to node 2 after completed healthy two-member active admission, including the
supported replacement. Native phase-0 RAP demotion/write revocation must finish
before peer DoReconfiguration continuation. The candidate then obtains actual
local RAP and peer GetLSN progress, promotes/catches up, activates the demoted
member and completes final RAP configuration and its committed FM reply.
The native default PC=CC path skips phase-3 Deactivate; neither member reopens
or rebuilds. Existing data and node/app/factory/runtime owners remain intact.
Poll `configuration.kind=planned_primary_transfer` and `ready=true`.
Only the exact new-Primary reply publishes node 2's endpoint in `nodes[1]`;
the old endpoint is withdrawn while pending, and its retained listener rejects
writes after real demotion. Proxy endpoint ACKs and optional native 30-second
ReplicaEndpointUpdated/Reply publication remain distinct from final completion.
Prior requests/replies remain in `configuration.history`; current swap
phase/error is in `nodes[].retained_work.local_replica.swap`.
Lost replies replay the retained continuation/completion without another Open.
New-Primary loss yields unknown, never failback or quorum reduction. Second/
reverse transfer, progress mismatch repair, swap abort, automatic failover,
restart and full native handler/interoperability qualification remain gated.
See the [source mapping and acceptance boundaries](../../../docs/cluster/MinimalFmCluster.md#healthy-planned-primary-transfer).

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
broader down/arbitrary-unknown cleanup and replacement outside the explicit
fresh-Idle slice below remain gated.

`Cluster::replace_idle_replica().await` explicitly authorizes **one fresh Idle**
after the original successful FM build, matched Primary RemoveReplicaReply,
normal non-force destination DeleteReplicaReply and independently verified
original runtime disposal/committed Close/exact usage release. Both original
node/app sessions must remain live. It is not automatically enabled by deletion,
and does not admit failed/absent/host-lost predecessors or app adoption.
The new replica ID/incarnation exceed the original IDs, with a new service/
replicator lifetime on the same resident factory/processes. The normal native
Add/Create/build sequence recopies nonempty Primary state and continues
replication; poll `idle_build.ready` for semantic completion. The successor
remains Idle/InBuild, non-serving and unconfigured.
Repeated calls retry/observe that same successor; a second replacement is gated.
`status()["idle_predecessor"]` retains original Add/Remove/Delete outcomes and
disposal proof, while the ordinary Idle status fields follow the successor.
Old messages/proofs cannot retire or complete the new lifetime. The successor
supports explicit normal removal/deletion with its own matching proof.
See the [source mapping and evidence limits](../../../docs/cluster/MinimalFmCluster.md#explicit-fresh-idle-replacement).

`Cluster::delete_idle_replica_via_fm().await` separately authorizes native
`DeleteReplica(force=false)` on the **original destination**,
after FM abandonment has been requested. It need not wait for
`RemoveReplicaReply`: native FM schedules destination Delete independently of
Primary pending-remove. Poll `peer_idle.fm_deletion` (also root `idle_deletion`).
`destination_deleted=true` records the matching canonical dropped
DeleteReplicaReply; `disposed=true` records the independent original-node
disposal/committed-Close/exact-usage-release receipt for an opened lifetime.
The reply and matching cleanup proof together produce
`state=deleted`/`complete=true`. Child observations or transport writes cannot
complete either barrier. The original Delete retries on native 10/60-second
cadence until its semantic reply, without another Close, Open or acquisition.
Destination deletion never clears pending-remove; only RemoveReplicaReply does.
Destination-owner loss leaves unresolved cleanup unknown, while Primary-owner
loss does not undo independently owned destination deletion.
The app host/package remain resident and `reusable=false`; no replacement is
authorized. Normal Delete produces no terminal ReplicaUp report. Delayed
original CreateReplica is ignored by the deleted tombstone and cannot reopen
the Idle. Original-session closing/closed/lost and explicit force deletion now
follow the complete bounded contract below; foreign identities and replacement
remain gated.
An original failed Add keeps its error even if an already-opened destination is
explicitly deleted; semantic deletion clears to-be-dropped, and joined disposal
clears destination-cleanup-required, separately from pending-remove.

The original-session Delete subprofile also covers normal Delete during an
existing Close/Drop/Abort, closed metadata, host loss and Delete-error escalation
to ForceAbort. Normal Delete waits/retries rather than superseding another
close; a later closed Delete sends no second RAP call. Error escalation sends
real RAP Abort on the existing retry and cannot report a locally deleted FT.

`delete_idle_replica_via_fm_with_force(true)` explicitly authorizes native force
for that same original target. Open force uses Obliterate/termination of the
original acquired app host, including superseding held cleanup. Without adopted
registration it finishes closure without killing the factory; closed/missing
force uses normal metadata Delete. Retries retain the original descriptor and
any explicitly escalated force flag.

`completion=host_lost` joins the native metadata reply with `host-loss.json`:
positive original-process exit, original Applied Delete intent, closed/deleted
unfenced RA state and fully drained original ownership. It sets
`host_lost=true`, **not** `disposed=true`. Original applied disposal/no-runtime/
failure histories remain valid after later app exit while the same Node owns
them, and take precedence over host-loss classification.
Unsolicited terminal reports still supply no operator-delete/reuse authority;
explicit Delete can join the original retained cleanup fact after such a report.
Metadata without proof stays incomplete with `cleanup_state=unproved`; losing
the original Node session leaves `state=unknown`.
All outcomes preserve independent Primary pending-remove, `reusable=false`,
historical Add errors and no recovery/replacement permission.
See the [complete bounded contract and acceptance matrix](../../../docs/cluster/MinimalFmCluster.md#complete-original-session-destination-delete-contract).

For the original **unanswered FM Add**, deletion also admits a destination
that has no FT yet. RA commits the native missing-FT deleted tombstone and
returns the canonical dropped reply, with the constructor's zero target/minimum
and invalid package/LSNs, without registration lookup, RAP Open/Close or usage.
A distinct `absence.json` sidecar carries the original Node's applied Delete
command/RA-operation lineage and committed never-opened metadata. It is
published only after original local work drains, with zero Open/Close history.
FM joins that independently validated proof with the matching native reply and
reports `completion=never_created`, `never_created=true`, `disposed=false`.
An opened lifetime instead reports `completion=disposed`. Neither an absence
observation alone nor a semantic reply alone completes cleanup. No runtime
disposal or release receipt is invented for a service that never existed.
The actual app host/package remain resident; `created_lifetime=false` and
`reusable=false` distinguish metadata deletion from a created/reusable service.
Late original CreateReplica is ignored without a dropped report or Open.
First admission also permits the original registered in-flight Open with its
committed runtime/usage ownership. Delete commits Closing, cancels RAP Open and
retries Close after the original callback drains, on the unchanged 15-second
Node cadence. A control returned after cancellation is retained for cleanup
and aborted before the service, never opened or published as Ready.
This branch freezes disposal as required: retries cannot downgrade it to
never-created absence. Held real Open and independently held provider/service
disposal, late traffic, reply replay and actual child adapters qualify the path.
The original unanswered FM Add also admits pre-registration Opening. Native
InCreate/no-registration Delete commits the tombstone/reply without RAP Close;
late lookup offers release their exact acquisitions without RuntimeOpen.
The distinct `pre-open-cancellation.json` proof binds original creation and
applied Delete lineage, and is published only after local lookup/usage drain
with no runtime route. Its join with DeleteReplicaReply reports
`completion=pre_open_cancelled`, `pre_open_cancelled=true`, `disposed=false`,
`never_created=false`, `created_lifetime=false`, `reusable=false`.
The FT retains its original 1/1 description, unlike missing-FT absence.
Observed pre-registration cancellation cannot downgrade to zero-history absence
on retry; registration winning the race instead requires genuine disposal.
Unobserved original creation retains the appropriate actual-outcome proof.
Held real lookup ownership after the native reply, late registration/traffic,
reply replay and actual child adapters qualify these separate barriers.
The original known pre-Open registration failure also supports normal closed
Delete without RAP Close. `failed-registration-deletion.json` binds the actual
applied registration failure/lookup/creation lineage and separate applied
Delete, and publishes only after original local ownership drains without a
runtime route. FM freezes the failed Add error and joins that receipt with
DeleteReplicaReply: `completion=failed_registration`,
`failed_registration=true`, with disposal/absence/pre-Open-cancellation flags
false, no created runtime lifetime and no reuse. The failed Add/error and
Primary pending-remove remain independent. Deleted metadata retires only the
matching failed-create report placeholder, not a runtime cleanup obligation.
Actual Hosting/process/Channel acceptance uses an existing dynamic Drop=1
registration policy for qualification, without changing production defaults.
Late registration/Create, lost replies and commit/lineage guards are covered.
Terminal runtime Open failure has a separate `failed-open-deletion.json`
receipt: the original runtime's error reply follows Abort/final release, then
the Applied terminal Open completion, exact original usage release and separate
Applied closed Delete must all match. No RAP Close is fabricated. FM freezes
the failed Add error and joins that receipt with DeleteReplicaReply, reporting
`completion=failed_open`, `failed_open=true`, `disposed=true`. Other cleanup
origin flags remain false. Failed Add and Primary pending-remove stay independent;
no reuse is granted. Actual process/Channel tests hold provider/service disposal
and fail the initial Idle role callback within RAP Open, using the existing
dynamic Open Drop=1 threshold for qualification only. Production defaults and
native wire schemas are unchanged.
Host loss (including later registration failure after runtime loss),
unproved cleanup histories, arbitrary targets,
future incarnations and replacement are not authorized by this branch.

The separate original **opened Idle app-host-loss** path sends native
ReplicaDown-stage/dropped-list ReplicaUp on its still-live destination node
session. FM requires its frozen original Add and returns the entry in the
processed ACK list. RA commits closed metadata Delete and sends
DeleteReplicaReply without RAP Close; FM retries that metadata request with
the original Idle descriptor. The unchanged native report retry is 30 seconds.
Status records `lost`/`destination_lost`, clears current readiness and retains
the historical Add reply/error. `native_replica_deleted` is metadata only:
there is no disposal/absence/failure cleanup receipt, completed physical
cleanup, reuse or replacement. Primary pending-remove needs its own reply,
and explicit FM Primary-side removal remains available after loss.
Actual two-child acceptance kills the original destination app, holds Primary
removal, exercises stale/lost ACK and metadata reply replay, and verifies
continued Primary writes. Pending-Add and commit/owner cases have separate
model/store coverage.
Normal RAP Close/Drop now preserves the original committed opened Idle's origin
through Closing. AppHostDown can supersede that pending Close/Drop and use the same
loss path, without treating partial provider/service cleanup or a late Close
reply as disposal. Successful Close/Drop winning first instead leaves later
HostDown stale; completed Drop keeps ReplicaDropped stage and its disposal proof.
Controlled owned-Node/process/Channel tests hold real provider/service disposal,
kill the app, and verify metadata reply replay, late traffic and independent
Primary removal/writes. The pending-Drop provider-held case also exercises
an actual 30-second lost-ACK retry while both original Nodes/peer/FM links progress.
Separate opened-origin/loss/ACK commit barriers and both races for each mode are
covered by model/store tests. No child-node Close-control feature is added.
Opening loss, Abort/ForceAbort loss and broader unknown targets remain
gated; no wire schema or native handler qualification is added.

Successful normal Close of that original opened Idle now uses the same native
ReplicaDown-stage/dropped-list report and processed ACK/closed metadata Delete,
without requiring HostDown. Only a matching Applied successful normal RAP Close
with the retained opened origin enables it. Real disposal and exact original
usage release remain independently certified by `Node::instance_disposal()`;
ACK/metadata replay does not replace or change that proof or issue another
Close. The app host/runtime/factory stay resident. FM records lost volatile
state, not a reusable placement: even a valid retirement sidecar is not reuse
authority in this terminal-report flow, and Primary pending-remove needs its
own reply. Controlled process/Channel tests hold provider/service disposal,
lose/retry an ACK at the unchanged 30-second cadence, replay metadata replies
and retain healthy Primary writes. Initial-Open, failed or other-mode Close
does not gain this down-stage reporting authority.

Normal `CloseMode::Drop` of that original opened Idle now uses
**ReplicaDropped stage**, not ReplicaDown, while sending the same canonical
ReplicaUp dropped entry. Only committed opened-origin intent and matching
successful RAP completion enable this path. A non-dropped down/Ready ACK
cannot clear it, even before the first retry. The processed-list ACK/closed
metadata Delete and original disposal/usage proof remain independent, with
no second RAP Close, Primary pending-remove completion or reuse/replacement
authority. A later Drop cannot supersede an already-closing Close or relabel
its closed Down report. Controlled provider/service disposal, actual 30-second
lost-ACK retry, stable proof/metadata replay and healthy Primary writes are
covered separately from intent/completion/ACK commit and both-list ACK tests.
Matching HostDown during opened-origin Drop now uses ReplicaDown instead and
cannot turn partial cleanup or late RAP replies into disposal proof.
Initial-Open and failed/ForceAbort reporting remain gated.
No new wire types, child-node control operation or native handler qualification
is added; see the [bounded Drop guide](../../../docs/cluster/MinimalFmCluster.md#normal-opened-destination-drop-reporting).

Normal non-force `CloseMode::Abort` now supports that original opened Idle too.
It sends RAP Abort (`flags=4`) and reports **ReplicaDropped stage** only after
matching successful cleanup and Applied completion. Shared Node/RA admission
requires original opened-peer lineage; local-only Idles, initial Open and
arbitrary closed origins remain rejected. Non-force Abort cannot supersede a
pending close, even without runtime registration, or relabel a closed report.
The existing original disposal/usage proof remains independently available
before ACK and stable through metadata replay, without a fabricated Close
callback. Strict dropped-description ACKs, the unchanged 30-second retry,
metadata deletion and independent Primary removal/no-reuse boundaries are
preserved. Controlled real provider/service disposal verifies actual Abort,
lost-ACK retry and stable proof/metadata replay; model/store tests cover
admission, both ACK lists and intent/completion/ACK commit barriers.
HostDown during Abort, failed/ForceAbort reporting and broader replacement remain
gated. No new child-node control feature or native handler qualification is
added; see the [bounded Abort guide](../../../docs/cluster/MinimalFmCluster.md#normal-opened-destination-abort-reporting).

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
original FM session; that background path grants no FM creation/promotion authority.
Normal original-Idle deletion is separately admitted as described above.
Peer loss does not dispose an application, fabricate a reply or reconnect.
This profile admits CLI `create` on node1, `status`, and `quit` only; it rejects
new FM generations, mutation of the fixed service, and node2 FM creation.
Build completion still leaves Idle/InBuild metadata, no service endpoint and
denied access. There is no quorum addition, serving secondary, failover,
replacement, or recovery. Removal
before peer Open through the local API remains gated; both original owners retain
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
General endpoint publication outside the bounded planned swap, multi-FT
deployment and FMM remain gated.

The programmatic `Cluster::begin_generation_handshake()` starts the bounded
ordinary-FM GenerationProposal/Reply and GenerationUpdate/Reject round.
It requires two admitted nodes without pending lifecycle/control requests.
This is opt-in, not a CLI command or a change to default bootstrap. Proposed,
receive and send generations remain distinct; only receive changes on a
matching Update. The round waits at `awaiting_upload`, `complete=false`:
the RA queues per-entity committed-inventory reads, then sends native correlated
LFUMUpload/Reply on the same session. Only a reply matching current receive
generation advances send generation. FM retains both exact original inventories
in `generation_handshake.uploads`; both processed reports set `phase=complete`,
`complete=true` and allow new-generation mutations/service updates.
Lost replies retry the frozen metadata after the native 30-second interval,
without blocking owner shutdown or reopening replicas. Existing Primary data
and original node/app/runtime ownership remain intact. This is one live-session
round for stateless/independent singleton profiles, not application-data upload,
unknown-partition reconstruction, peer-profile upload, nonempty NodeUp,
full ReplicaUp batching, FM/FMM recovery or durable restart.
See the [source mapping and remaining gates](../../../docs/cluster/MinimalFmCluster.md#live-session-lfum-upload).

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
