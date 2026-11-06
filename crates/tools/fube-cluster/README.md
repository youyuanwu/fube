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

Use `create`, `status`, `delete` and `quit` in this variant. `AddPrimary`/`AddPrimaryReply`
use the native shared ReplicaMessageBody/ReplicaReplyMessageBody schemas and
distinct actions; no local Open observation becomes an FM acknowledgment.
FM stays pending until the exact original semantic Primary/Ready reply.
The original creating-primary request retries under native default per-replica
10/60-second minimum/maximum action intervals, without changing identity or
reopening the replica. Dynamic FM configuration is not enabled by this profile.
Independently send `write\n` then `read\n` to the reported application endpoint.
The bounded fixture accepts one fixed nonzero public mutation, applies its
admitted sequence before awaiting quorum completion, and returns progress/value.
It rejects writes before runtime/engine role, epoch, readiness and access agree.
This is volatile singleton acknowledgment, not durability or replicated copy.

Normal stateful `delete` is admitted only after the original Primary creation
completes. It sends native `DeleteReplica(force=false)` and waits for the
matching `DeleteReplicaReply` plus original Node disposal/accounting proof.
Lost replies retry the same lifetime with native 10/60-second action intervals.
Normal Delete produces no terminal ReplicaDown/ReplicaDropped-report ACK.
The deleted tombstone remains retained; stateful replacement/recreation,
force deletion, promotion, `DoReconfiguration`, copy/build, FMM and recovery
remain explicitly gated. `quit` uses the original
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
Drop/Abort and closed-AddPrimary reporting stay staged.
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
barriers, a lost DeleteReplicaReply, independent sibling contact and gated
force deletion/recreation. The
archive includes both fixed app binaries; no consumer-side build fallback exists.
Owned app-crash/lease-loss compositions additionally exercise real FM channels,
native processed-versus-dropped reply semantics, a lost ACK with the actual
30-second retry, metadata-only DeleteReplicaReply and unaffected sibling data.
They retain their selected trusted construction premises rather than claiming
full native handler execution or unexpected-loss CLI/Federation parity.
The same composition covers graceful Close with separately held provider and
service disposal, no premature report/release, original-host survival and both
listener releases, stale/duplicate/lost ACKs and unchanged original disposal
proof. Close uses native ReplicaDown stage; it does not enable Drop/Abort reports.
