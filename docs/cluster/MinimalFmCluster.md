# Minimal native-free FM cluster

**Profile:** one foreground in-memory FM, two separate RA+Hosting processes,
and one fixed stateless application profile. Explicit placement only; no
automatic recovery, reconnect, failover or native FM/Federation parity.

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
  describes the 45-case/59-type/199-field codec-adapter evidence. Source schema
  and codec execution are not deployed native handler/interoperability evidence.
- Existing runtime/client/data profiles and required native regressions remain
  separate; no unrun native qualification follows from native-free success.
- [Deferred decisions](../mvp-deferred/minimal-fm-cluster.md) preserve excluded
  stateful/PLB/FMM/HA/recovery/upgrades/autoscale/Naming/HM/provisioning/parity work.
  [Reusable-lifecycle decisions](../mvp-deferred/reusable-stateless-lifecycle.md)
  distinguish retained hosts from optional host/package recycling.
