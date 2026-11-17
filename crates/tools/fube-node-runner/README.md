# Standalone externally managed Node

`fube-node` runs one Linux Node/RA/Hosting owner and its application child without
the local FM launcher's parent/stdin contract. An external FM owns the native
reliability exchanges. The execution profile is one fresh volatile partition
with explicit, immutable `target` and `minimum`, satisfying
`1 <= minimum <= target <= 64`. The statically known Node pool may exceed
target, up to 64 Nodes: list every other pool member in each Node's `peers`.
Initial active membership still contains target replicas; unused placements
are spares, not voters. A one-Node pool has no peers. The ceiling is
a fube resource bound, not an SF replica-set-size rule.

The runner is a thin process/transport owner over
[`fube_node::control::ingress`](../../libs/fube-node/src/control/ingress.rs).
The local cluster uses that same native FM/peer dispatch, topology checks and
capability admission. There is no standalone-only creation/configuration state
machine: retries, epochs, roles and completion belong to the shared Node/RA.

This is experimental. Native-free tests exercise variable-size Node deployments,
real runtime/replicator construction, copy/build, application replication,
lost-reply replay, no-data-loss Primary app-host failover, explicit fresh-Secondary
repair and owned shutdown. Repair uses shared Node/Hosting activation driven by
the committed fresh replica Open's registration lookup, not a runner command.
No native FM/Federation interoperability, reconnect, resize or restart recovery is claimed.
The [process integration tests](tests/standalone.rs) also lose the original FM
Channel, verify continuing acknowledged writes on the same Primary, and require
explicit shutdown with retained control uncertainty. Staggered shutdown is
qualified both after native deletion and after a peer exits before deletion.

## Build and run

```sh
cargo build --locked --workspace --bins --examples --all-features
target/debug/fube-node --config /etc/fube/node.json
```

The package and the run-directory parent must already exist. The configured
`run_directory` itself must **not** exist: the runner reserves it once and never
adopts a previous owner. Successful shutdown retains `status.json`; it does not
make the old configuration restartable. Disable container automatic restart.

The user supplies a trusted minimal ExeHost package supported by
`fube-local-host`. The parameterized `VolatileStatefulExternalFm` package profile
requires resolved volatile stateful manifest sizes to match the configured
target/minimum. The configured local cluster uses this same Hosting profile;
legacy local package profiles retain their 1/1 checks. Hosting still owns actual process/registration/usage/lease cleanup;
running inside an OCI container does not enable native SF container activation.

### Independent node shutdown

SIGTERM/SIGINT initiates native ordinary `Close`, not service deletion or a new
Drop. An already accepted close/deletion continues in its original mode. The
runner observes local completion, then separately reaps its owned application
and joins the executor, store and original Channels. Other nodes do not have
to receive their shutdown signals together.

Loss of an original Channel retains `control_unknown` and a diagnostic
`control_error`, refuses new control intent, and never reconnects or promotes.
The original owner remains alive until explicitly stopped; connectivity loss
does not revoke a surviving writer. Successful local shutdown does not clear
that uncertainty. `cleanup.clean` and exit code zero mean owned local shutdown
succeeded, not that control remained healthy, FM deletion was acknowledged,
pending remote reports completed, or the deployment is recoverable.
`cleanup.control_error` retains the lost-session diagnostic separately from
`cleanup.error`. Startup, protocol/authority validation and local cleanup errors
still produce a failure exit, even if physical resources were subsequently
reaped. Protocol/limit failures are not classified as ordinary disconnection.

This follows the native distinction between local RA close and Federation
connection health; it does not implement Federation `Depart`, membership or
lease recovery. Source baseline `3988b451`: `ReconfigurationAgent::Close`,
`CloseAllReplicas` and `SiteNode::OnConnectionFault`.

### Process-binary tests

The configured-size process matrix covers 1/1, 2/1, 3/3, 4/2, 4/3, 4/4 and 5/3:
bootstrap, initial membership, lost build/final-reply replay, stable LFUM,
Primary app-host failover where applicable, continuing nonempty data, normal
active deletion/replay and owned shutdown. At 3/3 and 4/4, failover retains the
down former Primary when minimum requires it; a retained member supplies no
vote. Closing the remaining Secondaries cannot acknowledge a new operation.
Invalid bounds, missing sizes and package/native-description mismatches reject
before the corresponding directory acquisition or replica Open.

The additional 3/2 integration tests launch three actual `fube-node` processes
with separate run directories and concrete loopback-alias IPs. A native fixture
FM selects the middle Node as Primary and drives the real application children
through bootstrap, copy/build, 3/2 admission, acknowledged writes/readback,
lost-reply replay, refusal and owned shutdown. A separate case kills the exact
Primary application, waits for its native loss report, promotes an original
Secondary through real GetLSN/Deactivate/Activate, verifies retained data and
new acknowledged writes, and replays the final reconfiguration.

#### LFUM qualification

Stable inventory is qualified across the configured-size matrix above.
Held build/admission/failover/repair overlaps below retain their 3/2 evidence;
configurability does not extend every overlap or recovery profile.

| Standalone live-session profile | Current process evidence |
|---|---|
| Empty pre-placement and stable configured membership | Correlated proposal/update/upload/reply, full committed member inventory, acknowledged data on original owners and new-generation control/replay, including singleton and minimum=target. |
| Original Idle build and same-Primary admission | Public copy/Secondary role barriers, superseding generations and stale replies. Initial configuration can be admitted before build-upload ACK; exact peer retries resume after send advancement without another Open or mutated inventory. |
| First original Primary app-host failover | Real Primary role and public IPrimaryReplicator final-configuration barriers. Query-only survivor PC, candidate phase-4 IC and completed survivor views are checked before continuing nonempty data. |
| First explicit post-failover repair | Normal AddReplica/CreateReplica demands shared Hosting activation after predecessor cleanup. Repair configuration and fresh Secondary completion can precede ACK, but cannot finish held final RAP work. Exact replays activate no second host; the fresh member supplies quorum after another survivor loss. |
| Second live failover/repair cycle | Promote the repaired replica and repair the next lost Primary. Freeze the latest completed membership/fresh identities for superseding uploads during actual promotion/final RAP, build and Secondary activation. Survivors retain their owners/data; a new write uses the second repaired member's actual quorum. |
| Native deactivation during repair activation | Close the fresh Secondary while its public role callback and two frozen generations remain held. Stale ACK retires only its upload; current ACK cannot finish Close/deactivation or repair. Original service/provider disposal follows role release; host/group/root cleanup is separate. Healthy survivors acknowledge a new write, but Primary alone cannot use the interrupted replica's vote after another loss. |

Shared RA owns generic committed inventory and independent PC/IC/CC views,
without a lineage allowlist or upload-pending peer-control gate. The fixture FM
only validates known membership and explicit intent. Each accepted update owns
fresh reads; Node retains independent correlated requests and frozen retries,
including same-generation uploads. Diagnostic `pending_uploads` counts all such
owners separately from current-generation `upload_pending`; only a reply
matching current receive advances send generation.

See the [shared current contract/source map](../../../docs/cluster/MinimalFmCluster.md#live-session-lfum-upload)
and [LFUM alignment plan](../../../docs/cluster/RustRaPort.md#lfum-alignment-plan).
Copy/build interruption and removal supersession remain separate qualification.
Deactivation keeps Node/RA live while the replica closes; whole-RA shutdown of
accepted snapshot jobs and automatic interrupted-repair recovery are not claimed.
Healthy planned transfer is qualified at 2/1 and 3/2, including either Secondary
as the 3/2 candidate. FM sends native DoReconfiguration to the old Primary with
P/S, S/P and (at 3/2) S/S roles, unchanged identities/sizes and the next Primary
epoch. Shared RA owns demotion, peer continuation, timed quorum/history GetLSN,
catch-up, every Secondary's committed activation and final RAP/FM completion.
Unequal progress is permitted when history selects the forwarded candidate.
An unchanged Secondary commits activation metadata without a RAP request,
even with its GetLSN callback pending; the demoted member also needs no second
RAP round trip after its real phase-0 role change.
Publish the new Primary only after its successful native DoReconfigurationReply;
ReplicaEndpointUpdated/Reply is not completion. Process tests replay lost final
replies, fence the old writer, retain owners/data and require the unchanged
Secondary's real vote after the old Primary closes. Non-responding or
uncatchable Secondaries that SF would restart explicitly block completion:
native Secondary restart/rebuild is not implemented. Other sizes,
abort/reverse transfer and generation/LFUM overlaps remain unqualified or gated.
Nonempty NodeUp reconciliation, full initial ReplicaUp batching/final ACK,
FM/FMM rebuild and restart remain gated. Native-free acceptance is not complete
original C++ handler or native FM/Federation interoperability evidence.

```sh
cargo build --locked --workspace --bins --examples --all-features
cargo nextest run --locked --workspace --all-targets --all-features \
  --test-threads 1 --no-fail-fast -E 'package(fube-node-runner)'
```

These are ordinary non-ignored Rust tests, included in archived workspace CI.
They require no Docker daemon, image build, native Service Fabric or onebox.
Build the executables before testing; archived runs use their bundled binaries.
Set `FUBE_CLUSTER_TEST_ROOT` to an existing absolute directory to choose where
fixtures live. Successful fixtures are removed; failed status/log evidence is
retained.

The delivered component is the standalone process binary and its native FM
contract. Container images, networking and orchestration belong to the user;
they are not a separate fube test tier. If deployed in a container, use a
container init so the runner is not the PID-1 namespace reaper, provide writable
run-directory storage, and disable automatic restart. Process-level tests do
not claim container-environment qualification or recovery.

## Configuration

Example for the first of three original placements:

```json
{
  "schema_version": 1,
  "deployment": "11111111-1111-4111-8111-111111111111",
  "name": "node1",
  "node": {
    "identity": { "id": "1", "instance": 1 },
    "address": "10.42.0.11:19002",
    "replica_id": 10,
    "replica_instance": 1
  },
  "fm": {
    "identity": { "id": "f", "instance": 1 },
    "address": "10.42.0.10:19001",
    "generation": 1
  },
  "peers": [
    {
      "identity": { "id": "2", "instance": 1 },
      "address": "10.42.0.12:19002",
      "replica_id": 20,
      "replica_instance": 1
    },
    {
      "identity": { "id": "3", "instance": 1 },
      "address": "10.42.0.13:19002",
      "replica_id": 30,
      "replica_instance": 1
    }
  ],
  "partition": "22222222-2222-4222-8222-222222222222",
  "target": 3,
  "minimum": 2,
  "package": "/opt/fube/package",
  "application": "fabric:/External",
  "package_version_instance": 1,
  "parameters": [
    ["TargetReplicaSetSize", "3"],
    ["MinReplicaSetSize", "2"],
    ["Arguments", "--stateful-lifecycle --data-replicator --replication-listen 10.42.0.11:19003 --service-listen 10.42.0.11:19004"]
  ],
  "run_directory": "/run/fube/node1-session1",
  "startup_timeout_ms": 30000,
  "shutdown_timeout_ms": 20000
}
```

The sample parameters above apply to `hosting-local-app` and its `pkg-stateful`
manifest. Package that executable under `LocalService/Code/hosting-local-app`.
Its default target/minimum parameters remain 1/1 for existing local fixtures;
the explicit overrides select 3/2 here. A custom application supplies its own
declared parameters, listeners and client protocol.
Both size fields are required; there are no compatibility defaults. To select
3/3, set `minimum` to 3 and make the resolved package minimum 3 too.
Size changes during the deployment remain unsupported. For a spare, configure
an additional distinct placement and include it in every original peer list
before startup; dynamic joining is not supported.

Node IDs are nonzero hexadecimal numbers, not UUIDs. Configuration supplies the
original Node/replica instances, replica IDs, bootstrap generation and
package-version instance. Every Node has the same deployment, partition, initial
generation and service/package facts, but its own local placement, peer list and run directory.
The FM may select any original placement as Primary; no node number or
application role flag selects it.

The runner uses the configured concrete IPv4 socket for both RA binding and
publication. It does not need a separate advertised IP or store `0.0.0.0`.
All configured Node IPs must be distinct. Concrete loopback aliases are allowed for
same-host testing, not as evidence of cross-container connectivity. Container
deployments must use their reachable container IPs, without NAT/port translation,
service VIPs or load balancing between replica endpoints.

RA peer connections bind their actual source IP to that Node's configured IP
and use ephemeral source ports. Lower numeric Node IDs dial higher IDs.
Acceptors associate the actual source IP with an original peer, not accept
order or an ephemeral port. The native transport handshake completes the local
mesh barrier without waiting for application/reliability messages. Original
session loss never reopens a listener or replaces a peer Channel.

General TCP is plain, unencrypted and unauthenticated, without a security
opt-in. TLS is deferred. PToP identity/generation checks are reliability
validation, not authentication. Runtime/Hosting/RAP/finite-lease IPC stays on
loopback inside each Node's container.

## External FM contract

Use the existing [`fube-ra` native codecs](../../libs/fube-ra/src/protocol.rs) and
[`control::wire`](../../libs/fube-node/src/control/wire.rs) compositions/Link.
The public profile identifier in status is `external-fm-volatile-configured-v1`;
`replica_set` reports the actual target/minimum and their immutability.
It describes an execution subset, not a new native wire schema or HTTP API.

All traffic retains native PToP headers (`0x8013`) with exact configured
sender/recipient instances. Ordinary-FM Generation headers (`0x8018`) must match
the configured owner/number/scope exactly on requests/replies that carry them.
Bootstrap correlation and semantic one-way replies are different mechanisms:

| Actions | Native actor/priority and delivery |
|---|---|
| NodeUp / NodeUpAck | FM=11 / RS=13, high priority. Ordinary correlated request/reply; the ACK carries generation. |
| ReplicaUp / ReplicaUpReply | FM=11 / RA=12, high priority. Semantic one-way exchange; reply carries generation. |
| AddPrimary, AddReplica, RemoveReplica, DoReconfiguration, DeleteReplica and their replies | Requests RA=12, replies FM=11, high priority. One-way; requests carry generation. |
| CreateReplica/Reply, GetLSN/Reply, Deactivate/Reply, Activate/Reply | RA=12, high priority, one-way, each carrying generation. |
| ServiceTypeNotification/Reply, ServiceTypeEnabled/Disabled and their replies | FM=11 outbound reports / RA=12 ACKs, normal priority, one-way; ACKs carry generation. |
| ReplicaEndpointUpdated/Reply | FM=11 / RA=12, normal priority, one-way; reply carries generation. |

Generation negotiation, node activation/deactivation, service updates and load
forwarding also use the shared control path. Dispatch is not unconditional
capability admission: the Node/RA and owned Hosting profile retain their
existing unsupported-operation, inventory, lifecycle and recovery gates.
Ordinary FM commands require the Node's admitted receive generation; changing
the configured header without the native generation handshake grants nothing.

Use the [native reliability catalog](../../../docs/cluster/NativeRaProtocol.md)
for field order, object scopes, headers and codec evidence. Preserve complete
opaque service/replication endpoint strings from native replies, including
replication actor/incarnation/suffix fields. Native node instance, RA replica
instance, package-version instance and replication actor incarnation are
different identity domains; do not substitute one for another.

| Exchange | Required behavior |
|---|---|
| NodeUp / NodeUpAck | Node requests fresh bootstrap on its original FM Channel. Validate the configured Node instance/PToP context; return the matching activated native version and fixed generation. |
| Final empty ReplicaUp / ReplicaUpReply | Complete the original empty-inventory handshake before any local application authority. No nonempty reconciliation/recovery upload is accepted. |
| AddPrimary / reply | Select exactly one initial Primary. Supply the actual package/type/version, configured partition and local replica identity, matching volatile target/minimum description, and native initial-role fields. For a singleton, this completes initial membership. |
| AddReplica / reply | Ask that Primary to build each original destination. Wait for its actual reply, not CreateReplicaReply or local Idle readiness. |
| DoReconfiguration / reply | For target > 1, admit the exact original live Ready identities with P/P and I/S roles, unchanged service description and data-loss epoch. Every required peer commit and final RAP completion precedes the final native reply. |
| Service-type notification/status and matching replies | Process and ACK actual native work when emitted. These are not a mandatory pre-placement factory-ready handshake. |
| ReplicaUp / reply | Match the real processed-report semantics. A verified app-host loss can inform explicit FM failover intent; report ACKs are not physical disposal or replacement-host authority. |
| Endpoint-updated reply, when applicable | Match the exact native local lifetime; registration/listeners do not constitute client-ready publication. |
| Normal DeleteReplica / reply | Tear down the whole service through exact per-member deletion. Local physical cleanup is a separate observation. The configured-size matrix does not qualify broader force-deletion branches. |

After fresh Node bootstrap, FM may issue placement for its known package/type.
RA retains actual registration lookup/Open and semantic completion; the FM must
not wait for a fabricated pre-create Ready notification. ServiceTypeNotification
is emitted for actual FT/type association, not as a prerequisite to creating
that first FT.

After initial admission, an external FM may drive the same supported native
reconfiguration as the local cluster. Primary failover requires an actual
matched app-host loss and eligible surviving history/quorums, not a lost FM
connection. The shared ingress enforces the declared immutable sizes and original peer
bindings; the Node/RA enforces exact epochs, replica lifetimes, runtime authority,
commit barriers and callback completion. Capacity retains the original incoming
frame without acknowledging admission, in both launchers.

Unsupported native branches still reject explicitly. The runner does not expose
the local launcher's owned replacement-host control: fresh-host provisioning
belongs to shared Node/Hosting after admitted replica creation. It does not invent an external FM,
automatic placement, reconnect or restart protocol. The deprecated
ChangeConfigurationReply remains explicitly logged/rejected by the shared
adapter, never a progress gate. Definite rejected control produces a failed
command receipt without blocking unrelated cleanup; uncertain work stays owned.

### Explicit spare restoration

The external FM may build an unused preconfigured placement with normal
AddReplica/CreateReplica/Open/copy. A fresh Idle receives the surviving
Primary's compatible deactivation history, never an old runtime or vote.
After the actual Ready/Idle AddReplicaReply, one same-Primary
DoReconfiguration retains the exact down S/N member in PC, preserves every
live P/P and S/S identity, and admits the fresh I/S member in CC. Target,
minimum and service version stay unchanged. All survivor phase commits and
final RAP/FM completion remain required; a build reply alone restores no vote.
If completed Primary failover already removed the down member above minimum,
the next configuration instead keeps the surviving membership as PC and adds
the Ready Idle to CC, restoring target without reintroducing the removed owner.

Real-process cases cover 3/3 and 4/4 after either Primary or Secondary app-host
loss, using one spare. They preserve nonempty data and original survivor
owners, replay lost build/final replies, and prove that the spare supplies an
actual quorum vote after enough original Secondaries are lost. Additional
3/2, 4/2, 4/3 and 5/3 cases qualify restoration from target-minus-one membership
after Primary failover, with the same copy, replay and owner guarantees.
Losing the spare then prevents a fresh write. Predecessor cleanup remains independently owned.
Two sequential replacements are qualified on target-plus-two static pools at
3/3, 4/4, 3/2, 4/2, 4/3 and 5/3, including another Primary loss, an original
Secondary loss and loss of the first replacement. Each new intent starts from
the latest completed membership and awaits the exact new predecessor's processed
deletion. Old requests cannot rebuild or reintroduce removed owners; every
surviving host/runtime stays unchanged. Lost build/final replies replay in each
cycle, and the final quorum/no-quorum check uses the second spare's real vote.
More than two cycles are not process-qualified. Dynamic joins, automatic
placement, old-placement reuse and generation-upload overlaps during spare
repair remain outside this qualification.

### Original-node replica restoration

A fresh native peer `CreateReplica` can restore an app-host-lost volatile
placement after the exact old replica's normal deletion. Shared RA's existing
replacement-history predicate validates the source/generation, service, epoch,
deactivation history and newer replica ID/instance before any acquisition.
The committed Open's registration lookup drives shared Node/Hosting activation.
That owner drains original native/IPC work, positively reaps the exited
application and drains its owned group/root, then replaces only the Activation.
The runner retains the original Node/RA/store and FM/peer Channels but does not
intercept creation to launch a host. Forced predecessor cleanup remains recorded
but does not reject restoration once exact process/group/root cleanup succeeds;
failed or incomplete cleanup still blocks acquisition. A new host gets a fresh
consumer gate rather than reviving the old session.

The external FM drives actual `AddReplica` copy/build and same-Primary P/P, S/S,
I/S admission with unchanged sizes and data-loss epoch. Native final completion
is required before claiming restored membership. `status.json` retains bounded
`retired_applications` physical evidence for the latest 32 replacements and reports current replica identity
and local configuration membership; these diagnostics are not restart-adoption
authority. `host_replacement_count` counts every replacement, independently of
retained diagnostic history; restoration does not stop at that history bound.
No host restarts merely because a replica was normally deleted.

Native baseline `3988b451`: `AppHostClosedProcessor`,
`CanReopenDownReplica`, `FailoverUnit::CreateReplica` and `Replica::Reuse`.

Limits remain the shared bounded profile: 64-KiB native frame/body cap,
16 headers/4-KiB header values, four-times-body decode budget, 32 local command/
effect observation slots, and finite per-Channel reservations from
`control::wire::limits()`. Native production retry defaults are unchanged.
Configuration files are at most 64 KiB; unknown fields, duplicate placement
identity/IP/replica IDs and invalid parameter/topology inputs reject before
application acquisition.

## Readiness, failure and shutdown

`status.json` is atomically updated local diagnostic state, not an FM ACK or
application-serving capability. Process live, original mesh connected, native
bootstrap complete, factory registered, replica phase and local configuration
complete are separate fields. The FM waits for its matched final native reply
before completing create/publishing a client-ready endpoint. Actual application
access independently follows native partition authority and the real quorum;
do not introduce an FM-observation-controlled write gate.

On loss of an original FM/peer Channel, the runner records `control_unknown`,
stops admitting new control intent and retains the original owner until local
shutdown. It does not promote, reconnect, adopt or terminate a healthy writer
merely because its FM connection disappeared. Existing data serving remains
subject to its original runtime/partition authority and actual replication
quorum.

For healthy teardown, FM sends normal DeleteReplica to each original member and
consumes the actual replies, then the orchestrator sends SIGTERM/SIGINT to each
runner. The external package profile enables native active Delete only for the
exact completed configured owner, with unchanged service/current epoch and
normal non-force intent. Legacy profiles keep their prior active-delete gates.
This follows pinned SF `ReconfigurationAgent.cpp:776-834,2239-2242`
(`DeleteLocalReplica` / `DeleteReplicaMessageProcessor`): normal open-replica
deletion uses Delete and the existing Close/commit path, not fake disposal or
healthy-membership resizing.

Local signals stop new lifecycle admission and drive ordinary native Close,
continuing an already accepted close/deletion rather than changing its mode;
they are not fabricated native FM Delete replies. Cleanup checks original
application/group reaping, Hosting-root removal, RA quiescence/executor close,
memory-store close and original Channel disposal/join. `cleanup.clean` reports
joined **local resources**, not acknowledgment of retained remote reporting
work; `pending_work` is reported separately. Lost-session diagnostics remain in
`control_unknown` and `cleanup.control_error` even after successful local shutdown.
Startup, protocol/authority and cleanup errors still produce a failure exit.

The run directory and final status are retained for the operator. Restart is
unsupported: verify teardown of every old application/Node before a fresh
deployment. Old directories, stores, runtimes and sessions are never adopted.
Volatile state is not preserved by retaining the directory or restarting a
container.
