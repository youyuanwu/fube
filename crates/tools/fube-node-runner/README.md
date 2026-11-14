# Standalone externally managed Node

`fube-node` runs one Linux Node/RA/Hosting owner and its application child without
the local FM launcher's parent/stdin contract. An external FM owns the native
reliability exchanges. The execution profile is one fresh volatile partition,
three original Nodes and immutable target/minimum **3/2**.

The runner is a thin process/transport owner over
[`fube_node::control::ingress`](../../libs/fube-node/src/control/ingress.rs).
The local cluster uses that same native FM/peer dispatch, topology checks and
capability admission. There is no standalone-only creation/configuration state
machine: retries, epochs, roles and completion belong to the shared Node/RA.

This is experimental. Native-free tests exercise three separate Node binaries,
real runtime/replicator construction, copy/build, application replication,
lost-reply replay, no-data-loss Primary app-host failover and owned shutdown.
No native FM/Federation interoperability, replacement-host orchestration,
reconnect, resize or restart recovery is claimed.
The [process integration tests](tests/standalone.rs) also lose the original FM
Channel, verify continuing acknowledged writes on the same Primary, and require
explicit shutdown with an honest failed-control outcome.

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
`fube-local-host`. The new `VolatileStatefulExternalFm` package profile requires
volatile stateful default sizes 3/2. Existing local package profiles retain their
1/1 checks. Hosting still owns actual process/registration/usage/lease cleanup;
running inside an OCI container does not enable native SF container activation.

### Process-binary tests

The crate-local integration tests launch three actual `fube-node` processes
with separate run directories and concrete loopback-alias IPs. A native fixture
FM selects the middle Node as Primary and drives the real application children
through bootstrap, copy/build, 3/2 admission, acknowledged writes/readback,
lost-reply replay, refusal and owned shutdown. A separate case kills the exact
Primary application, waits for its native loss report, promotes an original
Secondary through real GetLSN/Deactivate/Activate, verifies retained data and
new acknowledged writes, and replays the final reconfiguration.
Another case advances the ordinary-FM generation through correlated proposal,
empty live-session LFUM upload and semantic completion before placement/build.
A live 3/2 case uploads every member's full committed peer configuration,
continues acknowledged writes on the original owners and replays native control
under the new generation. Shared RA owns inventory and completion; the fixture
FM only validates and acknowledges known test membership. In-flight build,
reconfiguration, swap/repair inventory and restart recovery remain gated.
Each accepted generation update owns fresh inventory work. Shared Node upload
owners retain independent correlated requests, frozen retries and completion,
including multiple uploads in the same generation. Diagnostic `pending_uploads`
counts them separately from the current-generation `upload_pending` flag.

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

Node IDs are nonzero hexadecimal numbers, not UUIDs. Configuration supplies the
original Node/replica instances, replica IDs, bootstrap generation and
package-version instance. Every Node has the same deployment, partition, initial
generation and service/package facts, but its own local placement, peer pair and run directory.
The FM may select any original placement as Primary; no node number or
application role flag selects it.

The runner uses the configured concrete IPv4 socket for both RA binding and
publication. It does not need a separate advertised IP or store `0.0.0.0`.
All three Node IPs must be distinct. Concrete loopback aliases are allowed for
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
The public profile identifier in status is `external-fm-volatile-three-v1`.
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
| AddPrimary / reply | Select exactly one initial Primary. Supply the actual package/type/version, configured partition and local replica identity, volatile 3/2 description, and native initial-role fields. |
| AddReplica / reply | Ask that Primary to build each original destination. Wait for its actual reply, not CreateReplicaReply or local Idle readiness. |
| DoReconfiguration / reply | Admit all three exact live Ready identities with P/P and I/S roles, unchanged service description and data-loss epoch. Every required peer commit and final RAP completion precedes the final native reply. |
| Service-type notification/status and matching replies | Process and ACK actual native work when emitted. These are not a mandatory pre-placement factory-ready handshake. |
| ReplicaUp / reply | Match the real processed-report semantics. A verified app-host loss can inform explicit FM failover intent; report ACKs are not physical disposal or replacement-host authority. |
| Endpoint-updated reply, when applicable | Match the exact native local lifetime; registration/listeners do not constitute client-ready publication. |
| Normal DeleteReplica / reply | Tear down the whole service through exact per-member deletion. Local physical cleanup is a separate observation. Force deletion is not admitted. |

After fresh Node bootstrap, FM may issue placement for its known package/type.
RA retains actual registration lookup/Open and semantic completion; the FM must
not wait for a fabricated pre-create Ready notification. ServiceTypeNotification
is emitted for actual FT/type association, not as a prerequisite to creating
that first FT.

After initial admission, an external FM may drive the same supported native
reconfiguration as the local cluster. Primary failover requires an actual
matched app-host loss and eligible surviving history/quorums, not a lost FM
connection. The shared ingress fixes configured sizes at 3/2 and original peer
bindings; the Node/RA enforces exact epochs, replica lifetimes, runtime authority,
commit barriers and callback completion. Capacity retains the original incoming
frame without acknowledging admission, in both launchers.

Unsupported native branches still reject explicitly. The runner does not expose
the local launcher's owned replacement-host control or invent an external FM,
spare placement, reconnect or restart protocol. The deprecated
ChangeConfigurationReply remains explicitly logged/rejected by the shared
adapter, never a progress gate. Definite rejected control produces a failed
command receipt without blocking unrelated cleanup; uncertain work stays owned.

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
exact completed original 3/2 owner, with unchanged service/current epoch and
normal non-force intent. Legacy profiles keep their prior active-delete gates.
This follows pinned SF `ReconfigurationAgent.cpp:776-834,2239-2242`
(`DeleteLocalReplica` / `DeleteReplicaMessageProcessor`): normal open-replica
deletion uses Delete and the existing Close/commit path, not fake disposal or
healthy-membership resizing.

Local signals stop new lifecycle admission and drive owned Drop/Close cleanup;
they are not fabricated native FM Delete replies. Cleanup checks original
application/group reaping, Hosting-root removal, RA quiescence/executor close,
memory-store close and original Channel disposal/join. `cleanup.clean` reports
joined **local resources**, not acknowledgment of retained remote reporting
work; `pending_work` is reported separately. An observed control/startup/cleanup
error produces failure status/exit even if physical cleanup subsequently joins.

The run directory and final status are retained for the operator. Restart is
unsupported: verify teardown of every old application/Node before a fresh
deployment. Old directories, stores, runtimes and sessions are never adopted.
Volatile state is not preserved by retaining the directory or restarting a
container.
