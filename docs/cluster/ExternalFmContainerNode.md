# Standalone Nodes with an external FM

`fube-node` runs one Node/RA/Hosting process and its application child under a
user-supplied Failover Manager (FM). Three Node processes form **one volatile
partition** with one Primary, two ActiveSecondaries and fixed target/minimum
replica-set sizes **3/2**.

The feature supports fresh startup, application use, explicitly FM-driven
Primary app-host failover and owned shutdown. The runner is a thin owner over
the same [Node control ingress](../../crates/libs/fube-node/src/control/ingress.rs)
used and tested by the local cluster, not a separate reliability implementation.
It is experimental: there is no replacement-host orchestration, reconnect or
restart recovery. The deliverable is the standalone process binary and its
native reliability contract. Container images, networking and orchestration
belong to the user; fube does not ship a production FM or a container test tier.

## What runs where

```text
                      User-owned FM
                     /      |      \
                    /       |       \
                 Node 1   Node 2   Node 3
                    \---- RA peer mesh ----/
                   |        |        |
                 local    local    local
                  IPC      IPC      IPC
                   |        |        |
                  app      app      app
                   \--- copy / replication ---/
```

Each Node owns its local RA, Hosting, runtime registration, finite lease and
application process/group cleanup. The external FM supplies placement,
identities, epochs and configuration intent through native reliability
messages. RA/RAP performs the actual Open, build, role changes and completion;
the FM does not call application callbacks or manufacture replication ACKs.

Any original Node can be the initial Primary. Neither the Node number nor an
application command-line role flag chooses it.

## Connections and addresses

| Configuration or path | Purpose |
|---|---|
| `node.address` | This Node's concrete RA listener address. |
| `fm.address` | The external FM listener that this Node connects to. |
| `peers[].address` | The other two Nodes' RA listeners, used for direct `CreateReplica` and `Activate` exchanges. These are not FM addresses. |
| Application replication endpoint | An application-owned listener used for direct copy/replication. Its complete endpoint is exchanged during replica creation/build. |
| Application service endpoint | The application's own client protocol, returned through native replica replies. |
| Runtime/Hosting/RAP/lease IPC | Container-local or host-local loopback connections; not exposed as remote control endpoints. |

There is **one full-duplex TCP connection per Node-FM pair**, initiated by the
Node. FM commands, Node reports and replies use that same connection. Three
Nodes therefore establish three FM connections, not a separate connection for
each direction.

Each original Node pair also shares one full-duplex RA control connection.
The lower numeric Node ID connects to the higher ID and binds its actual source
IP to its configured Node IP. The acceptor associates that source IP with an
original peer; it does not use connection arrival order or an ephemeral source
port. The mesh is established before application startup, without waiting for
reliability traffic on every link. FM/RA control connections do not carry
application data.

Use distinct, static IPv4 addresses for the three Nodes. The runner binds and
publishes each configured concrete address directly, without `0.0.0.0` or a
separate advertised-IP field. NAT/port translation, service VIPs and load
balancing between replica endpoints are not supported by this profile.
Container networking must preserve the configured peer source IPs. Concrete
loopback aliases are also supported for same-host process use and tests.

General TCP is plain, unencrypted and unauthenticated, without a security
opt-in. TLS is not implemented. Exact PToP Node-instance and generation checks
provide reliability validation, not authentication.

## Usage

### 1. Build the runner and prepare an application package

Build on Linux with the repository's pinned Rust toolchain:

```sh
cargo build --locked --workspace --bins --examples --all-features
```

The executable is `target/debug/fube-node`. The following example packages the
existing `hosting-local-app` sample:

```sh
mkdir -p package/LocalService/Code
cp crates/samples/hosting-local-app/pkg-stateful/ApplicationManifest.xml package/
cp crates/samples/hosting-local-app/pkg-stateful/LocalService/ServiceManifest.xml \
  package/LocalService/
cp target/debug/hosting-local-app package/LocalService/Code/
```

Install the runner and package in each Node environment. The examples below use
`/opt/fube/fube-node` and `/opt/fube/package`; other absolute paths are valid.
No ImageStore upload or native Service Fabric installation is required.

The `VolatileStatefulExternalFm` package profile requires one volatile stateful
service with default target/minimum sizes 3/2 after parameter substitution.
The sample manifest defaults to 1/1, so the configuration below explicitly
overrides both sizes and selects its data-replicator lifecycle mode.

A custom application supplies its own supported ExeHost package, declared
parameters, replication listener and client protocol. OCI deployment does not
enable native SF container activation: the runtime remains in its normal
ExeHost profile, including `Fabric_IsContainerHost=false`.

### 2. Assign the original topology and FM authority

Choose the deployment UUID, partition UUID, original Node/replica instances,
package-version instance and FM generation once. For example:

| Process | Hex Node ID | Instance | Control address | Replica ID |
|---|---|---|---|---|
| FM | `f` | 1 | `10.42.0.10:19001` | Not applicable |
| Node 1 | `1` | 1 | `10.42.0.11:19002` | 10 |
| Node 2 | `2` | 1 | `10.42.0.12:19002` | 20 |
| Node 3 | `3` | 1 | `10.42.0.13:19002` | 30 |

Node IDs are nonzero hexadecimal numbers, not UUIDs. The FM and all three Nodes
have distinct numeric IDs; the three Node IPs and replica IDs are also distinct.
Replica instances are positive and fixed for the deployment.

Provide your own FM listening at the configured address. It must implement the
[native FM exchanges](#external-fm-contract), including bootstrap and semantic
ACKs; accepting TCP alone does not admit a Node. The
[process fixture](../../crates/tools/fube-node-runner/tests/standalone.rs)
is an executable reference for the supported sequence, not a production FM.

### 3. Write one immutable configuration per Node

This is the complete configuration for Node 1:

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

For Nodes 2 and 3, change `name`, the local `node` placement, the two `peers`,
the IPs in `Arguments` and `run_directory`. Keep the same deployment, partition,
FM authority/generation, application, package version and 3/2 description.
Each local placement must appear unchanged in the other Nodes' peer lists.

The package and run-directory paths are absolute. The package and run-directory
**parent** must already exist and be accessible to the Node user; the configured
run directory itself must **not** exist. The runner reserves that directory once
and never adopts a previous owner. Keep it outside the package directory.

Configuration is a regular JSON file of at most 64 KiB. Unknown fields,
duplicate placements/parameters and unsupported topology inputs reject before
application acquisition. There are at most 32 parameters, with a 4-KiB value
limit and 16-KiB aggregate name/value budget. Startup timeout defaults to 30,000
ms, with a 100-60,000 ms range; shutdown defaults to 20,000 ms, with a
1,000-30,000 ms range. See the [schema](../../crates/tools/fube-node-runner/src/config.rs)
and [runner reference](../../crates/tools/fube-node-runner/README.md) for all
validation rules.

### 4. Start the FM and all three Node processes

Start the FM listener first, then launch this command in each Node environment
with its own configuration:

```sh
/opt/fube/fube-node --config /etc/fube/node.json
```

Start all three Nodes without waiting for one Node to become service-ready
before launching the next. Each runner first establishes its original peer
mesh, then connects to the FM and completes fresh `NodeUp` and final empty
`ReplicaUp` handshakes. Only then does it start its owned application.

The runner is independent of the local FM launcher's parent/stdin contract.
In a container, use an init/signal-forwarding supervisor so the runner is not
the PID-1 namespace reaper. Provide a private writable run-directory parent,
keep package/configuration inputs read-only where practical, and disable
automatic container restart. Container launch/stop and networking remain the
user's responsibility.

### 5. Create the service through the external FM

The FM drives the native sequence below:

1. Validate each original Node's identity and answer `NodeUp` with the matching
   activated `NodeUpAck` and fixed generation. Answer its final empty
   `ReplicaUp` with the matching `ReplicaUpReply`.
2. Select any original Node as Primary and send `AddPrimary` with the actual
   package/type/version, partition and replica identities and volatile 3/2
   service description. Wait for `AddPrimaryReply`.
3. Send `AddReplica` to that Primary for each other original placement,
   one destination at a time. Wait for each actual `AddReplicaReply`.
   Primary RA drives direct peer creation and real RAP Open/copy/build;
   `CreateReplicaReply` or Idle readiness alone does not complete FM build.
4. Send one same-Primary `DoReconfiguration` admitting all three exact live
   Ready replicas: Primary P/P and the built Idles I/S, with unchanged service
   sizes and data-loss epoch. Wait for the matched final
   `DoReconfigurationReply`, after peer activation commits and final RAP
   completion. Retain its canonical unit/epoch for subsequent operations.
5. Complete create and publish the Primary endpoint only after that final
   native completion. Preserve the full opaque service/replication endpoint
   strings returned by the replicas.

ACK actual service-type notification/status and endpoint-update reports when
emitted. There is no mandatory pre-placement factory-ready notification: after
fresh bootstrap the FM can place its known package/type, while RA retains the
actual registration lookup and Open checks.

For the sample package, the application is `fabric:/External`, the service is
`fabric:/External/Singleton`, the application type is `HostingLocalStatefulApp`,
the service package is `LocalService` and the service type is
`LocalStatefulType`. The [fixture's request construction](../../crates/tools/fube-node-runner/tests/standalone.rs)
shows the corresponding native descriptions and role/epoch fields.

### 6. Verify application serving

Use the Primary service endpoint returned by the native flow, not an RA or FM
control socket. For the sample, if the FM selected Node 2, the configured
endpoint is `10.42.0.12:19004`. From a machine that can reach it:

```python
import socket

for command in (b"write\n", b"read\n"):
    with socket.create_connection(("10.42.0.12", 19004), timeout=2) as connection:
        connection.sendall(command)
        with connection.makefile("r") as response:
            line = response.readline()
    print(line, end="")
    assert line.startswith("OK "), line
    if command == b"read\n":
        assert "value=acknowledged" in line, line
```

This line protocol belongs to the sample, not the generic Node. A custom
application supplies its own serving probe and client.

In the initial singleton configuration, membership is below minimum two and
writes are denied. Building an Idle does not grant active membership. As normal
RAP installs the configuration, writes follow native partition authority,
configured minimum and actual replication quorum. They can become valid before
the FM observes the final reply. FM create/endpoint publication still waits for
that reply; there is no extra FM-observation-controlled write gate.

### 7. Observe status and shut down

The runner atomically updates `status.json` inside its run directory:

```sh
cat /run/fube/node1-session1/status.json
```

| Field | Observation |
|---|---|
| `stage` | `starting`, `running`, `stopping` or `stopped`. |
| `control_bootstrapped` | The original fresh bootstrap completed. |
| `fm_connected`, `peers_connected` | Original control-connection observations. |
| `registered`, `application_pid` | Actual local application registration/process observations. |
| `replica_phase`, `replica_role` | Committed local replica state. |
| `local_configuration_complete` | Local RA configuration completion, not the FM's receipt of its reply. |
| `generations` | Admitted receive/send generations, current-generation `upload_pending` readiness and separate `pending_uploads` ownership count; not restart readiness. |
| `control_unknown`, `error` | Observed error/control uncertainty; not permission to promote or reconnect. |
| `cleanup` | Final local cleanup outcome, including separately reported `pending_work`. |

Process running, bootstrap complete, registration, replica readiness and
application serving are separate observations. Status is diagnostic state,
not an FM ACK or client-serving capability. Do not make a startup probe require
service readiness before the FM can create the service.

For healthy whole-deployment teardown:

1. Stop new application traffic and placement/configuration work. Send normal,
   non-force `DeleteReplica` for each exact original member using the completed
   native identities and epoch, and consume the actual replies.
2. Send SIGTERM or SIGINT to each original runner through the process supervisor.
   The runner drives owned Close/Drop, application/group reaping, Hosting-root
   cleanup, RA/executor/store close and original Channel disposal/join.
3. Observe process exit and final status. Exit code zero requires clean local
   shutdown with no observed startup/control/cleanup error. An error produces
   a failure exit even when physical resources were subsequently cleaned.

`cleanup.clean` means positively joined **local resources**, not acknowledgment
of all remote reporting work. `cleanup.pending_work` reports those remaining
obligations separately. Neither a transport write, `DeleteReplicaReply` nor a
termination signal is by itself proof of complete physical cleanup. An outer
forced kill is not graceful provider disposal.

The run directory and final status are retained. Before a fresh deployment,
independently verify that all old Nodes/applications stopped and choose fresh
deployment/lifetime identities and unused run directories. Reusing an old
directory, changing only a deployment UUID or restarting a container does not
recover volatile state or fence a surviving writer.

## External FM contract

The profile identifier is `external-fm-volatile-three-v1`. It names the supported
execution subset, not a new native wire schema or an HTTP orchestration API.
A Rust FM can reuse the [native codecs](../../crates/libs/fube-ra/src/protocol.rs)
and [wire compositions/Link](../../crates/libs/fube-node/src/control/wire.rs).
A non-Rust FM implements the same framing, actors, priorities, PToP identity
headers, generation headers and semantic exchanges.

The [runner reference](../../crates/tools/fube-node-runner/README.md#external-fm-contract)
lists the accepted actors/actions and delivery rules; the
[native reliability catalog](NativeRaProtocol.md) documents payloads and codec
evidence. Node instance, RA replica instance, package-version instance and
replication actor incarnation are different identity domains.

Native FM/peer messages use shared topology, immutable-size and capability
admission; the Node/RA owns retries and reconfiguration state. A successful
socket write is not a semantic ACK, and a retry is not another Open or authority
to choose a different Primary. No standalone-only frozen configuration limits
the shared RA to its initial admission. Primary app-host failover still needs
matched failure observations, valid history and separate PC/CC quorum evidence.
Native retry defaults are unchanged. The shared bounded wire profile retains
its 64-KiB frame/body cap, 16 headers with 4-KiB values, bounded decode budget
and 32 local command/effect observation slots.

Ordinary-FM generation rollover also supports a stable, fully admitted 3/2
configuration on the original live sessions. Each Node uploads its complete
committed membership with cleared LSNs; only the Primary marks its inventory
as a Primary report. A matching correlated LFUM reply advances send generation,
without reopening replicas or changing application authority. Every accepted
update owns a fresh snapshot; retries preserve only their own frozen upload,
and replies retire only their original request, including stale-generation
replies. In-flight build,
reconfiguration, swap/repair inventory and restart reconciliation remain gated.

## Failure behavior and limits

| Event or operation | Behavior |
|---|---|
| Original FM/peer connection loss after startup | Records control uncertainty, stops admitting new control intent and retains the original owner until explicit shutdown. No reconnect, promotion or peer replacement. |
| Lost semantic reply on a surviving Channel | The FM retries the same admitted operation and matches the actual native completion. |
| FM loss while the Primary can still serve | FM loss alone does not revoke the runtime lease or prove the Primary stopped. Writes remain subject to normal partition authority and actual quorum. |
| Invalid configuration or unsupported intent | Explicit rejection/error; no successful no-op or widened authority. |
| Verified Primary app-host loss | The external FM can drive shared native GetLSN/reconfiguration/Deactivate/Activate; it must await final completion before publishing the new Primary. |
| Resize, replacement-host acquisition or restart reconciliation | Unsupported branches remain explicit shared Node/RA or supervisor gates; dispatch alone grants no authority. |
| FM/Node restart or volatile state loss | No adoption, rejoin or data recovery. Tear down the old deployment before starting a fresh one. |

The profile supports one immutable original three-Node, one-partition volatile
3/2 service only. It does not provide persisted state, elections, a production
FM, rolling upgrades, multiple partitions or native FM/Federation interoperability.
The shared native ingress does not expose the local launcher's replacement-host
supervision, peer reconnection or runtime adoption.

## Process-level verification

The [integration tests](../../crates/tools/fube-node-runner/tests/standalone.rs)
launch actual Node binaries and application children under a native fixture FM.
They cover initial 3/2 startup/build/admission, singleton write denial, real
write/readback, lost build/final-reply replay, writes before FM observation,
generation/membership refusal, FM-loss retention, verified Primary app-host loss
with real peer failover and continuing writes, stable replicated generation
rollover with full membership/data preservation, and owned shutdown.

```sh
cargo nextest run --locked --workspace --all-targets --all-features \
  --test-threads 1 --no-fail-fast -E 'package(fube-node-runner)'
```

Build the executables first as above. These ordinary, non-ignored tests also run
from the workspace archive in CI. They require no Docker daemon, image build,
native Service Fabric or onebox. Their boundary is the process binary and
protocol, not the user's container environment or containerized HA.
