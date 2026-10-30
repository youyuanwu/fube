# Basic pure-Rust cluster design

**Status: proposed architecture, not an implemented cluster.**

**Baseline:** `3e0c334262ce228df6fb2f6f56863c11862f4ae7`, inspected 2026-10-29.

The goal is a small Linux cluster that runs Rust applications without
Fabric/FabricHost, native Service Fabric nodes, or onebox. Build a single
authoritative **Failover Manager (FM)** and a long-running **Reconfiguration
Agent plus Hosting (RA+Hosting)** process for each logical node. Reuse the
existing application-side runtime and volatile replication engine.

This is not a plan to reproduce a full Service Fabric node. In particular,
omit Federation and native distributed node-to-node leases, **not** the finite
lease between each application runtime and its local RA/Hosting.

The companion [Rust RA port and SQLite state-store design](RustRaPort.md)
describes the native storage seam, reliability-code port and durable local
recovery extension. It does not change this document's initial fresh-RA or
ephemeral-FM restart policy.

## 1. Decisions and initial profile

| Area | Decision |
|---|---|
| Processes | One non-HA FM; several independent RA+Hosting processes; owned application processes beneath each RA. |
| Deployment | Linux loopback, initially Linux x86-64 to match existing qualification; one machine, explicitly configured membership. |
| Authority | FM authorizes desired transitions. RAs execute authorized local transitions and report observations. Applications enforce runtime and replication authority. |
| Local runtime lease | Keep finite actor-15 polling, terminal retirement, independent OS watchdog and callback/destructor hard deadlines. No infinite/lease-free mode. |
| FM connection | Registration, heartbeat and snapshot reconciliation, distinct from the local lease. Missing heartbeat means suspect/unknown, not fenced. |
| Stateful data | Existing qualified **volatile classic V1 `fube-data` subset**; no new replication engine, storage durability or transaction layer. |
| Recovery | Prefer unavailability to conflicting primary authority. Initial automatic promotion on connectivity timeout is prohibited. |
| FM restart | Ephemeral state; confirmed old-deployment teardown/fencing before a fresh cluster identity. No transparent controller recovery. |
| Discovery | FM's versioned observed endpoint registry and a small cluster API, not native Naming/FabricClient gateway parity. |
| Reliability protocol | Preserve and port all native RA–FM and RA–RA message contracts, including their processing semantics. Replace Federation routing/transport, not the reliability protocol. |
| Compatibility | Preserve the independently qualified native-node/runtime profile. Native reliability payload/header conformance does not by itself establish interoperability with a native FM or Federation. |

Different replicas of the same partition initially run in **different app
processes**. Separate RA processes model logical nodes, not independent
physical fault domains: machine, kernel, power, disk and loopback are shared.
This environment can qualify process isolation and control behavior, not
physical-node availability.

Out of scope: controller election/consensus/HA, Federation, distributed node
leases, dynamic membership, durable catalog recovery, persisted replicas,
transactional replication, whole-set state preservation, native management
gateway equivalence, rolling executable upgrades, containers, hostile tenants,
TLS and remote-machine qualification. The runtime's current rejection of
unsupported security/host profiles must remain intact.

These initial execution-profile exclusions do not remove RA–FM or RA–RA
messages from the port. Preserve every exchange's contract and track its
implementation and qualification; a temporarily disabled feature is unfinished
work, not permission to omit its messages or return successful no-ops.

## 2. What exists, and what must be built

Only this section describes existing capabilities. Subsequent sections are
proposed contracts unless explicitly identified as existing behavior.

| Existing evidence | Reuse boundary / missing work |
|---|---|
| `fube-runtime` performs activated host/runtime registration, package loading and fresh lease establishment: [lib.rs:153–253](../../crates/libs/fube-runtime/src/lib.rs#L153-L253). Its context parser rejects unsupported host/security settings and nonloopback IPC: [context.rs:58–104](../../crates/libs/fube-runtime/src/context.rs#L58-L104). | Keep `Runtime::attach` and the app programming model. Hosting must supply genuine, matching activation metadata, not introduce an unactivated fallback. |
| Local authority renews using correlated, send-relative actor-15 TTLs: [lib.rs:250–295](../../crates/libs/fube-runtime/src/lib.rs#L250-L295). Terminal authority and independent callback/watchdog fencing live in [lease.rs:14–193](../../crates/libs/fube-runtime/src/lease.rs#L14-L193). | Reuse unchanged authority semantics. Implement the long-running node-side lease owner and terminal host records. |
| `fube-test-host` admits owned host/PID, runtime, type and lease traffic: [lib.rs:174–338](../../crates/libs/fube-test-host/src/lib.rs#L174-L338). | Useful registration/IPC patterns, not a general RA. Replace hardcoded node identity and one-runtime/type assumptions with explicit admitted records. |
| Host entry points run complete bounded scenarios and require native/dependency inputs: [lib.rs:63–79](../../crates/libs/fube-test-host/src/lib.rs#L63-L79), [385–434](../../crates/libs/fube-test-host/src/lib.rs#L385-L434). The qualified profile is singleton stateless/native SF 11.8.121.4; broader design sections are proposals: [RustTestHost.md:19–63](../runtime/RustTestHost.md?plain=1#L19-L63). | Extract reusable Hosting mechanics or extend through separate profile-specific entry points. Do not claim today's `run_manifest` is an interactive cluster host or already supports the mixed Rust app. |
| The current controller has stateless unit states and checks semantic reply identity/flags: [controller.rs:12–24](../../crates/libs/fube-test-host/src/controller.rs#L12-L24), [150–176](../../crates/libs/fube-test-host/src/controller.rs#L150-L176). | Build long-lived per-replica RA state, FM integration, stateful orchestration and reconnect recovery. |
| Process launch uses cleared environment, owned groups and parent-death handling; cleanup observes group drain/reap: [process.rs:112–181](../../crates/libs/fube-test-host/src/process.rs#L112-L181), [335–379](../../crates/libs/fube-test-host/src/process.rs#L335-L379). | Reuse containment concepts, separating Rust launch from native loader preflight. Existing escaped-descendant/supervisor-death limits remain explicit: [ManifestTestHost.md:69–80](../runtime/ManifestTestHost.md?plain=1#L69-L80). |
| The host manifest subset excludes stateful/config/data support: [ManifestTestHost.md:82–125](../runtime/ManifestTestHost.md?plain=1#L82-L125). The runtime already validates digested config/data and settings: [packages.rs:875–940](../../crates/libs/fube-runtime/src/packages.rs#L875-L940). | Extend host-side package validation/materialization deliberately. Do not weaken the runtime reader or pretend config/data bytes alone constitute a valid deployment. |
| Runtime stateful control validates action-specific shapes and dispatches build/remove/configuration/status: [stateful_reconfiguration.rs:42–122](../../crates/libs/fube-runtime/src/stateful_reconfiguration.rs#L42-L122). [FabricRuntimeHost.md:197–286](../runtime/FabricRuntimeHost.md?plain=1#L197-L286) describes multi-stage RA/RAP semantics. | Build the missing node-side state machine; do not bypass app-side RAP with direct role setters. |
| `Engine::bind_partition` uses runtime partition authority and provider supervision; standalone `bind` is a different seam: [engine.rs:301–354](../../crates/libs/fube-data/src/engine.rs#L301-L354). Applied quorums are per configuration: [quorum.rs:23–87](../../crates/libs/fube-data/src/quorum.rs#L23-L87). | Reuse `fube-data`, actual copy/catch-up and existing access gates. FM supplies membership/epochs, not data acknowledgments or elections. |
| Existing data qualification is a bounded volatile V1 profile, not durable storage or universal native parity: [FubeData.md:3–14](../runtime/FubeData.md?plain=1#L3-L14), [237–287](../runtime/FubeData.md?plain=1#L237-L287). | Pure-Rust cluster tests will be new evidence. They cannot inherit native qualification simply by reusing the engine. |

The workspace also contains the [P1 store](RaStore.md) and
[RA protocol/minimal P2 foundation](RaProtocolFoundation.md), not a cluster
FM/RA executable ([Cargo.toml:1–15](../../Cargo.toml#L1-L15)).
Proposed component names below
describe responsibilities, not already available binaries or CLI commands.

## 3. Process and traffic architecture

```text
                  cluster API / operator / test observer
                                 |
                         FM (one authority)
                   catalog, placement, epochs,
                  reconciliation, endpoint registry
                          /               \
                FM–RA control         FM–RA control
                       /                   \
             RA+Hosting A <-- peer --> RA+Hosting B       ...
              |          |                 |
           app A1     app A2             app B1
           runtime    runtime            runtime
              |                             |
           provider <---- fube-data ----> provider
              ^                             ^
              +------ app-owned clients ---+
```

Five paths remain distinct:

1. **FM–RA control:** ported native reliability messages over bounded loopback
   connections, with separate deployment/session admission where needed.
2. **Local RA/Hosting–runtime IPC:** existing app-facing Hosting/RAP actors and
   schemas. Actor 7 handles host registration, actor 9 runtime/type registration,
   actor 15 local lease polls, actor 8 host controls, and actor 12 lifecycle,
   reconfiguration and supported reports.
3. **Application traffic:** app-owned listeners/protocols. A client resolves
   through FM, then contacts the app directly.
4. **Replication/copy traffic:** direct `fube-data` peer sessions selected by
   configuration. Neither FM nor RA proxies application data or supplies ACKs.
5. **RA–RA control:** ported native peer-reliability exchanges, including
   `CreateReplica`, `GetLSN`, `Deactivate`, `Activate` and their replies. These
   coordinate replica lifecycle/reconfiguration; they are not Federation
   membership or application-data replication.

RAs do not elect primaries or negotiate membership with one another. The FM
is authoritative only within its admitted deployment; its assertion alone does
not revoke previously issued application authority. RAs do execute the native
peer-reliability choreography under FM-authorized configurations; omitting
Federation does not remove that responsibility.

## 4. Bootstrap, ownership and restart

An operator-controlled launcher owns the FM and RAs, their private run
directories and a cleanup inventory. Static bootstrap specifies cluster profile,
FM address, allowed logical node IDs, RA control addresses, package roots and
placement limits. Membership changes during a run are unsupported initially.

Startup sequence:

1. Acquire exclusive ownership of the local deployment directory; refuse a second
   cooperative FM/launcher for that deployment. A local lock is not distributed
   consensus or proof that old apps are dead.
2. Verify no prior owned deployment remains live. Create a fresh random
   `cluster_id` and `fm_incarnation`; provision them to each owned RA through
   private bootstrap configuration.
3. Start the FM, then the configured RAs. Each RA registers its node incarnation,
   admitted profile/capabilities and initial inventory. Unconfigured nodes,
   duplicate active incarnations and mismatched identities are rejected.
4. FM acknowledges a reconciled registration before issuing placement commands.
   Partial bootstrap remains visible; it does not silently reduce desired
   replica counts or stateful minimums.

Cleanup inventory is a resource-ownership aid, **not** a durable catalog or
replica recovery log. Record OS process identities resistant to PID reuse
(owned child handles/pidfds where applicable, plus start identity), never just
numeric PIDs or process names.

**FM crash/restart:** no replacement FM adopts running replicas in this first
profile. The owner stops/fences the old FM, every old RA and all owned app
serving processes; independently verifies absence and cleanup; then starts a
new cluster identity. Inability to verify termination blocks restart. Deleting
metadata, changing a UUID, waiting a heartbeat timeout, or observing a closed
control socket is not sufficient. Existing apps may continue under local
authority until explicitly stopped.

**RA crash/restart:** while FM survives, a new RA incarnation is admitted only
after the old RA and its potentially serving app instances are proven fenced.
It starts empty, with fresh host/runtime/replica incarnations; it does not adopt
or reuse the old runtime connection. Any surviving volatile replicas elsewhere
can supply recovery only through the stateful protocol in section 10.

Later durable recovery would require persistent catalog/epoch and command
history, an exclusive authority-recovery protocol, replica inventory adoption
and tested crash consistency. It is separate work, not an incremental claim
that ephemeral FM restart is already safe. All-replica loss loses volatile data.

## 5. Authority and identity contracts

### Three independent decisions

- **Placement authority:** FM decides desired replicas, roles, epochs and allowed
  local transitions.
- **Local process authority:** RA/Hosting admits and renews a particular live
  host's finite lease; the runtime terminally fences it on authority loss.
- **Serving/replication authority:** the runtime's current unit, role/access,
  configuration and engine readiness must all permit the operation.

The sample already checks service/replicator/engine roles, epoch, readiness and
read/write access together
([data.rs:41–76](../../crates/samples/rust-runtime-app/src/data.rs#L41-L76)).
Keep that gate; an endpoint, role label, standalone engine bind or FM command
receipt must not substitute for it.

### Identity hierarchy

| Identity | Allocator and fence |
|---|---|
| Cluster / FM incarnation | Bootstrap owner; bound to every FM–RA exchange. Reject another authority rather than adopting it on reconnect. |
| Node ID / node incarnation | Configured logical ID plus fresh admitted RA lifetime. A process restart is not the same node instance. |
| Application/service generation | FM catalog; deletion/recreation of the same name creates a new generation. |
| Partition / replica ID / replica incarnation | FM; partition identity is stable within a service generation, replica incarnation changes on replacement. Never reuse an exited incarnation. |
| Previous/current epoch and configuration revision | FM serializes allocation per partition. Keep native field encoding and the distinction between configuration and data-loss epochs. |
| Host/package activation and code/package sequence | Hosting per owned launch; unique host identity and matching package version/context. |
| Runtime ID and IPC connection generation | Runtime chooses its ID; RA learns it only through owned admission. Bind all later traffic to that connection and host lifetime. |
| Command ID / desired revision / report sequence | Control protocol; semantic idempotence and ordered observations, not application authority. |

FM revision numbers are not native epochs, and node/replica incarnation is not
an LSN. A new epoch does not physically fence an isolated old primary.
Replies that lack a field on the existing IPC wire are bound through the
owning connection/unit record; do not invent an echoed runtime ID.

## 6. FM–RA protocol and convergence

**All native RA–FM and RA–RA message contracts are mandatory porting scope.**
Preserve action names, actor/direction, headers, complete body schemas and native
serialization semantics, including nested/inherited fields, defaults, epochs,
instances, errors, acknowledgment meaning, retries and stale-message handling.
Do not replace them with a generic Apply/status protocol or move RA–RA exchanges
into an invented FM-only sequence.

Use explicit version/profile admission and finite frame/queue limits around
the ported messages. Initially use configured, bounded loopback TCP sessions
for FM–RA and RA–RA delivery instead of Federation routing. Any new transport
envelope is separate from the native reliability message: deployment/session
identities supplement, not replace, native generation and incarnation checks.
Do not alias FM/FMM identities or synthesize acknowledgments for an absent
capability. Native payload/header conformance does not require Federation
membership, node leases or its routing framing, and does not alone qualify
end-to-end native-node interoperability.

The [RA port design](RustRaPort.md#native-reliability-message-preservation)
defines requirements for the complete source-pinned inventory and
codec/behavioral evidence gates. The [complete pinned catalog](NativeRaProtocol.md)
and [first-pair source-baseline codec oracle](../../tests/ra_protocol/README.md)
now exist, alongside a minimal P2 execution seam; they do not enable a deployed
stateless cluster or qualify full native handlers/traces. Unknown versions,
malformed messages, oversized frames and contradictory
identities fail closed. Profiles reject unsupported feature admission before
execution; unimplemented exchanges are tracked gaps, not completed ports.

### Exchange inventory

The following are supplementary bootstrap, observation and reconciliation
responsibilities, not substitutes for native messages such as `NodeUp`,
`ReplicaUp`, `AddInstance`, `DoReconfiguration` or their acknowledgments.
Map each responsibility to the native exchanges first; justify any additional
transport-level exchange separately.

| Exchange | Required meaning |
|---|---|
| Register / registration acknowledgment | Match cluster, FM, configured node, RA incarnation and capabilities; establish a new session generation. |
| Heartbeat | Report liveness of this session plus last applied desired revision/report sequence. It is not a local app lease or fencing certificate. |
| Inventory snapshot / delta / acknowledgment | Observed hosts, registrations, units, pending operations, errors, endpoints and terminal records. Snapshot carries a barrier sequence; deltas resume after it. |
| Operation observation | Track the native action/reply and its target, immutable payload and finite execution budget. Local API status must not replace the native reply or its semantic postcondition. |
| Reconcile request | Compare a consistent inventory with desired state; query unknown outcomes before retrying or compensating. |
| Drain/retire | Authorize removal, cancel unsafe pending work and report lifecycle plus process cleanup separately. |

Every supplementary envelope binds protocol version, cluster identity,
sender/recipient node incarnations, session generation and message sequence;
FM–RA sessions also bind the admitted FM identity. Per-replica work additionally
tracks service generation, partition/replica/incarnation, expected
epoch/configuration revision and local command identity without requiring new
fields in native bodies. Partition-level reconfiguration records list all
participants and phase dependencies. RA–RA connections admit only configured
peers in the same deployment and fence stale sessions independently.

Local monotonic clocks govern deadlines. A retransmission cannot reset an
already admitted operation's deadline. Do not compare raw monotonic timestamps
from different processes; carry bounded budgets and keep the original
receiver-side admission deadline.

### Command outcomes and idempotence

The statuses below describe local API/operation bookkeeping. They do not
replace native action/reply pairs, introduce new native acknowledgment
postconditions, or assume native bodies echo a new command ID.

`Accepted` means the RA recorded a valid intent. `Executing` means side effects
may exist. Only `Succeeded` establishes the command's stated semantic
postcondition. `Failed`, `Cancelled` and `Unknown` retain cleanup/reconciliation
obligations; cancellation is not rollback. `Rejected` means no new intent was
admitted, not necessarily that no older equivalent work exists.

FM and RA commit in-memory intent/state before enqueueing effects. This gives
ordered local behavior, **not** durable exactly-once execution. An identical
command ID/payload returns existing status and must not spawn a second
incarnation. Reusing the ID with different semantics is rejected. Retries reuse
the desired identity and intent; they do not allocate another replica.

An FM timeout or lost transport acknowledgment leaves an unknown outcome.
RA reports matching local semantic completion when available, otherwise
inspects the owned unit/process and performs authorized cleanup. No
indistinguishable conflicting actor-12 operation may overlap another on the
same unit. A successful socket write is never an Open/Build/Close result.

On reconnect, invalidate the old FM–RA session, exchange a full snapshot and
barrier, then resume deltas. Sequence gaps, delta buffer overflow or conflicting
revisions force resnapshot. Old-session frames/replies cannot complete new
commands. FM must reconcile pending intent with actual state before advancing
the partition. A disconnected RA may finish still-valid, already authorized
local work and safe cleanup; it cannot invent a new placement, promotion,
incarnation or recovery decision.

All queues, snapshots, retries and inventories have declared limits. Retain
bounded completion caches and tombstones plus generation/revision high-water
fences. Eviction must not permit old commands to be re-executed: expired/unknown
request IDs require reconciliation, not automatic acceptance. If a necessary
fence cannot be retained within limits, stop admission until safe reclamation
or deployment teardown. Backpressure must not starve lease service or cleanup.

## 7. Long-running RA and Hosting

### Replica records

Maintain a record per partition/replica/incarnation, separate from host/runtime
registrations. It contains desired revision, native service/package description,
previous/current configurations and roles, observed lifecycle/access, runtime
binding, pending command/deadline, last accepted semantic replies, endpoint
versions, fault state and cleanup obligations.

Conceptually:

```text
Absent -> Materializing -> ProcessStarting
       -> HostRegistered -> RuntimeRegistered -> TypeRegistered
       -> OpenPending -> Open [stateless]
                      -> InBuild -> Ready [stateful; only after real barriers]
       -> ClosePending / AbortPending -> Closed -> Retired

any nonterminal state -> Faulted / OutcomeUnknown -> reconciliation or cleanup
```

Track process state orthogonally: alive, exit observed, group drained and direct
child reaped. `Closed` does not mean exited; Open on an idle stateful replica
does not mean Ready. State changes follow matching semantic replies and actual
observations, not the desired state alone.

Serialize conflicting unit transitions. Fence every callback/result against
current connection, unit incarnation, phase and operation-specific epochs/flags.
Use primary-epoch versus full-epoch comparisons only where the existing action
contract requires them. Preserve error codes and API failure details.

Once an incarnation has successfully opened and later exits, it is terminal.
An Open retry must not respawn it. A replacement needs new FM authorization and
incarnation. Before reporting a serving owner fenced, also retire queued launch
and retry intents so old work cannot recreate it.

### Hosting and package materialization

The initial cluster package profile is one application service package, one
code package and a declared supported type set per deployment request.
Multiple independent requests may coexist. Start stateless; add the existing
mixed stateless/volatile-stateful package shape explicitly in the stateful
milestone. No silent type filtering or unsupported default-service rewriting.

Hosting must:

1. Validate trusted immutable package inputs, parameter names/values, declared
   types, versions, entry point, initialization data and supported topology.
   Reject unknown features, traversal, symlinks/nonregular files, resource
   collisions and bounds violations before launch. Extend the existing narrow
   manifest validator explicitly for config/data and stateful types.
2. Materialize the native-shaped application/service package artifacts,
   matching digested declarations, Code/Config/Data directories, settings,
   endpoint allocation files and owned work/log directories. The runtime's
   existing reader checks SharedProcess metadata and matching types
   ([packages.rs:683–764](../../crates/libs/fube-runtime/src/packages.rs#L683-L764),
   [802–841](../../crates/libs/fube-runtime/src/packages.rs#L802-L841)).
3. Inject the activated single-code-package environment with fresh host/code
   sequence identities and the local IPC address. Supply no native runtime
   loader paths in the pure-Rust launch profile. Preserve a separate unchanged
   native-runtime launch/preflight profile for compatibility tests.
4. Admit host start/finish, then runtime and each declared type, only for the
   expected owned PID, host, package/version and connection. IPC peer-provided
   identity alone is not ownership. Do not send Open before type admission.
5. Supervise launch, IPC, exit, process group, log draining and directory lifetime.
   Allocate distinct app processes for replicas of the same partition, with
   unique endpoint/work ownership. Initially prefer one active replica per
   app process even where different partitions could share one.

Use explicit static loopback endpoint allocations first; reject collisions.
Dynamic port allocation and richer placement policy are not prerequisites.
Package checksums/versions are not a substitute for verified executable bytes.
Config/data packages are activation inputs, not durable replicated state.
No live executable/config upgrade is promised in the initial profile.

### Reports, cleanup and security

Treat runtime/host unregister, app exit, `RAReportFault`, access revocation and
endpoint changes as typed state-machine inputs. A transient fault can initiate
authorized close/drop; only FM authorizes replacement. A permanent fault
retires the affected incarnation. Engine fault delivery does not itself prove
termination or replacement.

Decode and acknowledge only supported load/health routes with their actual
schemas and sequence rules. Store bounded local observations and preserve
the complete native RA–FM load-report exchange; do not replace its payload
with a lossy summary. This is not native Health Manager aggregation. Unsupported
features fail capability admission instead of entering endless retries.

Separate graceful service close, runtime unregister, forced termination,
group drain, reap and filesystem cleanup in reports. RA death requires
owner-level cleanup of apps; direct-child parent-death signaling alone is not
proof that every descendant terminated. Escaped descendants remain a trusted-
code limitation, not a solved sandbox guarantee. If ownership/absence cannot
be established, report cleanup failure and block unsafe replacement.

Bound host count, units, pending callbacks/commands, retry rate, report storage,
log bytes, package bytes and disk retention. Emit typed journals with monotonic
times, identities, phase/revision, error and cleanup status; redact private
bootstrap capabilities and app secrets.

Bind every initial listener to loopback, use private owner-controlled directories
and bootstrap admission credentials, and reject unsolicited/wrong-deployment
peers. This is a trusted same-machine profile, not protection from malicious
same-user code or a claim that loopback authenticates a process. Remote deployment
requires a separate authenticated/encrypted profile and renewed qualification.

## 8. Keep the local runtime lease

Hosting continues to answer correlated actor-15 polls with a **finite TTL**
for an admitted live host. The current bounded test-host profile uses 30 seconds
([RustTestHost.md:53–58](../runtime/RustTestHost.md?plain=1#L53-L58)); use that as the
initial cluster profile value, not an infinite grant or a heartbeat timeout.

The existing runtime establishes authority only after a fresh poll and renews
approximately once per second. It derives expiry from request **send** time,
not delayed reply arrival. Retirement/expiry is terminal: a later good poll
cannot revive the same authority. Actor-8 invalidation, connection loss, zero
TTL and invalid renewal retire it
([lib.rs:250–319](../../crates/libs/fube-runtime/src/lib.rs#L250-L319),
[lease.rs:115–151](../../crates/libs/fube-runtime/src/lease.rs#L115-L151)).

Keep the independent watchdog: effective deadline is the minimum of lease
expiry and callback deadlines; healthy polls cannot prolong a blocked callback
or destructor. Pending callback ownership prevents disarm. The current watchdog
uses a separate OS thread and `_exit(70)` after a 500 ms grace, checking at
10 ms intervals, independent of Tokio/user locks/diagnostics
([lease.rs:70–113](../../crates/libs/fube-runtime/src/lease.rs#L70-L113),
[160–193](../../crates/libs/fube-runtime/src/lease.rs#L160-L193)).
These are userspace scheduling bounds, not hard real-time guarantees during
kernel stalls, stopped processes or machine suspension.

**FM isolation does not invalidate this local lease.** An RA may continue
renewing its existing apps while disconnected from FM. Existing data service
continues only while all local/replication gates permit it; missing peer quorum
can independently make writes unavailable.

Conversely, an FM must not infer that an app expired because it has not heard
from the RA for 30 seconds. Only the RA/runtime know actual lease exchanges.
Fencing proof for recovery comes from matched completed authority revocation
and retirement, or independently confirmed owned process termination—not
elapsed time. Revoked host records never resume lease service on reconnect.

## 9. FM catalog, placement, API and discovery

FM owns application/service generations, partition catalog, desired replica
sets, configured minima/targets, placement, epoch allocation, pending
reconfigurations and a separate observed registry. Start with deterministic
placement over admitted nodes, one replica per logical node for a partition,
and explicit capacity checks; no optimizer or automatic balancing is needed.

The proposed local API has these operations, independent of final HTTP/CLI
spelling:

| Operation | Semantics |
|---|---|
| Create application/service | Validate supported package/topology; reserve catalog identity and idempotent operation; return pending status. Ready requires observed successful activation, not catalog insertion. |
| Delete | Record terminal desired generation, withdraw endpoints, close/retire all affected units and discharge process/package ownership. Return pending until required cleanup is proven; expose failures. |
| Query / operation status | Return desired and observed state separately, revisions, pending/errors, unknown/stale observations and cleanup status. Bound/paginate results. |
| Resolve | Return only eligible observed app endpoints, partition/replica/incarnation, role and registry/configuration version. Empty/unavailable is explicit; never fabricate a listener. |

Create/delete accept a caller request identity; identical retries return the
same operation within the bounded retention contract. Name reuse after deletion
requires a new generation. API timeout means query the operation, not assume
rollback. No exactly-once guarantee survives ephemeral FM restart.

An endpoint is eligible only after matching Open or endpoint-update observations
for an admitted unit and the required configuration/access phase. Endpoint
notification acknowledgment stops local retransmission; it does **not**
automatically publish into the registry. Never derive readiness from a declared
port or a successful endpoint acknowledgment.

On RA session loss, keep last-known data for diagnostics but mark it suspect
and withhold it from new resolve results until reconciliation. This conservative
discovery policy does not stop cached clients or revoke app authority. Clients
must tolerate stale endpoints and application-level unavailable responses;
stateful requests still pass the app's role/access/epoch gate. Resolve is not a
serving lease or a linearizable read from application state.

An app can return an opaque or empty endpoint; the general registry preserves
that representation rather than imposing the fixture's ping protocol. Typed
loopback endpoint adapters and independent probes belong to the initial test
applications. App endpoints and replication endpoints stay separate.

`fube-client`'s native gateway profile remains independent; implementing this
small management/resolve API does not make it compatible with native
FabricClient or Naming protocols.

## 10. Volatile stateful reconfiguration

### Invariants

Reuse `fube-data` through `Engine::bind_partition` and runtime-owned callbacks.
Keep real copy, application apply ACKs, epoch reconciliation, cancellation and
memory/backpressure bounds. Do not add a competing log/replication engine in FM
or RA. Port the native RA–RA control path alongside FM–RA commands and RAP actions;
the phase descriptions below do not replace those peer messages.

1. One FM serializes transitions for each partition. A partition transaction
   records previous/current configurations, local membership, required targets,
   allocated epochs and phase-specific observations.
2. Previous and current quorum obligations remain **separate**, including when
   the new primary was not a previous member. Missing members are missing votes;
   never shrink a membership list/minimum merely to make a wait succeed.
3. Received, applied, committed and client-completed progress are different.
   An LSN report alone is not proof of committed history or readiness.
4. New replicas remain InBuild until real copy finishes at the selected horizon,
   explicit application ACKs complete and required catch-up succeeds. Cancellation
   or cleanup completion cannot be relabeled successful Build.
5. Epoch/incarnation, control revisions and engine/build sessions fence delayed
   data and control. Preserve the existing native-shaped action/flag/default-
   field requirements; an Open body is not a valid generic control body.
6. Publish serving authority only after the runtime and engine agree on role,
   epoch, access and readiness. Quiesce/fence prior serving authority before
   granting a competing primary.

These are inherited constraints, not new durability promises. See
[FubeData.md:108–167](../runtime/FubeData.md?plain=1#L108-L167) for actual progress/copy
semantics and the previous-quorum-one case.

### Singleton first

Use an explicitly declared volatile target/minimum **1/1** service variant.
Open, establish the configured primary through RAP, perform nonzero writes,
read values and identified-retry results, then close and prove endpoint
withdrawal/cleanup. It is not an HA test. Do not quietly convert a 3/2 package
to 1/1 or claim a singleton retains data after its process dies.

### Three replicas and planned changes

Use a declared **3/2** profile across three admitted RAs/app processes. The
following are required transaction barriers, not a replacement wire protocol:

1. **Prepare:** verify participants/identities and freeze conflicting controller
   work. Record the old configuration and requested target; allocate a monotonic
   configuration epoch. A routine move does not invent a data-loss epoch.
2. **Build where needed:** authorize the new idle replica's Open; obtain its real
   replication endpoint. Drive `ReplicatorBuildIdleReplica` from the selected
   current primary, with actual copy and applied acknowledgments. Keep the old
   configuration's obligations while build proceeds.
3. **Establish transition evidence:** obtain action-appropriate status/epoch
   results and install the intended previous/current catch-up configurations.
   Begin the appropriate ordinary/Catchup `UpdateConfiguration` phases according
   to the existing RAP contract; EndReconfiguration belongs to completion, not
   permission to skip transfer/catch-up barriers. Record each semantic reply and
   flags before advancing; enforce both configurations' obligations.
4. **Transfer primary, if requested:** use the ordered RAP demotion/access-
   revocation and epoch/status/catch-up sequence. Establish that the old primary
   cannot still serve the old authority before completing the new primary's
   serving transition. This can be a qualified, matched demotion/revocation
   barrier; otherwise terminate the exact old owned process and observe its exit.
   An RA acknowledgment of intent is not that barrier.
5. **Complete:** require the promoted/current primary's real quorum/catch-up and
   end-reconfiguration results, role/epoch/access agreement and endpoint
   observations. Only then mark the new registry generation eligible.
6. **Retire:** remove departed idle/configured owners using their proper control
   path; retain tombstones and discharge cleanup. Removal cannot retroactively
   satisfy an earlier quorum by erasing its required members.

For primary-preserving secondary replacement, retain the current primary and
use the applicable build/configuration barriers; do not perform a needless
promotion. For primary transfer, step 4 and its intervening protocol operations
are mandatory. RA expands each transaction into the existing runtime's legal
action-specific trace, not one universal broadcast ordering. Before qualifying
each transition, tests must pin its exact command bodies, flags and barriers
against runtime models and the independent native tier, including the native
FM–RA and RA–RA message trace.

If a stage fails or its outcome is unknown, stop advancement, retain both
configuration obligations and reconcile/cancel/clean up the affected operation.
Do not roll back by assigning an older epoch or pretend a timed-out replica
never opened.

### Failure recovery is narrower than planned movement

Initially implement recovery only for **confirmed owned process failures**
with adequate surviving history/quorum evidence. A valid recovery must:

- Prove old serving authority and any old relaunch intents fenced. If the RA is
  unavailable, the local owner must establish termination of its relevant
  containment set; a dead RA PID alone is insufficient.
- Gather coherent previous/current configurations and epoch-bound replica
  progress. Establish the committed-prefix/recovery and catch-up obligations
  from the required surviving members; never pick a primary solely by maximum
  LSN or a majority of whichever replicas happen to answer.
- Choose an eligible, observed caught-up member under the serialized FM
  transaction and drive the legal promotion/configuration protocol. If evidence
  is insufficient, report unavailable and retain the obligation.
- Build replacement replicas from a valid surviving source, using real copy.
  Never invent Ready or zero-progress history to unlock service.

This does not attempt availability through arbitrary network partitions.
A future quorum-safe recovery protocol could justify broader promotion, but
would need a separate safety argument and fault tests; it is not supplied by
FM heartbeats, local lease expiration guesses or classic V1 data transport.
All-replica loss is terminal data loss for this profile, not automatic recovery
to an apparently healthy empty service.

## 11. Failure and uncertainty matrix

| Observation | What is actually known | Required behavior |
|---|---|---|
| FM↔RA link breaks; app and RA alive | FM observations are stale. RA may still renew its app lease and replicas may communicate. | Mark suspect; withdraw new resolves conservatively; reconcile on reconnect. No promotion based on timeout. |
| FM exits | No new placement decisions; old RAs/apps may remain authoritative locally. | Existing work may continue under its gates. No replacement controller adoption; owner tears down before fresh cluster identity. |
| RA heartbeat stops; no OS proof | Agent or network may be stalled; app termination is unknown. | Keep old serving owner potentially live; block conflicting primary replacement. |
| Exact RA process exit observed | Controller died, not necessarily all its apps/helpers. | Verify app containment termination and retire old intents before admitting a new node incarnation or conflicting owner. |
| App IPC EOF, zero TTL or invalidation | Runtime should terminally retire; watchdog is expected to terminate it. | Withdraw local eligibility; observe matched retirement/process exit before using it as recovery fencing proof. A notification alone is not a kill receipt. |
| Exact app exit observed while RA remains live | That process incarnation stopped. Pending launch/retry work may still matter. | Tombstone the incarnation and its launch intents; report fault. FM authorizes any new incarnation subject to stateful recovery gates. |
| Old primary demotion/revocation completes | Only the matched phase's qualified access/role postcondition is proven. | Use it only with matching epochs, no conflicting pending work and required catch-up/configuration evidence; otherwise require process fencing. |
| Secondary exits; primary still has quorum | Lost member's volatile state is gone; surviving quorum may still commit. | Keep declared obligations; build a replacement through real copy. Do not reduce quorum to heal the display. |
| Too few required previous/current members survive | No sufficient history/quorum evidence, even if a replica reports the largest LSN. | Stay unavailable. No forced primary or silent singleton downgrade. |
| Open/Build/Close times out or cancellation is requested | Remote outcome and callback disposal may be unknown. | Query/reconcile; retain cleanup obligation, prevent conflicting operations; force owned cleanup when authorized and necessary. |
| Callback/destructor stalls while lease renews | Lease health does not make the callback safe. | Existing callback deadline and independent watchdog still fence; cleanup failure stays visible. |
| Late reply/endpoint report/ACK after replacement | It may belong to a retired session, incarnation, epoch or build. | Reject as stale; no resurrection, publication or command completion. |
| Cleanup exceeds deadline or ownership is uncertain | Resource absence is unproven. | Fail cleanup and block unsafe reuse/restart; never kill/adopt unrelated resources. |
| Entire machine or replica set is lost | Shared physical fault domain; volatile data may be irrecoverable. | Fresh deployment only after fencing; explicitly report data loss, not preserved service state. |

## 12. Delivery milestones and acceptance evidence

These are future qualification gates, **not implementation performed by this
document**. No acceptance result below is claimed to have run.

Every milestone requires the complete native RA–FM/RA–RA message inventory and
conformance evidence for its enabled exchanges. Remaining exchanges retain
explicit implementation stages; completing M1–M4 does not erase those
obligations or establish complete protocol parity.

### M1 — Standalone stateless cluster

- Start one FM and at least two RA processes without SF packages, Fabric/FabricHost
  or onebox. Verify actual binary identities and absence of native runtime
  library mappings in the Rust processes.
- Create multiple stateless instances across RAs; observe complete
  host/runtime/type/Open admission. Resolve actual endpoints and connect using
  an independent client, not only RA events.
- Repeat create with the same request ID, drop replies and reorder stale
  reports: no duplicate processes/instances or stale endpoint resurrection.
- Delete one service without disrupting another; independently observe listener
  shutdown, unit retirement, process/group cleanup and name generation fencing.
- Isolate FM connectivity while local polls continue: do not infer app death or
  create a conflicting owner. Exercise local lease loss separately.

### M2 — Volatile stateful singleton

- Materialize valid mixed-type/config/data packages and reject unsupported
  manifests/security settings without weakening native validation.
- Use explicit 1/1 configuration and opt-in `fube-data-v1` initialization;
  assert nonzero acknowledged writes, values, request ledger and role/access/
  epoch observations from independent app clients.
- Verify identical retry versus conflicting payload behavior and ambiguous
  client results. Process loss loses singleton volatile state; never relabel a
  new empty incarnation recovered.
- Block real callbacks/destructors while polls remain healthy; confirm the
  existing hard-fence behavior and separate cleanup reporting.

### M3 — Three replicas and planned moves

- Use 3/2 across three RAs and distinct app processes. For replacement tests,
  provision another configured destination or free an eligible node explicitly.
- Perform nonzero writes, copy to a genuinely empty destination, hold its real
  application copy/apply ACK and observe InBuild/unavailable readiness. Release
  the barrier and require same-incarnation coherent completion.
- Complete planned primary transfers and secondary replacements; compare values,
  sequences, request ledger and epochs before/after, including repeated moves
  with no intervening write.
- Test previous/current quorum separation, a new primary outside the previous
  configuration, delayed application ACKs, required targets/ALL catch-up,
  canceled build cleanup and bounded unavailable quorum.
- Independently verify no stale primary serves after the qualified transfer
  barrier; an FM role table alone cannot pass.

### M4 — Observed failure, reconnect and stale-control recovery

- Terminate an exact owned app/RA, prove relevant fencing, and recover only when
  surviving history/quorum permits. Replay old Open/configuration/endpoint
  frames against replacement incarnations; none can revive authority.
- Separately partition FM–RA with apps still serving and renewing local leases:
  no automatic promotion. Restore connectivity, force snapshot reconciliation
  and prove convergence without duplicate commands/processes.
- Inject timeout after real side effects, cancellation, stale session deltas,
  buffer overflow and tombstone pressure. Unknown outcomes remain visible and
  bounded admission fails safely.
- Crash FM with live replicas: new deployment is refused until exact old
  processes are confirmed fenced. Demonstrate fresh identity and explicit
  volatile-data loss semantics, not transparent recovery.
- Verify interrupted startup, teardown failure, unrelated-process protection,
  bounded logs/resources and cleanup evidence under the original owner.

## 13. Testing and compiler-free CI

Retain three complementary tiers:

1. **Deterministic models:** FM desired/observed convergence; RA semantic
   state transitions; duplicate/reordered/lost control; epochs/incarnations;
   native FM–RA/RA–RA dispatch, acknowledgments and retries; quorum/copy barriers;
   backpressure and ownership accounting. Keep runtime
   child-process watchdog tests, including blocked callbacks and destruction:
   [model_tests.rs:658–728](../../crates/libs/fube-runtime/src/model_tests.rs#L658-L728).
2. **Native-free real-process cluster acceptance:** prebuilt FM, RAs, Rust apps
   and independent observers/clients; real loopback IPC, copy/replication,
   OS exits and controlled failure injection. Barrier on observed events and
   application contents, not sleeps or fabricated state changes.
3. **Smaller independent native interoperability tier:** retain pinned
   SF 11.8.121.4 standalone native-app hosting and selected real-SF/native-peer
   runtime/reconfiguration/data scenarios. Keep the existing required native
   profile intact unless a separate qualification decision changes it.
   Rust-to-Rust success alone cannot establish native parity.

Enforce the [protocol conformance gates](RustRaPort.md#native-reliability-message-preservation)
in CI: inventory completeness, native-to-Rust and Rust-to-native codec fixtures,
and state/action trace comparisons for enabled messages. Local round trips,
successful socket writes and generic command-status tests cannot substitute
for these checks.

Extend the declarative [tests/suites.toml](../../tests/suites.toml) with explicit
cluster suite/resource/profile routing when implementing; no new suite exists
yet. Do not reuse the existing `cluster_models` name as evidence for an FM/RA
cluster—it currently selects the repository's existing lifecycle-model group.

Follow the current producer/consumer split: Ubuntu22 builds workspace
executables once and archives tests; consumers execute transferred artifacts,
not `cargo build/test` or another hidden compiler fallback
([rust-build.yaml:15–44](../../.github/workflows/rust-build.yaml#L15-L44),
[92–128](../../.github/workflows/rust-build.yaml#L92-L128)).
Future cluster consumers must:

- Verify exact source revision/tree, CI run/attempt, target/profile, toolchain/
  lockfile, suite manifest and complete artifact inventory before allocation.
- Verify binary SHA256, executable mode, archive members and extracted bytes;
  reject symlinks, wrong-source/stale artifacts and missing files. Existing
  patterns are [runner.py:46–89](../../tests/native_runtime/runner.py#L46-L89)
  and [ci.py:332–381](../../tests/onebox/shared/ci.py#L332-L381),
  [411–431](../../tests/onebox/shared/ci.py#L411-L431).
- Require prebuilt inputs even for local acceptance. Missing artifacts fail with
  upstream producer guidance; the test does not compile Rust.
- Record executable hashes, identities, command/reply phases, actual endpoints,
  application contents/ledger, process observations and original-owner cleanup.
  Verify expected scenario coverage independently; command exit zero is not proof.
- Keep allocation ownership and emergency-cleanup capability outside uploaded
  diagnostic data; cleanup only original resources, including partial startup.
  Preserve failed scenarios even when later teardown succeeds.

Model tests and the independent quality/build jobs may still compile where
that is their declared purpose. The no-compilation rule applies to acceptance
consumers, not the upstream Rust producer.

## 14. Remaining choices and design boundary

Safety decisions above are fixed for the initial profile. These bounded
implementation choices remain open without changing those decisions:

- **Control transport and API spelling:** select outer session framing and
  HTTP/CLI surface without redesigning native RA–FM/RA–RA actions, headers,
  payload encodings or processing contracts. Retain finite bounds and explicit
  capability admission; native codec conformance is required, not optional.
- **Module/crate layout:** a focused cluster library with FM/RA entry points is
  a reasonable starting point; decide whether reusable host mechanics live in
  `fube-test-host` or a shared internal module. No dependency direction should
  pull native test libraries into production runtime/data.
- **RA port and persistence:** follow the [companion design](RustRaPort.md)
  for the store abstraction, SQLite backend and incremental native-state-machine
  port. Durable RA history is not durable application state or automatic FM
  recovery; it does not restore old runtime leases or serving authority.
- **Resource budgets:** choose finite profile values for inventory, retries,
  tombstones, packages and logs, with negative tests at each bound. These cannot
  be resolved by silently evicting safety fences or extending callback deadlines.
- **Broader recovery/placement:** defer durable FM recovery, automatic
  partition-tolerant promotion, dynamic membership and physical multi-machine
  fault domains. Each needs its own authority/recovery argument and qualification.

The next engineering decision is whether to implement **M1** under these
contracts. This design does not authorize implementation, commits or publication.
