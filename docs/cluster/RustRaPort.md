# Rust RA port and SQLite state-store design

**Status: proposed RA design. The [P1 store seam](RaStore.md) and
[protocol/bounded P2 foundation](RaProtocolFoundation.md) and
[native-derived stateless removal/close](RaStatelessRemoval.md), opening and
[stateless Drop/FM-resend replacement](RaStatelessReplacement.md) are implemented;
full P2 infrastructure, P3–P5 and RA/runtime recovery are not.**

This is the persistence and porting companion to the
[basic pure-Rust cluster design](PureRustCluster.md). Keep the native RA's
key/value-store boundary, implement an embedded SQLite backend, and port its
entity execution and reliability state machines into Rust above that boundary.
Replacing the database is a useful first seam, not a way to make the entire
native RA independent of Service Fabric.

The target remains separate FM and RA+Hosting processes, existing application
Hosting/RAP IPC, finite runtime-to-local-RA leases and volatile `fube-data`.
All native RA–FM and RA–RA message contracts must be preserved and ported.
Federation routing/membership and native node-to-node distributed leases remain
excluded; native peer-reliability messages are not excluded with them.
The [node-side Hosting design](RustHosting.md) specifies the proposed
`fube-hosting` dependency boundary and incremental native Hosting port.

## 1. Scope and compatibility

Port the native RA's **supported state-machine semantics**, not C++ ownership,
Begin/End scaffolding or the full native node dependency graph. Native RA
handles more features than the initial cluster will support. Preserve explicit
capability boundaries; features not yet enabled must not receive successful
no-op responses or be mistaken for qualified behavior. Every RA–FM and RA–RA
exchange remains required porting scope even when its execution feature is
temporarily disabled.

Compatibility goals are distinct:

| Boundary | Goal |
|---|---|
| Store behavior | Preserve typed records, insert/update/delete outcomes, consistent enumeration and commit-before-effects. Both backends run the same contract tests. |
| RA behavior | Preserve the selected epoch/incarnation, configuration, lifecycle, retry and recovery invariants; port corresponding native tests. |
| Reliability messages | Preserve all native RA–FM and RA–RA actions, directions, headers, serialized payloads and processing contracts. Stage delivery explicitly; no exchange may silently disappear from scope. |
| Application IPC | Reuse the existing Hosting/RAP schemas and runtime programming model. A local database format is not an IPC format. |
| Native disk migration | Not supported. Do not open/import native ESE/TStore databases or promise identical serialized record bytes. |

SQLite through the usual Rust bindings includes the **C SQLite engine**.
The RA logic is Rust and needs no native SF libraries, but this backend is not
an all-Rust dependency stack. Keep the interface backend-independent if a
strictly Rust-only storage implementation is later required. No dependency or
Cargo manifest change is made by this design document; the separate P1
implementation uses bundled SQLite through `tokio-rusqlite`.

## 2. Native implementation baseline

Native source references pin Service Fabric revision
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`, inspected 2026-10-29. They describe
source behavior, not a claim that the pinned SF 11.8.121.4 qualification binaries
were built from that revision.

| Native behavior | Porting consequence |
|---|---|
| [`IKeyValueStore`][native-store] enumerates rows by type and exposes asynchronous insert/update/delete on typed IDs and byte values. Concurrent distinct-ID work is allowed; concurrent inserts of the same record produce one success and one conflict. | Implement a small logical store interface, not ESE/TStore APIs. Serialize execution per entity; SQL writer serialization is permitted. |
| [`LocalStoreAdapter`][native-adapter] selects a provider, opens it and commits row operations through transactions. Linux [enables TStore][native-config]; [ESE is unsupported there][native-factory]. | SQLite replaces this local provider stack, not the RA algorithms. |
| [`TSLocalStore`][native-local] reuses SF's store/replicator infrastructure as a single-replica local database. | We do not need a replicated database or native transactional replicator merely to persist RA metadata. |
| [`EntityMap`][native-entity-map] distinguishes in-memory updates from stored updates, serializes durable records, and publishes changed entity state only after successful store completion. | Preserve explicit durable versus memory-only transitions and staged entity state. |
| [`EntityJobItem`][native-actions] executes queued actions after a successful commit and cancels them when the job was dropped or commit failed. | Effects cannot escape before the transition permitting them commits. This does not imply the native action queue itself is a durable outbox. |
| [`FailoverUnit`][native-fields] serializes selected fields: descriptions, epochs, local replica identity, state/deletion information, replicas, last-stable epoch and deactivation information. | Persist a deliberate recovery model, not the entire running RA, callback graph or socket/session state. |
| [`LocalStorageInitializer`][native-load] enumerates/deserializes records, rebuilds LFUM and applies load fixups. [`UpdateStateOnLFUMLoad`][native-fixup] marks persisted replicas down and volatile replicas dropped. | Recovery is an explicit state transition, not restoration of the saved Primary/Ready label. |

The C++ constructor also depends on Hosting, Federation, reliability wrappers,
IPC, scheduling and logging ([constructor dependencies][native-dependencies]).
The storage interface is only one dependency seam.

## 3. Target architecture and dependency adapters

```text
FM/peer RA messages / runtime replies / Hosting observations / timers
                            |
                 per-entity RA executor
          committed state -> proposed state + effects
                            |
                   logical RA store
                    /             \
              memory model     SQLite worker
                            |
              committed state + permitted effects
                            |
         FM/peer RA transport / Hosting / runtime IPC
```

Port or adapt responsibilities as follows:

| Responsibility | Treatment |
|---|---|
| Failover-unit/replica logic | Port selected lifecycle, epoch/configuration, semantic completion, retry, fault and recovery rules faithfully. |
| Entity scheduler and action queue | Implement Rust ownership/concurrency around the same ordering invariants. Serialize conflicting entity work; preserve required multi-entity barriers. |
| Hosting and process ownership | Integrate the long-running cluster host, package admission, app exits and independently verified cleanup. |
| Runtime IPC and local lease | Reuse existing codecs and runtime authority semantics. Keep lease service and callback/destructor supervision independent from database progress. |
| FM communication | Port every native RA–FM exchange and its acknowledgment/retry/generation semantics. Single-FM deployment is a profile boundary, not permission to redesign the messages or synthesize FM/FMM acknowledgments. |
| Peer RA communication | Port every native RA–RA exchange, including replica creation, progress discovery, activation/deactivation and replies. Preserve phase ordering, source checks and stale-message behavior. |
| Federation types/transport | Replace routing/transport with explicit configured FM–RA and RA–RA sessions. Retain native identity/incarnation/generation fields and checks; do not implement Federation membership or distributed node leases. |
| Clock, retry and diagnostics | Supply bounded timers, supervised execution and typed observations without exposing user callbacks to storage locks. |
| Upgrades, deactivation and broader native node features | Preserve all associated RA–FM/RA–RA contracts and assign their handlers/tests to explicit delivery stages. Execution remains capability-gated until qualified; disabled does not mean omitted from the port. |

There is no second data-replication engine in RA or FM. Continue using
`Engine::bind_partition`, real copy/catch-up and the existing application access
gate; see [the data profile](../runtime/FubeData.md).

### Native reliability-message preservation

The required scope is **all RA–FM and RA–RA exchanges**, not just the messages
used by the first stateless/stateful scenarios. Build a catalog pinned to the
native baseline from [RSMessage actions and actors][native-rs-messages],
[RA dispatch metadata][native-message-metadata], both FM/RA handlers and actual
send paths. Include common node-registration exchanges used by RA, separate
FM/FMM addressing and deprecated native handling; do not infer direction or
active use solely from an action name or registry entry.

The [source-pinned catalog](NativeRaProtocol.md) now reconciles 119 initialized
actions, 66 boundary actions/67 actual directions and two declaration-only
legacy names. [Addition/removal/ReplicaUp/notification codecs and native source-stream
evidence](RaProtocolFoundation.md#codec-evidence) are implemented; all remaining
codec/handler/trace obligations stay explicitly staged.
[Stateless opening failure/retry](RaStatelessOpening.md) now includes native
classification/threshold/health/cleanup/reply policy, retained usage and bounded
node-wide registration notifications. Opening/removal run against trusted
simulated registration/RAP/host-down events, not live Hosting. Eight selected
action codecs and source-derived tests do not establish original-handler
execution or deployed parity.
The enabled surface now includes generic generation lower-bound admission,
raw request-shaped replies, native Linux identifier comparison/returned spelling,
per-property dynamic retry configuration and separate notification work/timer/
captured-send semantics; old ASCII/normalized-Ready/immutable-policy restrictions
are not preserved as vanilla behavior. Actual native helper comparisons remain
separate from complete handler/runtime execution.
Catalog completeness
is not complete native protocol implementation.

For every exchange, record:

| Contract element | Required preservation/evidence |
|---|---|
| Identity and routing | Native action, actor, sender/receiver, generation headers and node/replica instance binding. Replacing Federation delivery does not replace these checks. |
| Payload | All serialized fields, including inherited/nested descriptions, field order, enum values, widths, defaults, optional values and errors. Native body encoding is not an incidental Rust layout. |
| Dispatch | Target entity, entity-creation permission, processing during close, source validation and stale-message rules. |
| Completion | Native request/reply or notification/ack meaning, retransmission, batching and terminal conditions. A transport ACK or generic status cannot substitute for them. |
| State and effects | Preconditions, committed transition, post-commit actions, duplicate/late handling and permitted follow-up messages, including RA–RA dependencies. |
| Delivery status | Rust type/codec/handler/test mapping, implementation stage and qualified capability profile. No unclassified exchange or permanently omitted message. |

For example, [ReplicaUp][native-replica-up] carries both replica and dropped
lists, a final-upload flag and FM/FMM identity. A generic inventory
acknowledgment loses that contract. `CreateReplica`, `GetLSN`, `Deactivate`,
`Activate` and their replies are native RA–RA reliability traffic, not
Federation membership traffic.

Use the native logical messages and serialized body/header contracts inside
any new bounded session envelope. Extra deployment/session identities may
strengthen admission, but must not overwrite native generations, conflate
FM/FMM, require invented fields in native bodies or change acknowledgment
postconditions. Outer routing/framing may replace Federation; preserving
payload/header codecs alone does not claim that a native FM can join this
cluster.

Required conformance gates are:

1. Check the entire catalog against pinned native registry, dispatch and
   send/receive paths. Every exchange has a mapping and delivery stage;
   deprecated behavior is explicitly classified, not silently deleted.
2. For each enabled exchange, use native C++ serialization to produce fixtures
   that Rust decodes, and validate Rust output with a native decoder. Cover
   nested/inherited fields, defaults and nondefault values, enum/error values
   and invalid/truncated inputs. Rust-only round trips are insufficient.
3. Port native behavioral scenarios and compare normalized state/outgoing
   message traces under equivalent preconditions. Include duplicate, stale,
   reordered and lost traffic; retry/ack boundaries; peer phases; and definite
   or unknown store completion. Do not normalize away safety-relevant fields
   or ordering.
4. Make missing catalog entries or missing codec/handler/trace evidence for an
   enabled capability CI failures. An unfinished feature must fail capability
   admission rather than receive success-shaped stub replies. It remains an
   explicit porting obligation.

Protocol preservation is complete only when every required exchange has its
implementation and evidence. Completing the initial cluster milestones is not
that claim. Real native interoperability and broader native feature behavior
also require their independent qualification; neither follows from fixtures
alone.

## 4. Logical store contract

Rust APIs may use futures and owned requests instead of native Begin/End.
Keep the following semantics explicit:

| Operation | Required outcome |
|---|---|
| Enumerate a record type | A complete, consistent committed snapshot, subject to declared capacity bounds. Never return partial success or a mixture of snapshot generations. |
| Insert | Create an absent typed ID; duplicate primary key is a record conflict. Never replace an existing value. |
| Update | Replace one existing typed ID's value; missing record is not-found. |
| Delete | Remove one existing typed ID; missing record is not-found. |
| Commit completion | Success means the requested durable transaction committed under the declared storage profile; scheduling or statement execution is insufficient. |
| Close | Stop new admission, account for queued/in-flight work, and report errors/uncertain outcomes explicitly. Dropping a caller future does not prove a worker stopped. |

IDs and record types have stable encodings; do not persist a Rust enum's
incidental ordinal or silently normalize identifiers. P1 uses bounded opaque
bytes and versioned **store** metadata. A future versioned RA entity envelope
belongs to P2, not the storage backend. The store does not reinterpret roles,
epochs or application payloads.

Keep the native-compatible single-record operation as the minimum interface.
For initial partition work, place tightly coupled replica state, accepted
command identity and reconstructible recovery intent in the same versioned
entity aggregate. A future requirement spanning separate entity/Hosting/
command records needs an **explicit atomic batch extension** and tests.
Several independent store calls are not an atomic multi-record transaction.

The in-memory backend must preserve the same logical errors, staged-commit
behavior and enumeration contract, including injected commit failure. It is
for models, not evidence of disk durability.

## 5. SQLite layout and execution

### Database ownership and schema

Use one database per RA, in a private owner-controlled directory on a supported
local filesystem. Do not share it between logical nodes or run it on a network
filesystem. Require exclusive cooperative RA ownership in addition to SQLite's
transaction locks: SQL locking alone does not prevent two node controllers from
alternately issuing valid writes.

The minimal logical layout is:

```sql
CREATE TABLE records (
    record_type TEXT NOT NULL COLLATE BINARY,
    record_id TEXT NOT NULL COLLATE BINARY,
    value BLOB NOT NULL,
    PRIMARY KEY (record_type, record_id)
) WITHOUT ROWID;
```

Use separate, versioned store metadata for format identity and node/cluster
binding. Any added metadata tables are part of the same database. Initial
creation is an explicit bootstrap operation; opening an existing deployment
with a missing, empty, corrupt or incompatible store fails rather than silently
creating an empty node.

The schema is not an operational CLI or implemented migration. See the
[P1 profile](RaStore.md) for the exact strict schema, metadata and selected
binding. The minimal P2 seam now stores a versioned selected-entity aggregate;
its [format and limits](RaProtocolFoundation.md#state-commit-and-effects) are
not a native disk codec, migration or restart/replay protocol.
Native failover-unit records are the source model, not a requirement to use
native disk serialization. Cluster-specific command/ownership records are
extensions, not fields that vanilla RA is claimed to persist.

### Writer and durability profile

Use a dedicated owned blocking database worker with a bounded request queue.
One writer is sufficient initially. Callers may submit distinct-ID operations
concurrently; serialization must not turn ordinary contention into a semantic
record conflict. Transactions and statement parameters stay on the worker;
no SQLite call or synchronization wait runs on the lease-service path.

The initial durable profile uses WAL journaling and `synchronous=FULL`.
Query/verify effective settings on connection initialization rather than
assuming a PRAGMA spelling took effect. Configure every connection that can
write. SQLite documents [WAL synchronization and concurrency][sqlite-wal]:
FULL synchronizes the WAL on each transaction commit; NORMAL can lose recent
commits after a power failure/hard reset. Durability still depends on the
filesystem, VFS and device honoring synchronization; process-crash tests alone
cannot establish power-loss safety.

Implement Insert with ordinary `INSERT`, never `INSERT OR REPLACE` or an
upsert. Check affected-row counts for Update/Delete. Map only actual duplicate
key violations to record conflict; do not hide malformed input, other constraint
violations, I/O errors or corruption behind that result.

Use finite operation budgets and bounded lock-contention handling. A timeout
while a transaction is running may leave its outcome unknown to the caller.
Resolve the worker's actual result or enter recovery before releasing
conflicting entity work; do not report rollback merely because a future was
cancelled. Do not start a second worker to race an indefinitely blocked first
writer. Escalate an unresponsive writer through supervised node failure and
owned cleanup without starving local lease handling.

### Checkpointing, bounds and errors

Bound record size/count, queued bytes, reader lifetime, disk usage and retained
command/tombstone history. Checkpoint work stays off lease/runtime ingress;
long readers must not pin unbounded WAL growth. `journal_size_limit` or a
checkpoint request alone is not a hard live-WAL limit. Refuse unsafe new writes
under resource pressure rather than deleting recovery fences to reclaim space.

Preserve the database/WAL set. Never delete `-wal`/`-shm` files to recover space
or copy a live main database file alone as a backup. Use SQLite-supported
consistent backup/maintenance if later required.

Disk-full, read-only, I/O, corruption and migration failures are explicit node
storage failures. Do not release uncommitted actions, acknowledge success,
reset the store or force native success-shaped defaults. Block new transitions
that require durable state, expose failure to FM, and enter the defined
supervised fencing/cleanup path. An in-flight commit may require unknown-outcome
reconciliation; a storage fault is not itself proof that apps stopped.

## 6. Entity execution and effects

For a selected event, the port must:

1. Validate source/session, entity incarnation, epoch and expected phase.
   Acquire serialized entity execution and create a private proposed state.
2. Produce an explicit memory-only or durable mutation plus permitted actions.
   Native memory-only updates are preserved only where they cannot change
   recovery identity, authority fences or durable obligations.
3. For durable work, commit the proposed record and any atomic recovery intent.
   The previously committed in-memory entity remains authoritative until the
   store result is known.
4. On success, publish the committed state, then release permitted effects in
   the required ordering. On definite failure, discard the proposal and cancel
   its effects. Unknown commit outcomes keep the entity fenced from conflicting
   work until resolved.
5. Match subsequent lifecycle/control replies semantically and commit the
   resulting phase where required before reporting durable completion.

Do not invoke arbitrary app/Hosting callbacks under database locks or assume
every native action can move outside the entity lock without analysis. The
native [post-commit action path][native-actions] documents ordering hazards.
Re-express those dependencies in the Rust executor rather than copying locks
or dropping guards indiscriminately.

The database cannot transact atomically with process creation, RAP callbacks
or FM delivery. A crash after an effect but before its completion record leaves
an unknown outcome. Reconstruct only phase-appropriate retries/cleanup from
committed intent; never replay an arbitrary Open/primary transition because it
appears unfinished. If an effect cannot be reconstructed safely, its intent
must be durable before release. Durable intent is not exactly-once execution.

## 7. Persistent versus live state

| Persist as recovery evidence | Recreate or revalidate after restart |
|---|---|
| Store format, cluster/FM binding, stable node identity and incarnation history | New RA admission/session and exclusive authority to operate the store |
| Service generation, partition/replica/incarnation, epochs and previous/current configurations | Actual local runtime/replica existence, roles, access and engine readiness |
| Accepted command identity/payload, committed phase and bounded result history | Pending operation outcome, reply correlation and permitted next action |
| Deletion/revocation fences, tombstones and revision high-water marks | Actual endpoint eligibility and FM registry publication |
| Package hashes, launch/ownership intent and cleanup obligations | Live OS ownership, process/containment membership and verified absence |

Persist selected logical state, not sockets, callback futures, executor locks,
pidfds, host registrations or lease grants. Persisted PIDs alone are unsafe
ownership proof; OS boot/start identities and live verification are required.
Pidfds/child handles cannot be serialized and reopened as proof of the old
ownership relationship.

An endpoint or saved Primary/Ready label is historical evidence, not current
authority. Raw monotonic deadlines cannot be reused across boot/process
lifetimes. Old pending operations enter reconciliation; a new recovery command
gets a deliberate new budget only after its old intent is resolved or retired,
not an automatic deadline reset.

## 8. Restart protocol and safety boundary

The first durable-RA recovery profile keeps FM alive and uses
**fence, reconcile, then rebuild**:

1. Obtain exclusive store ownership and validate format, cluster/FM binding
   and integrity. Refuse corrupt, missing or foreign deployment state.
2. Load committed entities and uncertain intents without enabling serving,
   replaying launches or restoring registrations.
3. Under the original external ownership capability, prove old RA/app
   incarnations fenced and retire their launch/retry intents. A dead RA PID,
   missing heartbeat or expected lease expiry is not sufficient app-exit proof.
   If ownership or termination cannot be established, block admission/reuse.
4. Durably allocate a fresh node incarnation before registering it with the
   same surviving FM. Report recovered records as historical/down/dropped/
   uncertain observations, not live service eligibility.
5. Reconcile with FM under a new session and inventory barrier. Reject stale
   commands/replies; preserve old epoch/configuration obligations and deletion
   fences. Old incarnation commands cannot become new launch permission.
6. FM authorizes fresh volatile replica incarnations when recovery evidence
   permits. Rebuild through actual copy/catch-up from eligible survivors;
   singleton/all-replica loss remains volatile data loss.

This follows the useful native distinction: [load fixups][native-fixup] mark
persisted replicas down but volatile replicas dropped. We do not implement
persisted application replicas simply by storing an RA record.

Local runtime leases remain finite and independent of FM connectivity. Never
restore an old lease grant or make it infinite during recovery. New hosts
require fresh registration and fresh correlated polling; existing
[runtime authority and watchdog rules](../runtime/RustStatelessRuntime.md#qualified-profile)
remain intact. There is no node-to-node distributed lease service.

Warm adoption of apps surviving RA restart is **deferred**: it needs its own
ownership proof, runtime reattachment and lease-handover protocol. SQLite
persistence alone does not add that protocol. Likewise, an unreachable RA that
may still renew its apps cannot be replaced by timeout-only promotion.

Durable RA state does not make FM durable. The cluster design's
[FM restart policy](PureRustCluster.md#4-bootstrap-ownership-and-restart)
still requires confirmed old-deployment teardown before a new FM/cluster
identity. A leftover database from that deployment is not automatically
rebound to the new authority.

## 9. Port sequence and evidence gates

These define delivery gates. P1's implementation/evidence is described in the
[store profile](RaStore.md). The [minimal P2 foundation](RaProtocolFoundation.md)
demonstrates selected commit/failure/unknown/cancellation invariants; full P2
and P3–P5 remain incomplete, not completed cluster test results.
They refine the [cluster milestones](PureRustCluster.md#12-delivery-milestones-and-acceptance-evidence).

| Stage | Deliverable and required evidence |
|---|---|
| P1: store seam | Memory and SQLite backends share insert/conflict/update/delete/not-found, snapshot, capacity, concurrency, close and failure tests. Reopen verifies durable content; migration/foreign-store failures are explicit. |
| P2: entity infrastructure | Minimal selected-pair slice implemented: staged state, durable/memory-only transitions, postcommit actions, definite failure/unknown fences and cancellation barriers. Broader lifecycle, multi-entity dependencies and recovery remain staged. |
| P3: stateless RA | Complete protocol catalog and first-pair source-stream codec oracle now exist. Broader codecs/native traces and all stateless handlers with Hosting, registration, Open/Close/Abort, fault/report and endpoints remain required. Drive the real Rust runtime through M1, including finite leases and independently observed process cleanup. |
| P4: stateful control | Port the native FM–RA and RA–RA message choreography alongside action-specific configuration, build/remove, progress/epoch, catch-up/cancel and endpoint/revocation rules for M2/M3. Require codec/trace conformance, real nonzero data/copy ACKs, separate previous/current quorum evidence and safe primary-transfer barriers. |
| P5: durable RA restart | Integrate the recovery protocol with M4: process exit, commit/effect crash windows, replayed commands, fresh incarnation, FM reconciliation and actual volatile rebuild. No saved Primary resurrection or warm adoption. |
| Protocol completion: cross-cutting | Port and qualify every remaining cataloged RA–FM/RA–RA exchange, including those gated out of initial profiles. Track additional delivery stages explicitly; P3–P5 are not permission to drop the remaining messages or claim complete protocol parity. |

Durable storage can be exercised early without enabling restart recovery.
Until P5 is qualified, keep the cluster's existing fresh-RA/fenced-cleanup
behavior; do not advertise recovered serving or alter its initial profile.

Port relevant native tests with their state-machine preconditions and expected
effects, not only their names. Native [store tests][native-store-tests] already
share a test body across implementations; use that pattern. Retain a traceable
mapping from native scenario to Rust test or explicit unsupported capability.
Do not equate passing store tests with complete RA parity.

Minimum fault coverage includes crashes before durable commit, after commit
before effect, after effect before completion, and after completion before FM
receives it; duplicate/late control; tombstone pressure; disk-full/read-only/
corrupt database; long reader/checkpoint contention; blocked writer while
local lease polling and callback deadlines continue; and interrupted teardown.
After a crash, inspect committed rows and independently probe app state and
owned process absence. Database contents or FM events alone cannot pass.

Use [the existing test/CI tiers](PureRustCluster.md#13-testing-and-compiler-free-ci):
model/store tests, native-free multi-process acceptance and independent native
interoperability. Acceptance consumes verified prebuilt executables; it must
not build Rust or install/start native SF. Any bundled or dynamic SQLite
runtime dependency must be declared in producer artifacts and the consumer
profile. Power-loss/filesystem guarantees require separate qualification beyond
ordinary process termination.

## 10. Limits and remaining implementation choices

SQLite solves local transactions, not state-machine correctness, app-data
durability, distributed fencing, FM recovery or cluster consensus. Do not use
its success to weaken runtime leases, quorum rules or native profile admission.

P1 selects the binding/build policy, opaque record format, finite budgets and
supervised writer-failure policy in the [store profile](RaStore.md). The selected
entity envelope is implemented in [the minimal P2 seam](RaProtocolFoundation.md);
broader entity codecs, restart loading and explicit migration remain unimplemented.
Add atomic batch semantics only where real cross-record invariants require
them. All RA–FM and RA–RA message contracts are required porting scope, not an
optional later protocol redesign. Broader native RA feature parity, persisted
service storage, warm app adoption, transparent FM restart and native database
migration require separate qualification beyond message preservation.

The next work is broader P2 lifecycle infrastructure and the cluster M1
interfaces, including real Hosting/RAP integration and native behavioral traces.
This design document itself supplies no runnable cluster commands or evidence
of those unimplemented stages.

[native-store]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Storage.Api.IKeyValueStore.h#L16-L64
[native-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Storage.LocalStoreAdapter.cpp#L205-L245
[native-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Store/StoreConfig.h#L177-L182
[native-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Store/StoreFactory.cpp#L75-L105
[native-local]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Store/TSLocalStore.cpp#L795-L805
[native-entity-map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityMap.h#L226-L288
[native-actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h#L228-L278
[native-fields]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.h#L566-L577
[native-load]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/LocalStorageInitializer.cpp#L34-L93
[native-fixup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L3962-L3996
[native-dependencies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/IReconfigurationAgent.h#L68-L80
[native-store-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Functional.Storage.KeyValueStore.cpp#L24-L35
[native-rs-messages]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RSMessage.cpp
[native-message-metadata]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Communication.MessageMetadataTable.cpp
[native-replica-up]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaUpMessageBody.h
[sqlite-wal]: https://www.sqlite.org/wal.html#performance_considerations
