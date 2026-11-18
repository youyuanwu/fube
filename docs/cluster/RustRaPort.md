# Rust RA port and SQLite state-store design

**Status: implemented store/executor and bounded live reliability profiles;
full RA, restart recovery and native interoperability are not qualified.**

| Area | Current implementation and evidence |
|---|---|
| Store and execution | [P1 store seam](RaStore.md), [bounded entity/protocol foundation](RaProtocolFoundation.md), commit-before-effects and per-entity serialization. Memory/SQLite contract tests do not enable durable RA restart. |
| Lifecycle and node coordination | Stateless and volatile stateful Open/Close/Drop/Abort, native FM creation/deletion/reporting, explicit fresh recreation and bounded node-wide coordination. Semantic replies and original physical disposal/accounting remain separate. |
| Replication and reconfiguration | Real public runtime/data-replicator Open, Idle copy/build, active admission, configured Primary failover, explicit repair and supported removal supersession. [Process profiles](MinimalFmCluster.md) retain separate PC/CC quorum/history obligations and exact incarnation/retry fences. Both launchers use configurable immutable sizes and qualify [explicit spare restoration at minimum and after above-minimum Primary failover](#configurable-replica-set-sizes-plan). Repeated/reverse healthy planned transfers are qualified at 2/1 and 3/2, including every Secondary's committed activation and 3/2 transfer after completed repair. |
| Hosting restoration | Shared Node/Hosting activates an installed-package fresh volatile host for a committed Open's registration lookup after predecessor cleanup. Successful forced cleanup is allowed; diagnostic history is bounded independently of lifetime replacements. Surviving Node/RA/store/Channels are retained; lost runtime/data is not adopted. |
| Ordinary-FM LFUM | Generic committed inventory, separate PC/IC/CC views and independently owned uploads/retries. Real-process coverage includes stable/build/admission, two live 3/2 Primary failover/repair cycles, cancelled original-Node repair copy/build, repair-Secondary deactivation and healthy fixed-3/2 planned-handoff phases during held role/upload work. Model/store coverage remains broader. See [current LFUM status](MinimalFmCluster.md#live-session-lfum-upload) and the [alignment plan](#lfum-alignment-plan). |
| Unsupported recovery/ownership | Nonempty startup reconciliation, full initial ReplicaUp batching/final ACK, durable RA restart, FM/FMM rebuild and FM-service execution remain gated. Broader deployment/upgrade and original C++ handler/interoperability qualification are separate work. |

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
The [node-side Hosting design](RustHosting.md#implemented-h1-model) now includes
the native-derived H1 registry and [H2 admitted residency/scheduling](RustHosting.md#implemented-h2-accounting-and-scheduling).
A separate real-accounting RA test composition exercises original completion/
ticket ownership and definite/unknown durable close; FSR remains memory-only.
The [owned registration-events node slice](HostingRegistrationEvents.md) now
adds actual serialized current-state/local status consumers and actor-9 Hosting
registration/lookup over finite local sessions. Matching FT/readiness and issued
acquisition premises are explicitly qualification-only; required FM/HM/RAP
completion and deployed processes remain gated. This is not the full production
RA/Hosting lifecycle adapter and does not replace native policy with a queue ACK.

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
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. They describe
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

### FM-call completion roadmap

This is the recommended dependency order, not a claim of complete FM support
or a replacement for P1–P5 and M1–M4. Split each stage into small,
independently qualified increments; completing one bounded profile does not
complete every native branch of its handlers.

The [source-pinned catalog](NativeRaProtocol.md) contains **58 FM-boundary action
definitions**, including common registration, plus **eight peer RA actions**.
There are 67 directional contracts because DoReconfiguration also has a peer
send path. The current 39 selected FM-boundary and eight selected peer action
codecs have bounded handler/helper coverage, not complete native contracts;
19 FM-boundary actions remain unselected, alongside broader branches of the
selected handlers.

Stage 1 is implemented for the bounded volatile 1/1 FM profile; see
[its supported paths and evidence limits](MinimalFmCluster.md#singleton-lifecycle-completion-and-fresh-recreation).
This does not qualify every native handler branch or complete P4/M2.

| Stage | Status and remaining scope | Completion gate |
|---|---|---|
| 1. Finish singleton lifecycle | Implemented for the bounded volatile 1/1 profile: Abort/ForceAbort reporting, closed-AddPrimary handling, supported DeleteReplica branches and explicitly authorized fresh-lifetime recreation. Broader lifecycle profiles remain separate. | Preserve native close/report/ACK semantics and exact original accounting. Reporting and metadata completion cannot substitute for disposal proof or relabel lost volatile data as recovered empty state. |
| 2. Complete node-wide coordination | Bounded implementation: broader P2 multi-entity execution; NodeActivateRequest/Reply, NodeDeactivateRequest/Reply, NodeUpdateServiceRequest/Reply, ServiceTypeEnabled/Disabled and their replies, existing registration notifications and complete ReportLoad codecs/forwarding. ReplicaEndpointUpdated/Reply codecs and helper integrate with bounded planned-swap publication in stage 5. | Preserve sequence/version checks and action-specific completion barriers. Deactivation replies follow required replica closure, not admission; ReportLoad remains best-effort without an invented per-report ACK. Keep one FT per deployed Node; qualify multiple FTs in RA model/store tests. |
| 3. Complete ordinary-FM generation and upload | Core live-session handshake/inventory/upload is implemented. Remaining overlap qualification and distinct startup/reporting work follow the [LFUM alignment plan](#lfum-alignment-plan); [current evidence](MinimalFmCluster.md#live-session-lfum-upload) distinguishes shared RA coverage from test-FM/process admission. FM/FMM rebuild belongs to stage 7, not this live-session completion claim. | Preserve proposed/receive/send generations, per-entity commit barriers and independent upload ownership. Only a reply matching current receive advances send generation; neither upload ACK nor startup/report ACK grants runtime authority. Keep restart serving disabled until stage 6. |
| 4. Build and remove remote replicas | Bounded original two-node volatile Idle creation, nonempty copy/continuing replication, FM Add/build, pending/failed/opened Primary removal and same-lifetime retry/replay are implemented. The [original-session destination Delete contract](MinimalFmCluster.md#complete-original-session-destination-delete-contract) covers normal/force and opening/closing/closed/failed/absent/lost targets and separate original cleanup proofs. Close/Drop/Abort and host-loss reporting retain native stage/ACK distinctions. [Explicit fresh replacement](MinimalFmCluster.md#explicit-fresh-idle-replacement) now rebuilds one successor after original successful build, Primary removal and normal Delete/disposal. Remaining: broader replacement/lifecycle/report profiles, active configuration/reconfiguration/removal and native interoperability qualification. | Copy completion grants no active membership or serving; AddReplicaReply completes FM build and Ready/Idle is not ActiveSecondary. Only RemoveReplicaReply clears Primary pending-remove. Destination Delete independently joins its semantic reply with original cleanup evidence. Neither metadata/report ACK nor cleanup proof authorizes a successor: the bounded replacement requires a separate explicit FM request and destination-local disposal/drain admission. Original history and stale-incarnation fences survive replacement. |
| 5. Implement reconfiguration | Immutable configured initial admission and [Primary app-host failover](MinimalFmCluster.md#immutable-configured-primary-failover) now use per-member GetLSN/history, separate PC/CC read quorums, history-first selection, ChangeConfiguration and per-survivor Deactivate/Activate commits before final RAP/FM completion. [Explicit configured repair](MinimalFmCluster.md#immutable-configured-primary-repair) restores one missing member at unchanged sizes and enables repeated live-session failover/repair. [Standalone Secondary-loss repair](MinimalFmCluster.md#standalone-secondary-app-host-loss-repair) preserves the healthy Primary and native minimum/history rules through committed removal and fresh readmission. [Pre-replacement Primary-loss supersession](MinimalFmCluster.md#primary-loss-during-secondary-removal) retains original PC/CC during pending Secondary removal; [sequential restoration](MinimalFmCluster.md#sequential-restoration-after-supersession) repairs its two known losses through partial/full admission, including Ready-Idle-first combined removal/admission at minimum. Named profiles retain healthy two-member retirement/replacement/planned transfer and [explicit 3/2 repair](MinimalFmCluster.md#post-failover-third-replica-repair) with [repeated cycles](MinimalFmCluster.md#repeated-live-session-primary-failover-and-repair). Repeated/reverse healthy planned transfer uses the latest completed membership, including after qualified repair. Remaining: general multi-loss repair, spare-node placement, broader interrupted recovery/supersession, swap abort, broader planned-transfer profiles and DatalossReport. | Preserve native phase ordering, separate previous/current quorum obligations, catch-up/cancellation, old-primary fencing and endpoint publication. The deprecated ChangeConfigurationReply is rejected, never an RA progress gate. Repair retains original Node/RA/store/Channels and physically retires predecessor owners; it neither adopts lost runtimes nor recovers persisted state. Current source-stream and native-free evidence is not complete original-handler/interoperability qualification. |
| 6. Enable durable RA restart | P5 loading/fixups, crash-window reconciliation, fresh incarnation and recovery with the original FM surviving. | Historical records cannot restore Primary authority. Prove predecessor fencing; rebuild volatile replicas from eligible survivors with valid history/quorum evidence or report data loss. |
| 7. Add FM/FMM ownership and rebuild | Ordinary FM rebuild over accepted reports, independent FM/FMM readiness and generation domains, owner-specific uploads, GFUMTransfer and FM-service replica handling. | Supply matching FM/FMM/common-layer counterparts and required FM-service functionality. Ordinary FM acknowledgments cannot stand in for FMM; this does not authorize transparent FM restart. |
| 8. Finish upgrades and catalog closure | Application/fabric upgrade and cancellation pairs, real Hosting/version adapters, remaining supported profiles and legacy/deprecated behavior. Resolve the PartitionNotification endpoint gap. | Every catalog entry needs its exact implementation or source-defined rejection/no-op behavior and evidence. Do not invent a successful handler to conceal a missing counterpart. |

Every enabled exchange requires bidirectional native codec evidence,
source-derived behavioral scenarios, normalized native/Rust handler traces and
real RA/Hosting/RAP acceptance where applicable. Cover stale, duplicate,
reordered and lost traffic, definite/unknown commits and independent physical
cleanup. Missing required evidence must fail capability admission. Existing
codec/helper and bounded native-free results do not establish full native
handler or interoperability qualification.

Message coverage is not full SF feature parity. Every native execution branch
also needs its backing functionality, including persisted-service storage and
genuine upgrade infrastructure where required; SQLite RA metadata supplies
neither. Until qualified, active unsupported branches remain explicit gates,
not completed contracts. Preserve source-defined legacy rejection/deprecation
and the native ServiceBusy no-op distinctly from unsupported success-shaped
stubs. Federation membership remains excluded, but peer reliability traffic
remains required.

Stage 2 is implemented within those explicit bounds; see
[node-wide coordination](MinimalFmCluster.md#node-wide-coordination-stage-2).
The coordinator includes accepted entries with private creation/close commits,
retains reply capacity through multi-entity completion, binds service jobs to
their owned group and suppresses group success after an updated job's failed
commit. Native 900-second close deadlines and 15-second checks terminate only
captured original hosts, once per monitor; termination is not closure proof.
Status work keeps native five-second minimum starts, 30-second retries,
current-sequence acknowledgments and independent history cleanup.

#### Configurable replica-set sizes plan

**S1-S4d are implemented: minimum-preserving Primary failover, configurable
standalone sizes and explicit replacement on a statically preconfigured spare,
including below-target restoration and two sequential spare cycles, plus a
qualified repeated/reverse healthy 3/2 planned handoff from the latest completed membership.
The 3/2 volatile restart-required path closes/drops a Secondary before native
handoff completion, followed by separately requested static-spare rebuild.
Fixed 3/2 also qualifies application-crash failover/removal after completed
healthy handoff, same-original-Node fresh repair and another handoff.
Healthy fixed-3/2 handoff also qualifies generation/LFUM rollover at its
committed phases without replacing native completion or writer authority.
Follow-on transitions remain separately gated.**
Configurability means choosing service sizes at creation; live resizing is a
separate capability. Do not introduce another size-specific execution profile.

Current shared RA/Node/runtime admission accepts general sizes satisfying
`1 <= minimum <= target <= 64`; 64 is a fube resource bound, not an SF rule.
Both launchers use the same parameterized Hosting/profile contract and shared
size/pool validation. A static pool can contain more Nodes than desired members;
unused placements have empty LFUM inventory and no replica vote.
Standalone requires explicit sizes, compares resolved package
sizes before acquiring its run directory and admits normal active deletion
only for an exact completed current membership. This does not make every
lifecycle branch generic: healthy planned transfer is qualified at 2/1 and 3/2,
and broader LFUM overlaps/repair retain their separately qualified profiles. Configured failover
retains the down former Primary as a CC Secondary when removal would violate
minimum. Shared RA distinguishes that ordinary failover from interrupted
Secondary removal using the committed transition and incoming epoch, not just
the former Primary's role.

Source baseline: Service Fabric
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:

- `ServiceModel/reliability/failover/ServiceDescription.cpp:249-258` permits
  positive stateful minimum no greater than target, including equality.
- `fm/FailoverUnit.cpp:1036-1048` swaps the former Primary to Secondary;
  `fm/ReconfigurationTask.cpp:325-344` removes down CC members only while
  CC exceeds minimum. The failed Primary has no unconditional exemption.
- `ra/FailoverUnitProxy.cpp:1393-1455` checks configured CC size against
  minimum separately from available PC/CC majority. Minimum is not the number
  of acknowledgements required for each write.
- `fm/ReconfigurationTask.cpp:357-418` accounts for Ready Idles during
  removal/admission and desired target separately from current membership.
- `ra/FailoverUnit.cpp:269-297` gives a fresh Idle the creating Primary's
  deactivation history; copy completion is not active membership.

| Increment | Scope and dependencies | Completion gate |
|---|---|---|
| S1. Minimum-preserving Primary failover (implemented) | Shared RA admits a retained down former Primary without confusing it with Secondary-removal supersession. Test FM swaps Primary/Secondary roles before removing down members only above minimum. RA/FM regressions first failed against the old implementation; removal above minimum and interrupted-removal paths remain covered. | Real 3/3 and 4/4 process cases preserve minimum, retain nonempty data and acknowledge a new majority-backed write after final reconfiguration, including a lost final reply and exact replay. Another Secondary loss cannot acknowledge a new operation. RA tests hold survivor/final commits, reject changed identities/invalid retained state or interruption epochs, preserve LFUM/runtime ownership and replay completion without another Open. |
| S2. Configurable standalone sizes (implemented) | Explicit target/minimum feed shared ingress and the parameterized Hosting package contract, also used by the configured local cluster. Resolved package/native descriptions must agree with immutable service sizes. Variable original topologies include a peerless singleton; successors retain the same declared sizes. Active deletion retains exact lifetime/current-epoch/completed-configuration fences, including refusal during unfinished failover. | Standalone processes cover 1/1, 2/1, 3/3, 4/2, 4/3, 4/4 and 5/3 with initial build/admission, lost-reply replay, stable LFUM, Primary failover where applicable, normal deletion/replay and owned shutdown. Local-cluster cases share Hosting validation and extend stable LFUM across configured sizes, retaining existing failover/repair qualification. Missing/invalid sizes and package/native-description mismatches reject before the corresponding work; shared model/package tests include 64/64, not 64 processes. |
| S3. Placement and replacement at minimum (implemented) | Known pool size is independent of target: standalone lists every other preconfigured Node; local cluster accepts `--nodes` / `Cluster::with_node_pool`. Initial membership uses target placements, leaving spares unopened. Explicit FM AddReplica builds a fresh Idle on an unused bound Node with the surviving Primary's compatible history. The existing shared RA removal/admission transition retains the down member in PC, removes it from CC and admits the Ready Idle without changing sizes. | Both launchers qualify 3/3 and 4/4 after either Primary or Secondary app-host loss, with preconfigured spares, stable membership/empty-spare LFUM, lost build/final-reply replay, nonempty copy, unchanged survivor owners and independent cleanup. Another original Secondary loss can still commit only with the replacement's real vote; losing that spare prevents a fresh write. RA models cover rejected/premature/changed-owner intent, fresh Open and survivor/final commit barriers, store veto and frozen LFUM. FM models fence further losses during build/admission. Dynamic joining, automatic placement and new spare-repair LFUM overlaps are not enabled. |
| S3a. Below-target spare restoration (implemented) | After above-minimum Primary failover removes the lost owner, identify its exact processed predecessor from the completed transition. Build on one unused static placement and reuse shared RA's existing unchanged-size Ready-Idle admission. PC contains the surviving target-minus-one membership; CC restores target without adding the removed owner back. | Both launchers qualify 3/2, 4/2, 4/3 and 5/3 with nonempty copy, lost build/final-reply replay, unchanged survivor owners, real replacement votes, loss-of-quorum and owned cleanup. RA models hold each survivor/final commit and reject premature or changed-owner admission; FM models require completed failover/deletion and fence each further loss during build/readmission. Arbitrary partial membership and new spare-repair LFUM overlaps remain gated. |
| S3b. Sequential static-spare cycles (implemented) | Start each explicit cycle from the latest completed membership, archive prior repair/deletion ownership, and exclude only exact processed predecessors removed by completed spare cycles from new-loss accounting. Shared RA distinguishes a completed CC used by a later ordinary failover from explicitly preserved interrupted-removal PC; shared Node forwards bound stale remote intent for native RA handling. | Both launchers qualify two cycles on target-plus-two pools at 3/3, 4/4, 3/2, 4/2, 4/3 and 5/3, including another Primary loss, another original Secondary and loss of the first replacement. Each cycle covers nonempty copy, lost build/final replies, old-request/reply fencing, unchanged survivors and owned cleanup; the second spare supplies a real final quorum vote. FM models fence each further loss; RA model/store tests preserve replay, old PC during interruption, private/failing commits and runtime ownership. More than two cycles are not process-qualified; automatic placement, dynamic joins, old-placement reuse and new spare-repair LFUM overlaps remain outside scope. |
| S4. Healthy 3/2 planned handoff (implemented) | Extend native P/S, S/P choreography with the unchanged S/S member, preserving 2/1. Shared RA validates unchanged exact membership, binds continuation to the former Primary and uses timed PC/CC read quorum plus deactivation-history/highest-LSN selection, without failover-only catch-up-capability redirection. Secondary activation commits metadata without an extra RAP request; all ACKs precede final RAP/FM completion. Shared mesh ingress admits the exact native peer continuation; launchers remain explicit-intent/process owners. | Both launchers qualify nonempty data, lost final reply/replay, old-writer fencing, immutable sizes, unchanged runtime/process owners and the remaining Secondary's actual quorum vote. Standalone covers either original Secondary as candidate and denies a fresh write after both Secondaries close. Shared model/store tests cover unequal/delayed progress, history selection, metadata-only activation with absent/pending/completed GetLSN, malformed identities/source and private/failed/unknown commits. FM records early endpoint updates without publishing readiness. S4a adds sequential handoff; S4b replaces the original restart-required gate with volatile Close/drop and separately requested spare rebuild. S4d adds healthy fixed-3/2 transfer-generation/LFUM overlap. Other sizes, abort and broader overlaps remain separately gated or unqualified. |
| S4a. Repeatable latest-membership handoff (implemented) | Each fresh handoff derives PC, roles, endpoints and exact lifetimes from the latest completed admission, repair or swap, not the original configuration. Retain that baseline during a new GetLSN query; clear prior query ownership when starting a new swap. Bound FM/peer ingress delegates current-role/source/epoch validation to shared RA, including the legacy two-member path. The test FM archives completed repair/swap owners and explicitly selects a current Secondary through `Cluster::transfer_primary_to(one_based_destination)`; repeating the same destination replays the current transition. | Shared model/store tests exercise candidate, former-Primary and unchanged-Secondary roles across repeated epochs, absent/pending/completed GetLSN, failed new-intent commits and stale RAP callbacks/requests. Both launchers qualify 2/1 repeats and 3/2 A-B-A-C with fresh writes after each transfer, lost final replies, stale earlier intent/completion fencing, unchanged owners, old-writer rejection and actual quorum/no-quorum. Local 3/2 cases transfer to and back from a freshly repaired Primary/Secondary placement or spare; standalone transfers after two 3/2 spare cycles and a completed generation round. Membership remains unchanged within each transfer. Abort, restart-required recovery, other sizes and concurrent transfer-generation/LFUM overlaps remain outside this increment. |
| S4b. Volatile Secondary restart-required handoff (implemented) | Replace S4's original restart gate with SF's missing/unknown/uncatchable Ready-Secondary handling. Preserve PC/CC quorum membership, project restart-pending members down to RAP and retry force Deactivate with invalid history. Shared Node/Hosting closes the exact volatile lifetime; committed DeactivateReply marks it down/Dropped without same-handoff rebuild. A subsequent explicit FM build/removal/admission restores target on a static spare. | Both launchers qualify a 3/2 missing-progress handoff with nonempty data, lost native final completion/replay, old-writer fencing, real disposal and unchanged survivors. The explicit spare subsequently supplies an actual quorum vote; losing it denies another fresh ACK. Model/store tests cover unknown/uncatchable progress, former-Primary and unchanged-Secondary Close, identity/history/source fencing, failed/unknown commits and normal metadata deletion. Shared progress-wait configuration retains SF's 30-second default. Persisted restart, same-handoff volatile rebuild, other restart-required sizes, swap abort and concurrent generation/LFUM overlap remain separately gated or unqualified. |
| S4c. Application crash after completed handoff (implemented) | Use the latest completed membership for shared RA failover/progress admission and closed volatile replacement, preserving interrupted-removal PC/history. The thin test FM admits a completed healthy swap as the next failover baseline and retains earlier transition receipts. Native completion remains authoritative. | Both launchers qualify fixed 3/2, either first candidate and loss of the new Primary, former Primary or unchanged Secondary. Explicit fresh-host repair on the same original Node restores target, then another handoff uses the rebuilt identity/history. Cases preserve nonempty writes, replay lost completion, fence earlier replies/Delete, retain Node/RA and survivor owners, prove predecessor cleanup and genuine quorum/no-quorum, and finish owned teardown. Model/store cases cover missing progress, malformed ownership/history and failed/unknown commits. Unfinished handoffs, broader sizes, persisted reopen, concurrent generation/LFUM and Node/FM restart remain outside this increment. |
| S4d. Healthy planned-handoff LFUM overlap (implemented) | Keep shared RA's generic committed inventory; the thin FM freezes the prior successful membership and explicit healthy fixed-3/2 transfer intent. Validate native demotion/continuation, queried Secondary PC, candidate GetLSN/catch-up, activation IC and stable completion views. Bound peer generations are a receive lower bound, not implicit local generation updates. Shared transport ignores stale peer messages without closing the original bound Channel; malformed envelopes/foreign incarnations remain fatal and fixed-FM refusal is unchanged. | Both launchers qualify either first candidate with held public demotion/Primary-role/final-RAP callbacks, missing actual progress, staggered updates, superseding uploads and delayed older ACKs across activation. Upload completion cannot supply progress, any Secondary's matching activation or final RAP/FM completion. Lost final reply replays at the new generation; nonempty writes, old-writer fencing, original owners, actual quorum/no-quorum and owned teardown remain checked. Shared model/store tests cover reverse/latest-membership views and failed/unknown commit barriers. Handoff crashes/abort, other sizes, restart-required/spare-pool overlaps, redirection and durable restart remain separate. |
| Follow-on size-dependent transitions | Migrate remaining named-profile production paths and their tests to shared size/membership invariants as each transition is qualified. General planned transfer, broader test-FM LFUM overlap admission and live service-size updates need their own source-derived gates; S2 does not enable them implicitly. | Fixed sizes may remain test inputs, not general production admission rules. Keep unsupported functionality explicit and preserve native wire contracts. |

Backward compatibility for runner configuration, launcher profile names and
size-specific aliases/defaults is not required for this work. Update dependent
callers and tests rather than adding a legacy configuration path. This does
not relax native SF message or public runtime/data-replicator contracts.

Reliability validation/execution remains in shared RA/Node/Hosting; the FM
supplies explicit membership intent and launchers own transport/processes.
Use public runtime/data-replicator contracts for tests, not engine hooks.
Each increment requires focused shared model/store tests, real-process checks
for its admitted launchers, formatting/linting and updated current-status docs.
This plan does not enable dynamic resizing, restart recovery, arbitrary
multi-loss recovery or native FM/Federation interoperability.

#### LFUM alignment plan

Source baseline: Service Fabric
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. The
[current contract/source map](MinimalFmCluster.md#live-session-lfum-upload) is
the implementation reference; this table tracks remaining work, not release
history. **Repeated cycles, repair-Secondary deactivation with held role/uploads
and cancelled fixed-3/2 original-Node repair copy/build are qualified in both
launchers. Supported removal supersession and broader copy/build interruptions
remain separate work.**

SF queues inventory reads over the owner's entity set, rather than enabling
LFUM one reconfiguration lineage at a time. Keep that generic shared RA path.
Extend the test FM only to validate known identities and admitted intent; do not
move RA policy, reconstruction or writer selection into either launcher.

| Work item | Prerequisite and scope | Completion gate |
|---|---|---|
| Contract closure (alongside A/B) | Preserve generic reads and independent uploads; close remaining common-layer scheduling/closing gaps against SF before claiming complete LFUM parity. Fube uses the configured retry base interval; SF additionally applies configurable additive jitter. | Port/qualify the native retry-delay bounds without resampling the body or weakening request ownership. Test accepted snapshot/upload work during closing and failed/unknown commit barriers. Retain explicit unsupported transport/restart profiles rather than implying native Federation recovery. |
| A. Broader interrupted live-session repair | Both launchers qualify repeated cycles, native deactivation during fresh Secondary activation and cancellation of fixed-3/2 original-Node Primary repair before active admission, during public copy-state enumeration or after a completed build with its FM reply lost. Shared RA admits removal only of a nonmember Idle under completed membership and independent deletion of the exact local Idle; test FM retains cancelled intent and separate removal/deletion/disposal receipts. Remaining: supported removal supersession and broader interruption profiles. | Retain older uploads across interruption/supersession and deliver stale/duplicate/lost replies. Check committed PC/IC/CC, fresh replica identity and frozen retries; no duplicate host activation or old-runtime adoption. RemoveReplicaReply, destination Delete, physical disposal and LFUM ACK retain independent barriers. Preserve survivor data/owners, genuine host cleanup and actual quorum/no-quorum behavior. Cancellation/deactivation never authorizes a successor or restarting cancelled repair. |
| B. Transfer and candidate-redirection overlaps | S4d qualifies healthy fixed-3/2 transfer-generation/LFUM overlap in both launchers, with either original Secondary as first candidate. Shared model/store coverage includes reverse/latest-membership phase views. Broader sizes, repeated/repaired/spare-pool process overlaps, restart-required handoff and candidate redirection remain separate qualifications; inventory support alone does not enable them. | Preserve demotion/continuation, timed GetLSN quorum/history selection, catch-up, every Secondary's metadata activation and final RAP barriers. Completed demotion reports the previous Primary correctly; redirection reverts CC roles to PC. Frozen uploads and stale ACKs remain independently owned; stale peer generations cannot destroy the bound session. ACK cannot finish transfer or make FM's final configuration ready; preserve separate native early-endpoint publication and old-writer fencing. |
| C. Ordinary-FM initial discovery/reporting | Distinct from live-session LFUM. Implement the shared pending-replica-upload owner and full processed/dropped ReplicaUp batching/final-ACK join together with nonempty NodeUpAck entity processing. Use model/store entities first; do not enable deployed restart/reopen by exposing these handlers. | Apply node activation/version/upgrade checks and all owner-entity jobs before node readiness. Process both reply lists successfully before consuming IsLast; stale incarnation or failed/private commit cannot clear the wrong obligation. Retry pending/final reports independently; report ACK is neither physical disposal nor serving authority. |
| D. Recovery and FM ownership (separate tracks) | Stage 6 supplies RA load/fixups, fresh incarnation and predecessor fencing. Stage 7 supplies ordinary FM rebuild and independent FMM/FM-service domains with real counterparts. An FM rebuild over surviving live RAs does not inherently require RA restart; enabling either restart path requires its own recovery qualification. | FM processes accepted reports before ACK and suppresses ACK on processing failure; echoed stale/already-ready replies do not replace current inventory. Preserve owner-specific generations/readiness, valid history/quorum and data-loss handling. Neither LFUM nor GFUM permits saved-Primary resurrection or transparent runtime adoption. |

For each increment, update the source-derived scenario mapping and run the
shared model/store tests plus real-process acceptance for each admitted
launcher/profile. Cover accepted work completing while RA begins closing,
multi-entity barriers, failed/unknown commits, capacity and independent cleanup
where applicable. Use public runtime/data-replicator contracts for application
barriers, not engine-specific hooks. Codec/schema/helper evidence, normalized
original C++ handler traces and native FM/Federation interoperability are
different evidence tiers: close missing native evidence alongside each enabled
contract, rather than treating the final process test as full SF parity.

#### Supported replica choreography

The endpoint helper preserves the native separation from FM message state.
Its timeout and pending/completed/reset behavior now integrate with real RAP
endpoint updates and FM replies in the bounded planned-Primary-transfer path.
Native invariants still require active reconfiguration; ordinary singleton Open
does not fabricate a reconfiguration to publish a fresh endpoint.

The bounded stage-4
[peer Idle profile](MinimalFmCluster.md#peer-created-idle-replica-stage-4-increment)
uses fresh startup and the original fixed FM generation. Primary RA issues
CreateReplica to the exact original peer; destination RA commits before
registration/Open and before publishing the native reply with its real
replication endpoint. Idle remains InBuild and non-serving. The reply records
remote Open only; separate `Cluster::build_idle_replica()` explicitly requests
real RAP build/copy. Its original-session semantic completion proves the
copy/application-ACK barrier, including nonempty state and retry results;
continuing writes are also applied and ACKed on the destination. The Idle stays
non-serving and unconfigured. Manual copy infers no AddReplicaReply. Separate
`Cluster::add_idle_replica()` now sends native FM AddReplica, automatically
continues peer Open into RAP build, and emits AddReplicaReply only after matching
build success. Primary/FM bookkeeping becomes Ready/Idle, while the actual
destination remains Idle/InBuild. Lost replies retry/replay the original
request/result without another Open/build; creation failures require cleanup,
ordinary build errors retain pending retry, and Close/removal fence late success.
No active membership, catch-up quorum or quorum change is inferred from Idle
build. Restart/FMM remain gated; LFUM coverage follows its separate plan above.
Explicit `Cluster::remove_idle_replica()`
now abandons that opened original Idle through
ReplicatorRemoveIdleReplica/Reply, fencing late build completion and retaining
terminal incarnation tombstones. Failed-build retries instead use RAP's
nonterminal cleanup before rebuilding; no successful build implicitly requests
removal. Neither path disposes the destination service or changes membership.
FM `remove_idle_replica_via_fm()` also cancels the original pending creation:
native InCreate removal replies with the original InBuild descriptor and no RAP
call. A late destination Open remains an owned non-serving lifetime, not deletion
or permission to build; if Open wins at the Primary, post-Open RAP removal applies.
An original failed creation now marks FM to-be-dropped/pending-remove while
retaining its Add error. Its original descriptor is echoed by the Primary's
absent-after-abort path without RAP. Semantic completion clears pending-remove,
not the failed Add outcome or the independent destination cleanup obligation.
`delete_idle_replica_via_fm()` now separately authorizes normal deletion of the
already-opened original abandoned destination. Its matching DeleteReplicaReply
and original disposal/committed-Close/exact-usage-release receipt are independent
barriers; only their join completes destination cleanup. Native FM does not wait
for Primary RemoveReplicaReply to schedule destination Delete, and deletion
cannot clear pending-remove. Reply-loss retry/replay, held provider/service
disposal, delayed peer traffic and real copied-data deletion preserve original
ownership and healthy Primary serving. No new wire shape or active membership
is introduced. For the original unanswered FM Add, normal Delete can now precede
destination CreateReplica: the native missing-FT tombstone/reply joins an
independently validated applied-Delete absence sidecar, with zero Open/Close,
lookup or usage history. Completion explicitly says `never_created`, not
`disposed`; late creation is ignored and cannot acquire a lifetime. Lost replies,
commit barriers, independent Primary pending-remove and public original-child
acceptance preserve that distinction. Registered in-flight Open now follows
native Delete/Close cancellation and retries until the original callback and
resources drain; returned controls are aborted before the service, not discarded.
That branch requires genuine disposal proof, never absence, and retains the
independent Primary pending-remove and commit barriers. Controlled provider/service
holds, lost replies, late traffic and original child adapters exercise it.
Pre-registration Opening now follows native immediate Close without runtime
registration, preserving the original 1/1 FT description. Applied creation/Delete
lineage and drained late lookup/acquisition ownership produce a distinct
`pre_open_cancelled` proof, never disposal or zero-history absence. Native
reply/proof joins, held real acquisitions, late registration and public original
child adapters preserve the independent cleanup/removal barriers.
Known pre-Open registration failure now supports normal closed Delete without
RAP Close, joining applied original failure/lookup/creation and separate Delete
lineage with exact drained local ownership. The failed Add/error survives;
`failed_registration` completion is neither runtime disposal, pre-Open
cancellation nor zero-history absence. Controlled real Hosting/process/Channel
acceptance selects the existing native Drop=1 policy; production defaults are
unchanged. Late offers/registration/Create, reply replay and commit/owner
barriers are qualified separately from native C++ handler execution.
Terminal runtime Open failure now joins original-runtime Abort/final-release
reply provenance, Applied failure, exact original usage release and separate
Applied closed Delete without RAP Close. `failed-open-deletion.json` remains
distinct from successful-Close and no-runtime proofs. FM freezes the Add error;
`failed_open` completion retains that failure and independent pending-remove.
Actual provider/service disposal holds, lost-reply replay, late Create/Add and
healthy Primary writes use the existing native dynamic Open Drop=1 policy for
controlled Node/process/Channel qualification, without changing production
defaults.
The original opened Idle's AppHostDown now retains native ReplicaDown stage,
sends the dropped wire entry immediately and retries at the existing 30-second
interval. FM requires its original Add and live destination node session,
returns the processed ACK list and tracks closed metadata Delete separately
from disposal. Pending versus acknowledged Add and independent owner/commit
barriers have model/store coverage; actual two-child acceptance kills the
destination app, holds Primary removal, loses/replays ACK and metadata replies
and verifies healthy Primary writes. Historical Add replies remain retained
without restoring readiness. No cleanup sidecar or replacement is fabricated.
Normal Close started from the original opened Idle now retains its origin
through Closing; AppHostDown can supersede it without converting partial
provider/service cleanup or late Close replies into disposal proof.
Controlled owned-Node/process/Channel acceptance holds real provider/service
cleanup, kills the app and verifies existing report/ACK/metadata and independent
Primary removal/write behavior. Close/loss/ACK commit barriers and both race
outcomes have model/store coverage. No child-node Close-control feature,
wire shape, retry cadence or native handler qualification is added.
Successful normal Close now records a distinct Applied terminal origin and
uses the native down-stage/dropped-list report, processed ACK and closed
metadata deletion without HostDown or a second RAP Close. Actual original
disposal/usage proof remains stable and independently observable before ACK
and after metadata replay; it is not FM retirement/replacement authority in
this terminal-report flow. Held provider/service disposal and the real native
30-second lost-ACK retry have controlled Node/process/Channel coverage, with
separate completion/ACK commit tests and continued Primary writes/removal.
Normal opened-Idle Drop now uses OnDropped/ReplicaDropped stage instead,
with dropped-description ACK eligibility before and after the first retry.
Committed original Close/Drop intent and matching successful RAP completion
remain separate from disposal proof, FM ACK/closed metadata deletion and
Primary pending-remove. Held provider/service disposal, actual 30-second
lost-ACK retry, stable proof/metadata replay and original Primary writes have
controlled Node/process/Channel coverage; intent/completion/ACK commits, both
ACK lists, late relabeling and excluded origins have model/store coverage.
Matching HostDown during the original committed opened-origin Drop now supersedes
it through AppHostDown/ReplicaDown. Late RAP replies cannot certify disposal;
successful Drop committing first retains ReplicaDropped and genuine disposal
proof. Held provider/service process loss, an actual 30-second lost-ACK retry,
metadata replay, independent Primary removal/writes and separate
intent/loss/ACK commit/race barriers qualify this bounded extension.
Normal non-force Abort now uses RAP Abort (`flags=4`) and native
OnDropped/ReplicaDropped reporting after actual cleanup and committed completion.
Node/RA share original opened-peer lineage admission; local-only, initial-Open
and arbitrary closed lifetimes remain rejected. Abort cannot supersede pending
closure merely because registration disappeared or relabel closed report state.
Actual original disposal/accounting proof remains independent of FM ACK/closed
metadata deletion and Primary pending-remove. Held provider/service Abort
disposal, actual lost-ACK retry, stable proof/metadata replay and Primary writes
have controlled Node/process/Channel coverage; admission, both ACK lists and
intent/completion/ACK commit barriers have model/store coverage.
The original-session Delete matrix is now complete within the volatile fixed
profile: normal Delete waits/retries across existing closure, closed Delete is
metadata-only, errors escalate through real Abort, and explicit force uses native
Obliterate/owned-host termination or no-registration/closed completion.
Locally deleted FTs retain message stage None; no synthetic report is left by
Delete-error ForceAbort or loss during normal Delete. Native replies join
original disposal, absence, cancellation, known failure or positive owned-process
loss independently of Primary pending-remove. Actual applied histories survive
later app exit with the same Node owner, never owner adoption; creation races
can supply the actual stronger cleanup fact without rewriting the Add outcome.
Unproved metadata and original owner loss stay incomplete/unknown.
The consolidated model/store and actual process/Channel matrix includes held
cleanup, force supersession, no-runtime force, proof conflicts, commit uncertainty
and reply replay. HostDown during Abort and failed/ForceAbort reporting outside
explicit Delete, broader unknown/foreign identities, broader replacement and recovery
remain separate gates. No wire schema, timer or native handler qualification changes.
This detour does not complete stage 3 or the full stage-4 FM build/removal
choreography.

Stage-two codec evidence contains 101 bidirectional cases, 73 types/233 fields,
105 bundle files and 26 native truncation rejections. Native-derived model/store
and actual native-free process/IPC tests do not supply complete normalized C++
handler traces or native FM/Federation interoperability. Stages 3–8 and those
broader evidence gates remain incomplete.

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

Follow the [FM-call completion roadmap](#fm-call-completion-roadmap) for the
remaining dependency order and the P1–P5/M1–M4 gates for qualification.
This design document itself supplies no runnable cluster commands or evidence
of the unimplemented stages.

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
