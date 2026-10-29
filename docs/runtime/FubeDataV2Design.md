# Fube Data V2: transactional replication design

**Status:** reviewed proposal, not an implemented or qualified V2 feature.
**Date:** 2026-10-29. **Initial target:** Rust-owned volatile transactions over
`fube-data`, packaged as a new `fube-txn` crate.

This document is the complete design and acceptance contract. It does not
authorize implementation or change the support claims of the existing engine.
“Proposed,” “must,” and the milestone requirements below describe future work;
the explicitly cited **current** and **reference** observations describe code.

## 1. Decisions and scope

1. **Keep the lower engine.** Build a Rust transaction/log/state-manager layer
   above `fube-data`; do not replace quorum replication, transport, elections,
   membership, or native SF control.
2. **Make log ingestion the lower provider.** A V1 operation is an opaque
   upper-log record, not an immediately visible collection mutation.
3. **Ship a bounded custom volatile profile first.** Use a custom test provider
   and an explicitly Rust-owned record/copy format. Do not call it native V2
   interoperability or Reliable Collections.
4. **Separate record replication, transaction decisions, and visibility.**
   Quorum completion of one record is not a transaction commit; an ACK is not a
   client commit or, in the volatile profile, a durable write.
5. **Treat copy as a design gap, not an existing adapter.** Preserve the lower
   exact-horizon invariant for the first profile. Native V2 copy negotiation and
   horizon extension require a later, separately qualified contract.
6. **Keep production native-library-free.** Use source-guided Rust algorithms
   and contracts. C++/COM can be a separate oracle, not a production dependency.
7. **Persistence is an end-to-end milestone.** A WAL alone must not enable
   persisted SF manifests or produce a persistence claim.

### 1.1 Independent compatibility tiers

| Tier | Intended claim after its gates pass | Explicitly not implied |
|---|---|---|
| A: Rust volatile transactions | Atomic multi-key/multi-provider transactions among Rust replicas, under native SF authority, bounded in-memory log/copy | Native transactional-log interoperability, restart durability, Reliable Collections |
| B: native V2 interoperability | Precisely qualified native/Rust primary and secondary combinations, upper record bytes, copy and lifecycle behavior for a pinned SF binary | Arbitrary SF releases, managed/native format interchange, disk/API parity |
| C: persistent recovery | Qualified persisted SF lifecycle, durable log/checkpoint/copy publication and recovery/rejoin | Native on-disk compatibility or backup/restore |
| D: providers and compatibility products | Individually qualified collection API/isolation, native or managed serialization, queue semantics, disk import/export or C ABI | Automatic parity from implementing A, B or C |

These are separate claims, not a promise that B must precede all work on C.
The default roadmap is A, then B, then C; a Rust-only persistent profile could
instead follow A, but would need all C gates without inheriting a B claim.

### 1.2 Goals and non-goals

**Goals:** preserve the lower control/transport boundary; define correct
transaction ownership and atomic visibility; bound all retained state; make
promotion and copy safe with in-flight transactions; provide narrow,
claim-specific acceptance gates and a migration path to persistence.

**Not initial goals:** a new consensus algorithm; automatic TStore or
ReliableConcurrentQueue port; production COM/CLR/KTL dependencies; persisted
manifests; TLS; Windows; rolling executable upgrades; cross-release/native disk
compatibility; arbitrary provider code; secondary application reads; distributed
transactions across SF partitions; exactly-once execution.

## 2. Baselines and evidence limits

| Baseline | Identity and authority |
|---|---|
| Fube source | `youyuanwu/fube`, `3e0c334262ce228df6fb2f6f56863c11862f4ae7` (“Centralize Rust CI artifacts and compiler-free test consumers (#13)”) |
| Native reference source | `microsoft/service-fabric`, `51a7703a1f83749a9234d3ceb4296d8622e42cca`; immutable source links below |
| Existing fube qualification | Documented Linux x86-64, SF **11.8.121.4**, volatile **classic V1**, `RequireServiceAck=true`, trusted clear loopback TCP, activated ExeHost/SharedProcess; no V2, persistence, TLS, rolling-upgrade or whole-set restart guarantee ([FubeData.md:3–14][f-profile]) |

Current production engine/runtime/application are documented as independent of
FabricRuntime, FabricCommon, COM and a native replicator
([FubeData.md:27–32][f-native-free]). Native nodes remain the membership, role,
epoch and placement authority in the current design
([FubeData.md:3–8][f-authority]).

**Evidence limit:** the pinned native source is an architectural and algorithmic
reference, not proof of the behavior or build provenance of SF 11.8.121.4 or any
deployed binary. No new oracle, build, test, or cluster execution was performed
for this document. Tier A–D claims remain unqualified. Fube links are relative
to this document and describe the fube baseline above; line numbers can drift
after future changes. Native links are pinned GitHub blobs and need no sibling
checkout.

A bounded single-model GPT-6.1 Sol design review identified three contract gaps;
the release-aware admission, session lifetime and replicated retry-binding
revisions below resolve them. Design review is not runtime qualification.

## 3. Why the layering fits

### 3.1 Current and reference architecture

**Native reference:** `ComTransactionalReplicator` constructs the real V1
replicator, wraps its state-replicator interface and builds the transactional
wrapper above it. `TransactionalReplicator` constructs a state manager and
logging replicator
([ComTransactionalReplicator.cpp:204–269][n-com],
[TransactionalReplicator.cpp:85–120][n-create]).
This is already V2 over V1, not a replacement quorum protocol.

The reference downward seam is `IStateReplicator`: replicate `OperationData`,
obtain the lower LSN and asynchronous completion, get copy/replication streams
and maximum message size. The reverse `IStateProvider` supplies epoch/data-loss,
copy and last-committed callbacks
([IStateReplicator.h:12–29][n-state-replicator],
[IStateProvider.h:12–68][n-state-provider]).

**Current fube:** `OperationData` owns and preserves byte segments; provider
callbacks and explicit-ACK streams are separate from admission/quorum waiters
([api.rs:77–170][f-data-provider],
[api.rs:173–267][f-stream],
[engine.rs:569–660][f-submit]). The runtime service returns an owned custom
`Replicator`, whose control methods cover role, epoch, configuration, catch-up,
build and lifetime rather than application storage
([stateful_api.rs:128–203][f-runtime]).

### 3.2 Proposed component ownership

```text
native SF node: membership / role / epoch / reconfiguration / access authority
          |
 fube-runtime service + TxnReplicator lifecycle wrapper
          |                             |
          | delegates control           | gates application readiness
          v                             v
 fube-data Engine <---- fube-txn log adapter / stream drains
          |                    |
     Fabric TCP          log + transaction manager
     peer replicas       visibility coordinator / state manager
                               |
                     Rust TxnStateProvider implementations
                     (first: bounded custom test map)
```

| Proposed owner | Responsibilities | Must not own |
|---|---|---|
| `fube-data` | V1 admission/LSNs, configuration quorums, TCP peers, lower copy/replication, lower epoch reconciliation | Transaction semantics, registry or collection visibility |
| `fube-txn` log adapter | Lower `StateProvider`, owned primary admission, stream ingestion/ACK, original replay provenance, log-tail progress | Elections, fabricated quorum knowledge |
| Transaction/log manager | Record codec, transaction table, ordered decisions, bounded log, promotion cleanup, stable/checkpoint tracking | Native control authority |
| State manager | Transactional provider name/ID/type/parent registry, factory validation, routing, copy/checkpoint coordination | Collection-specific locks or serialization parity |
| Visibility coordinator | One atomic root across provider roots and registry, commit serialization, read snapshots and publication | Independent membership or epoch decisions |
| Upper provider | Private prepare/apply, immutable versions, checkpoint/copy state, context unlock | Lower transport ACK policy |
| Runtime wrapper | Orchestrate lower and upper lifecycle, readiness/access/incarnation fencing, supervision, abort ownership | Bypass lower build/configuration/catch-up |

The names and crate split are proposals, not existing APIs.

## 4. Lower seam and progress contract

### 4.1 Exact mapping

| Native reference concept | Proposed fube mapping | Required interpretation |
|---|---|---|
| `ReplicateAsync(data, out LSN)` | Epoch-fenced `Engine::submit_in_epoch`, followed by owned local log ingestion and a separate `Replication::wait` | Admission/LSN assignment is not quorum completion |
| `GetCopyStream` / `GetReplicationStream` | Existing engine stream accessors | One supervised drain per stream; preserve order and original data |
| Lower `GetLastCommittedSequenceNumber` | Adapter `last_committed` returns its reconciled accepted **log tail** | Never return the last visible transaction's commit LSN instead |
| Lower `UpdateEpoch(previous-last)` | Quiesce admissions, reconcile log and transaction state to canonical boundary, install epoch marker | Preserve native boundary; no invented lower sequence |
| Lower `GetCopyState` | Bounded exact-horizon snapshot-plus-log stream in A | Not native staged-copy compatibility |
| Maximum message size | Enforce engine operation bytes/segments before admission | Upper framing and routing metadata consume the same budget |
| Data loss | Fence all access and invoke an explicit upper reconciliation/rebuild policy | Never restore readiness on an unverified empty image |

Current fube has separate stream accessors, epoch-fenced submission and
owner-assigned sequences
([engine.rs:516–522][f-stream-access],
[engine.rs:569–660][f-submit],
[engine.rs:1258–1295][f-owner-submit]). Primary local application must precede
quorum waiting; the sample uses an owned admission future
([lib.rs:13–39][f-order],
[sample data.rs:258–317][f-owned-sample]).
**Here that application is log ingestion only.**

The reference API named `GetLastCommittedSequenceNumber` actually returns
`CurrentLogTailLsn`; `GetLastStableSequenceNumber` is separate
([LoggingReplicatorImpl.cpp:435–444][n-frontiers]).
Reference replication initiates lower replication, then locally appends, and
adopts the returned lower LSN
([ReplicatedLogManager.cpp:382–395][n-replicate-log],
[ReplicatedLogManager.cpp:915–957][n-assigned]).
These observations support the mapping; they do not prove the adapter exists.

### 4.2 Distinct frontiers, not one “committed LSN”

Proposed metrics and invariants:

| Name | Meaning |
|---|---|
| Assigned | Primary obtained a lower LSN for an admitted record |
| Received | Secondary received a lower operation; it need not be accepted yet |
| Log-accepted | Complete validated record and replay provenance owned by the upper log; contiguous lower prefix |
| Flushed | Accepted log crossed the profile's flush boundary: memory acceptance in A, durable storage synchronization in C |
| Record-quorum | Lower waiter proves this record's required configuration quorum; primary knowledge, not inferred from secondary ACK |
| Prefix-applied | Accepted end decisions through a record prefix have been evaluated into a private/versioned state image |
| Published transaction | All effects of a committed transaction became visible atomically; associated with its end-record LSN, not every operation LSN |
| Stable | Upper low-watermark justified by reconciliation/checkpoint policy; not merely the latest visible commit |
| Checkpoint / installed-copy | Completed image plus required transaction/history metadata, with a specific covered prefix and installation generation |

Track transaction ID, begin/operation/end positions and original epoch separately
from these replica-local frontiers. There is no single total ordering of all
frontiers across replicas. Primary visibility requires both successful local
preparation and end-record quorum. A secondary may privately prefix-apply an end
without knowing whether the primary returned success; it must not claim quorum
or serve application reads on that basis.

Current fube already distinguishes assigned, received, applied, committed and
completion observations
([api.rs:269–282][f-progress],
[lib.rs:21–30][f-progress-note]).
The lower names are not to be silently relabeled as transaction metrics.

### 4.3 Record taxonomy and initial codec

**Proposed A format:** a versioned, length-delimited, segment-preserving envelope
with record kind, transaction identity, operation ordinal, provider ID,
payload/metadata lengths, originating epoch and integrity digest. A replicated
record's authoritative LSN comes from lower admission/operation metadata, not
from guessing it before serialization. Retain the original bytes and segments
for replay even if decoded structures are cached. Reject unknown versions,
invalid lengths, duplicate ordinals with different content and illegal
transaction transitions before ACK.

A has begin, operation, commit-end, abort-end and replicated barrier records.
Provider add/remove are transactional operations addressed to the state manager.
Epoch-boundary markers and local checkpoints/indexes are local metadata, not
additional opaque submits. A omits single-operation fast paths and record
batching initially. IDs combine replica-incarnation identity and a monotonic
counter; they cannot be reused within retained/copyable history.

Begin metadata also replicates the application request identity and canonical
**full-request** digest bound to that transaction ID. This is separate from the
per-record integrity digest. Canonicalization covers operation order, provider
targets, values, registry actions and explicit request preconditions/options;
it excludes assigned LSNs and later execution results. End metadata identifies
the same transaction/binding. A successor reconstructs pending bindings and
decisions from canonical begin/end history, not just the old primary's local
response ledger.

**Reference, not an A wire specification:** native logical kinds include
begin/operation/end/barrier/update-epoch/backup, while index, checkpoint,
truncation and information kinds are physical
([LogRecordType.h:18–46][n-kinds]).
An epoch marker can use the previous epoch's last LSN; native empty-log
bootstrap uses both zero and one
([LoggingReplicatorImpl.cpp:1340–1417][n-epoch],
[LogManager.cpp:286–330][n-bootstrap]).
Therefore neither transactions, physical records nor epoch changes receive
fabricated fresh engine LSNs. A uses the lower engine's returned nonzero
sequences with an empty-prefix sentinel zero; this is **not** native log
bootstrap compatibility. B must qualify its own bootstrap mapping.

## 5. Transaction, visibility and failure semantics

### 5.1 Initial transaction API and concurrency model (proposal)

A exposes begin, point read, put/delete, provider add/remove, commit, abort and
outcome-query concepts. These are Rust API concepts, not C ABI signatures.
The first provider is a bounded versioned map; no ranges, queues, external side
effects or arbitrary user callbacks are supported.

- A transaction captures one immutable coordinator root containing **both**
  registry and all provider roots. Reads use that root plus its own write set.
- Writes remain private. Commit validates all point read/write versions and
  relevant registry generations against the latest published root, under one
  partition commit sequencer. Conflicts are rejected before log admission.
  Validating the read set as well as writes prevents write skew in this
  restricted point-operation model. Read-only transactions use one pinned root.
- Initially serialize complete write sessions. One owned exclusive
  **write-session lease** is acquired before final validation/reservations and
  held across all awaits through publication, outcome finalization and context
  unlock, or fenced failure handoff/cleanup. It is a logical scheduling lease,
  not an engine/transport mutex. Concurrent clients may stage bounded work and
  read snapshots, but no next writer validates against the published root until
  its predecessor finalizes. Failure closes readiness before another writer can
  enter. This sacrifices throughput for a simpler correctness and
  termination-headroom proof.
- Lock acquisition order is coordinator, then providers by numeric ID, then
  keys by canonical byte order. No engine/transport lock is held across provider
  work or awaits. Provider preparation uses bounded immutable/private state;
  it cannot synchronously re-enter the commit sequencer.
- The final visibility step swaps **one** coordinator root. Iterating provider
  callbacks and publishing each provider separately is forbidden. Transactional
  registry changes participate in the same root.
- Provider prepare returns owned contexts. Apply and unlock each execute at
  most once for a given replay/application generation, with deterministic
  generation-aware replay checks. Unlock releases contexts/locks, not the
  transaction's correctness history or deduplication evidence.

This is a custom isolation contract, not a claim about TStore isolation.
Reference `IStateProvider2` has transactional apply/context/unlock and
checkpoint/copy lifecycle methods distinct from the lower provider
([IStateProvider2.h:64–155][n-provider2]). Reference committed-end processing
applies transaction constituents and retains contexts for a separate unlock
([OperationProcessor.cpp:768–920][n-apply],
[OperationProcessor.cpp:455–530][n-unlock]); these mechanics alone do not prove
atomic visibility for an arbitrary provider.

### 5.2 Primary commit flow (proposal)

1. Acquire the write-session lease. Validate authority, access status, role,
   epoch, incarnation and upper readiness, and satisfy the clean lower-capacity
   gate in §8. Validate the full staged transaction and reserve its bounded
   log/provider/context/terminal and retry-binding resources before admitting
   begin.
2. Transfer the commit attempt to an owned replica task. For each begin and
   operation record: submit in the captured epoch, ingest locally using its
   assigned LSN, then await both its quorum completion and actual lower release
   before the next record admission. Release is the distinct gate in §8, not an
   inference from the waiter. The one-record-at-a-time lane never waits for a
   transaction end before allowing an operation to be ACKed.
3. Prepare the complete private provider/registry version batch. Preparation
   failure before an end decision requests an owned abort-end while authority
   permits it. If abort cannot be replicated, leave the attempt unresolved and
   fence/fault; never report that an admitted transaction was safely undone.
4. Submit commit-end, ingest it locally, complete private prefix application
   and await end-record quorum. The initial lane has already obtained earlier
   record quorums. Both successful local application **and** successful end
   replication are required.
5. Serialize the final incarnation/epoch/access check and root swap with
   lifecycle invalidation under the coordinator's publication gate. Publish the
   entire root, finalize the outcome, unlock all contexts, then release the
   write-session lease and return success. No provider await runs under the
   short publication gate. Readers may see the complete root before unlock and
   before the caller receives success. Unlock failure yields unknown/fault and
   never rolls back an already published canonical commit. A fence crossing
   after end admission likewise yields an unknown client outcome; it cannot
   erase the replicated decision. The canonical successor may complete it.

End-record quorum suffices for the replication part of client commit success;
the end record need not be released before responding. The next session must
pass the clean-capacity gate, so it cannot consume retained history left by this
one. Promotion aborts/barriers use the same release-aware admission lane.

A proposed commit success includes completed visibility and unlock on the
responding primary, not survival of total volatile replica-set loss.
Reference primary processing awaits record apply and replication and then
unlocks
([TransactionManager.cpp:328–407][n-transaction],
[TransactionManager.cpp:410–516][n-commit]).
A intentionally keeps prepared effects private until publication.

### 5.3 Secondary drain and ACK flow (proposal)

1. Consume copy installation before ordinary replication publication. Validate
   stream generation, lower LSN/epoch provenance, envelope and transaction chain.
2. Charge and take ownership of record bytes in the upper log. Cross the
   profile flush boundary. Advance the adapter's accepted log tail.
3. ACK log acceptance and promptly dispose the operation and ACK capture.
   Begin/operation ACK must **not** wait for final transaction completion.
4. Process end decisions in log order into versioned provider/registry roots.
   For an abort, discard private state without publishing partial effects.
   For a commit, apply all constituents as a unit and release contexts.
   Secondary root publication is internal; A serves no secondary reads.

A fixes its secondary ACK policy at bounded memory-log acceptance. C changes
that boundary to durable flush. Optional native commit-apply-before-ACK behavior
belongs to B's negotiated/qualified profile, not an undocumented A option.

**Why ACK ownership matters:** current fube has a shared default limit of 64
provider operations; the operation ACK closure holds a permit and data charge
until capture disposal
([engine.rs:374–406][f-capture-init],
[engine.rs:2691–2764][f-captures]).
Retaining every operation until commit can prevent a transaction's end record
from arriving—even if early operations have been ACKed but their captures have
not been released. Copy bytes into charged upper ownership, ACK, and drop the
lower capture; never use capture lifetime as transaction-log storage.

The native reference processes flushed records and ACKs secondary records after
flush; commit records additionally await apply before ACK only when
`EnableSecondaryCommitApplyAcknowledgement` is enabled
([OperationProcessor.cpp:333–356][n-flushed],
[SecondaryDrainManager.cpp:805–889][n-drain],
[SecondaryDrainManager.cpp:908–970][n-ack]).
Its nonpersisted profile selects `MemoryLogManager`
([LoggingReplicatorImpl.cpp:159–168][n-memory]).
Neither reference memory flush nor proposed A ACK means disk durability.

### 5.4 Outcomes, retry and fault contract (proposal)

| Observed result | Client meaning |
|---|---|
| Rejected before begin admission | No replicated transaction attempt was accepted; retry after resolving validation/pressure/authority |
| Committed response | Complete transaction visible on responding primary, with end quorum and local completion |
| Confirmed abort | Canonical abort decision completed; no user effects visible |
| Disconnect, timeout, cancellation, lost authority or post-admission error | **Unknown**; may later commit or abort |
| Outcome absent after retention expiry, rebuild or total volatile loss | Unknown, not evidence of abort |

Dropping a current lower `Replication::wait` does not undo admission
([engine.rs:140–159][f-wait]); current lower quorum completion is independent of
provider local application
([engine.rs:3505–3564][f-quorum]).
The upper owner must therefore outlive the requesting socket/future, complete
or fence its work, and retain all admitted-record ownership.

Reserve and install an in-flight request-identity/digest binding under the
coordinator before first admission. Matching concurrent duplicates attach to or
query that owned attempt; a different digest conflicts. Replicate the binding in
begin metadata (§4.3), retain it with pending chains and reconstruct its decision
from canonical end history. Copy both pending and final bindings; reconcile away
noncanonical suffix bindings along with their records. A canonical committed
decision is not evidence that the old primary returned a response.

Request identities include the installed dataset-generation token. A keeps
accepted bindings/outcomes without online expiry inside its charged, finite
ledger; ledger exhaustion rejects new attempts before begin rather than
silently forgetting deduplication evidence. Explicit pre-admission rejections
can release their never-replicated binding. Copy/recovery preserve the dataset
generation; an explicitly new empty volatile dataset has a new token. Query of
an absent identity, an old generation or unavailable history returns “outcome
unavailable,” never “aborted.” A retry operation cannot create a fresh attempt
on a missing binding; new execution is a distinct API operation with a fresh
identity and caller acknowledgment of any prior unknown outcome. Post-expiry
retry policy is deferred until a later ledger-retention design; A has no expiry.
These bounds do not promise exactly-once execution. External effects require
application-level idempotency or a separately designed outbox; they are outside A.

Before admission, capacity/validation/conflict errors are ordinary rejections.
After admission, malformed records, provider panic, irreconcilable history,
partial provider application, unlock failure or supervised timeout must close
read/write readiness and fault the replica. Never skip a failed record and
advance a frontier. If a durable/replicated end decision exists, recovery must
honor it even when the original caller saw an error. Rebuild the custom provider
from a valid image/log rather than attempting an unproven compensating callback.

## 6. Provider registry and lifecycle

### 6.1 Upper provider contract (proposal)

Call the new abstraction `TxnStateProvider`, not the existing lower
`StateProvider`. It includes private transactional prepare/apply and context
unlock; checkpoint prepare/perform/complete/recover; staged copy
begin/set/end; abort/discard installation; and role/lifetime notifications.
A implements volatile snapshots and the custom provider only. Durable recovery
and backup/restore operations must return explicit unsupported status until
their tier is implemented, never silent success.

The state manager owns names, stable provider IDs, registered type/version
factories and parent/child relationships. Creation/removal and registry-root
changes are transactional; IDs are not reused while any history, snapshot or
reader can refer to them. Validate factory availability, child graph acyclicity
and deterministic child ordering before admission. Copy registry metadata before
routing provider state. Unknown type/version during copy faults the staged image
without changing the serving root. Concurrent removal cannot invalidate a
pinned read/copy root; reclamation waits for pins and safe truncation.

The reference state manager unwraps provider-ID envelopes for routing, with
distinct native and managed segmented metadata formats
([NamedOperationData.h:12–44][n-named],
[StateManager.cpp:1290–1344][n-routing]).
TStore is a separate `IStateProvider2` implementation with its own initialization,
checkpoint and transactional apply paths
([Store.h:25–43][n-store],
[Store.h:1536–1555][n-store-init],
[Store.h:1799–1818][n-store-checkpoint],
[Store.h:2251–2280][n-store-apply]).
Implementing a transactional replicator supplies neither those formats nor those
collection semantics.

### 6.2 Open, role, epoch and promotion (proposal)

Use `Engine::bind_partition`, not standalone `bind`, for hosted replicas.
Current `bind_partition` installs partition authority and provider supervision;
standalone binding leaves blocking callback/destructor fencing to its caller
([engine.rs:313–354][f-bind]). Preserve native status/access checks, cancellation,
watchdogs, capture disposal and incarnation fencing; cancellation alone does
not establish quiescence
([stateful_api.rs:160–203][f-runtime-control]).

Proposed wrapper state progression:

```text
Closed -> Opening -> LowerOpen/UpperInitializing -> SecondaryInstalling
       -> SecondaryCaughtUp -> PrimaryReconciling -> PrimaryReady
any live state -> Closing/Faulted -> owned drain or fenced abort -> Closed
```

Delegate lower configuration/catch-up/build work and preserve its ordering.
Upper readiness is a conjunction: current native authority and access, valid
incarnation/epoch, lower ready, installed registry/provider image, drained
required log prefix, and completed upper reconciliation. A Primary role flag or
`Engine` readiness alone is insufficient.

On promotion: stop new writes; drain old work under ownership; reconcile to
native `previous_epoch_last`; retain/apply complete canonical end decisions;
abort begin/operation chains with no canonical end under the new authority;
replicate a promotion barrier and await its quorum/local completion; then open
application readiness. If the boundary cannot be reconstructed from retained
history, fail readiness and rebuild—do not guess a committed boundary from the
largest transaction ID or visible state.

Epoch changes record a boundary marker at the given previous-last position.
Drop only noncanonical suffixes, rebuilding provider/registry versions and
outcome ledger consistently. Old-incarnation tasks and ACKs cannot publish,
unlock a new generation's contexts or resurrect readiness.

The reference opens V1 before upper V2, and closes/changes role through V1
before the upper layer
([ComTransactionalReplicator.cpp:654–711][n-open],
[735–799][n-close], [830–885][n-role]).
Its promotion path drains/quiesces work, gathers pending transactions and starts
abort/barrier processing
([LoggingReplicatorImpl.cpp:989–1072][n-promotion]).
These are useful ordering evidence, not a reason to bypass fube's own lifetime
and authority contract. Close stops admissions and completes/fences upper
owners before disposal; abort owns cleanup of private contexts and staged copy,
and must not return “cancelled” while unsafe callbacks remain unsupervised.

## 7. Copy and reconciliation

### 7.1 Existing gap

Current fube waits for provider progress at a fixed horizon, then requests
`copy_state(horizon, None)`; its volatile path ignores persisted copy-context
messages
([engine.rs:3632–3652][f-copy],
[engine.rs:2552–2553][f-copy-context]).
The provider contract requires coherent state/history **exactly** at `up_to`
and authentic original epoch/payload/segment provenance for optional
`replication_checkpoint`
([api.rs:77–170][f-data-provider]).
The existing documented copy protocol uses a fixed horizon and a subsequent
explicit replication finish fence
([FubeData.md:131–136][f-copy-doc]).

The native reference allows null context for volatile state, but otherwise builds
progress-vector/head/tail context and staged state/log copy
([CopyStream.cpp:165–215][n-copy-null],
[LoggingReplicatorImpl.cpp:1603–1649][n-copy-factory],
[CopyStream.cpp:249–325][n-copy-select],
[409–507][n-copy-batch], [CopyStage.h:17–46][n-copy-stages]).
After state copy it can **increase** the effective `uptoLsn` to a checkpoint's
stable boundary; metadata carries multiple copy/log boundaries
([CopyStream.cpp:328–348][n-copy-extend],
[380–406][n-copy-scan], [537–552][n-copy-meta]).
Its epoch path explicitly recognizes copying beyond V1's requested horizon
([LoggingReplicatorImpl.cpp:1385–1405][n-copy-epoch]).
Therefore native copy bytes cannot simply be returned by fube's existing
exact-horizon provider.

### 7.2 Tier A exact-horizon image (proposal)

At lower requested horizon `H`, capture one immutable **log-prefix** image,
not the source's latest externally visible collection map. The image includes:

- Format/version, replica/copy generation, original epoch boundaries, exact `H`,
  covered prefix and integrity manifest.
- Provider registry, names/IDs/types/children, provider roots and version metadata
  needed to reconstruct the prefix through `H`.
- Begin/operation chains still pending at `H`, including transactions whose end
  will arrive after `H`; include payloads/ordinals and provider references, not
  just a list of transaction IDs.
- End decisions through `H`, pending request-identity/full-request-digest
  bindings, dataset generation, final replay/deduplication outcomes and retained
  record history/reconciliation base. Retain authentic original lower operations
  for the replay checkpoints the adapter offers.
- Prefix-application/publication information sufficient to distinguish a locally
  accepted end decision from a client-confirmed publication.

A source may have accepted commit-end at or before `H` without yet returning
success. Build a deterministic prefix-applied image containing that decision,
while keeping the source's client-visible root separately quorum-gated.
The target may install this private prefix image but serves no reads until
canonical reconciliation/role readiness. A later canonical boundary before that
end requires rebuilding from a retained base plus original records; the copied
“committed” map alone is not enough to undo it. Conversely, an end at `H+1`
must find its complete pending chain in the image. This avoids both copying
future state and losing transactions straddling copy.

Proposed copy sequence:

1. Reserve finite copy/version/history pins before capture. Materialize off the
   owner/transport locks; return within the lower callback supervision budget or
   fail the build attempt explicitly.
2. Emit a versioned manifest, registry, provider snapshot chunks, pending chains,
   required history/outcomes and a terminal manifest digest for exactly `H`.
   Chunk sizes obey lower byte and segment accounting.
3. Target builds a private installation generation. ACK intermediate chunks only
   after charged staging ownership; validate completeness and type factories.
   The final chunk is ACKed only after a complete root/registry/log-tail install
   at `H`. An aborted/missing terminal manifest never publishes staged state.
4. Resume replication at the lower continuation boundary. Validate exact
   duplicates against original provenance; divergent duplicates fault. Catch-up
   and the explicit finish fence must include successful upper installation and
   required prefix application before the wrapper reports build/read readiness.
5. Release pins and reclaim abandoned generations only after owned stream/
   callback disposal. Concurrent source writes use later roots; they cannot
   change the captured image.

**Integration decision:** prefer the upper adapter for the bounded custom
profile. Its terminal-chunk ACK and installed-log-tail publication are the
proposed bridge to existing lower copy progress. Milestone A1 must demonstrate
that lower build/catch-up cannot overtake that ACK/install boundary, including
empty images and cancellation. If it cannot, add a narrowly specified
`copy-install-completed(generation, H)` lower/upper fence before A acceptance;
do not weaken exact-horizon semantics or claim the existing API already solves
this. This is a fixed acceptance condition with a safe fallback, not permission
to ship without an installation fence.

Finite pins can reject/retry a copy build under pressure. No snapshot may retain
unbounded history or silently substitute a newer horizon when an old `H` is no
longer reconstructible.

### 7.3 Tier B copy contract work (deferred milestone, mandatory before claim)

B must specify and qualify progress-vector context, no-op/partial/full/
false-progress selection, native state/log stage formats, effective-horizon
negotiation, overlapping replication deduplication and terminal install/drain
fences. The reference progress vector selects those copy modes; false-progress
truncation invokes upper undo and transaction-map reconciliation, not merely
lower queue truncation
([ProgressVector.cpp:92–257][n-vector],
[TruncateTailManager.cpp:134–365][n-truncate]).

The decision rule is explicit: an upper adapter is sufficient only if it can
translate every supported native extended boundary into a verified lower build
and replication fence without violating exact-horizon users. Otherwise introduce
an opt-in negotiated copy-session API carrying requested/effective horizons,
installation generation and completion boundary. Existing V1 callers retain
their invariant. Unknown native copy modes/versions are rejected. No B release
may assume that EOS, an ACKed byte stream, or lower log-tail readiness alone
means upper provider installation completed.

## 8. Boundedness, truncation and termination headroom

Current engine defaults are 1 MiB / 1024 segments per operation, 1024 retained
operations / 16 MiB, 64 provider operations and an 8-second callback timeout
([api.rs:12–74][f-settings]).
Its transport classifies replicated opaque payloads as **data**, not control
([transport.rs:429–486][f-transport],
[transport.rs:589–603][f-transport-class]).
Therefore existing transport control reservations do not reserve commit/abort/
barrier capacity inside an upper transaction log.

Quorum completion and queue release are also distinct **current** behaviors:
waiters resolve at quorum, while release additionally depends on current/
previous `all_received` and copy/idle-build retention owners
([engine.rs:3505–3564][f-quorum]). The observable snapshot exposes progress,
owned operations and owned bytes
([engine.rs:77–90][f-snapshot-fields], [524–535][f-snapshot]).
Admission still makes its own authoritative resource reservation, including
outgoing-copy headroom
([engine.rs:569–646][f-admission-budget]); a snapshot is not a reservation.

**Proposed initial A bounds:** at most 16 staged transactions; 256 point
operations and 1 MiB encoded logical data per transaction; one active replicated
write session; a 64 MiB total upper owned-data allowance with subordinate caps
of 16 MiB staged transaction data, 32 MiB retained log/history, 16 MiB provider
versions, 16 MiB copy staging/pins and 1 MiB outcome ledger. Subcaps overlap
under the global cap; they are not additive allowances. Allocation/descriptors,
contexts and metadata must be charged, not just payload bytes. Engine-owned
memory remains separately charged by the engine; do not hide upper copies there.
These are proposed conservative starting limits, not measurements or throughput
promises. Registration and every copy chunk must also fit configured bounds.

Termination rule:

- Before begin, reserve upper space for the entire transaction's log chain,
  provider prepare result, one terminal decision and cleanup/barrier metadata.
  Reject before admission if any reservation fails.
- A initially requires the default lower operation/queue capacities (1 MiB,
  1024 segments, 1024 queue entries, 16 MiB). Validate that one maximum charged
  upper record **plus** the engine's maximum-copy byte reserve fits the queue,
  with at least two operation slots (one admission and one copy reservation).
  Envelope/descriptors count against that maximum. The current copy-reserve
  calculation includes segment descriptors and fixed overhead
  ([engine.rs:216–221][f-copy-reserve]); payload size alone is not sufficient.
- **Clean-capacity gate:** before a new replicated session, require the
  current-incarnation snapshot's `progress.completed >= progress.assigned`,
  no prior outstanding upper submit, and sufficient observed owned-byte/slot
  headroom for the next maximum record plus copy reserve. Only the lease owner
  can produce upper records. Copy/lifecycle tasks retain their own accounting.
- **Per-record release gate:** after local ingestion and quorum for record `s`,
  wait until a current-incarnation snapshot has `progress.completed >= s`;
  recheck owned-byte/slot headroom before the next admission. The current engine
  releases entries/charges separately at this frontier, not when it resolves
  quorum waiters ([engine.rs:3537–3564][f-release]). Use bounded owned waiting,
  never a busy loop, and stop on authority/incarnation change or deadline.
  Final `submit_in_epoch` is authoritative: a racing copy charge can still
  reject with `ResourceLimit`. Retry only the unadmitted record under bounded
  owned waiting; never advance the chain or fabricate its LSN on rejection.
- Staged transactions cannot incrementally fill the lower retained queue:
  no next replicated record is admitted while its predecessor remains retained.
  A healthy quorum with a lagging member or an unadopted build can nevertheless
  prevent release. A deliberately sacrifices progress in that case: before
  begin, reject/defer; after admission, report unknown and fence/fault on the
  bounded deadline. Do not equate this with quorum loss or advertise full
  quorum-only write availability. Upper headroom does not solve retained lower
  pins, and the adapter cannot evict native-selected members itself.
- Stop new sessions before pressure consumes lifecycle/copy and terminal
  reserves. Bound pending submit retries and preserve one owned terminal record
  rather than a retry queue.
- Commit/abort/barriers have priority over new begin/operation work. Reservations
  remain inside the total budget. Lower flow-control and transport accounting
  are always enforced; reserves are not a bypass.
- Network failure can prevent any terminal record from reaching quorum. Return
  unknown/fence/fault on deadline; headroom guarantees bounded ownership and no
  **self-inflicted** capacity deadlock, not availability during partition.
- Before enabling pipelining/interleaved transaction replication, add proven
  lower admission reservations or an equivalent verified credit scheduler across
  engine operation/byte limits and copy/transport charges. An upper semaphore
  alone is insufficient. That optimization is not part of initial A.

The reference throttles begin/operation records, not all logical kinds
([LogicalLogRecord.cpp:58–64][n-throttle]); that is the rationale for explicit
upper terminal headroom, not proof the fube engine already has it.

**Truncation rule (proposal):** reclaim only a contiguous prefix that a completed
image can reconstruct and that is older than every pending-transaction begin,
canonical reconciliation base, active read/version pin, copy pin, required
original replay checkpoint and retained outcome dependency. Use a typed
low-watermark computation; stable, visible commit and checkpoint positions are
not interchangeable. A must stop new work when no safe prefix can be reclaimed,
not expire a pending transaction's history to make room. Long reads/transactions
have bounded leases; expiry releases private work or starts owned abort/fencing,
never silently removes a replicated decision. Failed/cancelled copy releases
pins after disposal, not on caller timeout alone.

Expose assigned/log-accepted/quorum/prefix-applied/published/stable/checkpoint
positions; oldest pin and pending begin; bytes/records by budget; capture count;
copy generation; authority/incarnation; and unknown outcomes. These are proposed
diagnostic requirements for acceptance and fault analysis.

## 9. Persistent roadmap

**Current:** persisted manifests and openings are explicitly rejected
([packages.rs:432–438][f-persisted-package],
[stateful.rs:504–523][f-persisted-open]).
Do not remove those checks merely because a file log is added.

**C design contract (future):**

1. Define new versus existing replica recovery, local identity/incarnation and
   SF lifecycle behavior. Restore registry, providers, pending transactions,
   decisions, original replay metadata and progress history before readiness.
2. Define local durable flush and remote durable-ACK rules. Commit success must
   include the chosen durability profile's local log sync, required remote
   durable quorum and successful local apply/publication. No volatile memory
   ACK may be counted as persistent evidence.
3. Implement a versioned checksummed log with length framing, torn-tail
   detection, idempotent replay and explicit corruption failure. Validate
   transactions across checkpoint boundaries. Do not silently skip corruption
   or invent an abort to regain service.
4. Make checkpoint and staged-copy publication atomic across manifest, provider
   files, registry, pending history and covered LSN. Sync data, publish with
   atomic filesystem operations and sync parent directories; recover cleanly
   from every interruption before/after publication. A renamed file without
   the necessary directory persistence is not the whole protocol.
5. Implement progress-vector rejoin/context, partial/full/false-progress copy,
   transaction-aware undo/rebuild, log-tail truncation and retention fences for
   pending transactions, readers and copy. A committed transaction is not
   necessarily stable enough for truncation.
6. Enable persisted SF package/open paths only after crash/restart/rejoin and
   lifecycle gates pass on a pinned environment. Specify disk-format migration
   separately; initially use a Rust-owned format and reject unknown versions.

Backup/restore, incremental backup chains, native disk import/export and
application restore policy are later separate products. None is implied by C.

## 10. Milestones and acceptance gates

These are proposed future verification activities, **not executions or results
from this design pass**. No tier can borrow a lower tier's qualification label.

| Milestone | Work boundary | Required evidence before acceptance |
|---|---|---|
| A0: model and custom provider | Record/state transitions, immutable root, registry, owned commit, budgets | Deterministic state-machine traces and fault-injection model show atomic publication, session exclusion through unlock, replicated retry binding, separate release/quorum gates and bounded ownership |
| A1: volatile Rust integration | Runtime wrapper, lower log adapter, drains, exact-horizon copy, epoch cleanup | All A scenarios below on Rust replicas under native control; actual-release admission and terminal-install/lower-build fence demonstrated or explicitly extended; production dependency audit remains native-free |
| A2: volatile qualification | Pinned SF binary/environment, pressure and fault campaign | Repeated A scenarios with recorded environment, limits, traces and unsupported modes; only Tier A claim |
| B: native V2 compatibility | Native codec/bootstrap/routing, staged copy/context, overlap and false progress | All B scenarios, both primary/secondary directions, exact native provider/version/settings matrix; source references alone do not count |
| C: persistent recovery | Durable log/checkpoints/copy publication plus persisted SF lifecycle | All C scenarios with process/host/storage fault injection; persisted manifest/open checks changed only with the full contract |
| D: collection products | Provider-specific APIs, isolation, data formats, lifecycle | Separate conformance suites per map/queue, native/managed bytes, C ABI or disk claim |

### 10.1 Tier A scenarios

| Scenario | Required observable result |
|---|---|
| Multi-key, multi-provider and registry transaction | A pinned read sees one complete root, never half a commit; provider add/remove and user updates agree |
| T1 paused after end admission/quorum/root swap and during unlock, while T2 and epoch invalidation contend | T2 cannot validate until T1 finalizes; publication check/swap is serialized with invalidation; readers see only complete roots; unlock failure fences before a next writer and cannot undo a published decision |
| Conflict, write skew, abort, provider prepare/unlock failure | Conflicts reject before admission; abort exposes no partial writes; post-admission faults fence service, retain decision ownership and rebuild without skipping |
| Caller disconnect/cancel before and after each admission/end/publication | No orphaned owner; known pre-admission rejection distinguished from unknown; retry/query never turns unknown into “aborted” |
| Concurrent matching/conflicting duplicate, primary loss before local outcome recording, copy with pending binding, successor query | Begin history reconstructs one identity/digest-to-attempt binding and canonical decision; matching duplicate joins, conflicting digest rejects; missing/old-generation query stays unavailable and cannot start a fresh retry |
| Crash/kill around begin, operation, end admission, quorum and publication | Surviving canonical replicas resolve retained decisions and pending chains; no primary publishes partial state; total volatile-set loss is explicitly allowed to lose state |
| Promotion with pending transactions and accepted unpublished end | Canonical completed ends retained; noncanonical suffix rebuilt; pending chains aborted/barriered before readiness; stale callbacks cannot publish |
| Copy at begin/middle/end boundaries and concurrent registry change | Exact `H`, complete pending chains, original replay provenance; end at `H+1` applies exactly once to the installed generation |
| Copy races, empty copy, duplicate overlap, cancelled terminal chunk | No lower build/upper readiness before validated install; matching duplicate is idempotent, divergent duplicate faults; abandoned image never leaks into serving root |
| More than 64 operations | At least 128 small operations within the 256-op/byte bounds finish with captures released per record; no “hold until commit” deadlock |
| Oversize transaction/record, exhausted log/version/copy/outcome budgets | Bounded rejection/backpressure or explicit fault, no unbounded allocation, no hidden engine-accounting bypass, terminal resources preserved |
| Quorum continues but a lagging member or idle-build retention pin holds a near-full queue | No next record passes actual-release/headroom gate; no incremental self-fill; pre-admission deferral or post-admission unknown/fence on deadline, distinct from quorum failure |
| Slow callbacks, stalled peers, long readers and concurrent copies | Time/space limits enforced; unknown/fence during loss of quorum; pins are retained/released correctly and cannot silently force unsafe truncation |

For fault cases, record both the caller result and the canonical log/state result;
they may differ without violating the unknown-outcome contract.

### 10.2 Tier B native oracle

Use a **new native transactional** test service with the same deliberately small
custom provider contract and a known native transactional-replicator version.
The current documented classic V1 profile is not a native V2 oracle
([FubeData.md:3–14][f-profile]).

Test Rust-primary/native-secondary **and** native-primary/Rust-secondary,
including promotion both ways, transaction commit/abort, provider registry
routing, initial bootstrap and maximum segmented records. Cover no-op, partial,
full and false-progress copy; checkpoint-driven horizon extension; overlapping
replication; EOS/terminal install races; epoch changes mid-copy; native
commit-apply-ACK settings; and unsupported-version rejection. Differentially
compare decisions, prefix/tail/stable positions, provider images and lifecycle
traces—not just equal final maps. A source-inspired Rust codec needs captured
and decoded native evidence; byte equality for one transaction is insufficient.

Pin native binary version, provider type/version, native versus managed
serialization, configuration, architecture and test-service build provenance.
Keep oracle libraries/toolchains outside production dependencies. B's first
claim is the custom native provider only, not TStore or all Reliable Collections.

### 10.3 Tier C and D scenarios

C injects interruption before/after local and remote flush, commit-end, checkpoint
prepare/perform/complete, file and directory publication, staged-copy install and
tail/head truncation. Exercise new/existing replica opens, whole-process and
whole-set restart, rejoin to a different canonical history, torn records,
corrupt metadata, replay twice, and pinned-read/pending-transaction truncation.
Success means either a consistent supported recovery or an explicit fail-closed
corruption outcome, never partially visible transactions or invented durability.

D adds provider-specific lock/isolation, range/phantom, queue ordering,
cancellation, checkpoint, wire and disk/API conformance. These tests are added
only for the particular compatibility claim being made.

## 11. Alternatives, risks and deferred decisions

### 11.1 Alternatives

| Alternative | Decision and reason |
|---|---|
| Replace the V1 engine/consensus | Reject: unnecessary scope and conflicts with retaining native control authority |
| Directly import C++ logging/state-manager libraries | Reject for production baseline: broad native coupling, contrary to native-free goal |
| Implement TStore/Reliable Collections first | Reject: mixes transaction, provider, serialization and recovery claims; use a custom provider to isolate the seam |
| Pure in-memory map with one replicated “transaction blob” | Useful model/oracle for atomic publication, but not the chosen log architecture: it would not exercise pending chains, native-style record boundaries or later copy/recovery contracts |
| Full native-byte compatibility in the first Rust milestone | Defer: extended-horizon copy, bootstrap and upper formats require independent qualification |
| Rust-owned volatile records and immutable version roots | Choose for A: smallest actionable native-free transaction contract with explicit future compatibility boundary |

Reference logging-replicator build dependencies include LogRecordLib,
txnreplicator common, data utilities, LogicalLog, TestHooks, KtlLoggerUser and
Ktlfull; state manager independently links Ktlfull
([loggingreplicator/lib/CMakeLists.txt:1–69][n-build],
[statemanager/lib/CMakeLists.txt:1–46][n-state-build]).
This is not a lightweight, ready-made Rust/C ABI boundary.
When deriving algorithms or code later, retain the applicable Microsoft MIT
copyright/license notices and record provenance; this design does not propose
copying large native implementation blocks.

### 11.2 Risk and decision register

| Risk | Initial decision / mitigation | Later milestone decision |
|---|---|---|
| Copy-install race or extended native horizon | Exact `H`, private staging and terminal fence; no readiness on stream completion alone | B selects/proves adapter translation or opt-in engine copy-session extension |
| Log-tail named “committed” mistaken for transaction commit | Typed frontiers and separate publication/root metric | Preserve distinctions in durable metrics and API |
| Termination starvation / capture deadlock | Release-gated single-record lane, clean initial capacity, pre-reserved upper terminal resources, prompt ACK disposal; accept stricter-than-quorum availability | Pipelining only after cross-layer credit proof |
| Provider partial visibility or reentrancy | Custom immutable-root provider, central publication and deterministic lock order | Arbitrary/TStore providers need their own validated visibility protocol |
| Promotion of speculative prefix | Native boundary reconciliation, retained rebuild base and barrier | B/C add native progress-vector/undo contracts |
| Retry ambiguity or volatile loss | Replicated identity/digest binding, nonexpiring bounded A ledger, explicit unavailable queries, no exactly-once claim | Durable outcome retention/expiry contract at C |
| Source/deployed-binary mismatch | Pin sources separately; require observed native oracle matrix | Each B/C release requalifies its binary/profile |
| Memory and copy pressure | Global charged budget, bounded pins, fail/retry builds rather than move `H` | Tune numeric defaults from acceptance evidence, without relaxing boundedness |

**Deferred without blocking A:** native record/managed envelope implementation,
native bootstrap mapping, native copy-session extension shape, file-log backend
and persistent filesystem layout, backup/restore, TStore/queue ports, range-query
isolation, interleaved commit scheduling, rolling upgrades, TLS and C ABI.
Each is outside A's claim and has a later gate above. The initial implementation
decision is concrete: create `fube-txn`, implement the custom bounded provider
and owned serial transaction/log path, prove exact-horizon installation and
promotion, and qualify **only volatile Rust transactions** before expanding.

## Source references

Link labels in the text identify files and line ranges. Fube references are
relative to this document; native references are pinned to the baseline in §2.

[f-profile]: FubeData.md#L3-L14
[f-native-free]: FubeData.md#L27-L32
[f-authority]: FubeData.md#L3-L8
[f-data-provider]: ../../crates/libs/fube-data/src/api.rs#L77-L170
[f-stream]: ../../crates/libs/fube-data/src/api.rs#L173-L267
[f-submit]: ../../crates/libs/fube-data/src/engine.rs#L569-L660
[f-runtime]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L128-L203
[f-runtime-control]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L160-L203
[f-stream-access]: ../../crates/libs/fube-data/src/engine.rs#L516-L522
[f-owner-submit]: ../../crates/libs/fube-data/src/engine.rs#L1258-L1295
[f-order]: ../../crates/libs/fube-data/src/lib.rs#L13-L39
[f-owned-sample]: ../../crates/samples/rust-runtime-app/src/data.rs#L258-L317
[f-progress]: ../../crates/libs/fube-data/src/api.rs#L269-L282
[f-progress-note]: ../../crates/libs/fube-data/src/lib.rs#L21-L30
[f-capture-init]: ../../crates/libs/fube-data/src/engine.rs#L374-L406
[f-captures]: ../../crates/libs/fube-data/src/engine.rs#L2691-L2764
[f-wait]: ../../crates/libs/fube-data/src/engine.rs#L140-L159
[f-quorum]: ../../crates/libs/fube-data/src/engine.rs#L3505-L3564
[f-snapshot-fields]: ../../crates/libs/fube-data/src/engine.rs#L77-L90
[f-snapshot]: ../../crates/libs/fube-data/src/engine.rs#L524-L535
[f-admission-budget]: ../../crates/libs/fube-data/src/engine.rs#L569-L646
[f-copy-reserve]: ../../crates/libs/fube-data/src/engine.rs#L216-L221
[f-release]: ../../crates/libs/fube-data/src/engine.rs#L3537-L3564
[f-bind]: ../../crates/libs/fube-data/src/engine.rs#L313-L354
[f-copy]: ../../crates/libs/fube-data/src/engine.rs#L3632-L3652
[f-copy-context]: ../../crates/libs/fube-data/src/engine.rs#L2552-L2553
[f-copy-doc]: FubeData.md#L131-L136
[f-settings]: ../../crates/libs/fube-data/src/api.rs#L12-L74
[f-transport]: ../../crates/libs/fube-data/src/transport.rs#L429-L486
[f-transport-class]: ../../crates/libs/fube-data/src/transport.rs#L589-L603
[f-persisted-package]: ../../crates/libs/fube-runtime/src/packages.rs#L432-L438
[f-persisted-open]: ../../crates/libs/fube-runtime/src/stateful.rs#L504-L523
[n-com]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L204-L269
[n-create]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L85-L120
[n-state-replicator]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/IStateReplicator.h#L12-L29
[n-state-provider]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/IStateProvider.h#L12-L68
[n-frontiers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L435-L444
[n-replicate-log]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L382-L395
[n-assigned]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L915-L957
[n-kinds]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/LogRecordType.h#L18-L46
[n-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1340-L1417
[n-bootstrap]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LogManager.cpp#L286-L330
[n-provider2]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProvider2.h#L64-L155
[n-apply]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L768-L920
[n-unlock]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L455-L530
[n-transaction]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L328-L407
[n-commit]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L410-L516
[n-flushed]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L333-L356
[n-drain]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L805-L889
[n-ack]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L908-L970
[n-memory]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L159-L168
[n-named]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/NamedOperationData.h#L12-L44
[n-routing]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1290-L1344
[n-store]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L25-L43
[n-store-init]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L1536-L1555
[n-store-checkpoint]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L1799-L1818
[n-store-apply]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L2251-L2280
[n-open]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L654-L711
[n-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L735-L799
[n-role]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L830-L885
[n-promotion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L989-L1072
[n-copy-null]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L165-L215
[n-copy-factory]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1603-L1649
[n-copy-select]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L249-L325
[n-copy-batch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L409-L507
[n-copy-stages]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/CopyStage.h#L17-L46
[n-copy-extend]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L328-L348
[n-copy-scan]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L380-L406
[n-copy-meta]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L537-L552
[n-copy-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1385-L1405
[n-vector]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/ProgressVector.cpp#L92-L257
[n-truncate]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TruncateTailManager.cpp#L134-L365
[n-throttle]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/LogicalLogRecord.cpp#L58-L64
[n-build]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/lib/CMakeLists.txt#L1-L69
[n-state-build]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/lib/CMakeLists.txt#L1-L46
