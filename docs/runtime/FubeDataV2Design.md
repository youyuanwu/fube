# Fube Data V2: transactional replication design

**Status:** source-derived port proposal after semantic audit; not an
implemented or qualified V2 feature.
**Created:** 2026-10-29. **Revised:** 2026-10-30 after SF API alignment (#19)
and the Astra source-semantic review.
**Initial target:** Rust-owned volatile transactions over
`fube-data`, packaged as a new `fube-txn` crate. The initial scope narrows
supported capabilities, not the transaction or copy algorithms.

This document is the complete design and acceptance contract. It does not
authorize implementation or change the support claims of the existing engine.
“Proposed,” “must,” and the milestone requirements below describe future work;
the explicitly cited **current** and **reference** observations describe code.

## 1. Decisions and scope

1. **Correct the lower semantic mismatches, then keep the engine.** Build a Rust
   transaction/log/state-manager layer
   above `fube-data`; do not replace quorum replication, transport, elections,
   membership, or native SF control.
2. **Make log ingestion the lower provider.** A V1 operation is an opaque
   upper-log record, not an immediately visible collection mutation.
3. **Port native state machines from the first milestone.** Use the native
   logical records, incremental transaction behavior and staged copy pipeline.
   A small test provider isolates the interface; it does not define a new
   replicator protocol, mandatory root-swap scheme or request ledger.
4. **Separate record replication, transaction decisions, and visibility.**
   Quorum completion of one record is not a transaction commit; an ACK is not a
   client commit or, in the volatile profile, a durable write.
5. **Port the existing V2 copy capability and stages.** Add the semantic
   equivalent of `IFabricStateProviderSupportsCopyUntilLatestLsn` and preserve
   native checkpoint/log overlap. Do not substitute a custom exact-horizon image
   or invent a negotiated copy protocol. Ordinary classic providers keep their
   existing copy policy.
6. **Keep production native-library-free.** Use source-guided Rust algorithms
   and contracts. C++/COM can be a separate oracle, not a production dependency.
7. **Persistence is an end-to-end milestone.** A WAL alone must not enable
   persisted SF manifests or produce a persistence claim.
8. **No alternate correctness algorithms.** Preserve SF's V2/V1 control
   delegation, transaction states, log/apply/unlock separation, provider
   isolation and copy/recovery rules. Rust ownership, execution and finite
   admission limits may differ in representation, but cannot replace native
   semantics with a serialized fallback.

### 1.1 Independent compatibility tiers

| Tier | Intended claim after its gates pass | Explicitly not implied |
|---|---|---|
| A: source-derived volatile implementation | Native transaction/record/copy state machines among Rust replicas under SF authority, with a bounded memory log and test provider | Executed mixed-native qualification, restart durability, whole Reliable Collections |
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
claim-specific acceptance gates and a migration path to persistence. Port
the authoritative algorithms instead of building a custom transactional
prototype first and replacing its semantics later.

**Not initial goals:** a new consensus algorithm; automatic TStore or
ReliableConcurrentQueue port; production COM/CLR/KTL dependencies; persisted
manifests; TLS; Windows; rolling executable upgrades; cross-release/native disk
compatibility; arbitrary provider code; distributed transactions across SF
partitions; exactly-once execution; a custom replicated envelope, retry ledger,
optimistic root coordinator or copy negotiation protocol.

## 2. Baselines and evidence limits

| Baseline | Identity and authority |
|---|---|
| Current fube integration source | `youyuanwu/fube`, `2f28b963b2b55020bb32ac73e7874b37b3a509fc` (SF API alignment, #19) |
| Original design's historical fube baseline | `3e0c334262ce228df6fb2f6f56863c11862f4ae7` (#13); removed `Engine`/provider/control APIs are not compatibility interfaces |
| Native reference source | `microsoft/service-fabric`, `51a7703a1f83749a9234d3ceb4296d8622e42cca`; immutable source links below |
| Historical fube qualification | Linux x86-64, SF **11.8.121.4**, volatile **classic V1**, `RequireServiceAck=true`, trusted clear loopback TCP, activated ExeHost/SharedProcess; no V2, persistence, TLS, rolling-upgrade or whole-set restart guarantee ([FubeData.md:3–20][f-profile]) |

Current production engine/runtime/application are documented as independent of
FabricRuntime, FabricCommon, COM and a native replicator
([FubeData.md:35–40][f-native-free]). Native nodes remain the membership, role,
epoch and placement authority in the current design
([FubeData.md:3–8][f-authority]).

**Evidence limit:** the pinned native source is an architectural and algorithmic
reference, not proof of the behavior or build provenance of SF 11.8.121.4 or any
deployed binary. No new oracle, build, test, or cluster execution was performed
for this document. Tier A–D claims remain unqualified. Fube links are relative
to this document and describe the fube baseline above; line numbers can drift
after future changes. Native links are pinned GitHub blobs and need no sibling
checkout.

**Semantic authority:** the local native implementation is authoritative for
what SF does at its recorded revision. Current fube code is authoritative for
available Rust integration and its limits, not for redefining native behavior.
SDK/mssf-shaped signatures alone do not establish native implementation parity.
The revisions below were checked against that local SF source, including
control pass-through, commit/apply/unlock, promotion and copy. Current
API-alignment deployed qualification is explicitly **NOT RUN**
([SfApiAlignment.md:242–263][f-qualification]); historical runs do not qualify
the new integration.

The original draft's review concerned a custom serial/root/ledger prototype.
The subsequent source audit found that those mechanisms were not SF's
algorithms. This revision supersedes them rather than merely labeling their
differences; the earlier review is not approval of the new port.

**R0 lower implementation (2026-10-31):** [FubeData.md's audit/corrections][f-v1-differences]
records corrected DD-01–DD-07, explicit latest-assigned copy capability and the
callback-ready truthful constructor-tail seam with offline regressions. The
Astra findings remain historical audit context, not unresolved lower semantics.
This is implementation evidence, not a native deployment qualification or an
A0 transaction/log/state-manager kernel. Final independent review/publication is
separate from these phase-reviewed local changes.

### 2.1 Additional audit findings and design resolutions

The revised upper core is source-derived; its implementation remains A0/A1.
Detailed pre-R0/native evidence and current correction regressions are recorded
in the V1 reference. The table retains the original audit resolutions; all four
lower seams below are corrected by R0, without implementing upper kernels.

| Finding | Priority | Required design resolution |
|---|---|---|
| [DD-04: promotion stream completion](FubeData.md#dd-04-promotion-does-not-finish-the-secondary-stream-generation) (Astra N1) | P1 | Role-scoped EOS, native processing/ACK drain completion and a new stream generation after demotion, in the same engine (§5.3, §6.2) |
| [DD-05: admission retry distinction](FubeData.md#dd-05-admission-errors-lose-the-native-retry-distinction) (Astra N2) | P2 | Lossless typed/native-coded or admission-stage mapping of transient queue fullness versus permanent sizing/resource rejection (§5.4) |
| [DD-06: local-not-PC general quorum](FubeData.md#dd-06-local-not-pc-quorum-greater-than-one-requires-an-extra-vote) (Astra N3) | P2 | Native PC availability/order statistic for quorum greater than one, retaining the separate remote-PC-quorum-one rule and issued membership (§8, §10.1) |
| [DD-07: extended-copy epoch validation](FubeData.md#dd-07-extended-copy-tails-fail-unconditional-epoch-reconciliation) (Astra N4) | P1 | Independent upper/lower tails, duplicate-control no-op gates and source role/copy-specific validation; preserve genuine new-epoch mismatch errors (§4, §6.2, §7.3) |

The source audit is bounded, not an exhaustive or deployed equivalence proof.
The earlier custom-core review remains superseded; this review does not authorize
alternate transaction, copy, quorum or promotion algorithms.

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
([api.rs:77–165][f-data-provider],
[api.rs:168–229][f-stream],
[api.rs:218–259][f-submit]). `IStatefulServiceReplica::open` returns
`Box<dyn IPrimaryReplicator>`; common lifecycle/progress and primary-only control
are split into `IReplicator` and `IPrimaryReplicator`
([stateful_api.rs:133–248][f-runtime]).
`FabricReplicator` shares one private engine among control/state views; the
engine, old concrete streams and old traits are no longer public construction
or compatibility paths
([fabric_replicator.rs:19–70][f-fabric],
[lib.rs:44–80][f-removed]).

### 3.2 Proposed component ownership

```text
native SF node: membership / role / epoch / reconfiguration / access authority
          |
 fube-runtime service + fube-txn IPrimaryReplicator lifecycle wrapper
          |                             |
          | delegates control           | gates application readiness
          v                             v
 fube-data FabricReplicator <---- log adapter / stream drains
      (one private Engine)
          |                    |
     Fabric TCP          log + transaction manager
     peer replicas       state manager / provider versioning
                               |
                     Rust IStateProvider2 implementations
                     (first: bounded custom test map)
```

| Proposed owner | Responsibilities | Must not own |
|---|---|---|
| `fube-data` | V1 admission/LSNs, configuration quorums, TCP peers, lower copy/replication, lower epoch reconciliation | Transaction semantics, registry or collection visibility |
| `fube-txn` log adapter | Lower `IStateProvider`, native V2 copy capability, owned record admission, stream ingestion/ACK, original log provenance, log-tail progress | Elections, fabricated quorum knowledge, mandatory replay workaround |
| Transaction/log manager | Record codec, transaction table, ordered decisions, bounded log, promotion cleanup, stable/checkpoint tracking | Native control authority |
| State manager | Transactional provider name/ID/type/parent registry, factory validation, routing, copy/checkpoint coordination | Collection-specific locks or serialization parity |
| Upper provider | Transactional Apply/Unlock, source-compatible isolation/version contexts and checkpoint/copy state; first a small test provider | Lower transport ACK policy or a partition-wide root-swap requirement |
| Runtime wrapper | Compose lower/upper Open, ChangeRole, Close and Abort; gate application access and own cleanup | Intercept every native configuration/build/removal or replace lower progress |

The names and crate split are proposals, not existing APIs.

### 3.3 Native control composition: preserve pass-through

The local SF wrapper explicitly labels its control forwarding as pass-through.
This determines the proposed Rust routing, not an assumption that every method
must run an upper hook
([ComTransactionalReplicator.cpp:478–603][n-pass-through]).

| Runtime-facing method | Native implementation responsibility | Proposed Rust composition |
|---|---|---|
| Open | V1 Open, then upper transactional Open; abort V1 if upper Open fails | Wrap `FabricReplicator` and open upper log/state manager before returning success |
| ChangeRole | V1 ChangeRole, then upper logging/state-manager transition | Wrap common lifecycle; preserve lower-first role ordering and fence failed transitions |
| Close / Abort | V1 first, then upper close/abort | Stop new application admissions, preserve lower-first delegation, and retain upper drain/cleanup ownership |
| UpdateEpoch / OnDataLoss | Forward to V1; V1 calls its upper logging `IStateProvider` | Delegate once; implement upper work in lower-provider callbacks, not a second wrapper callback |
| Current progress / catch-up capability | Forward to V1 | Keep lower role-specific observations; do not substitute visible-transaction LSNs |
| Configurations / catch-up / build / removal | Forward to V1 | Use existing canonical root or native invocation-bound lower controls; no second upper control authority |

Open/close/role ordering and failure behavior are in
[ComTransactionalReplicator.cpp:654–885][n-lifecycle];
Abort invokes lower then upper at
[482–486][n-abort]. Upper epoch/data-loss callbacks dispatch to the logging
replicator through [StateProvider.cpp:59–109][n-provider-callbacks].

**Current fube integration:** construction installs a native invocation binder
and coherent progress source for the private engine
([engine.rs:708–735][f-native-install]).
Runtime configuration/build/removal dispatch chooses that bound control instead
of the service-returned fallback, while progress can use the installed source
([stateful_binding.rs:695–720][f-native-dispatch],
[stateful_reconfiguration.rs:245–280][f-native-progress]).
Only one live binder/progress source is allowed
([stateful_binding.rs:309–323][f-progress-install],
[525–542][f-binding-install]).

**Decision:** keep those lower bindings. A plain lifecycle wrapper need not
receive configuration/build/remove or progress interception: that is consistent
with native V2 delegation. Do not require a generic decorator, replace an
installed binder, or construct another engine. Enforce log reconciliation and
copy installation through the upper `IStateProvider` and destination drains;
enforce application readiness through the upper service/lifecycle owner. If a
source-required copy completion is missing, represent that existing stage/
completion locally (§7), not a new protocol or control-routing redesign.
A1 must exercise bound and
root control paths and show that neither bypasses the relevant provider/drain
guarantees. R0 corrects DD-01–DD-07 through shared lower behavior and
explicitly tested seam mappings; an upper wrapper must not hide them with a new
commitment, catch-up, copy or promotion policy.

## 4. Lower seam and progress contract

### 4.1 Exact mapping

| Native reference concept | Proposed fube mapping | Required interpretation |
|---|---|---|
| `ReplicateAsync(data, out LSN)` | `IStateReplicator::replicate`; use explicit `EpochCheckedReplication::replicate_in_epoch` for origin-fenced admission, then local log ingestion and `ReplicationCompletion::wait` | `ReplicationAdmission.sequence_number` is available before quorum; its `completion` is a separate observer |
| `GetCopyStream` / `GetReplicationStream` | Synchronous fallible `get_copy_stream` / `get_replication_stream`, followed by async `IOperationStream::get_operation` | One consumer shared with origin views; retain authentic original data |
| Lower `GetLastCommittedSequenceNumber` | Adapter `IStateProvider::get_last_committed_sequence_number` returns its reconciled accepted **log tail** | Never return the last visible transaction's commit LSN instead |
| Lower `UpdateEpoch(previous-last)` | Preserve native role/drain/duplicate gates and applicable provider validation; write/flush/process a new native epoch record at its source boundary | No unconditional upper/lower tail equality after extended copy; canonical false-progress handling is separate |
| Lower `GetCopyState` | Sync `get_copy_state` factory plus async enumeration of native copy stages, using the explicit V2 latest-LSN capability | Provider factory is not enumeration or completed installation; no provider-payload ACK |
| Maximum message size | Enforce engine operation bytes/segments before admission | Upper framing and routing metadata consume the same budget |
| Data loss | Delegate lower control into the native logging/data-loss-handler path and preserve its state-changed result and error behavior | No invented empty-state reset, duplicated callback or readiness grant |

Current fube supplies canonical state access and explicit epoch-fenced admission
over the same owner
([fabric_replicator.rs:112–142][f-state-access],
[extensions.rs:76–86][f-epoch-admission]). Primary local application must precede
quorum waiting; the sample uses an owned admission future
([lib.rs:13–32][f-order],
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

Replay and origin are deliberately **not** members of the canonical SDK traits.
The existing optional `ReplayCheckpointProvider` is a classic fube workaround,
not a required native V2 callback. With DD-02 corrected, V2 establishes
the native no-write/catch-up behavior without requiring that hook; construct
the lower facade with no replay provider for the source-conformance path.
If a legacy fixture still supplies it, preserve its original-byte/epoch/segment
rules without letting it define transaction commitment or copy semantics.
Use `ReplicatorDiagnostics` origin streams when needed, read LSN from
`IOperation::get_metadata().sequence_number`, and obtain the original epoch from
`OperationWithOrigin.epoch`. `IOperation` has neither a public epoch field nor a
native transaction/atomic-group feature: the supported lower kind is Normal,
with atomic-group ID -1. V2 transaction identity lives in the opaque upper log
([api.rs:168–229][f-stream],
[extensions.rs:14–95][f-capabilities]).
Canonical and origin getters share the same single-consumer stream; do not
start two competing drains.

### 4.2 Distinct frontiers, not one “committed LSN”

Proposed metrics and invariants:

| Name | Meaning |
|---|---|
| Assigned | Primary obtained a lower LSN for an admitted record |
| Received | Secondary received a lower operation; it need not be accepted yet |
| Log-accepted | Complete validated native records and provenance owned by the contiguous upper log; extended copy can advance its tail beyond lower progress |
| Flushed | Accepted log crossed the profile's flush boundary: memory acceptance in A, durable storage synchronization in C |
| Record-quorum | Lower waiter proves this record's required configuration quorum; primary knowledge, not inferred from secondary ACK |
| Applied | Logged records completed their source-derived Apply processing; operation and transaction commit LSNs remain distinct |
| Transaction completion / visibility | Commit completion requires the source apply/replication/unlock path; visibility and read isolation are enforced by provider lock/version contexts, not a new global root |
| Stable | Upper barrier-established stability frontier used by checkpoint/reconciliation policy; not merely the latest visible commit or lower release |
| Checkpoint / installed-copy | Completed image plus required transaction/history metadata, with a specific covered prefix and installation generation |

Track transaction ID, begin/operation/end positions and original epoch separately
from these replica-local frontiers. There is no single total ordering of all
frontiers across replicas. During extended copy, even one replica's upper log
tail may lead its lower queue frontier (§7.3). The provider getter must report
the real upper tail without overwriting lower progress or imposing universal
tail equality at every control boundary (DD-07).
Local provider Apply may precede replication
completion; it does not independently authorize commit success. Secondary
apply does not prove the primary returned success. Preserve the source's
`IsReadable`, provider visibility-version registration and transaction contexts,
rather than adding a universal secondary-read ban or global publication gate.

Current fube already distinguishes assigned, received, applied, committed and
completion observations
([api.rs:384–398][f-progress],
[lib.rs:27–32][f-progress-note]).
The lower names are not to be silently relabeled as transaction metrics.
Use `ReplicatorDiagnostics::snapshot` for diagnostics/accounting, not a
per-record-release prerequisite for transaction admission.
`IReplicator::get_current_progress` and `get_catch_up_capability` are separate
scalar observations; they are not an atomic pair or upper stable frontier
([extensions.rs:88–103][f-diagnostics],
[stateful_api.rs:178–184][f-progress-getters]).

**Native stable-LSN rule:** the primary advances stability after its barrier's
apply and replication succeed; secondaries use the barrier's advertised
`LastStableLsn`, not their own highest received/ACKed LSN
([CheckpointManager.cpp:1188–1204][n-stable-primary],
[1554–1578][n-stable-secondary]). Preserve this distinction in the upper log.
A conservative local retention floor may be earlier than stable, but must not
manufacture a later stable LSN from a released lower record.

### 4.3 Port native records and codecs

Port `LogRecord`/`LogicalLogRecord`, begin/operation/end transaction records,
barrier and epoch-update serialization/deserialization for the supported native
subset. Preserve native field values, logical section sizes/version handling,
buffer boundaries and null-versus-empty metadata/undo/redo; do not wrap them in
a custom digest/ordinal/request-identity envelope
([LogicalLogRecord.cpp:78–96][n-logical-codec],
[TransactionLogRecord.cpp:110–166][n-transaction-codec]).
Provider routing uses native `NamedOperationData`/state-manager formats
([NamedOperationData.h:12–44][n-named]).

Native logical kinds include
begin/operation/end/barrier/update-epoch/backup, while index, checkpoint,
truncation and information kinds are physical
([LogRecordType.h:18–46][n-kinds]).
Backup and separate atomic/redo-only transaction APIs may be unsupported
initially; reject them before admission rather than substitute a different
record kind. Include the native single-operation transaction path in A.
An epoch marker can use the previous epoch's last LSN; native empty-log
bootstrap uses both zero and one
([LoggingReplicatorImpl.cpp:1340–1417][n-epoch],
[LogManager.cpp:286–330][n-bootstrap]).
Therefore neither transactions, physical records nor epoch changes receive
fabricated fresh engine LSNs. Establish this native bootstrap/log-tail mapping
in the foundation, not a custom zero-based wire convention deferred until B.
Replication admission still supplies the authoritative operation LSN; copied
logical records and physical offsets follow their own source serialization.
Port native transaction-ID generation/replay advancement and sign/type handling,
not replica-incarnation UUID/counter identities
([TransactionBase.cpp:78–127][n-transaction-ids]).

Application retry identities, digests and result ledgers can live in optional
provider payloads or client fixtures, but are not fields of the replicator's
begin/end records. Such a fixture must not become a conformance prerequisite.

## 5. Transaction, visibility and failure semantics

### 5.1 Initial transaction API and concurrency model (proposal)

A implements native transaction creation, operation enlistment, commit, abort,
dispose and visibility-version registration through the source transaction and
state-manager interfaces. A small point-map provider exercises Apply/Unlock,
checkpoint and copy; it is not a new transaction coordinator or a whole TStore
implementation.

Port `TransactionBase`, `Transaction`, `TransactionStateMachine` and
logging `TransactionManager`/`TransactionMap` responsibilities. Native
`Transaction` caches its first operation; adding a second writes begin plus
operation records, and subsequent operations are incrementally replicated.
Single-operation commit uses `BeginTransactionAsync`; a multi-operation commit
uses the native end record. Read-only commit does not invent a replicated write
([Transaction.cpp:238–310][n-add-operation],
[406–485][n-private-commit],
[TransactionManager.cpp:328–407][n-transaction]).

The state machine guards operations on each transaction, including read,
commit, abort and retry transitions. It is not an exclusive partition-wide
write-session lease. Port its actual transitions, including returning a
timed-out/canceled commit attempt to Active after a classified retriable failure,
rather than interrupting accepted work arbitrarily or replacing the states with
full-request staging or an irreversible custom “unknown” state
([TransactionStateMachine.cpp:91–193][n-transaction-states]).
Different transaction chains may interleave in the shared log. Retain source
LSN ordering and record-dispatch barriers; do not wait for every previous record
to leave the lower queue before another transaction can admit a record.

Provider isolation and visibility remain provider responsibilities. Source
`IStateProvider2` receives operation LSN, transaction/commit LSN and role/apply
context, then returns an operation context for Unlock
([IStateProvider2.h:137–155][n-provider2]).
TStore has its own lock/version and primary/secondary/recovery paths
([Store.h:2251–2394][n-store-locks]).
The test provider must preserve atomic visibility using transaction-held
provider contexts for its supported reads/writes; it cannot expose a partial
multi-operation commit. There is no mandatory optimistic read-set validation,
partition-wide immutable root or commit-time batch-Prepare callback.
Native visibility-version registration and provider isolation must be ported
before exposing the corresponding read modes, not replaced by a single pinned
global map snapshot
([Transaction.cpp:387–404][n-visibility-registration],
[VersionManager.cpp:276–320][n-version-registration]).

Preserve transaction Abort/Dispose synchrony as well: native `Abort()` enters
the abort state and launches owned asynchronous work; its synchronous success
does not mean a replicated abort or Unlock has already completed. Cached-first
abort unlocks that context without writing an end record; an already replicated
chain follows the native abort-end/retry path
([Transaction.cpp:131–189][n-transaction-abort],
[310–383][n-abort-task]).
Do not confuse this transaction boundary with runtime control Abort disposal.

### 5.2 Primary commit flow (proposal)

**Source-derived ordering:** native starts replication, appends the assigned
record locally, flushes before logged-record application, and on committed end
applies transaction constituents. Its primary completion waits for apply and
replication, then invokes Unlock **also on failure** to release pending
contexts; the native async path maps failure to transaction-aborted status
([ReplicatedLogManager.cpp:382–395][n-replicate-log],
[OperationProcessor.cpp:333–356][n-flushed],
[768–920][n-apply],
[TransactionManager.cpp:498–516][n-commit-wait]).
Local apply and quorum may overlap; this is not a native
quorum-before-provider-apply rule. Do not copy native status naming into a
promise that an interrupted client can prove rollback.

Implement that native flow through the current Rust admission/completion seam:

1. Enlist provider operations under the source transaction/context rules.
   Cache only the first operation as native does; do not require a complete
   request, a replicated retry binding or a clean lower queue before enlistment.
2. When a record must be replicated, validate native role/access/epoch and
   reserve finite ownership for that record. Admit through `replicate_in_epoch`,
   attach its early LSN and completion observer, then append to the ordered
   local log. Accepted log/transaction ownership outlives the requesting future.
3. Begin/Add return after their source admission boundary while owned record
   processing separately waits for apply/replication. Continue admitting later
   operations and other transaction chains subject to ordinary backpressure;
   queue release is not part of this return boundary
   ([TransactionManager.cpp:236–260][n-add-manager], [328–353][n-begin-manager]).
4. Commit selects the native single-operation begin or multi-operation end
   path. Group commit/flush and logged-record dispatch drive Apply. Committed
   multi-operation end processing invokes constituent provider Apply calls in
   transaction order; it does not introduce a global publication/root swap.
5. Complete the native apply-plus-replication wait, invoke context Unlock on
   success and failure, and update the transaction state/result. Provider
   contexts protect visibility until the source unlock boundary. Preserve native
   error/retry decisions rather than treating every failed attempt as a
   confirmed abort or an automatic replica fault.

The shared log's LSN ordering and flush/dispatcher barriers must be ported from
`ReplicatedLogManager` and record dispatch, not implemented as “only one write
transaction may exist.” Completion requires the source commit record's local
apply and replication; it does not require release of that record or all
preceding records from the lower queue. A successful volatile transaction still
does not survive loss of the entire replica set. Fencing never grants success
or proves that context destruction/unlock has finished.

### 5.3 Secondary drain and ACK flow (proposal)

1. Consume copy installation before ordinary replication publication. Poll
   `IOperationStream::get_operation`; read metadata/data through the fallible
   getters, and origin through the explicitly selected origin stream. Validate
   stream generation, lower LSN/epoch provenance, native framing and transaction chain.
2. Decode native logical data, attach the lower operation LSN, charge and append
   to the upper log. Flush and advance accepted log-tail progress.
3. Follow native `EnableSecondaryCommitApplyAcknowledgement`: ACK after flush;
   when enabled for a commit record, also await Apply before ACK. Ordinary
   begin/operation ACK does not await the transaction's eventual end. Preserve
   this source setting rather than inventing a fixed alternative ACK policy.
4. The source dispatcher processes committed transaction constituents and
   non-Primary Unlock. Provider read visibility follows its context/version
   rules plus native `IsReadable`; receipt or ACK alone never grants reads.
5. At role-scoped replication EOS, flush/process the native replication-finished
   information record, wait for logical processing and outstanding ACKs, then
   signal successful upper drain completion. Cancellation/error is not that
   boundary; a later secondary role consumes a new generation (§6.2, DD-04).

Use the native memory-log abstraction for the volatile profile and the chosen
durable-log implementation for C. A memory flush is not disk durability.

**Why ACK ownership matters:** current fube has a shared default limit of 64
provider operations; the operation ACK closure holds a permit and data charge
until capture disposal
([api.rs:26–40][f-capture-init],
[engine.rs:3483–3536][f-captures]).
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
Its successful drain completion follows EOS and required processing/ACKs
([SecondaryDrainManager.cpp:862–889][n-drain-finished],
[224–235][n-drain-completion]).
Its nonpersisted profile selects `MemoryLogManager`
([LoggingReplicatorImpl.cpp:159–168][n-memory]).
Neither reference memory flush nor proposed A ACK means disk durability.

### 5.4 Outcomes, retry and fault contract (proposal)

| Observed result | Client meaning |
|---|---|
| An operation rejected before its record admission | That operation consumed no new lower LSN; previously enlisted/admitted transaction work remains governed by the native transaction state |
| Successful commit | Native apply/replication result and required Unlock invocation; an Unlock error follows the source service-fault path, not proof of clean disposal; read-only commit need not replicate |
| `Abort()` accepted | Native abort state/task was admitted; synchronous success is not completed distributed abort or context cleanup |
| Native abort task reaches Aborted | Its source abort/cleanup path completed; do not infer this state from a lost commit response |
| Client disconnect, lost observation, or timeout/cancellation after admission | The caller may not know the distributed outcome; preserve owned work and the source transaction result/state |
| Lost object/history or total volatile replica-set loss | No durable outcome-query or exactly-once promise is introduced |

Dropping a current lower `ReplicationCompletion::wait` does not undo admission
([api.rs:232–259][f-wait]); current lower quorum completion is independent of
provider local application
([engine.rs:4404–4464][f-quorum]).
The upper owner must therefore outlive the requesting socket/future, complete
or fence its work, and retain all admitted-record ownership.

Use the shared `fube_runtime::{Error, ErrorKind, Result, ApiFuture}` contract.
Preserve typed causes, native codes and `is_uncertain`; do not classify errors
by their display strings. A canceled completion observer is explicitly
uncertain, but the upper attempt's actual admission stage still determines the
transaction outcome: an unmarked error after a previously admitted begin is
not proof that nothing happened
([error.rs:273–335][f-errors],
[api.rs:240–259][f-wait]).
Managed user-error captures must be released through their execution ownership
before owner shutdown, just like other retained provider captures
([error.rs:1–12][f-error-ownership]).

Port native commit retry/cancellation and abort/dispose state transitions.
`PrivateCommitAsync` retries only its classified retriable results and returns
canceled/timeout state through the source state machine
([Transaction.cpp:406–485][n-private-commit],
[TransactionStateMachine.cpp:130–193][n-transaction-states]).
`TransactionBase` identifies no-write-quorum, reconfiguration-pending,
replication-queue-full and service-too-busy as retriable
([TransactionBase.cpp:197–209][n-retry-statuses]).
Map these actual conditions/native codes, not every ResourceExhausted error or
display-string match, into the Rust retry path. Retrying a transaction object is
not a cross-replica application request-deduplication protocol.

**Implemented lower admission mapping (DD-05):** R0 exposes
`ErrorKind::ReplicationQueueFull` (HRESULT 0x80071bcb) versus
`ReplicationOperationTooLarge` (0x80071c2f), retaining causes/native conversion
through runtime/data and callers
([error kinds](../../crates/libs/fube-runtime/src/error.rs#L31-L43),
[lower queue stages](../../crates/libs/fube-data/src/queue.rs#L80-L102)).
Lower capacity-recovery and permanent-oversize regressions pass; this upper
transaction retry loop/state kernel remains A0/A1. A prevalidated record's actual
queue-full rejection is retriable; oversize/unsupported records and other
permanent quota failures must not be mislabeled queue-full.
Do not infer this distinction from display text or a racy diagnostics snapshot.
Regress capacity recovery followed by successful commit on the same transaction,
native retry timeout/cancel states, and permanent oversize without endless retry.
New public error variants are not the only acceptable representation.

Do not add a mandatory request/digest binding, dataset-generation token,
outcome-query ledger, ledger-expiry policy or replicated result cache to the
transactional replicator. An optional application provider may implement its own
idempotency payloads outside the native records; that application behavior is
not part of the V2 port or its correctness oracle.

Provider Apply/Unlock failures, invalid canonical history and hard ownership
deadline failures use explicit fault/cleanup paths; never skip a failed record
and advance a frontier. Native Apply failure reports service error/fault
([OperationProcessor.cpp:952–989][n-service-error]).
Native Unlock failure reports service error and still marks record processing;
commit return status is chosen from the apply/replication path, not a guarantee
that Unlock succeeded
([OperationProcessor.cpp:607–615][n-unlock-error],
[TransactionManager.cpp:498–516][n-commit-wait]).
Ordinary source-retriable backpressure is not an invented terminal fault merely
because the transaction already has earlier records. Recovery and
false-progress handling honor canonical log decisions and native undo/replay
rules, not an ad hoc whole-root rebuild or compensating callback.

## 6. Provider registry and lifecycle

### 6.1 Upper provider contract (proposal)

Name the proposed upper trait `fube_txn::IStateProvider2`, distinct from lower
`fube_data::IStateProvider`. Model its responsibilities and synchrony on native
`IStateProvider2`: async Apply with operation LSN, transaction/commit LSN,
role/redo/undo context and optional metadata/data; synchronous Unlock of the
returned context; synchronous PrepareCheckpoint and async perform/complete/
recover; synchronous copy-state stream factory and async begin/set/end
installation; role/open/close and synchronous Abort.
Native has no generic per-transaction Prepare method. Interrupted copy must be
discarded without inventing a successful EndSettingCurrentState callback
([IStateProvider2.h:12–155][n-provider2-full]).
A implements the native volatile memory-log/checkpoint/copy meanings of the
callbacks it invokes. Durable Existing-open and backup/restore capabilities
remain unsupported until their tiers are implemented; do not use successful
stubs for required callbacks or claim disk recovery from memory-only behavior.

Port the state manager's metadata, named provider lookup/IDs, factories,
parent/child lifecycle and transactional add/remove/lock-context handling.
Preserve active/deleted metadata and source recovery/copy routing, rather than
putting registry changes in a new immutable global root. The small provider
must not allow removal or reclamation to invalidate an outstanding source
transaction/read/copy context. Unsupported provider types fail explicitly.

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

Use synchronous `IStatefulServiceFactory::create_replica` and capture the
factory-supplied `NativeIntegrationProvider` separately from SDK identity
arguments. In runtime-owned `IStatefulServiceReplica::open`, validate
`OpenMode::New` and call `FabricReplicator::create_with_capabilities` with:
`Arc<dyn IStatefulServicePartition>`, captured `NativeRuntimeIntegration`,
`Arc<dyn IStateProvider>` for the upper log, no replay workaround for the
source-conformance path, `ReplicationEndpointOptions`, and `Settings`.
Supply `StateProviderCapabilities { supports_copy_until_latest_lsn: true }`,
the implemented explicit native-equivalent construction capability (§7),
not a new SDK discovery member. `create` retains classic defaults. Keep its
`Arc<FabricReplicator>` and state view in the upper owner; return a boxed
`IPrimaryReplicator` lifecycle wrapper over that same instance
([stateful_api.rs:118–148][f-factory],
[fabric_replicator.rs:29–70][f-fabric],
[stateful_binding.rs:160–180][f-native-capture]).
There is no supported public standalone `Engine`, extra partition
CreateReplicator method, SDK discovery hook, or second lower owner.
`Existing` is Unsupported and `Invalid` is InvalidArgument before activation
([stateful_api.rs:235–255][f-open-mode]).

For A, construct callback-ready empty log/registry metadata before lower
creation: the current engine invokes the provider's initial progress getter
while constructing the owner, not only after the wrapper's later Open
([engine.rs:629–660][f-initial-progress]).
Do not return a fabricated zero while a nonempty log is still initializing.
The native factory likewise initializes its lower state-provider bridge with
the upper logging provider before exposing the composed replicator
([ComTransactionalReplicator.cpp:280–295][n-provider-init]).
R0 proves this construction boundary passes nonempty actual fixture record tails
1/27 (including source-shaped zero/one markers) through to lower initial progress,
with the getter called before the lower facade is returned
([public witness](../../crates/libs/fube-data/tests/sdk_contracts.rs#L222-L297)).
The fixture is not native record serialization or a transaction log kernel.
Native zero/one records, registry/log initialization and persisted recovery still
belong to A0/A1/C as applicable; callers must initialize them before lower creation.

Preserve native status/access checks, cancellation, watchdogs, capture disposal
and incarnation fencing; cancellation alone does not establish quiescence
([stateful_api.rs:133–248][f-runtime-control]).

Proposed wrapper state progression:

```text
Closed -> Opening -> LowerOpen/UpperInitializing -> SecondaryInstalling
       -> SecondaryCaughtUp -> PrimaryReconciling -> PrimaryReady
any live state -> Closing/Faulted -> owned drain or fenced abort -> Closed
```

Delegate controls exactly as §3.3; keep upper installation/reconciliation
requirements inside provider/drain completion, not assumed wrapper interception.
Read/write access requires valid native lifetime/incarnation/epoch, native
partition status, and the applicable source logging/provider readiness.
`IsReadable` has role-specific native behavior below; do not replace it with an
extra “all pending aborts completed” predicate. A Primary role flag, diagnostic
ready flag or lower ACK alone is not an application access grant.

**Native promotion:** `BecomePrimaryAsync` waits for copy/replication drain,
quiesces activity, collects pending transactions, changes the logging role and
**starts** an owned abort-or-barrier task before returning. That task aborts
pending transactions; when there are none it establishes the primary through
a barrier. The role callback does not synchronously await that whole task
([LoggingReplicatorImpl.cpp:989–1076][n-promotion],
[1110–1245][n-promotion-task]). `TransactionalReplicator::ChangeRoleAsync`
then transitions the state manager; primary `IsReadable` checks partition read
status, not a universal “all pending aborts finished” predicate
([TransactionalReplicator.cpp:388–474][n-upper-role],
[LoggingReplicatorImpl.cpp:422–433][n-readable]).

**Port decision:** preserve that asynchronous role-return/task distinction,
native `IsReadable`, and abort-or-barrier branch. Do not require all aborts plus
another promotion barrier, or block the service behind a serial prototype gate.
Pending chains, complete canonical end decisions and their provider contexts
remain governed by the source drain/reconciliation state. Invalid canonical
history fails explicitly; it is not permission to invent a committed boundary
from visible state or transaction ID.

**Implemented lower stream lifecycle (DD-04):** native lower secondary close during
promotion closes dispatch queues and waits for their source drain
([SecondaryReplicator.CloseAsyncOperation.cpp:41–104][n-secondary-close]).
R0 freezes old-generation ingress, dispatches eligible contiguous buffered records,
closes senders successfully, then waits for actual service ACKs
([lower dispatch close](../../crates/libs/fube-data/src/engine.rs#L4066-L4103)).
It deliberately does not wait for ACKs before EOS: §5.3 can flush/process after
observing EOS. Its empty/nonempty role-cycle and 64-capture tests pass.
A later demotion needs a fresh copy/replication generation in the same engine,
preserving native transferred state rather than forcing another copy; old views
remain terminal and old captures fenced. Do not replace
successful EOS with pump cancellation, discard buffered eligible records, add
a synthetic operation/ACK, or delay role return until the abort-or-barrier task
finishes.

Epoch changes record a boundary marker at the given previous-last position.
Drop only noncanonical suffixes through the source transaction-map/provider
reconciliation paths. Old-incarnation tasks and ACKs cannot publish,
unlock a new generation's contexts or resurrect readiness.
Native `PerformUpdateEpochAsync` waits for primary LSN ordering, checks the
source epoch/progress boundary and returns for an already represented epoch
before tail equality. For a new applicable epoch, it validates the expected log
tail, writes the record at `previousEpochLastLsn`, flushes
and awaits processing; it is **not** a generic instruction to truncate every
upper tail. Its idle-copy tail mismatch returns invalid-operation
([LoggingReplicatorImpl.cpp:1340–1417][n-epoch]).
Keep canonical suffix selection and false-progress undo/rebuild in explicit
reconciliation paths, not a speculative extra wrapper UpdateEpoch.
The lower secondary duplicate-control path also completes without a provider
callback
([SecondaryReplicator.UpdateEpochAsyncOperation.cpp:43–92][n-secondary-epoch]).
R0 preserves these gates: duplicate secondary controls return before provider
calls; admission-only controls do not query/equate upper and lower tails.
Actual original-operation barriers/primary provider controls still preserve
new-epoch errors and their applicable checks (DD-07, §7.3).

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
The native upper `Abort` launches `CloseAsync` without blocking
([TransactionalReplicator.cpp:378–386][n-upper-abort]); fube's synchronous
Abort disposal/supervision guarantee is a deliberate stronger Rust lifetime
contract, not source-identical abort synchrony.

### 6.3 Current API values and owned execution

Use `IReplicator` plus `IPrimaryReplicator`, not the removed monolithic trait.
Configuration installation and ID-only `remove_replica` return synchronous
applied/disposed `Result<()>`; lifecycle/build/catch-up remain asynchronous.
Keep native-issued incarnation, revision, full epochs, local votes and removal
purpose outside public SDK descriptors. Freeze Build authority at method call,
before returning its future; never resolve a later “latest replica with this ID”
([stateful_api.rs:194–233][f-primary-control],
[engine_binding.rs:181–304][f-bound-controls]).

Runtime-invoked controls already enter owned execution. For application-owned
async callers, reserve `ExecutionRuntime::try_reserve` **before** moving captures,
then await `permit.run_owned(move || synchronous_call(...))`, not the SDK
method's `Result`. Do not use `block_on`, hold provider/engine locks needed by
the call, or re-enter the same engine from a provider callback. Unknown contexts
and same-engine reentry fail rather than turning a queued operation into
success
([SfApiAlignment.md:133–163][f-owned-execution],
[execution.rs:372–380][f-reserve],
[578–588][f-run-owned],
[482–498][f-reentry]).
Use the supplied bounded runtime/provider/cleanup ownership facilities; do not
add an independent scheduler. Upper pumps, listeners and arbitrary spawned
descendants remain service-owned and must be stopped/joined on Close/Abort.

`Epoch` now uses `data_loss_number` / `configuration_number`;
`INVALID=(-1,-1)` differs from valid `ZERO=(0,0)`. Keep full epochs for authority
and use explicit `EpochDomains::primary` only at the lower admission boundary.
Use `IdleSecondary` / `ActiveSecondary` and typed public roles/statuses;
public enum numeric values are not RAP's private values and must not be cast
into native envelopes
([stateful_api.rs:30–58][f-values],
[extensions.rs:9–25][f-epoch-domains]).

## 7. Copy and reconciliation

### 7.1 Port the existing native V2 capability

Native V2's `ComStateProvider` implements
`IFabricStateProviderSupportsCopyUntilLatestLsn`. The lower V1 provider bridge
detects that marker and the queue manager selects
`LastSequenceNumber + 1` as replication start. Ordinary providers instead use
the committed/previous-configuration boundary
([ComStateProvider.h:16–28][n-latest-marker],
[ComProxyStateProvider.cpp:19–27][n-latest-detection],
[ReplicationQueueManager.cpp:165–193][n-latest-start]).
This is an existing local provider capability, not a new peer negotiation.

**Implemented R0 capability:** `StateProviderCapabilities` is supplied to
`FabricReplicator::create_with_capabilities`
([construction](../../crates/libs/fube-data/src/fabric_replicator.rs#L49-L94)).
Its true flag selects assigned+1 replication start and inclusive assigned
provider horizon; default classic still selects max(committed,previous quorum)+1
([selection](../../crates/libs/fube-data/src/engine.rs#L2740-L2750)).
The provider factory keeps the exact requested argument, volatile null context,
supervision/bounds and enumeration-EOS finish-tail snapshot. A staged upper
provider owns checkpoint horizon extension and its metadata, not a fabricated
lower prefix. The latest-copy test proves horizon100/start101/finish101 with
committed0 and real finish-tail ACK.
Do not add discovery to SDK traits or infer V2 support from a type name.
Classic providers without the capability retain their current copy policy.
Native staged copy serialization is A0/A1; this implemented construction seam
is not a substitute custom snapshot protocol.

Native volatile copy can have null context; persisted/rejoin paths use
progress-vector/head/tail context. Fube's current volatile receiver ignores
persisted context messages
([CopyStream.cpp:165–215][n-copy-null],
[LoggingReplicatorImpl.cpp:1603–1649][n-copy-factory],
[engine.rs:3342–3343][f-copy-context]).
A qualifies the native volatile path; C must add the source persisted context
and rejoin paths before enabling Existing-open.

### 7.2 Native copy state machine and pending transactions

Port `CopyStream`, checkpoint selection, copy-stage buffers, state-manager
current-state enumeration and `SecondaryDrainManager`; use native stage and
metadata serialization from A. The source stage sequence chooses no-op,
full-state, partial-log or false-progress copy, then state/progress-vector/log
processing as applicable
([CopyStream.cpp:249–305][n-copy-select],
[CopyStage.h:17–46][n-copy-stages],
[ProgressVector.cpp:92–257][n-vector]).
Do not replace it with a complete exact-horizon map/transaction image,
custom terminal digest or an outcome ledger.

Full copy uses the completed checkpoint's provider/state-manager state and
enough logical log to reconstruct transactions crossing that checkpoint.
The source chooses a replay starting point before the earliest pending begin
when necessary and retains the physical reader range
([CheckpointManager.cpp:621–631][n-copy-pending]).
Port that log/transaction-map relationship; do not serialize pending chains in
a newly invented side protocol. Native logical records provide the continuation
when a transaction's end arrives in subsequent copy/replication.

Destination state installation calls BeginSettingCurrentState, SetCurrentState
and EndSettingCurrentState at the native stage boundary. State items are ACKed
after SetCurrentState; interrupted/null state copy must not invoke a successful
EndSettingCurrentState
([SecondaryDrainManager.cpp:974–1085][n-copy-state-drain]).
The logging copy drain then reconstructs metadata/log state, performs required
checkpoint completion and processes records. Preserve source ACK, flush,
application-processing and cancellation ordering; a native invocation-bound
Build must use these same provider/drain semantics, not an extra upper Build.

### 7.3 Extended copy and false progress

Copy may extend beyond the originally selected `uptoLsn` to the checkpoint's
stable LSN so installation finishes before promotion. Metadata carries the
source starting, checkpoint, copied and log-tail boundaries, and live
replication may overlap those copied logical records
([CopyStream.cpp:328–348][n-copy-extend],
[380–406][n-copy-scan], [409–507][n-copy-batch], [537–552][n-copy-meta]).
Preserve source duplicate/overlap and tail/epoch checks; native's idle-copy
epoch path explicitly detects an incompatible tail rather than silently
relabeling it
([LoggingReplicatorImpl.cpp:1385–1405][n-copy-epoch]).

**Implemented lower epoch seam (DD-07):** after copy extends requested 90 to upper tail
100, lower progress may remain 90 until overlapping replication 91–100 drains.
Keep those real frontiers distinct. Native duplicate secondary UpdateEpoch
does not invoke the provider; the upper duplicate-epoch branch also returns
before tail equality after its source epoch/progress validation
([SecondaryReplicator.UpdateEpochAsyncOperation.cpp:43–92][n-secondary-epoch],
[LoggingReplicatorImpl.cpp:1368–1401][n-epoch-duplicates]).
R0's actual-byte upper100/lower90 fixture delays overlap ACKs, proves duplicate
controls make no provider callback/getter/truncation, then drains overlap; its
new incompatible idle-copy/original-epoch negatives preserve failure
([regressions](../../crates/libs/fube-data/src/engine_tests.rs#L3530-L3675)).
The latest-LSN marker alone does not fix this seam. Do not fake progress, truncate a valid copied
suffix, or disable all epoch-tail checks for providers with the marker.

Port source copy-log completion/rename, progress-vector reconciliation and
false-progress transaction-aware undo through provider Apply/Unlock
([TruncateTailManager.cpp:134–365][n-truncate]).
Do not substitute lower queue truncation or wholesale root reconstruction.
The Rust integration may represent native capability/session ownership
explicitly, but must not introduce requested/effective-horizon negotiation,
a `copy-install-completed` protocol, or a generic control decorator as an
alternate correctness mechanism. If existing fube boundaries cannot express
a source-required completion, extend only that local representation while
preserving the native stage/ACK contract; do not ship the unsupported path.

## 8. Boundedness, truncation and termination headroom

Current engine defaults are 1 MiB / 1024 segments per operation, 1024 retained
operations / 16 MiB, 64 provider operations and an 8-second callback timeout
([api.rs:12–74][f-settings]).
Its transport classifies replicated opaque payloads as **data**, not control
([transport.rs:429–486][f-transport],
[transport.rs:613–618][f-transport-class]).
Therefore existing transport control reservations do not reserve commit/abort/
barrier capacity inside an upper transaction log.

Quorum completion and queue release are also distinct **current** behaviors:
waiters resolve at quorum, while release additionally depends on current/
previous `all_received` and copy/idle-build retention owners
([engine.rs:4404–4464][f-quorum]). The observable snapshot exposes progress,
owned operations and owned bytes
through `ReplicatorDiagnostics`
([engine.rs:98–114][f-snapshot-fields], [779–791][f-snapshot],
[extensions.rs:88–103][f-diagnostics]).
Admission still makes its own authoritative resource reservation, including
outgoing-copy headroom
([engine.rs:849–927][f-admission-budget]); a snapshot is not a reservation.

Port native `CheckpointManager`/`LogTruncationManager` throttling and record
classification. Begin/operation records are throttleable; terminal/control
records are not blanket-classified the same way
([LogicalLogRecord.cpp:58–64][n-throttle]).
Lower replication-queue-full can still reject an unadmitted record; use the
source transaction retry/state rules rather than claim terminal admission can
never fail.

Finite Rust record, transaction/context, provider-version, checkpoint and copy
quotas must account for payloads, descriptors and retained captures. These are
explicit admission limits, not a replacement commit algorithm. Validate
record/copy sizing against the existing lower queue and copy reservations
([engine.rs:238–243][f-copy-reserve]).
Reserve owned capacity before taking user/provider captures, and retain charges
until actual disposal. Do not allocate unlimited transaction/log owners.
An admitted earlier record cannot be forgotten because a later record hits a
quota; return the source-consistent retry/error/state and preserve ownership.

**No artificial release gate:** native Begin/Add can return after admission and
launch their apply/replication completion work while other records remain in
the queue
([TransactionManager.cpp:236–260][n-add-manager],
[328–353][n-begin-manager]).
Do not require `completed == assigned`, one partition-wide write lease, a
whole-request reservation or release of each predecessor before admitting the
next record. Port interleaved transaction/LSN ordering and source backpressure.
R0 corrected DD-02, DD-03 and DD-06 instead of compensating with a lower-availability
upper scheduler. Quorum can progress while a lagging member retains history;
ordinary bounded queue exhaustion remains backpressure, not an invented
“fault because release did not follow quorum” rule.
Preserve native general PC availability/apply order statistics even when the
issued local member is not in PC, plus its separate remote-PC-quorum-one
exception. Membership authority and independent release/idle ownership remain
unchanged; a stricter invented commit quorum is not a source-preserving limit.

**Native retention:** lower replication queue release and upper log truncation
are separate. Port the source stable-barrier, completed-checkpoint,
earliest-pending-transaction, physical-reader/copy and provider-version
dependencies, including the checkpoint's replay start before a pending begin
([CheckpointManager.cpp:621–631][n-copy-pending],
[1188–1204][n-stable-primary]).
Use the native transaction map and log/index metadata; do not expire pending
transactions/history, reclaim a copied reader range before disposal, or invent
a ledger-dependent truncation floor. Source retry/cancel/abort transitions
govern pending transactions; hard ownership watchdogs remain a separately
disclosed runtime limit.

Expose assigned/received/logged/flushed/quorum/applied/stable/checkpoint
positions, retained log ranges and pending transactions, captures/budget
ownership, copy stages and native identity/epoch. Diagnostics explain source
state; they do not grant authority or insert another admission/commit fence.

## 9. Persistent roadmap

**Current:** persisted manifests and openings are explicitly rejected
([packages.rs:432–438][f-persisted-package],
[stateful.rs:517–536][f-persisted-open],
[stateful_api.rs:235–255][f-open-mode]).
Do not remove those checks merely because a file log is added.

**C design contract (future):**

1. Define new versus existing replica recovery, local identity/incarnation and
   SF lifecycle behavior. Restore registry, providers, pending transactions,
   decisions and native log/progress history before the source readiness boundary.
2. Define local durable flush and remote durable-ACK rules. Commit success must
   include the chosen durability profile's local log sync, required remote
   durable quorum and source local Apply/completion with its Unlock/fault path
   (§5.4). No volatile memory ACK may be counted as persistent evidence.
3. Port native physical/logical log metadata, recovery dispatch, idempotent
   replay and transaction-chain handling. A Rust log backend must provide the
   source flush/order/truncation guarantees and explicit torn/corrupt-input
   handling; it cannot introduce another transaction-decision record format.
4. Port native checkpoint prepare/perform/complete, copy-log completion/rename
   and state-manager/provider checkpoint publication. Preserve coordinated
   covered LSNs, pending log history and crash recovery. Implement necessary
   filesystem data/directory synchronization; a renamed file alone is not the
   whole durability protocol. Do not replace native copy stages with a custom
   all-state manifest/root installation protocol.
5. Implement progress-vector rejoin/context, partial/full/false-progress copy,
   transaction-aware undo/replay, log-tail truncation and retention fences for
   pending transactions, readers and copy. A committed transaction is not
   necessarily stable enough for truncation.
6. Enable persisted SF package/open paths only after crash/restart/rejoin and
   lifecycle gates pass on a pinned environment. Native KTL-container/disk
   interchange remains a separate compatibility claim even when logical
   record semantics are preserved; reject unsupported versions explicitly.

Backup/restore, incremental backup chains, native disk import/export and
application restore policy are later separate products. None is implied by C.

## 10. Milestones and acceptance gates

These are proposed future verification activities, **not executions or results
from this design pass**. No tier can borrow a lower tier's qualification label.

| Milestone | Work boundary | Required evidence before acceptance |
|---|---|---|
| R0: correct V1 foundation | Implemented lower DD-01–DD-07, explicit latest-LSN capability and callback-ready actual-tail construction seam | Offline phase-reviewed regressions pass, including quorum-one release/ALL, EOS-before-ACK role generations, typed retry/sizing, local-not-PC quorum, extended-copy duplicates, latest start/finish and nonempty bootstrap. No A0 kernels or new native qualification claimed |
| A0: native kernels and test provider | Native records/codecs, transaction states/map, source Apply/Unlock and checkpoint/copy callbacks | Deterministic source-equivalent traces for read-only/single/multi-operation transactions, interleaving, retry/cancel/abort, ID/replay handling and provider visibility |
| A1: volatile Rust integration | Current canonical APIs, lower-bound pass-through, lifecycle/log/provider drains, native memory-log/checkpoint/copy | Requires R0 and A0; native copy stages and incremental transaction flow work with one engine/binder and bounded owned execution; no release-gated serial/root/ledger substitute |
| A2: volatile qualification | Pinned SF binary/environment, pressure and fault campaign | Repeated A scenarios with recorded environment, limits, traces and unsupported modes; only Tier A claim |
| B: mixed-native qualification | Qualify the already source-derived records, transactions, provider routing and copy in both directions | All B scenarios and exact native provider/version/settings matrix; any unsupported feature is explicit, not implemented by an alternate algorithm |
| C: persistent recovery | Durable log/checkpoints/copy publication plus persisted SF lifecycle | All C scenarios with process/host/storage fault injection; persisted manifest/open checks changed only with the full contract |
| D: collection products | Provider-specific APIs, isolation, data formats, lifecycle | Separate conformance suites per map/queue, native/managed bytes, C ABI or disk claim |

### 10.1 Tier A scenarios

| Scenario | Required observable result |
|---|---|
| Read-only, single-operation and multi-operation transactions | Source caching, commit and abort branches, native ID/LSN conventions and states; read-only commit/cached-first abort add no synthetic end record |
| Concurrent interleaved transaction chains with spare queue capacity | Disjoint transactions admit records while preceding records remain retained; no partition write lease or per-record-release prerequisite |
| Multi-key/multi-provider and provider add/remove transaction | Native transaction/context boundaries protect supported read isolation; no partial transaction becomes visible and registry metadata follows source transactional add/remove |
| Provider Apply/Unlock, replication and flush failures | Source completion/error/state and required context cleanup; no skipped record, fake abort proof or blanket hard fault for source-retriable queue pressure |
| Native invocation-bound configuration/build/removal and lower progress | Existing lower binder remains selected; no second engine/binder; exact target/purpose and completed/disposed sync semantics survive; upper provider/install guarantees hold without outer-wrapper interception |
| Direct async sync-control call, reserved owned call and provider reentry | InvalidExecutionContext/reentry rejection is explicit; reserved call completes applied/disposed boundary; dropped observer is not rollback or proof of disposal |
| Native initial progress and no-write promotion with replay capability absent | ALL at the represented prefix follows corrected native rules; a new record beyond that prefix still needs its required ACKs |
| Caller disconnect and native commit timeout/cancel/retry | No orphaned record/context owner; native transaction state/result and retry transitions are preserved without a request ledger or invented rollback |
| Transient queue-full versus permanent operation-size rejection | Capacity recovery allows the same transaction commit to retry successfully; native timeout/cancel states follow retriable failure, oversize does not enter endless retry, and mapping uses no display strings |
| Primary-to-None after an admitted write loses quorum | Corrected terminal cancellation/disposal, not a prerequisite that all writes become committed |
| Current quorum 1, remote PC quorum 1, and idle retention | Corrected distinct native release/commit and ALL rules; no unconditional remote-minimum formula or manufactured quorum; PC/idle release ownership remains independent |
| Local-not-PC with PC quorum greater than one | Native availability/order statistic: CC at 10, PC quorum 2, remote apply [10, 0] yields PC quorum 10; insufficient native session count and remote-PC-quorum-one exception retain their own behavior |
| Promotion with/without pending transactions | Native abort-or-barrier task lifetime, asynchronous role return and role-specific readability; no extra wait-for-all-aborts service gate |
| Empty/nonempty secondary-to-primary-to-secondary stream cycles | Old stream reaches EOS after eligible records; successful upper drain then waits for logical processing/outstanding ACKs; new secondary uses a fresh generation on the same engine without forcing a copy; pressure causes neither hangs nor cancellation-shaped success |
| Copy across checkpoint/pending-transaction boundaries | Native completed state plus replay from before earliest pending begin; later end finds the original chain, not a custom pending-transaction image |
| Latest-LSN copy, checkpoint-driven extension and live replication overlap | Native capability/start/stage/metadata and duplicate handling; no exact-horizon fallback or peer negotiation |
| Extended copy 90→100, lower at 90, overlapping replication delayed | Duplicate epoch control is a native no-op without forced tail equality, provider callback or truncation; overlap later drains; a new incompatible idle-copy epoch still fails explicitly |
| Empty/interrupted copy and cancellation at state/log boundaries | Correct Begin/Set/End, flush/checkpoint completion and ACK ordering; no successful End for interrupted installation |
| More than 64 small operations in a supported bounded transaction | Ordinary records are ACKed/disposed without retaining lower captures until final transaction commit; native flow continues under available capacity |
| Oversize records, exhausted quotas, slow readers/peers and concurrent copy | Explicit source-consistent admission/backpressure or hard ownership failure; bounded retention without silent transaction/history expiry |

For fault cases, record both the caller result and the canonical log/state result;
they may differ without violating the unknown-outcome contract.

### 10.2 Tier B native oracle

Use a **new native transactional** test service with the same deliberately small
custom provider contract and a known native transactional-replicator version.
The historical classic V1 profile is not a current-migration or native V2 oracle
([FubeData.md:3–20][f-profile]).

Test Rust-primary/native-secondary **and** native-primary/Rust-secondary,
including promotion both ways, transaction commit/abort, provider registry
routing, initial bootstrap and maximum segmented records. Cover applicable
no-op, partial, full and false-progress copy; native copy-until-latest capability,
checkpoint-driven horizon extension; overlapping replication; EOS/state/log
installation races; epoch changes mid-copy; native
commit-apply-ACK settings; and unsupported-version rejection. Differentially
compare decisions, prefix/tail/stable positions, provider images and lifecycle
traces—not just equal final maps. A source-inspired Rust codec needs captured
and decoded native evidence; byte equality for one transaction is insufficient.
Also compare first-operation deferral/single-operation commit, interleaved
transaction chains, apply-before-quorum completion with provider locking,
failure-path Unlock, primary/secondary barrier stability, and asynchronous
promotion abort-or-barrier ownership. These are implementation requirements
already in A, not permission to retain a custom core until B.
Include role-scoped stream EOS across repeated promotion/demotion, native
PC availability/order statistics when local-not-PC, lossless queue-full retry,
and duplicate versus new epoch controls with extended-copy overlap delayed.

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
Success means source-consistent supported recovery or an explicit fail-closed
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
| One replicated transaction blob / custom envelope or request ledger | Reject as the core: changes native incremental records, transaction/retry states and copy/recovery semantics |
| Claim mixed-native qualification before the Rust foundation exists | Defer the claim only: native records, bootstrap and applicable copy semantics are implementation requirements in A, with execution evidence added in B |
| Serial release-gated transactions / optimistic global root coordinator | Reject as the core: alternate concurrency, visibility and availability algorithms, not a semantic-preserving port |
| Native transaction/log/provider/copy kernels with Rust ownership | Choose from A: narrow feature support and bounded memory without replacing correctness algorithms |
| Generic outer native-control decorator / replacement binder | Do not require: native V2 forwards these methods to V1; preserve fube's issued lower control path and fix only a demonstrated provider/drain fence gap |

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
| Copy installation or extended horizon | Native latest-LSN capability, stages/completion, independent upper/lower tails and duplicate/new-epoch validation (DD-07) | B/C qualify additional native profile paths, not a new negotiated copy protocol |
| Confusing bound-control pass-through with a missing V2 hook | Native-aligned routing in §3.3; upper guarantees live in provider/drain/lifecycle owners, not redundant configuration/build/remove callbacks | Add only specifically demonstrated integration fences |
| Log-tail named “committed” mistaken for transaction commit | Typed log/quorum/apply/transaction/stable frontiers | Preserve distinctions in durable metrics and API |
| V1 mismatch hidden by an upper workaround | R0 resolves DD-01–DD-07 with source regressions and lossless seam mappings | Do not accept V2 on an unchanged lower semantic mismatch |
| Queue pressure / retained captures | Native throttling/retry, lossless full-versus-size admission mapping (DD-05), interleaved flow and prompt capture disposal | Tune explicit admission limits without adding an all-release commit prerequisite |
| Local-not-PC quorum behavior | Native general PC progress/availability with separate remote-quorum-one exception (DD-06) | Preserve issued membership and independent history retention, not a stronger custom quorum |
| Provider partial visibility or reentrancy | Native Apply/Unlock and provider version/lock contexts; explicit ownership/reentry rules | TStore or other provider isolation needs its own source conformance |
| Pending promotion transactions | Role-scoped EOS/processing completion (DD-04), asynchronously owned abort-or-barrier task and source readability | B/C qualify progress-vector/undo/rejoin paths |
| Retry ambiguity or volatile loss | Source transaction status/state/retry and owned work; no exactly-once/result-ledger promise | Persistence and any optional application idempotency remain separate |
| Source/deployed-binary mismatch | Pin sources separately; require observed native oracle matrix | Each B/C release requalifies its binary/profile |
| Memory and copy pressure | Charged finite owners, source checkpoint/log reader pins and explicit backpressure; native checkpoint-driven copy extension remains valid | Tune explicit admission limits without relaxing ownership or changing the copy state machine |

**Required before A acceptance:** R0 resolution of DD-01–DD-07, native record/transaction
semantics and bootstrap mapping, incremental/interleaved record processing,
native V2 latest-LSN copy capability and applicable native copy stages,
role-scoped stream completion, lossless admission retry and copy/epoch seams,
provider-context visibility and source retry/promotion behavior. No custom
root/ledger/serialized-copy algorithm substitutes for these requirements.

**Deferred without blocking A:** separate atomic/redo-only APIs not initially
supported, broader managed/provider serialization, disk/KTL-container
interchange, backup/restore, TStore/queue products, broader provider read modes,
rolling upgrades, TLS and C ABI. Each must reject unsupported use explicitly.
Create `fube-txn` as a source-derived volatile implementation with a small
test provider and current Rust integration; B qualifies mixed-native behavior
and C extends the source recovery paths to persistence.

## Source references

Link labels in the text identify files and line ranges. Fube references are
relative to this document; native references are pinned to the baseline in §2.

[f-profile]: FubeData.md#L3-L20
[f-native-free]: FubeData.md#L35-L40
[f-authority]: FubeData.md#L3-L8
[f-v1-differences]: FubeData.md#known-semantic-differences-from-native-v1
[f-qualification]: SfApiAlignment.md#L242-L263
[f-data-provider]: ../../crates/libs/fube-data/src/api.rs#L77-L165
[f-stream]: ../../crates/libs/fube-data/src/api.rs#L168-L229
[f-submit]: ../../crates/libs/fube-data/src/api.rs#L218-L259
[f-runtime]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L133-L248
[f-runtime-control]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L133-L248
[f-factory]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L118-L148
[f-open-mode]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L235-L255
[f-primary-control]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L194-L233
[f-fabric]: ../../crates/libs/fube-data/src/fabric_replicator.rs#L19-L70
[f-removed]: ../../crates/libs/fube-data/src/lib.rs#L44-L80
[f-state-access]: ../../crates/libs/fube-data/src/fabric_replicator.rs#L112-L142
[f-epoch-admission]: ../../crates/libs/fube-data/src/extensions.rs#L76-L86
[f-capabilities]: ../../crates/libs/fube-data/src/extensions.rs#L14-L95
[f-diagnostics]: ../../crates/libs/fube-data/src/extensions.rs#L88-L103
[f-native-install]: ../../crates/libs/fube-data/src/engine.rs#L708-L735
[f-initial-progress]: ../../crates/libs/fube-data/src/engine.rs#L629-L660
[f-native-dispatch]: ../../crates/libs/fube-runtime/src/stateful_binding.rs#L695-L720
[f-native-progress]: ../../crates/libs/fube-runtime/src/stateful_reconfiguration.rs#L245-L280
[f-progress-install]: ../../crates/libs/fube-runtime/src/stateful_binding.rs#L309-L323
[f-binding-install]: ../../crates/libs/fube-runtime/src/stateful_binding.rs#L525-L542
[f-native-capture]: ../../crates/libs/fube-runtime/src/stateful_binding.rs#L160-L180
[f-bound-controls]: ../../crates/libs/fube-data/src/engine_binding.rs#L181-L304
[f-progress-getters]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L178-L184
[f-owned-execution]: SfApiAlignment.md#L133-L163
[f-reserve]: ../../crates/libs/fube-runtime/src/execution.rs#L372-L380
[f-run-owned]: ../../crates/libs/fube-runtime/src/execution.rs#L578-L588
[f-reentry]: ../../crates/libs/fube-runtime/src/execution.rs#L482-L498
[f-values]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L30-L58
[f-epoch-domains]: ../../crates/libs/fube-runtime/src/extensions.rs#L9-L25
[f-errors]: ../../crates/libs/fube-runtime/src/error.rs#L273-L335
[f-error-ownership]: ../../crates/libs/fube-runtime/src/error.rs#L1-L12
[f-admission-errors]: ../../crates/libs/fube-data/src/engine.rs#L869-L915
[f-queue-errors]: ../../crates/libs/fube-data/src/queue.rs#L73-L80
[f-resource-code]: ../../crates/libs/fube-runtime/src/error.rs#L213-L224
[f-promotion-stream]: ../../crates/libs/fube-data/src/engine.rs#L4332-L4343
[f-epoch-equality]: ../../crates/libs/fube-data/src/engine.rs#L4288-L4293
[f-order]: ../../crates/libs/fube-data/src/lib.rs#L13-L32
[f-owned-sample]: ../../crates/samples/rust-runtime-app/src/data.rs#L258-L317
[f-progress]: ../../crates/libs/fube-data/src/api.rs#L384-L398
[f-progress-note]: ../../crates/libs/fube-data/src/lib.rs#L27-L32
[f-capture-init]: ../../crates/libs/fube-data/src/api.rs#L26-L40
[f-captures]: ../../crates/libs/fube-data/src/engine.rs#L3483-L3536
[f-wait]: ../../crates/libs/fube-data/src/api.rs#L232-L259
[f-quorum]: ../../crates/libs/fube-data/src/engine.rs#L4404-L4464
[f-snapshot-fields]: ../../crates/libs/fube-data/src/engine.rs#L98-L114
[f-snapshot]: ../../crates/libs/fube-data/src/engine.rs#L779-L791
[f-admission-budget]: ../../crates/libs/fube-data/src/engine.rs#L849-L927
[f-copy-reserve]: ../../crates/libs/fube-data/src/engine.rs#L238-L243
[f-copy-factory]: ../../crates/libs/fube-data/src/supervised.rs#L268-L286
[f-copy-context]: ../../crates/libs/fube-data/src/engine.rs#L3342-L3343
[f-classic-copy-start]: ../../crates/libs/fube-data/src/engine.rs#L2693-L2702
[f-settings]: ../../crates/libs/fube-data/src/api.rs#L12-L74
[f-transport]: ../../crates/libs/fube-data/src/transport.rs#L429-L486
[f-transport-class]: ../../crates/libs/fube-data/src/transport.rs#L613-L618
[f-persisted-package]: ../../crates/libs/fube-runtime/src/packages.rs#L432-L438
[f-persisted-open]: ../../crates/libs/fube-runtime/src/stateful.rs#L517-L536
[n-com]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L204-L269
[n-provider-init]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L280-L295
[n-pass-through]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L478-L603
[n-lifecycle]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L654-L885
[n-abort]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L482-L486
[n-provider-callbacks]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/StateProvider.cpp#L59-L109
[n-create]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L85-L120
[n-state-replicator]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/IStateReplicator.h#L12-L29
[n-state-provider]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/IStateProvider.h#L12-L68
[n-frontiers]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L435-L444
[n-replicate-log]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L382-L395
[n-assigned]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp#L915-L957
[n-kinds]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/LogRecordType.h#L18-L46
[n-logical-codec]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/LogicalLogRecord.cpp#L78-L96
[n-transaction-codec]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/logrecordlib/TransactionLogRecord.cpp#L110-L166
[n-transaction-ids]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L78-L127
[n-transaction-states]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionStateMachine.cpp#L91-L193
[n-retry-statuses]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L197-L209
[n-visibility-registration]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L387-L404
[n-version-registration]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L276-L320
[n-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1340-L1417
[n-bootstrap]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LogManager.cpp#L286-L330
[n-provider2]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProvider2.h#L64-L155
[n-provider2-full]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/IStateProvider2.h#L12-L155
[n-apply]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L768-L920
[n-service-error]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L952-L989
[n-unlock-error]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L607-L615
[n-transaction]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L328-L407
[n-commit-wait]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L498-L516
[n-add-operation]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L238-L310
[n-private-commit]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L406-L485
[n-transaction-abort]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L131-L189
[n-abort-task]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L310-L383
[n-add-manager]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L236-L260
[n-begin-manager]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp#L328-L353
[n-stable-primary]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1188-L1204
[n-stable-secondary]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L1554-L1578
[n-flushed]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp#L333-L356
[n-drain]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L805-L889
[n-drain-finished]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L862-L889
[n-drain-completion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L224-L235
[n-secondary-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.CloseAsyncOperation.cpp#L41-L104
[n-secondary-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.UpdateEpochAsyncOperation.cpp#L43-L92
[n-epoch-duplicates]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1368-L1401
[n-ack]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L908-L970
[n-memory]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L159-L168
[n-named]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/NamedOperationData.h#L12-L44
[n-routing]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/statemanager/StateManager.cpp#L1290-L1344
[n-store]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L25-L43
[n-store-init]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L1536-L1555
[n-store-checkpoint]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L1799-L1818
[n-store-apply]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L2251-L2280
[n-store-locks]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/tstore/Store.h#L2251-L2394
[n-open]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L654-L711
[n-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L735-L799
[n-role]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L830-L885
[n-promotion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L989-L1076
[n-promotion-task]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1110-L1245
[n-upper-role]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L388-L474
[n-readable]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L422-L433
[n-upper-abort]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/TransactionalReplicator.cpp#L378-L386
[n-copy-null]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp#L165-L215
[n-latest-marker]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/ComStateProvider.h#L16-L28
[n-latest-detection]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ComProxyStateProvider.cpp#L19-L27
[n-latest-start]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationQueueManager.cpp#L165-L193
[n-copy-pending]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp#L621-L631
[n-copy-state-drain]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L974-L1085
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
