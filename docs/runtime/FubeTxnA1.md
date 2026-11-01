# Private A1 integration status and provenance

The private transaction kernels now use actual fube-data admission/LSNs and
separate local flush, Apply, quorum and Unlock. Full provider/stream lifecycle
integration is implemented and both-model reviewed as a private Phase2 unit. Native
Full-copy registry/hierarchy/checkpoint integration (Phase3) and
aggregate acceptance remain unfinished. This is not a public runtime construction
API, native-binary qualification, mixed-native or recovery/restore claim.

## Native algorithms versus Rust adapters

Authority: Service Fabric source `51a7703a1f83749a9234d3ceb4296d8622e42cca`.
Source-derived fixtures are not executions of that native binary.

- **Native admission:** V1 `ReplicationQueueManager.cpp:369–409` calls ordinary
  `OperationQueue::TryEnqueue`; its sequence advances only after success.
  `OperationQueue.cpp:285–365` can return REQueueFull. It does not distinguish
  transaction Begin, End or Barrier payloads.
- **Native upper policy:** `LogicalLogRecord.cpp:58–65` permits writer/truncation
  throttle only for Begin/Operation. `CheckpointManager.cpp:882–930` supplies
  ServiceTooBusy from those actual conditions. End/Barrier bypass that upper
  throttle, NOT V1 queue-full.
- **Native retries:** `CheckpointManager.cpp:1144–1239,1632–1640` uses ordinary
  Barrier admission, retries failures while Primary/not-closing with doubled
  delay capped5000ms, then awaits Apply/replication and resets2ms. The separate
  transaction whitelist is exactly `TransactionBase.cpp:197–208`. Allocation
  exhaustion must not be disguised as throttle or added to that whitelist.
- **FUBE ADAPTATION — lifecycle binding:** optional hooks inside common
  fube-data root/bound entries implement native lower-first composition
  (`ComTransactionalReplicator.cpp:654–711,735–885`). Wrapper-only composition
  cannot observe controls bound directly to the existing lower engine. Hooks
  do not install another binder or change native membership/role authority.
- **FUBE ADAPTATION — bootstrap bridge:** native `LogManager.cpp:246–341` writes
  the zero/one image after lower Open. Fube samples progress during construction,
  so a one-time actual-provider-getter rebase reconciles the still-unused lower
  queue. No caller supplies an LSN. This is not a ported native queue command.
- **FUBE ADAPTATION — execution/ownership:** host deadlines, pure-publication
  reentry checks and supervised capture/owned-future retention are Rust plumbing.
  The upper retains accepted disposition/Apply/quorum/Unlock independently of
  observer Drop; application objects never enter lower owner/destructor locks.
  These mechanisms are not queue/admission algorithms or native qualification.

## Withdrawn policy and finite profile

The earlier End/Barrier `LogReservation`/renewable grants were newly designed
allocation/backpressure policy, not native semantics. They could reject ordinary
writes earlier and preferentially admit termination/dispatch. They are removed,
not relocated into txn: Begin/Operation/End/Barrier use canonical lower replicate.
UpdateEpoch remains a local marker, not a replicated new LSN.
The earlier local review PASS did not establish native admission equivalence.

Finite Rust object/history allocations remain explicit safety bounds. They use
uniform record allocation, with no protected End/Barrier/epoch/checkpoint capacity
or full-queue progress guarantee. Allocation failure returns documented
InsufficientResources before lower assignment; a pre-record group allocation
failure is an explicit terminal Rust adapter. An actual lower admission error
retains native fault/group-retry behavior. At persistent native queue-full, retries
may remain stalled; only actual queue progress changes eligibility.

After real lower assignment, local write failure retains the actual Activity,
map and LSN, completes flush/Apply with error, awaits actual quorum and invokes
required Unlock. Physical construction failure creates no substitute Information
record or successful drain. Full physical writer/checkpoint ownership is Phase3.
Supported finite limits are not native allocator/queue-size equivalence.

Concrete **FUBE ADAPTATION** limits, not native sizing equivalence:

| Boundary | Current private limit | Observable effect |
| --- | --- | --- |
| Retained admission payloads | 32 (`kernel/runtime_lower.rs:104`) | Uniform pre-record InsufficientResources, never End/Barrier priority |
| Admission execution | ≤30 seconds, also bounded by caller's reserved deadline (`runtime_lower.rs:436–439`) | Execution expiry is failure/fencing, not queue-full or native retry timing |
| Memory log | 512 KiB–16 MiB (`kernel/memory_log.rs:97–103`) | Uniform allocation exhaustion; no future checkpoint capacity guarantee |
| Progress vector | 128 entries (`kernel/processor.rs:1068–1079,1224–1232`) | InsufficientResources before publication, no automatic native trim claim |
| Memory-log copy readers | 16 (`memory_log.rs:506–509`) | InsufficientResources; successful pins retain head until explicit release |

Preexisting R0 copy-headroom allocation mechanics are unchanged by this
correction; they are not End/Barrier preference or native queue-size equivalence.

## Private Phase2 provider/lifecycle/drain unit

`kernel/runtime.rs:74,190,251` constructs ONE lower/binder, bootstraps after lower
Open and executes actual Initialize/Open/Recover before successful provider roles/
app readiness. A private weak forwarding owner supplies supported transaction/
version callbacks; dynamic registry/factory/hierarchy is not implemented here.

Native Idle→Active waits copy only and pauses dispatch around provider roles;
Primary promotion joins old copy/replication/log processing. Generation-owned
pumps ACK normal secondary only after local flush and drop the lower capture
before next poll. Copied LSNs stay embedded; replication uses lower metadata.
Native Information at current tail dispatches buffered groups on EOF/role/Close/
Abort without a fabricated replicated LSN. Close owns drain/provider cleanup;
sync Abort retains scheduled work and is not completed upper drain.

Failure completion is not disposal: a failed Information construction reports
the terminal **attempt** error promptly while a strong cleanup continuation waits
actual logical processing, actual quorum observers and service cleanup entries.
Provider Close never runs under possible Apply. Cleanup has its own completion;
no failed attempt becomes successful drain, no boundary is fabricated and no
automatic allocation/Information retry or fairness guarantee is implied.
Unresolved terminal failures may require native disposal/process fencing.

Temporary app readiness and native service cleanup eligibility are distinct.
Cached Abort can Unlock during a role/fault app fence; true service closure
retains native closed-state behavior. Direct provider Unlock error sends the
native StateManager fault notification without a processor service-error latch.
Terminal/fault state dominates readiness reopen, and closed-owner version
callbacks reject after actual cleanup.

First fault follows `RoleContextDrainState.cpp:54–68`: closes upper admission/
reports once, DOES NOT issue lower Abort or synthesize accepted quorum success.
Runtime's native reconfiguration/Close/Abort remains sole control authority.
Native read/write semantics include transient write revocation and readable Active
fallback; role alone is not writable admission.

Native None/Log staged frames through actual lower Build are **private ownership
fixtures**, not normal volatile null-context Full negotiation. Unsupported stages
reject; copied-log failed-flush waiter ACK asymmetry and no-ACK replication failure
are retained. Full/PV/registry/hierarchy/checkpoint/truncation are Phase3; no copied
state End or full runtime/native qualification credit is borrowed.
Copied UpdateEpoch uses the supported existing-record failed-flush settlement
path, not control epoch preflight. Actual primary epoch callbacks wait admitted
publication/order before the local prefix.
