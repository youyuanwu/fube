# Private A1 integration status and provenance

The private transaction kernels now use actual fube-data admission/LSNs and
separate local flush, Apply, quorum and Unlock. Full provider/stream lifecycle
(Phase2), native Full-copy registry/hierarchy/checkpoint integration (Phase3) and
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
| Retained admission payloads | 32 (`kernel/runtime_lower.rs:102`) | Uniform pre-record InsufficientResources, never End/Barrier priority |
| Admission execution | ≤30 seconds, also bounded by caller's reserved deadline (`runtime_lower.rs:421–424`) | Execution expiry is failure/fencing, not queue-full or native retry timing |
| Memory log | 512 KiB–16 MiB (`kernel/memory_log.rs:97–103`) | Uniform allocation exhaustion; no future checkpoint capacity guarantee |
| Progress vector | 128 entries (`kernel/processor.rs:913–920`) | InsufficientResources before publication, no automatic native trim claim |
| Memory-log copy readers | 16 (`memory_log.rs:499–502`) | InsufficientResources; successful pins retain head until explicit release |

Preexisting R0 copy-headroom allocation mechanics are unchanged by this
correction; they are not End/Barrier preference or native queue-size equivalence.
