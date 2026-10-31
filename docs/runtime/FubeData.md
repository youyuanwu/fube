# Fube Data: volatile classic V1 replication

`fube-data` is an owned Rust replication engine, separate from the SF-facing
control contract in `fube-runtime`. It implements a **qualified subset** of
classic V1: ordered nonzero sequences, explicit application acknowledgments,
current/previous configuration quorums, epoch reconciliation, bounded copy and
replication streams, retries and native Fabric TCP peers. Native SF nodes remain
the membership, role, epoch and placement authority; there is no Rust election.

The executed profile is **Linux x86-64, SF 11.8.121.4, volatile state,
RequireServiceAck=true, clear trusted loopback, activated ExeHost/SharedProcess**.
No persistence, transactional replicator/Reliable Collections, TLS/Windows
replication, rolling executable upgrade, whole-set restart preservation or
universal V1 parity is promised. Acknowledgment is not disk durability.

The [current SF/mssf four-axis alignment record](SfApiAlignment.md) separates
canonical signature from values, completion and capability. The deployed
results in [executed qualification](#executed-qualification) are **historical at
their exact old source/profile**, not qualification of this breaking migration.
Current API-alignment deployed/native validation is **NOT RUN**.
The [known semantic differences](#known-semantic-differences-from-native-v1)
below are unresolved implementation issues, not intended SF semantics.

## Components and selection

```text
native node -- Hosting/RAP control --> fube-runtime
                                      |
                         fube-data Engine (owned Rust)
                         |                         |
              application provider         Fabric TCP peers
              copy/apply + explicit ACK     Rust or native classic V1
```

`fube-data` depends on `fube-runtime` and `fube-transport`, not the reverse.
It reuses bounded framing/serialization but owns peer listener/session policy,
not the gateway client's connection assumptions. Production engine/runtime/app
have no FabricRuntime, FabricCommon, COM or native replicator dependency.
See [crate exports](../../crates/libs/fube-data/src/lib.rs),
[peer transport](../../crates/libs/fube-data/src/transport.rs).

The existing mixed `rust-runtime-app` keeps `RustRuntimeType` stateless and
`RustStatefulRuntimeType` volatile stateful in the same executable/code package.
The **default remains empty-state**. Exact service initialization bytes
`fube-data-v1` select the data implementation
([selector](../../crates/samples/rust-runtime-app/src/data.rs#L24-L30));
the data harness creates services with those bytes, preserving unrelated
stateless registration. This is not an environment role override.

## Owned API and provider obligations

- `FabricReplicator::create` consumes a canonical partition and explicit
  runtime-issued native integration, provider and endpoint/settings capabilities.
  Control and state-replicator views share one private engine/listener;
  no standalone public control authority is supplied
  ([public construction and Rustdoc](../../crates/libs/fube-data/src/fabric_replicator.rs#L19-L70)).
  This helper lives in data to preserve data → runtime dependency. Native
  partition `CreateReplicator`, mssf `try_get_com`, COM proxy/ABI and mutable
  native settings are not supported SDK members. Canonical root and private
  bound configuration/build/remove controls are functioning paths to that same
  owner, not a separate extension authority.
- `IStateProvider` supplies synchronous `get_last_committed_sequence_number`,
  `get_copy_context`/`get_copy_state` factories, async `update_epoch` and
  bool `on_data_loss` callbacks
  ([contract and Rustdoc](../../crates/libs/fube-data/src/api.rs#L129-L165)).
  The included `MemoryProvider::get_last_committed_sequence_number` reports
  **locally applied history**, potentially ahead of quorum. The standard name
  does not imply network commitment/durability. Custom providers/applications
  own their data, local-apply and retry policies.
- Separately optional `extensions::ReplayCheckpointProvider::replication_checkpoint(sequence, cancellation)` returns
  an authentic original operation at exactly that applied/completed-copy
  sequence: original epoch, bytes **and segment boundaries**. An absent capability
  means unavailable history; a supplied callback may return bounded `None`.
  Unavailable provenance must not be reconstructed from current
  state or relabeled with the current epoch. Callback/capture supervision is
  unchanged. The engine reserves bounded operation ownership before requesting
  a checkpoint; this is not a permanent extra retained-queue allowance.
- `IStateReplicator::replicate` asynchronously admits an early sequence and
  separate `ReplicationCompletion` waiter.
  The primary must apply that sequence locally **before** waiting for quorum.
  Its accepted operation and local apply must outlive a disconnected caller;
  canceling/dropping `wait` does not undo admission
  ([public completion wait](../../crates/libs/fube-data/src/api.rs#L240-L259),
  [sample owned admission](../../crates/samples/rust-runtime-app/src/data.rs#L258-L325)).
  Rejection before admission consumes no LSN; cancellation/drop/disconnect
  **after** admission is ambiguous, not rollback or proof of disposal. This
  primary local-apply-before-wait policy is not .NET prepare → quorum →
  ordered local commit parity.
- Secondary application pumps consume copy then replication; apply before
  `IOperation::acknowledge`. Provider `IOperationDataStream::get_next` is
  payload-only; destination `IOperationStream::get_operation` yields metadata/
  data/explicit ACK. Receipt, polling and Drop do not acknowledge.
  A late retired-owner ACK fails rather than reviving progress
  ([ACK and stream contract](../../crates/libs/fube-data/src/api.rs#L173-L267)).
- Provider callbacks run outside engine/transport locks. Returned errors,
  panics, cancellation and timeout are explicit; failed barriers cannot grant
  readiness. Qualified runtime watchdog guards supervise actual callback and
  capture disposal, not merely future cancellation
  ([supervision](../../crates/libs/fube-data/src/supervised.rs)).

Potentially waiting synchronous control calls require the native/provider owned
execution entry or an explicit capture-free reservation, not `.await` on a
Result or `block_on` in arbitrary async context. Unknown async entry returns
`InvalidExecutionContext`. Execution/native-binding/cleanup integration is
newly written fube infrastructure using standard Tokio/JoinSets with thin
deadline/context/disposal wrappers. Replay/origin/epoch-checked capabilities
reorganize preexisting fube behavior, not SDK ports. App-supplied scheduler
implicit children remain application-owned; services stop/join their tasks.
No arbitrary-descendant adoption or stalled external-runtime progress is promised.

`MemoryProvider` is the included bounded map/history/request-result ledger,
not a storage subsystem. Mutations contain `id`, `key`, `value`.
Identical retries retain the original sequence; conflicting identity payloads
fail. Ledger/history travels with copy and epoch reconciliation. It rejects
new identities at **4096 operations / 16 MiB**, without silent expiry
([provider](../../crates/libs/fube-data/src/provider.rs#L25-L72),
[admission/apply](../../crates/libs/fube-data/src/provider.rs#L125-L187)).
Applications implementing other providers own their payload semantics,
reconciliation, exact-boundary snapshots and retry guarantees.

The public `Mutation { id, key, value }` shape is unchanged.
`Mutation::operation_for_epoch(epoch)` emits version-2 application JSON carrying
the genuine admitted primary operation epoch. Both sample producers fence this
against their real authority: Rust uses the native snapshot's primary epoch
and explicit `extensions::EpochCheckedReplication::replicate_in_epoch`; the native oracle uses its actual provider
UpdateEpoch callback under the admission gate, returning pre-admission 503 if
that epoch is unavailable. Native secondary operation metadata has no epoch;
the opaque application bytes preserve its origin even when that secondary's
provider has never received UpdateEpoch. Legacy `operation()`/version-1 copy
records remain readable but provide **no** replay checkpoint.

Version-2 copy records carry the original concatenated UTF-8 JSON string and
original byte segment lengths, including empty segments and splits within a
UTF-8 code point. They do not re-encode the replay operation. Admission validates
nested JSON escaping and segment metadata against the 1 MiB copy-item limit;
provider accounting includes retained replay buffers/descriptors. Snapshot pins
keep their charges through reconciliation until actual disposal, preventing
truncation from recycling still-owned memory. Epoch/data-loss reconciliation
retains or discards matching replay metadata together with history and ledger.
These payload additions require updated participating sample executables;
rolling upgrades remain unsupported. No historical qualification below is
evidence for the new checkpoint path unless its exact source includes it.

## Progress, quorums, copy and failures

`assigned`, `received`, `applied`, `committed`, `client_completed` and `completed`
are different frontiers. Secondary received progress is not application ACK;
neither is a global quorum proof. The primary's queue `applied` may remain its
reconciliation frontier while actual provider state has advanced locally.
Inspect provider progress separately; socket-write success never means commit
([progress](../../crates/libs/fube-data/src/api.rs#L384-L397),
[quorum calculation](../../crates/libs/fube-data/src/quorum.rs)).

Current and previous configuration write obligations remain separate, including
the previous-quorum-one case when the new primary was not in that configuration.
The current local-not-PC quorum calculation nevertheless differs from native
for previous quorum greater than one (DD-06 below).
Required targets and ALL catch-up are not ordinary write quorum. Removal does
not shrink native membership to manufacture success. Epoch/role revisions,
replica and engine incarnations, original operation epochs and build sessions
fence stale data/ACK/copy receipts. Repeated promotion with no intervening write
binds a secondary session only from the exact current native-selected primary
descriptor/epoch, never an arbitrary epoch-less RequestAck. **Current fube
behavior (DD-02 below):** fresh network ACKs drive write commitment and ALL
catch-up. Native-advertised progress may attest
only the local already-completed prefix for WRITE_QUORUM catch-up; it is
separately labeled `native_catchup_prefix`, cannot acknowledge a new write, and
cannot satisfy ALL.

Copy pins coherent state **and retry ledger** at the native-selected
`replicationStart - 1` horizon, even while the provider and new writes advance.
Live replication buffers behind copy and joins at its explicit finish fence;
unavailable history fails instead of selecting a different boundary. Native
volatile copy context is null. A successful canceled Build invocation's scope
is distinct from the lifetime of the established configured peer.
Provider factories and state stream getters are synchronous; enumeration is
async. Provider payload enumeration has no ACK. Destination copy **and**
replication require a once-only fenced service ACK after apply. `Some(empty)`
is data, `None` is EOS; internal legacy-null EOS retirement cannot fill an
unapplied data gap or manufacture a new application operation/ACK.

Promotion freezes and drains the contiguous received prefix through actual
application ACK, including operations not yet dispatched because captures
exhausted their budget. It does not silently reuse their sequence numbers;
only a noncontiguous tail can be discarded. Original-operation epoch barriers
remain ordered during that drain. **Current gap (DD-04 below):** promotion does
not close the old replication stream generation, so this ACK drain is not
native stream EOS or completion of an upper V2 drain.
Every configuration installation captures the
latest assigned catch-up fence. Departed adopted sessions are retired when
absent from both active configurations, without retaining obsolete copy fences
or waiting for a delayed RemoveIdle to free their budgets.
The admitted control's absolute deadline and cancellation wake the owner even
when no peer, application ACK or retry event arrives.

An already completed native secondary has no primary target immediately after
demotion; RequestAck deliberately cannot establish one. If the promoted Rust
primary has no ordinary retained suffix, it requests a bounded
`extensions::ReplayCheckpointProvider::replication_checkpoint` and retransmits that **authentic original
operation** at the existing LSN with its unchanged original epoch/segments.
The duplicate establishes the native primary target without applying again or
assigning a new LSN. Only the resulting real receive/application ACK can satisfy
ALL. This is a fube-specific compatibility workaround associated with DD-02,
not an original SF provider callback or a required V2 algorithm.
One temporary charged callback owns this recovery path; there is no
permanently retained free anchor or unbounded extra queue.
Cooldown starts only after actual payload/capture disposal, fanout, publication
and producer-admission unlock. The next retry wakes independently, but holds no
data slot while waiting; an unavailable checkpoint or transport backpressure
cannot turn completion into an immediate retransmission loop. `None` is not
permission to fabricate an operation or grant catch-up.

Read/write admission requires current native access, live lifetime/host
authority, agreement of native service/replicator/engine roles and epoch, and
actual replication readiness
([authority gate](../../crates/samples/rust-runtime-app/src/data.rs#L41-L76)).
No Primary flag, management observation or diagnostic endpoint alone grants
access. Public SDK descriptors/diagnostics cannot construct private runtime-issued
proof; retaining proof cannot prevent revocation. Trusted clear IPC is not
cryptographic host authentication or global unforgeability. There is no
issuer-less standalone primary-control support. Offline model hosts drive real
runtime/RAP issuance but are not native deployment.
Terminal authority cannot revive. Per-unit Close/Abort removes only its
owned routes and drains work; actual shared-process termination affects both
service kinds. Existing lease, callback and destructor watchdog limits remain.

Successful volatile writes are acknowledged under the selected native quorum,
not preserved through loss of the entire replica set. Timeout/disconnection,
authority loss or failure after admission may yield **ambiguous completion**:
retry the same identity, never assume rollback. Epoch/data-loss reconciliation
can discard an ambiguous tail. Loss of all volatile state loses the ledger too.

### Default bounds

| Resource | Bound |
|---|---|
| One operation | 1 MiB / 1024 segments |
| Engine retained data | 1024 operations / 16 MiB, accounting descriptor overhead |
| Reserved control | 64 messages / 1 MiB |
| Peer connections, replicas/configuration, active copies | 64 each |
| Dispatched unacknowledged provider operations | 64 |
| Retry / provider callback deadline | 250 ms / 8 s |

`Settings::validate` rejects inconsistent/out-of-profile settings before opening;
resource exhaustion is explicit backpressure/rejection, not unlimited tasks.
See [settings and validation](../../crates/libs/fube-data/src/api.rs#L12-L82).
The current error surface collapses transient queue fullness and permanent
operation-size rejection into ResourceExhausted; native transaction retry needs
the lossless admission distinction specified in DD-05 below.
The sample's HTTP request limit is 4096 bytes; its client waiter is five seconds.

## Known semantic differences from native V1

**Audit date:** 2026-10-30. **Fube source:** `2f28b963b2b55020bb32ac73e7874b37b3a509fc`
(#19). **Authoritative native source:** local SF checkout
`51a7703a1f83749a9234d3ceb4296d8622e42cca`, linked immutably below.
These are source-comparison findings; no implementation correction or new
native qualification is claimed by this documentation update. Earlier
qualification records retain their exact historical scope.
An independent Astra source review confirmed DD-01–DD-03 and identified
DD-04–DD-07. The latter include lower integration gaps exposed by a native V2
consumer, not a claim that every current classic provider encounters them.

### DD-01: terminal Primary-to-None incorrectly requires commitment

**Current fube:** the role-change guard requires `assigned == committed` whenever
a Primary changes to any non-Primary role, including None
([engine.rs:2054–2063](../../crates/libs/fube-data/src/engine.rs#L2054-L2063)).
With an admitted uncommitted write and unavailable quorum, this rejects the
terminal role transition.

**Native:** the Primary-to-None path closes the primary without creating a
secondary. Its close takes pending replicate operations before closing sessions
and cancels them; the committed-tail invariant belongs to transferring the
queue into a new secondary, not every terminal close
([ChangeRoleAsyncOperation.cpp:276–348][sf-terminal-role],
[ReplicaManager.cpp:787–866][sf-terminal-close],
[ReplicationQueueManager.cpp:97–106][sf-demotion-transfer]).

**Required correction:** separate terminal disposal from planned queue transfer.
Primary-to-None must retire admission, cancel unresolved completion observers
and complete owned cleanup without manufacturing quorum commitment.
Preserve the source's queue-transfer checks for actual demotion to a secondary.
Required regression: admit a write, lose quorum, request None, and observe
terminal cleanup plus unsuccessful/uncertain write completion rather than
a commitment prerequisite. Do not infer rollback of already applied state.

### DD-02: native-issued initial progress is excluded from ALL catch-up

**Current fube:** newly configured peers start with receive/apply progress zero;
native progress is separately capped to the local completed prefix and used only
for WRITE catch-up, not ALL or ordinary record-quorum calculation
([engine.rs:2567–2595](../../crates/libs/fube-data/src/engine.rs#L2567-L2595),
[2621–2638](../../crates/libs/fube-data/src/engine.rs#L2621-L2638),
[3900–3940](../../crates/libs/fube-data/src/engine.rs#L3900-L3940)).
The [existing regression](../../crates/libs/fube-data/src/engine_tests.rs#L1936-L2000)
deliberately asserts that ALL remains pending despite native-issued completed
progress. The authentic replay-checkpoint path above compensates for that
different initial sender state.

**Native:** `CreateReplica` passes the native descriptor's `CurrentProgress`
into the replication session. `ReliableOperationSender` initializes receive,
apply and highest-operation frontiers from it. ALL catch-up uses the resulting
current-configuration apply frontier; there is no universal fresh-network-ACK
requirement for an already represented prefix
([ReplicaManager.cpp:1205–1216][sf-create-session],
[ReplicationSession.cpp:34–72][sf-session-progress],
[ReliableOperationSender.cpp:28–49][sf-initial-progress],
[ReplicaManager.cpp:1061–1106][sf-catchup-progress]).

**Required correction:** preserve the native sender initialization and subsequent
ACK rules using the runtime-issued, incarnation/revision-bound descriptor.
Do not accept arbitrary public descriptors or diagnostic observations as
authority, and do not let initial progress acknowledge records assigned after
that prefix. Initialize new/recreated sender state; do not blindly overwrite an
existing session's counters from a repeated descriptor. ALL uses the native
current-configuration apply/latest frontier; WRITE retains its catch-up fence
and required-target rules. V2 must not require an invented replay callback to
replace this native rule. Required regressions: no-write promotion and ALL at an already
reported prefix with a standard-only provider; then a new record beyond that
prefix still waits for its required real ACKs. Retained old “ALL must wait”
tests must change with the correction, not define the intended semantics.

### DD-03: quorum-one primary release waits for remote receive ACKs

**Current fube:** current quorum 1 can commit at the assigned tail, but
`all_received` still includes remote members and limits release
([quorum.rs:63–85](../../crates/libs/fube-data/src/quorum.rs#L63-L85),
[engine.rs:4436–4460](../../crates/libs/fube-data/src/engine.rs#L4436-L4460)).
With no previous configuration or idle build, a lagging secondary can therefore
pin already committed operations and exhaust the bounded queue.
The same remote `all_applied` minimum also holds ALL pending at the latest
assigned tail
([engine.rs:3929–3932](../../crates/libs/fube-data/src/engine.rs#L3929-L3932)).

**Native:** for `writeQuorum <= 1`, the current-configuration progress helper
returns the noninitialized sentinel for quorum/all/completed progress. The queue
manager interprets that result as commitment/completion through its applicable
tail, rather than waiting for current remote receive ACKs
([ReplicaManager.cpp:349–363][sf-quorum-one],
[ReplicationQueueManager.cpp:287–307][sf-quorum-one-release]).
The same sentinel is used for current-configuration catch-up progress; the queue
manager maps it to the latest tail rather than an unconditional remote minimum
([ReplicaManager.cpp:2439–2458][sf-current-catchup],
[ReplicationQueueManager.cpp:319–357][sf-catchup-sentinel]).
This is distinct from native's previous-quorum-one special case, where a remote
PC member's apply ACK must constrain progress
([ReplicaManager.cpp:2475–2519][sf-previous-quorum-one]).
Idle-build retention remains an independent source obligation.

**Required correction:** preserve those distinct current-quorum-one,
previous-configuration and idle-retention rules rather than applying an
unconditional remote-minimum release formula. Required regressions: current
quorum 1 with a lagging configured secondary and no PC/idle owner releases
committed entries and follows native ALL catch-up rules; a remote
previous-quorum-one obligation still constrains progress; an active idle build
still retains its required history.

### DD-04: promotion does not finish the secondary stream generation

**Current fube:** copy/replication channels and views are constructed once.
Secondary-to-Primary clears secondary state but retains the replication sender;
an empty stream keeps waiting instead of returning EOS
([engine.rs:529–532](../../crates/libs/fube-data/src/engine.rs#L529-L532),
[4332–4343](../../crates/libs/fube-data/src/engine.rs#L4332-L4343),
[api.rs:349–371](../../crates/libs/fube-data/src/api.rs#L349-L371)).
The current provider's ACK drain therefore does not supply the successful
stream-completion boundary required by native V2 promotion.

**Native:** secondary close during promotion closes dispatch queues and waits
for the required drain. The upper `SecondaryDrainManager` observes replication
EOS, processes the replication-finished information record, waits for logical
processing and outstanding ACKs, and only then signals its drain completion
([SecondaryReplicator.CloseAsyncOperation.cpp:41–104][sf-secondary-close],
[SecondaryDrainManager.cpp:862–889][sf-upper-drain-finished],
[224–235][sf-upper-drain-completion]).

**Required correction:** preserve role-scoped stream EOS and processing
completion without discarding the eligible contiguous prefix. Cancellation is
not successful native drain; do not manufacture an operation or ACK as EOS.
A later transition back to secondary needs a new stream generation in the same
engine, preserving native transferred state rather than forcing another copy,
with old views/captures fenced. Required regressions: empty and nonempty
secondary-to-primary-to-secondary cycles, delayed ACKs and capture pressure;
old drains finish, new drains consume only the new generation, and native upper
promotion cannot wait forever on a live empty channel.

### DD-05: admission errors lose the native retry distinction

**Current fube:** permanent operation byte/segment rejection and transient write
admission fullness both return `Error::ResourceLimit`; the queue uses that
variant for both size and retained-capacity checks
([engine.rs:869–915](../../crates/libs/fube-data/src/engine.rs#L869-L915),
[queue.rs:73–80](../../crates/libs/fube-data/src/queue.rs#L73-L80)).
They expose ResourceExhausted, with the same fallback native code, rather than
the native replication-queue-full cause
([error.rs:213–224](../../crates/libs/fube-runtime/src/error.rs#L213-L224),
[273–317](../../crates/libs/fube-runtime/src/error.rs#L273-L317)).
That surface alone cannot drive a source-faithful V2 retry classifier.

**Native:** queue fullness returns `REQueueFull`, propagated by the replica
manager. The transaction retry classifier includes replication-queue-full,
not every resource/size failure. A failed nonretriable commit faults the
transaction; timeout/cancellation returns it to Active only after a classified
retriable failure
([OperationQueue.cpp:291–359][sf-queue-full],
[ReplicaManager.cpp:2105–2144][sf-admission-error],
[TransactionBase.cpp:197–209][sf-transaction-retry],
[Transaction.cpp:448–477][sf-commit-retry]).

**Required correction:** retain a typed/native-coded cause or provide a tested,
lossless admission-stage mapping that distinguishes temporary queue pressure
from permanent invalid sizing and other exhausted resources. New public error
variants are not mandatory; display-string parsing and blanket ResourceExhausted
retry are not acceptable. Preserve admitted work and actual transaction state.
Required regressions: temporary full queue, capacity returned, same transaction
commit succeeds; timeout/cancellation takes the native retry-state branch;
oversized records fail without an endless queue-full retry loop.

### DD-06: local-not-PC quorum greater than one requires an extra vote

**Current fube:** availability and the remote apply order statistic subtract a
local vote only when the issued configuration has `local_member=true`
([quorum.rs:47–50,71–79](../../crates/libs/fube-data/src/quorum.rs#L47-L79)).
Native reconfiguration can legitimately issue `local_member=false` for PC
([stateful_reconfiguration.rs:1637–1673](../../crates/libs/fube-runtime/src/stateful_reconfiguration.rs#L1637-L1673)).
With CC satisfied at 10, PC quorum 2, local-not-PC, and remote apply `[10, 0]`,
fube's PC quorum frontier is 0.

**Native:** the general availability/progress helpers assume an immediately
ACKing primary and require `writeQuorum - 1` remote sessions/ACKs even in this
PC case. The source explicitly limits its local-not-PC exception to previous
quorum 1 with remote members; PC quorum 2 in the example yields 10
([ReplicaManager.cpp:349–390][sf-general-quorum],
[1316–1373][sf-quorum-availability],
[2475–2519][sf-previous-quorum-one]).
Independent all-member completion/idle retention still prevents inappropriate
history release; the source does not strengthen the general commit formula.

**Required correction:** port native PC availability and apply order statistics,
including the separate remote-PC-quorum-one branch. Do not rewrite issued
membership, mint a local vote or shrink the configuration to obtain the result.
Required regressions: local-not-PC with quorum 2 and unequal ACKs, enough and
insufficient remote sessions under the native rule, the remote-PC-quorum-one
exception, and independent release/idle ownership.

### DD-07: extended-copy tails fail unconditional epoch reconciliation

**Current fube:** secondary admission-only epoch controls query provider progress
without invoking UpdateEpoch, but publication still requires that value equal
the lower queue boundary
([engine.rs:4001–4053](../../crates/libs/fube-data/src/engine.rs#L4001-L4053),
[4288–4293](../../crates/libs/fube-data/src/engine.rs#L4288-L4293)).
Native V2 extended copy can leave upper log tail 100 while lower progress is 90;
with duplicate replication 91–100 delayed, a duplicate epoch control would
fault despite being a native no-op. This is a V2 integration counterexample,
not an already implemented V2 failure.

**Native:** duplicate secondary UpdateEpoch returns without a provider callback.
The upper logging provider also recognizes an already represented epoch before
its tail-equality check. It explicitly documents copied 100/lower 90 as possible,
but rejects a genuinely new incompatible idle-secondary epoch rather than
truncating that valid copied suffix
([SecondaryReplicator.UpdateEpochAsyncOperation.cpp:43–92][sf-secondary-epoch],
[LoggingReplicatorImpl.cpp:1368–1401][sf-upper-epoch]).

**Required correction:** preserve independent lower and upper frontiers, native
duplicate-control gating, and role/copy-specific callback validation. Adding the
copy-until-latest marker alone is insufficient. Do not fabricate lower progress,
report visible transaction progress instead of the upper log tail, truncate
valid copied history, or suppress all tail checks for V2 providers.
Required regressions: extended copy 90→100 with overlapping replication delayed,
duplicate UpdateEpoch succeeds without a reconciliation callback/truncation,
overlap is subsequently consumed correctly, and a new incompatible idle-copy
epoch still fails explicitly. This interleaving has not been deployed/qualified.

### Effect on the V2 port

DD-01–DD-07 must be resolved and their source-contract regressions pass before
accepting the [V2 foundation](FubeDataV2Design.md#10-milestones-and-acceptance-gates).
The current implementations and historical test expectations are not permission
to redefine SF behavior. Fixed memory budgets, `RequireServiceAck=true`, trusted
clear transport and unsupported persisted/TLS modes remain explicit profile
limits, separate from these semantic mismatches.

V2 also needs the native `SupportsCopyUntilLatestLsn` provider capability and
native staged copy semantics; the current classic-only exact-horizon policy is
not a replacement for that capability. See the
[V2 copy integration](FubeDataV2Design.md#7-copy-and-reconciliation).
This is an additional V2 integration prerequisite; it is not a code fix made
by this documentation update.

[sf-terminal-role]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/Replicator.ChangeRoleAsyncOperation.cpp#L276-L348
[sf-terminal-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L787-L866
[sf-demotion-transfer]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationQueueManager.cpp#L97-L106
[sf-create-session]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L1205-L1216
[sf-session-progress]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationSession.cpp#L34-L72
[sf-initial-progress]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReliableOperationSender.cpp#L28-L49
[sf-catchup-progress]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L1061-L1106
[sf-quorum-one]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L349-L363
[sf-quorum-one-release]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationQueueManager.cpp#L287-L307
[sf-previous-quorum-one]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L2475-L2519
[sf-current-catchup]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L2439-L2458
[sf-catchup-sentinel]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationQueueManager.cpp#L319-L357
[sf-secondary-close]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.CloseAsyncOperation.cpp#L41-L104
[sf-upper-drain-finished]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L862-L889
[sf-upper-drain-completion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp#L224-L235
[sf-queue-full]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/OperationQueue.cpp#L291-L359
[sf-admission-error]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L2105-L2144
[sf-transaction-retry]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/TransactionBase.cpp#L197-L209
[sf-commit-retry]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/common/Transaction.cpp#L448-L477
[sf-general-quorum]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L349-L390
[sf-quorum-availability]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicaManager.cpp#L1316-L1373
[sf-secondary-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/SecondaryReplicator.UpdateEpochAsyncOperation.cpp#L43-L92
[sf-upper-epoch]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.cpp#L1368-L1401

## Client fixture

Resolve the actual native-published service address; do not send HTTP to the
classic replication endpoint. Under that exact replica/incarnation owner path:

| GET suffix | Use |
|---|---|
| `service` | Authority-gated primary state |
| `diagnostics` | Observations on either role, not an access grant |
| `write?id=ID&key=KEY&value=VALUE` | Identified mutation; percent-encoded UTF-8 |
| `test?...` | Bounded qualification-only delay/probe controls, not application API |

Committed writes return HTTP 200, accepted ambiguous writes 202, malformed
requests 400 and unavailable authority/admission 503. Diagnostics distinguish
engine and provider progress. These are development fixtures, not a secured
production HTTP interface. The actual replication/copy transport is Fabric TCP.

## Native oracle and COM boundary

Only [`native-replicator-oracle`](../../crates/samples/native-replicator-oracle)
loads native SF libraries. Its node-supplied partition calls the real native
`CreateReplicator`, and `EndOpen` returns that exact native classic replicator.
It shares application payload/provider code, **never constructs the Rust
Engine**, and does not substitute a transactional engine.

Published `mssf-com` 0.10.0 interfaces and `mssf-pal` 0.10.0 `#[implement]`
reuse generated provider, data, async context, factory and replica definitions.
Typed patterns adapt the read-only `mssf-ext` reference at commit
`16914836baaae1641275de8eb92d945cd8b923a3`:
`src/{state_provider,stream,state_replicator,data,operation}.rs`.
No reference checkout is modified. Source attribution/MIT notices are retained.

The single narrow producer-stream exception is macro-generated nullable
`EndGetNext`: native **S_OK/null means EOS**, but the generated implementation
trait requires a non-null interface. The local adapter preserves the published
IID, method order, calling convention and pointer ABI without forging a Rust
interface from null. Non-null empty buffers remain data; cancellation/HRESULT
failure remain failure. Existing reference null-interface construction and
unsafe context downcasts are not copied. This exception is **test-only**, not
the production provider API. See the [detailed bridge rationale and
tests](../../crates/samples/native-replicator-oracle/README.md#typed-abi-provenance-and-dependency-boundary).

## Executed qualification

Source-guided port baseline:
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`](https://github.com/microsoft/service-fabric/tree/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication).
This source revision is **not assumed to match** deployed SF 11.8.121.4 binaries.
Microsoft/MIT notices accompany derived algorithms and regression scenarios.

Executed implementation snapshot:
`1414adca03635cde661b7e8ec19cf49dfdc113f8`, tree
`a2145f3d56cd3368cd5737c3f29f34367c70f453`:

Windows producer results below are historical. The current repository supports
Linux only and no longer runs Windows CI.

- [Two-worker CI 40781117241](https://github.com/youyuanwu/fube/actions/runs/40781117241):
  Linux/Windows portable, both compiler-free workers and strict aggregate
  **SUCCESS**. Rust-only14, baseline native-data17, hardened full legacy1 and
  all five archived native-client tests executed. All32+5 asserted separately;
  both original allocations have uploaded independent post-teardown absence.
- [Standalone CI 40780908481](https://github.com/youyuanwu/fube/actions/runs/40780908481):
  bootstrap, close, abort, manifest-close and manifest-abort **SUCCESS**.
  Forced reaping where recorded is not relabeled graceful shutdown.
- These are `workflow_dispatch` head executions, **not PR merge-tree evidence**.
  Later reviews/CI retain their own head/tree/executable hashes rather than
  relabeling these results.

Actual tests include Rust primary/native secondary **and** native primary/Rust
secondary nonzero writes and real nonempty native copy enumeration, snapshot
horizons with concurrent writes, delayed application ACKs, native planned swap
and abrupt primary failure, replacement copy, quorum unavailability, identical
retry/ambiguous results, stale traffic, interrupted copy/epoch transition,
data-loss changed/unchanged callbacks, bounds and per-unit/process isolation.
Both directions hold the destination's real final nonempty CopyEnd ACK while
independently observing InBuild/EOS, release it and require same-replica coherent
completion. Four post-copy no-write moves per direction include the next
Rust-to-native demotion and require fresh actual native ACK frontiers; descriptor
progress is not substituted. Invalid-frame tests require exact socket write/
flush plus independently captured destination kernel ingress before unchanged
state/epoch/ledger observations; this is not a native parser/rejection-callback
or TCP-ACK claim. Native InBuild diagnostics intentionally omit
GetCurrentProgress, whose native copy-in-progress error path would fault the
replica rather than merely observe it.
Linux FAS deployment is not assumed: where absent, exact auxiliary native
replica removals must produce actual node-selected data-loss epoch and callback/
content/sequence evidence; no manual provider callback or election substitutes.

The strict upstream bundle includes versioned inventory/hashes, source revision,
ELF dependencies and package inputs. Native consumer reports identify actual
executable hashes, native mappings, roles/epochs and independent observer data.
The Rust process mapping check and oracle's intentionally native mapping class
are separate. Codec/model tests are not counted as native interoperation.

See [build/run commands and cleanup contract](../Development.md#volatile-classic-v1-replication).
The [parallel CI routing and measured result](../Development.md#parallel-onebox-ci)
preserve the complete dependent profile chains; native-data alone is not full.
All allocations are uniquely owned; immutable original creation capabilities
survive partial failure. Minted app/type/upload absence and exact original
container/volume/network absence are independently checked. Physical teardown
does not prove every prior service retired gracefully, and failed historical
runs are not converted into passes.
