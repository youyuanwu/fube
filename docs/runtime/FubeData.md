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
The [source audit and R0 corrections](#known-semantic-differences-from-native-v1)
below record corrected lower contracts with offline regression evidence.
They do not qualify R0 against a deployed native cluster.

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
  ([public construction and Rustdoc](../../crates/libs/fube-data/src/fabric_replicator.rs#L23-L74)).
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
  Streams are role-generation views: promotion dispatches eligible records,
  supplies successful EOS, then waits for actual processing ACKs. Reacquire both
  views on a later secondary role; old views never revive. The sample refreshes
  after its lower role transition, without replacing the engine.
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
R0's general PC quorum calculation follows the native q-1 remote-session rule
even when local-not-PC; the q1 remote exception stays APPLY-gated (DD-06 below).
Required targets and ALL catch-up are not ordinary write quorum. Removal does
not shrink native membership to manufacture success. Epoch/role revisions,
replica and engine incarnations, original operation epochs and build sessions
fence stale data/ACK/copy receipts. Repeated promotion with no intervening write
binds a secondary session only from the exact current native-selected primary
descriptor/epoch, never an arbitrary epoch-less RequestAck. **R0 behavior
(DD-02 below):** runtime-issued new/recreated sender CurrentProgress initializes
receive/apply/highest and can satisfy ALL at that represented prefix. Repeated
descriptors do not reset live counters; diagnostics cannot mint that authority.
New beyond-prefix work still needs its applicable real ACKs.

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
remain ordered during that drain. Successful dispatch EOS precedes the final
ACK wait, so an upper consumer can flush/ACK after EOS (DD-04 below).
Demotion supplies fresh views around the transferred applied prefix.
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
assigning a new LSN. This optional path can elicit real ACKs from peers whose
runtime descriptor did not already represent the prefix. It is not a universal
ALL prerequisite: authentic issued CurrentProgress satisfies that prefix
directly. Replay is not an original SF provider callback or required V2 algorithm.
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
`ErrorKind::ReplicationQueueFull` preserves native HRESULT **0x80071bcb**
(-2147017781); operation sizing uses `ReplicationOperationTooLarge`,
**0x80071c2f** (-2147017681). Other ResourceExhausted errors remain distinct.
Retry capacity pressure only after recovery; do not retry every resource failure
or parse display text. No upper transaction retry kernel is supplied by R0.
The sample's HTTP request limit is 4096 bytes; its client waiter is five seconds.

## Known semantic differences from native V1

**Audit date:** 2026-10-30. **Fube source:** `2f28b963b2b55020bb32ac73e7874b37b3a509fc`
(#19). **Authoritative native source:** local SF checkout
`51a7703a1f83749a9234d3ceb4296d8622e42cca`, linked immutably below.
The audit headings below preserve historical differences at that source.
**R0 correction date: 2026-10-31.** DD-01–DD-07 are corrected in the lower
implementation and covered by focused offline source-contract regressions.
Earlier qualification records retain their exact historical scope; no new
native deployment qualification or upper transaction implementation is claimed.
An independent Astra source review confirmed DD-01–DD-03 and identified
DD-04–DD-07. The latter include lower integration gaps exposed by a native V2
consumer, not a claim that every current classic provider encounters them.

### DD-01: terminal Primary-to-None incorrectly requires commitment

**Pre-R0 audit:** Primary-to-None incorrectly shared the secondary-transfer
`assigned == committed` prerequisite.

**Native:** the Primary-to-None path closes the primary without creating a
secondary. Its close takes pending replicate operations before closing sessions
and cancels them; the committed-tail invariant belongs to transferring the
queue into a new secondary, not every terminal close
([ChangeRoleAsyncOperation.cpp:276–348][sf-terminal-role],
[ReplicaManager.cpp:787–866][sf-terminal-close],
[ReplicationQueueManager.cpp:97–106][sf-demotion-transfer]).

**R0 correction:** terminal None retires pending observers before session
cleanup, does not reconcile a provider or reset commitment, and releases owned
queue storage. Actual secondary demotion still requires commitment
([control admission](../../crates/libs/fube-data/src/engine.rs#L2128-L2177),
[terminal completion](../../crates/libs/fube-data/src/engine.rs#L4105-L4121),
[queue retirement](../../crates/libs/fube-data/src/queue.rs#L73-L79)).
The [pending-write regression](../../crates/libs/fube-data/src/engine_tests.rs#L4696-L4728)
observes assigned1/committed0, failed completion and unchanged provider state.
Terminal retirement also cancels/joins an in-flight optional replay before
publication
([ownership gate](../../crates/libs/fube-data/src/engine.rs#L4053-L4071),
[regression](../../crates/libs/fube-data/src/engine_tests.rs#L6205-L6249)).
Callback-free retirement does not reserve another provider slot before canceling
the callbacks occupying that budget; the
[all-64-callback regression](../../crates/libs/fube-data/src/engine_tests.rs#L813-L912)
observes terminal success and joined physical disposal under saturation.
Retirement is not rollback.

### DD-02: native-issued initial progress is excluded from ALL catch-up

**Pre-R0 audit:** initial native progress was only a capped WRITE attestation;
ALL unnecessarily required a new ACK or optional replay.

**Native:** `CreateReplica` passes the native descriptor's `CurrentProgress`
into the replication session. `ReliableOperationSender` initializes receive,
apply and highest-operation frontiers from it. ALL catch-up uses the resulting
current-configuration apply frontier; there is no universal fresh-network-ACK
requirement for an already represented prefix
([ReplicaManager.cpp:1205–1216][sf-create-session],
[ReplicationSession.cpp:34–72][sf-session-progress],
[ReliableOperationSender.cpp:28–49][sf-initial-progress],
[ReplicaManager.cpp:1061–1106][sf-catchup-progress]).

**R0 correction:** newly created/recreated sessions initialize receive/apply/
highest from the entered, frozen runtime descriptor's CurrentProgress.
Repeated descriptors preserve live counters; diagnostics cannot attest a vote
([issued initialization](../../crates/libs/fube-data/src/engine.rs#L2636-L2653),
[canonical proof](../../crates/libs/fube-data/src/engine_binding.rs#L181-L225)).
The [issued root/bound regression](../../crates/libs/fube-data/src/engine_native_tests.rs#L225-L332)
proves ALL at20 with no replay/fresh ACK, repeated and recreated descriptors,
and LSN21 still pending without a real ACK. The diagnostic-prefix test remains
a negative authority witness, not the source-conformance path.

### DD-03: quorum-one primary release waits for remote receive ACKs

**Pre-R0 audit:** current quorum1 committed locally but unconditional remote
minima pinned release and ALL.

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

**R0 correction:** the helper resolves CC<=1's native sentinel to the assigned
tail for commit/release/ALL. Remote-PC-q1 instead uses minimum APPLY for both
commit and release; idle retention remains independent
([quorum rules](../../crates/libs/fube-data/src/quorum.rs#L23-L90),
[ALL](../../crates/libs/fube-data/src/engine.rs#L4004-L4008),
[retention](../../crates/libs/fube-data/src/engine.rs#L4563-L4630)).
ALL uses CC, not combined PC commit or WRITE required-target constraints.
The [sentinel/PC regression](../../crates/libs/fube-data/src/quorum.rs#L167-L202)
and [idle retention test](../../crates/libs/fube-data/src/engine_tests.rs#L318-L395)
cover those separate obligations; the
[direct configured-lagging CCq1 test](../../crates/libs/fube-data/src/engine_tests.rs#L6252-L6282)
observes released storage/ALL with zero remote receive/apply and no idle owner.
Joint-configuration tests retain PC APPLY blocking while CC ALL succeeds.

### DD-04: promotion does not finish the secondary stream generation

**Pre-R0 audit:** engine-lifetime replication views stayed open after promotion.

**Native:** secondary close during promotion closes dispatch queues and waits
for the required drain. The upper `SecondaryDrainManager` observes replication
EOS, processes the replication-finished information record, waits for logical
processing and outstanding ACKs, and only then signals its drain completion
([SecondaryReplicator.CloseAsyncOperation.cpp:41–104][sf-secondary-close],
[SecondaryDrainManager.cpp:862–889][sf-upper-drain-finished],
[224–235][sf-upper-drain-completion]).

**R0 correction:** freeze ingress, dispatch the eligible contiguous boundary,
close senders successfully, then await actual service ACKs. EOS does not depend
on the last ACK, so an upper EOS-triggered flush can finish. Demotion replaces
views on the same engine with an already-copied transferred prefix; old views
remain terminal and revision checks fence captures
([EOS before ACK gate](../../crates/libs/fube-data/src/engine.rs#L4087-L4124),
[fresh views](../../crates/libs/fube-data/src/engine.rs#L4462-L4498)).
The [64-capture/gap/epoch-barrier test](../../crates/libs/fube-data/src/engine_tests.rs#L960-L1071)
and [empty/nonempty cycles](../../crates/libs/fube-data/src/engine_tests.rs#L1074-L1158)
prove EOS-before-ACK ordering. The
[sample role-cycle test](../../crates/samples/rust-runtime-app/src/data_tests.rs#L1296-L1348)
proves its pump refresh without recopy or a new engine. Failed/canceled controls
are not successful drain and no synthetic operation/ACK is used.

### DD-05: admission errors lose the native retry distinction

**Pre-R0 audit:** temporary fullness and permanent sizing shared ResourceLimit.

**Native:** queue fullness returns `REQueueFull`, propagated by the replica
manager. The transaction retry classifier includes replication-queue-full,
not every resource/size failure. A failed nonretriable commit faults the
transaction; timeout/cancellation returns it to Active only after a classified
retriable failure
([OperationQueue.cpp:291–359][sf-queue-full],
[ReplicaManager.cpp:2105–2144][sf-admission-error],
[TransactionBase.cpp:197–209][sf-transaction-retry],
[Transaction.cpp:448–477][sf-commit-retry]).

**R0 correction:** distinct ErrorKind values preserve **0x80071bcb queue-full**
and **0x80071c2f operation-too-large**, including causes, native conversion,
uncertainty and transport round trips
([runtime errors](../../crates/libs/fube-runtime/src/error.rs#L31-L41),
[admission](../../crates/libs/fube-data/src/engine.rs#L890-L957),
[queue stages](../../crates/libs/fube-data/src/queue.rs#L80-L102)).
These exact codes come from [FabricTypes.h:119–136,226–236][sf-admission-codes]
and [EnumOperators.cpp:85–128][sf-admission-conversion].
The [runtime fidelity test](../../crates/libs/fube-runtime/src/error_tests.rs#L9-L38),
[capacity-recovery test](../../crates/libs/fube-data/src/engine_tests.rs#L3398-L3463),
and [permanent-sizing test](../../crates/libs/fube-data/src/queue.rs#L275-L294)
prove lower retry recovery versus permanent rejection without consuming a
rejected LSN. Upper transaction timeout/cancel retry states are A0/A1 work,
not claimed implemented by these lower tests.
An operation that cannot fit even an empty Primary's permanent future-copy
headroom is ResourceExhausted, not retriable queue-full; the
[empty-budget regression](../../crates/libs/fube-data/src/engine_tests.rs#L5664-L5695)
checks repeated classification, unchanged LSN and successful smaller admission.

### DD-06: local-not-PC quorum greater than one requires an extra vote

**Pre-R0 audit:** local-not-PC quorum2 incorrectly needed two remote votes and
selected the lower of `[10,0]`.

**Native:** the general availability/progress helpers assume an immediately
ACKing primary and require `writeQuorum - 1` remote sessions/ACKs even in this
PC case. The source explicitly limits its local-not-PC exception to previous
quorum 1 with remote members; PC quorum 2 in the example yields 10
([ReplicaManager.cpp:349–390][sf-general-quorum],
[1316–1373][sf-quorum-availability],
[2475–2519][sf-previous-quorum-one]).
Independent all-member completion/idle retention still prevents inappropriate
history release; the source does not strengthen the general commit formula.

**R0 correction:** general quorum>1 needs q-1 remote sessions and selects
descending remote apply at q-2, irrespective of `local_member`. Issued membership
is unchanged; the native remote-PC-q1 APPLY exception stays separate
([implementation](../../crates/libs/fube-data/src/quorum.rs#L29-L90)).
The [unequal-progress/insufficient-session test](../../crates/libs/fube-data/src/quorum.rs#L167-L202)
proves CC10/PCq2 remote[10,0] -> PC10, missing required sessions fail,
and PCq1 release remains APPLY-gated. Independent receive/idle retention still
constrains storage, not the general commit formula.

### DD-07: extended-copy tails fail unconditional epoch reconciliation

**Pre-R0 audit:** admission-only controls still queried/equated an independently
copied upper tail, turning a native duplicate into a fault.

**Native:** duplicate secondary UpdateEpoch returns without a provider callback.
The upper logging provider also recognizes an already represented epoch before
its tail-equality check. It explicitly documents copied 100/lower 90 as possible,
but rejects a genuinely new incompatible idle-secondary epoch rather than
truncating that valid copied suffix
([SecondaryReplicator.UpdateEpochAsyncOperation.cpp:43–92][sf-secondary-epoch],
[LoggingReplicatorImpl.cpp:1368–1401][sf-upper-epoch]).

**R0 correction:** secondary controls validate the effective admission minimum
from node controls and accepted primary traffic: older epochs fail before equal
epochs return without copy/control gates or provider calls. New secondary/role
admission controls drain lower work without a provider getter/equality check.
Original-operation barriers and primary provider controls preserve the callback's
actual success/error without reimposing upper/lower tail equality
([admission minimum](../../crates/libs/fube-data/src/engine.rs#L2082-L2122),
[callback result](../../crates/libs/fube-data/src/engine.rs#L4172-L4187)).
The [upper100/lower90 overlap test](../../crates/libs/fube-data/src/engine_tests.rs#L3540-L3625)
keeps actual copied bytes, delays real ACKs, observes no duplicate callback/getter
or truncation, then drains 91–100. The
[represented-provider duplicate tests](../../crates/libs/fube-data/src/engine_tests.rs#L3627-L3690)
exercise promotion and original-operation barriers without post-callback getters.
The [new incompatible epoch negatives](../../crates/libs/fube-data/src/engine_tests.rs#L3692-L3749)
preserve real provider errors. The [owned canonical lower-entry test](../../crates/libs/fube-data/src/engine_native_tests.rs#L449-L523)
separately proves RAP's duplicate suppression and actual lower no-op execution;
[traffic-epoch and saturated-entry regressions](../../crates/libs/fube-data/src/engine_native_tests.rs#L525-L645)
cover newer StartCopy/replication admission during unfinished copy, equal success,
stale rejection and callback-free execution under the shared record limit.
These are offline fixtures, not a V2 log kernel or deployed qualification.

### Effect on the V2 port

R0 implements DD-01–DD-07 plus explicit latest-copy and truthful constructor
tail integration. Native transaction/log/state-manager kernels and staged copy
serialization remain A0/A1, not upper workarounds supplied by R0. Fixed budgets,
RequireServiceAck, trusted clear transport and unsupported persisted/TLS modes
remain explicit profile limits.

### R0 copy capability and callback-ready construction

`FabricReplicator::create` remains backwards-compatible classic copy. Opt in with
`create_with_capabilities(..., StateProviderCapabilities {
supports_copy_until_latest_lsn: true })`. This is explicit construction, not SDK
trait discovery, peer negotiation or type inference
([capability](../../crates/libs/fube-data/src/extensions.rs#L89-L97),
[helper](../../crates/libs/fube-data/src/fabric_replicator.rs#L53-L96)).
Latest selects inclusive assigned horizon and replication start assigned+1;
classic retains max(committed,previous quorum)+1. Provider supervision and the
enumeration-EOS finish tail remain unchanged. The
[latest copy regression](../../crates/libs/fube-data/src/engine_tests.rs#L3753-L3847)
starts at101/horizon100 with committed0 and waits for real finish-tail ACK101.

Initialize the provider before construction, in the runtime-owned service Open
entry. Lower construction already calls its fallible progress getter before the
returned lower object's later Open
([constructor](../../crates/libs/fube-data/src/engine.rs#L631-L662)).
A logging adapter must return its real log tail, not stable/visible progress or
temporary zero. The [public bootstrap witness](../../crates/libs/fube-data/tests/sdk_contracts.rs#L222-L297)
passes actual nonempty fixture records with source-shaped zero/one markers and
tail1/27 through construction, then assigns2/28. Its markers are **not native
record serialization or persisted recovery**. R0 neither initializes a real
transaction log for callers nor supports Existing-open. Native kernels are A0;
mixed deployment qualification remains unrun.

[sf-admission-codes]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/prebuilt/sdk/FabricTypes.h#L119-L236
[sf-admission-conversion]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/EnumOperators.cpp#L85-L128

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
