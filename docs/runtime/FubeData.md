# Fube Data: volatile classic V1 replication

`fube-data` is an owned Rust replication engine, separate from the SF-facing
control contract in `mssf-runtime`. It implements a **qualified subset** of
classic V1: ordered nonzero sequences, explicit application acknowledgments,
current/previous configuration quorums, epoch reconciliation, bounded copy and
replication streams, retries and native Fabric TCP peers. Native SF nodes remain
the membership, role, epoch and placement authority; there is no Rust election.

The executed profile is **Linux x86-64, SF 11.8.121.4, volatile state,
RequireServiceAck=true, clear trusted loopback, activated ExeHost/SharedProcess**.
No persistence, transactional replicator/Reliable Collections, TLS/Windows
replication, rolling executable upgrade, whole-set restart preservation or
universal V1 parity is promised. Acknowledgment is not disk durability.

## Components and selection

```text
native node -- Hosting/RAP control --> mssf-runtime
                                      |
                         fube-data Engine (owned Rust)
                         |                         |
              application provider         Fabric TCP peers
              copy/apply + explicit ACK     Rust or native classic V1
```

`fube-data` depends on `mssf-runtime` and `mssf-transport`, not the reverse.
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

- `Engine::bind_partition` installs the existing runtime's partition authority
  and callback/destructor supervision. `Engine::bind` is a standalone test seam,
  not native application authority
  ([constructors](../../crates/libs/fube-data/src/engine.rs#L313-L355)).
- `StateProvider` supplies progress, `update_epoch(previous_epoch_last)`,
  data-loss, copy-context and exact-horizon copy-state callbacks
  ([contract](../../crates/libs/fube-data/src/api.rs#L135-L170)).
  Provider progress means application state, not quorum knowledge.
- Optional `StateProvider::replication_checkpoint(sequence, cancellation)` returns
  an authentic original operation at exactly that applied/completed-copy
  sequence: original epoch, bytes **and segment boundaries**. Its default is
  bounded `None`; unavailable provenance must not be reconstructed from current
  state or relabeled with the current epoch. Callback/capture supervision is
  unchanged. The engine reserves bounded operation ownership before requesting
  a checkpoint; this is not a permanent extra retained-queue allowance.
- `Engine::submit` returns an admitted sequence and `Replication` waiter.
  The primary must apply that sequence locally **before** waiting for quorum.
  Its accepted operation and local apply must outlive a disconnected caller;
  canceling/dropping `wait` does not undo admission
  ([wait](../../crates/libs/fube-data/src/engine.rs#L140-L161),
  [sample owned admission](../../crates/samples/rust-runtime-app/src/data.rs#L258-L325)).
- Secondary application pumps consume copy then replication; apply before
  `Operation::acknowledge`. Receipt, polling and Drop do not acknowledge.
  A late retired-owner ACK fails rather than reviving progress
  ([ACK and stream contract](../../crates/libs/fube-data/src/api.rs#L173-L267)).
- Provider callbacks run outside engine/transport locks. Returned errors,
  panics, cancellation and timeout are explicit; failed barriers cannot grant
  readiness. Qualified runtime watchdog guards supervise actual callback and
  capture disposal, not merely future cancellation
  ([supervision](../../crates/libs/fube-data/src/supervised.rs)).

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
and `Engine::submit_in_epoch`; the native oracle uses its actual provider
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
([progress](../../crates/libs/fube-data/src/api.rs#L270-L283),
[quorum calculation](../../crates/libs/fube-data/src/quorum.rs)).

Current and previous configuration write obligations remain separate, including
the previous-quorum-one case when the new primary was not in that configuration.
Required targets and ALL catch-up are not ordinary write quorum. Removal does
not shrink native membership to manufacture success. Epoch/role revisions,
replica and engine incarnations, original operation epochs and build sessions
fence stale data/ACK/copy receipts. Repeated promotion with no intervening write
binds a secondary session only from the exact current native-selected primary
descriptor/epoch, never an arbitrary epoch-less RequestAck. Fresh network ACKs
drive write commitment and ALL catch-up. Native-advertised progress may attest
only the local already-completed prefix for WRITE_QUORUM catch-up; it is
separately labeled `native_catchup_prefix`, cannot acknowledge a new write, and
cannot satisfy ALL.

Copy pins coherent state **and retry ledger** at the native-selected
`replicationStart - 1` horizon, even while the provider and new writes advance.
Live replication buffers behind copy and joins at its explicit finish fence;
unavailable history fails instead of selecting a different boundary. Native
volatile copy context is null. A successful canceled Build invocation's scope
is distinct from the lifetime of the established configured peer.

Promotion freezes and drains the contiguous received prefix through actual
application ACK, including operations not yet dispatched because captures
exhausted their budget. It does not silently reuse their sequence numbers;
only a noncontiguous tail can be discarded. Original-operation epoch barriers
remain ordered during that drain. Every configuration installation captures the
latest assigned catch-up fence. Departed adopted sessions are retired when
absent from both active configurations, without retaining obsolete copy fences
or waiting for a delayed RemoveIdle to free their budgets.
The admitted control's absolute deadline and cancellation wake the owner even
when no peer, application ACK or retry event arrives.

An already completed native secondary has no primary target immediately after
demotion; RequestAck deliberately cannot establish one. If the promoted Rust
primary has no ordinary retained suffix, it requests a bounded
`StateProvider::replication_checkpoint` and retransmits that **authentic original
operation** at the existing LSN with its unchanged original epoch/segments.
The duplicate establishes the native primary target without applying again or
assigning a new LSN. Only the resulting real receive/application ACK can satisfy
ALL. One temporary charged callback owns this recovery path; there is no
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
access. Terminal authority cannot revive. Per-unit Close/Abort removes only its
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
The sample's HTTP request limit is 4096 bytes; its client waiter is five seconds.

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
