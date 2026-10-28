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
([selector](../../crates/samples/rust-runtime-app/src/data.rs#L20-L26));
the data harness creates services with those bytes, preserving unrelated
stateless registration. This is not an environment role override.

## Owned API and provider obligations

- `Engine::bind_partition` installs the existing runtime's partition authority
  and callback/destructor supervision. `Engine::bind` is a standalone test seam,
  not native application authority
  ([constructors](../../crates/libs/fube-data/src/engine.rs#L309-L354)).
- `StateProvider` supplies progress, `update_epoch(previous_epoch_last)`,
  data-loss, copy-context and exact-horizon copy-state callbacks
  ([contract](../../crates/libs/fube-data/src/api.rs#L135-L160)).
  Provider progress means application state, not quorum knowledge.
- `Engine::submit` returns an admitted sequence and `Replication` waiter.
  The primary must apply that sequence locally **before** waiting for quorum.
  Its accepted operation and local apply must outlive a disconnected caller;
  canceling/dropping `wait` does not undo admission
  ([wait](../../crates/libs/fube-data/src/engine.rs#L136-L157),
  [sample owned admission](../../crates/samples/rust-runtime-app/src/data.rs#L230-L282)).
- Secondary application pumps consume copy then replication; apply before
  `Operation::acknowledge`. Receipt, polling and Drop do not acknowledge.
  A late retired-owner ACK fails rather than reviving progress
  ([ACK and stream contract](../../crates/libs/fube-data/src/api.rs#L163-L244)).
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
([provider](../../crates/libs/fube-data/src/provider.rs#L21-L44),
[admission/apply](../../crates/libs/fube-data/src/provider.rs#L93-L143)).
Applications implementing other providers own their payload semantics,
reconciliation, exact-boundary snapshots and retry guarantees.

## Progress, quorums, copy and failures

`assigned`, `received`, `applied`, `committed`, `client_completed` and `completed`
are different frontiers. Secondary received progress is not application ACK;
neither is a global quorum proof. The primary's queue `applied` may remain its
reconciliation frontier while actual provider state has advanced locally.
Inspect provider progress separately; socket-write success never means commit
([progress](../../crates/libs/fube-data/src/api.rs#L247-L260),
[quorum calculation](../../crates/libs/fube-data/src/quorum.rs)).

Current and previous configuration write obligations remain separate, including
the previous-quorum-one case when the new primary was not in that configuration.
Required targets and ALL catch-up are not ordinary write quorum. Removal does
not shrink native membership to manufacture success. Epoch/role revisions,
replica and engine incarnations, original operation epochs and build sessions
fence stale data/ACK/copy receipts. Repeated promotion with no intervening write
still requires fresh peer ACKs of the authoritative inherited prefix.

Copy pins coherent state **and retry ledger** at the native-selected
`replicationStart - 1` horizon, even while the provider and new writes advance.
Live replication buffers behind copy and joins at its explicit finish fence;
unavailable history fails instead of selecting a different boundary. Native
volatile copy context is null. A successful canceled Build invocation's scope
is distinct from the lifetime of the established configured peer.

Read/write admission requires current native access, live lifetime/host
authority, agreement of native service/replicator/engine roles and epoch, and
actual replication readiness
([authority gate](../../crates/samples/rust-runtime-app/src/data.rs#L37-L72)).
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

At source `e78eada771f842a02f4321e857588ce07c50a7e9`, tree
`424275f027a24363242484a270b9b8bd320b43e6`:

- [Main CI 40733937944](https://github.com/youyuanwu/fube/actions/runs/40733937944):
  Linux/Windows portable jobs and compiler-free native consumer **SUCCESS**;
  5 archived native-client tests executed; Rust-only data **14/14** and full
  mixed-native plus preserved legacy runtime **18/18** required scenarios PASS.
- [Standalone CI 40733943872](https://github.com/youyuanwu/fube/actions/runs/40733943872):
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
Linux FAS deployment is not assumed: where absent, exact auxiliary native
replica removals must produce actual node-selected data-loss epoch and callback/
content/sequence evidence; no manual provider callback or election substitutes.

The strict upstream bundle includes versioned inventory/hashes, source revision,
ELF dependencies and package inputs. Native consumer reports identify actual
executable hashes, native mappings, roles/epochs and independent observer data.
The Rust process mapping check and oracle's intentionally native mapping class
are separate. Codec/model tests are not counted as native interoperation.

See [build/run commands and cleanup contract](../Development.md#volatile-classic-v1-replication).
All allocations are uniquely owned; immutable original creation capabilities
survive partial failure. Minted app/type/upload absence and exact original
container/volume/network absence are independently checked. Physical teardown
does not prove every prior service retired gracefully, and failed historical
runs are not converted into passes.
