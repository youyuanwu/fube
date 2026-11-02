# Rust stateless application runtime

`fube-runtime` is an experimental **application-side** Rust hosting/RAP runtime.
Real Service Fabric nodes start the executable, authorize registration, place
stateless instances and replace failed application processes. It does not load
FabricRuntime/FabricCommon, adapt COM or call a native helper for runtime work.
The native SF node, deployment tools and OS libraries are still expected.

Current contracts are a [breaking SF/mssf alignment](SfApiAlignment.md).
The native outcomes later in this page are **dated historical evidence at their
recorded source/profile**, not qualification of the changed whole implementation.
No live/deployed qualification was run in the current alignment workflow.
The separate [owned local Hosting profile](../cluster/HostingLocalActivation.md)
now runs a minimal native-free stateless package through actual process/IPC
registration and H2 teardown. It does not replace the native deployment profile
or qualify production RA/RAP service callbacks.

## Qualified profile

| Boundary | Qualification |
| --- | --- |
| Platform/node | Linux x86-64, native SF 11.8.121.4, repository unsecured onebox |
| Activation | SF-activated normal ExeHost, one code package, SharedProcess |
| Services | Singleton and concurrent Int64-range stateless partitions in one process |
| IPC | Actual node-injected loopback address, trusted clear transport; no reconnect |
| Lifecycle | Native type registration, Open, graceful Close, Abort flag 4, failure cleanup |
| Controls | Correlated stateless RAPQuery; service-description updates |
| Packages | Owned config/data metadata/settings; real unchanged-code configuration upgrade and package notifications |
| Reporting | Native FM load/move retention; HM instance health with sequencing/expiry; runtime transient-fault replacement |
| Discovery | Separate fube-client resolution, then real RPC to that returned endpoint |
| Recovery | Exact owned-process termination, new native host/PID/incarnation, usable endpoint |
| Authority | Fresh node lease poll before serving; terminal IPC/lease/host authority |

This is not a complete replacement for every FabricRuntime feature. Named
partition decoding has model coverage, not native qualification. Secret
decryption/reference resolution, rolling code upgrade/rollback, general stateful services, replication/storage,
containers, activator/multicodepackage hosts, Windows, TLS and COM ABI compatibility
are outside this profile. Unsupported security is rejected, never downgraded.
The broader [design](RustFabricRuntime.md) retains these separate capability tiers.
The same crate/executable now has a separately qualified
[volatile empty-state stateful profile](RustStatefulRuntime.md); it does not
turn this stateless profile into replicated storage.

## Owned programming model

Implement `IStatelessServiceFactory::create_instance` and
`IStatelessServiceInstance::{open,close,abort}`. Construction is synchronous
and fallible; lifecycle Open/Close return boxed `Send` futures. The API uses
UTF-8/UUID, boxed participants and partition trait objects rather than COM lifetimes
([factory/lifecycle Rustdoc](../../crates/libs/fube-runtime/src/api.rs#L84-L109)).

1. `Runtime::attach()` consumes actual SF-injected activation identity and
   completes host/runtime registration plus a fresh lease poll.
2. `register_stateless_service_factory(name, Box<dyn IStatelessServiceFactory>)`
   synchronously waits for native registration. Reserve
   `runtime.execution_runtime().try_reserve()` **before** moving the factory, then
   `run_owned(move || runtime.register_stateless_service_factory(...))` and
   await that owned observation. Do not block an async executor directly.
   There is no invented type-unregister operation.
3. Factory creation receives service type, URI, borrowed initialization bytes,
   partition GUID and public stateless instance ID. A factory-owned context
   handle supplies activation snapshots; explicit `InstanceIntegrationProvider`
   captures native incarnation/integration separately, not as SDK arguments.
4. Open receives `Arc<dyn IStatelessServicePartition>` and cancellation.
   Successful Open may return an **empty opaque endpoint** when no client address
   is advertised; emptiness is not failure or serving/deployment readiness.
   If advertising a listener, bind it **before returning** its endpoint string.
5. Close asynchronously disposes resources. Abort synchronously disposes them
   before returning; neither may rely on a dropped future as proof of cleanup.
6. Explicitly await `shutdown()` for orderly unregistration.

The [empty-endpoint lifecycle regression](../../crates/libs/fube-runtime/src/lifecycle_tests.rs#L1054-L1134)
executes Rust RAP Open, duplicate Open and successful graceful Close with
independent factory/callback counts and blocked actual destruction before Close
ACK. Error, cancellation and retired-authority fences remain in force.
This delivers the [H4 runtime prerequisite](../cluster/RustHosting.md#h4-owned-activation-and-real-stateless-lifecycle),
not H4 owned activation or production deployed RA/RAP interoperability.

The [compiling Rustdoc example](../../crates/libs/fube-runtime/src/lib.rs#L12-L31)
uses the actual public reservation/registration APIs. Direct unknown async
context returns `InvalidExecutionContext`; invalid-context registration retains
the exact boxed factory for once-only typed recovery, not hidden destruction.
Execution/native-binding/cleanup types are newly written fube integration,
not SDK ports. Ordinary Tokio tasks/JoinSets supply scheduling, with thin
deadline/context/disposal wrappers. Application-scheduler implicit children stay
application-owned; services stop/join their tasks, not the runtime on their
behalf. A stalled external executor has no progress guarantee.
The [sample main](../../crates/samples/rust-runtime-app/src/main.rs#L628-L672)
awaits `Runtime::shutdown()` **then** final shared RPC/peer-listener cleanup;
unit Close/Abort removes routes/work but does not stop those process listeners.

See the actual [listener-backed fixture](../../crates/samples/rust-runtime-app/src/main.rs).
Its default routes check partition validity per RPC. `/FailOpen`, `/FailClose`,
`/Concurrent` and `/Uncooperative` suffixes are **acceptance-only failure fixtures**,
not application deployment conventions. The last intentionally ignores authority
in both listener/main to prove independent hard process fencing.

The activation snapshot contains real host/package/incarnation, work/log/temp
directories, node IP, deployed endpoint ports and owned configuration/data state
([context.rs:9-52](../../crates/libs/fube-runtime/src/context.rs#L9-L52)).
Node context comes from the actual Start registration reply. No missing-runtime-
access fallback discovers a node address or invents host identity.

## Owned packages and context refresh

`activation_context()` now returns `Arc<ActivationContext>`, not a borrowed
startup-only value. `context_handle()` returns a clonable current-context provider:

```rust
let current = runtime.context_handle();
let snapshot = current.snapshot();
let config = snapshot.packages()?.configuration_package("Config")?;
let value = config.section("Main")?.parameter("Value")?.plaintext()?;
```

Keep the snapshot/package Arc as long as needed; later updates do not alter its
metadata/settings. A factory may retain its own creation-time owned snapshot;
later service creation/type registration uses the current context.
The environment-only parser is bootstrap, not a loaded package context:
`packages()` explicitly errors before loading
([context.rs:29](../../crates/libs/fube-runtime/src/context.rs#L29),
[current provider:90](../../crates/libs/fube-runtime/src/package_context.rs#L90)).

The reader uses registration's deployment directory and native application/package
identity, validates `App.<rollout>.xml`, `<service>.Package.Current.xml`, its
selected versioned service manifest and deployed endpoints, then loads every
declared configuration package's `Settings.xml`. Digested overrides apply to
existing targets only. It rejects missing/malformed/duplicate/unsupported data,
not empty-default substitutes. Shared packages retain the native `.lk` path.
Code/profile compatibility compares normalized declared fields plus native
digested rollout/checksum/sharing/policy metadata, not independently rehashed
filesystem code bytes. Qualification additionally hashes the running executable
([reader:576](../../crates/libs/fube-runtime/src/packages.rs#L576),
[upgrade harness:662](../../tests/onebox/shared/runtime.py#L662)).
The reader admits the qualified ordinary ExeHost declarations; setup/external/
activator/periodic entry points, nonempty custom code-environment declarations and
unsupported host/security policies fail explicitly rather than being ignored.
Linux artifact opens are nonblocking before regular-descriptor validation, so a
FIFO cannot strand attachment before the type check. Owner Drop retires authority
before releasing callback captures, including application-controlled destructors.

Lookups are case-insensitive. Configuration parameters expose copied raw value,
MustOverride, IsEncrypted and Type. `plaintext()` refuses encrypted or unresolved
typed values; no decryption or secret-reference resolution is supplied.
Native override application changes Name/Value/nonempty Type, **not** base
IsEncrypted/MustOverride, even when the override XML contains its own encryption
flag ([packages.rs:84](../../crates/libs/fube-runtime/src/packages.rs#L84)).

**A retained path is not a directory lease.** Old descriptions/settings remain
readable from owned memory; native deployment dependencies and garbage collection
control old files independently. `path().exists()` is only a momentary observation.
No post-upgrade retention duration or directory pinning is promised.

### Package subscriptions

Keep the RAII handle returned by `subscribe_configuration` or `subscribe_data`.
Callbacks receive `PackageChange::{Added,Removed,Modified}` with owned packages;
Modified carries previous/current packages. Drop or explicit `unregister()`
removes the registration from future category-list snapshots; repeated unregister
is harmless. There is no initial replay, veto, callback-drain or remote undo
([subscriptions:46-130](../../crates/libs/fube-runtime/src/package_context.rs#L46-L130)).

Publication precedes synchronous callbacks. Configuration runs before data,
with a separately copied registration list at each category's start. For each
handler in registration order, modifications/removals follow old declaration
order, then additions follow new declaration order. Matched names generate
Modified only when **digested rollout changes**, not from a settings/version diff.
Self-unregister cannot recall further events already copied for that dispatch;
a configuration callback can register a handler for the subsequent data category.
Getters, registration/unregistration and captured-handle destructors run without
publication/registration locks held across user code
([updater:17](../../crates/libs/fube-runtime/src/package_context_update.rs#L17)).

One local update worker serializes refreshes; this is stronger than a claim of
native global callback FIFO. User callbacks run on a blocking worker, not the
socket reader. An async supervisor outside that worker enforces a receipt-relative
deadline, including queue delay, capped at 30 s. Panic, timeout, IPC closure,
shutdown or authority failure prevents applied-success reply admission and
terminally retires/aborts the host. A blocked callback cannot evade the independent
hard fence through healthy lease renewal. Shutdown cannot disarm the fence while
a package worker is active
([lib.rs:322](../../crates/libs/fube-runtime/src/lib.rs#L322)).

Successful updates require the original host/application/package/code/service
incarnation and a newer native version-instance counter; conflicting/stale input
is terminal, not falsely acknowledged. An exact current duplicate is idempotent
and does not reread newer Current.xml or replay callbacks. The separate positive
code-activation instance is **first learned** on the original trusted IPC
connection after startup identity/sequence matches, then pinned. It is not aliased
to another counter or independently prevalidated from startup environment.

Success is replied only after publication and callbacks, with deadline/cancellation/
authority checked again at the transport writer's pre-frame boundary. Sender
cancellation is not remotely observable, and local failure does not roll back an
already-published snapshot or revoke delivered callbacks/already-started frames
([reply fencing:156](../../crates/libs/fube-transport/src/connection.rs#L156)).

## Wire and ownership invariants

Generic IPC is an explicit transport role, separate from the gateway notification
exception. Requests and one-way sends carry host/PID IPC identity; consuming
correlated replies do not. Lifecycle RA actor **12** is used in both directions;
Open/Close semantic replies are one-way, not `RelatesTo` responses
([connection.rs:349](../../crates/libs/fube-transport/src/connection.rs#L349),
[connection.rs:509](../../crates/libs/fube-transport/src/connection.rs#L509)).

Each service unit has independent operation ownership; callbacks never run on
the socket reader or under registry/state locks. Duplicate Open reuses the
stored endpoint; duplicate Close acknowledges a disposed unit. Stale epochs and
incarnations are fenced. All successful Open replies, including cached duplicates,
check terminal authority at the common publication boundary
([lifecycle.rs:346](../../crates/libs/fube-runtime/src/lifecycle.rs#L346)).
Incompatible operations follow native retry/drop
semantics; Abort/Drop can cancel an opening instance, followed by abort cleanup.
Graceful Close errors become successful native Close only **after** cleanup,
while internal failure counters remain observable
([lifecycle.rs:252](../../crates/libs/fube-runtime/src/lifecycle.rs#L252),
[lifecycle.rs:574](../../crates/libs/fube-runtime/src/lifecycle.rs#L574)).

RAPQuery uses the common DTO but a **default service description**, not Open's
manifest identity. Query validation is operation-specific and replies reflect
actual local operation/state. Equal-version description updates acknowledge
without changing stored data; only newer versions apply
([lifecycle.rs:615](../../crates/libs/fube-runtime/src/lifecycle.rs#L615),
[lifecycle.rs:651](../../crates/libs/fube-runtime/src/lifecycle.rs#L651)).
Unknown required actions/schema fail the host, never silently succeed.

## Authority and process-wide failure

Lease renewal is anchored to monotonic **request send time plus native TTL**,
not reply receipt. Initial registration TTL is retained as diagnostic data but
does not grant serving authority; a fresh correlated poll does. Retired authority
cannot be renewed. Poll failures leave the previous deadline, not a new lease
([lease.rs:46](../../crates/libs/fube-runtime/src/lease.rs#L46)).

**Dropping the attached runtime, failed/cancelled shutdown, IPC loss, invalidation
or lease expiry is process-wide and terminal.** An independently owned OS thread
uses atomic authority state and `_exit(70)` without stdio locks or application
cleanup. Nominal hard termination is 500 ms after retirement/expiry, checked
every 10 ms; the qualified acceptance allowance is one second. There is no
reconnect/revival. This can stop unrelated services sharing that application
process, by design
([lease.rs:88](../../crates/libs/fube-runtime/src/lease.rs#L88),
[lib.rs:372](../../crates/libs/fube-runtime/src/lib.rs#L372)).

Cancellation is advisory. Service callbacks must own and dispose their resources.
Direct blocking of arbitrary async workers is not a supported entry for waiting
SDK calls; use owned execution. Authority fencing is independent of application
executor progress. Successful normal shutdown requires no active
callbacks and proven abort/disposal before watchdog disarming.

## Capability inventory

| Operation | Behavior |
| --- | --- |
| Open/Close/Abort, stateless query, description update | Implemented; historical native tests at recorded source, current offline regression |
| Lease polling and terminal IPC authority | Implemented; historical native poll/IPC-loss test and current model lease races |
| InvalidateLease | Terminal retirement, correlated acknowledgment; model coverage |
| UpdateCodePackageContext/upgrade refresh | Same-incarnation unchanged-code refresh; atomic snapshots and config/data events |
| Instance health/load/fault/move-cost reporting | Bounded native application-runtime paths; completion levels below |
| Query reported load | Bounded local last-written metrics via native RAPQuery, distinct from independent FM retention |
| Partition-scoped health | Implemented distinct kind/tag 3; current source-contract/model coverage, not new deployed qualification |
| Named partitions | API/codec/model support; not native-qualified by this suite |
| General stateful/replication/security-required profiles | Out of scope here; separate empty-state stateful subset, no insecure/native fallback |

If an unsupported operation becomes necessary for continued hosting, placement
or restart, that scenario is a blocker—not successful profile qualification.
Management `sfctl replica remove` **without force** exercises genuine native
Abort. Force-remove kills the host and is not accepted as Abort evidence.

## Partition reporting

Canonical reports synchronously return `Result<()>` on a supported reserved
execution entry; only owned observations are awaited. Information is available
via object-safe `get_partition_info`; extension identity is only an observation,
never routing authority. The runtime binds private wire records to the original
partition/public instance/RAP incarnation and checks terminal authority and
generation validity at admission and at the writer's pre-frame boundary
([API and Rustdoc](../../crates/libs/fube-runtime/src/api.rs#L84-L150),
[writer fence](../../crates/libs/fube-runtime/src/reporting.rs#L244-L249),
[load admission/completion](../../crates/libs/fube-runtime/src/reporting.rs#L269-L383)).

| API | Native application path | Successful completion |
| --- | --- | --- |
| `report_load(&[LoadMetric])` | actor 12, `RAPReportLoad`, partition/service/node metrics | Complete local frame write, **not** RA/FM acknowledgment or persistence |
| `report_move_cost(MoveCost)` | Same path, `_MoveCost_` value Zero/Low/Medium/High = 0/1/2/3 | Same one-way write semantics; not an imperative move |
| `report_instance_health(&HealthInformation)` | actor 7, `ReportHealth`, kind/tag 2, originating stateless instance | One correlated, identity/sequence-matched HM **per-report** success |
| `report_partition_health(&HealthInformation)` | actor 7, `ReportHealth`, kind/tag 3, partition-only entity | Same exact-result completion |
| `report_fault(FaultType)` | actor 12, `RAReportFault`, native FUD/replica identity | Initial local write, or coalescing with that pending stage's initial result; **not** recovery acknowledgment |

Load requires 1–64 unique, currently declared custom metrics with values below
`INT_MAX`. Unknown metrics are explicitly rejected rather than silently filtered.
Partial updates retain other metrics; no report TTL or automatic clearing is
promised. RAPQuery exposes up to **65 total retained names**, shared by custom
metrics and move cost, from successful local writes for that generation. No
move-cost slot is reserved: accumulating 65 custom names through partial reports
or description updates causes a first move-cost report to return ResourceLimit.
Removed manifest metric names remain in this bounded local retention until
closure; further new names can hit the retention limit. FM retention is separate,
partition-keyed and can outlive
an instance. Its native query returns stateless secondary reports in the primary
load array. `_MoveCost_` retention proves receipt, not that effective PLB policy
uses the cost in placement decisions
([load implementation](../../crates/libs/fube-runtime/src/reporting.rs#L269-L383)).

Health supports only Ok/Warning/Error. Source/property are nonempty, bounded
UTF-16 text without NUL; `System*` sources and `DeleteProperty` are reserved.
Descriptions exceeding 4096 UTF-16 units are rejected, not silently truncated.
`sequence_number: 0` allocates one process-monotonic positive sequence;
positive values are explicit and negative values invalid. HM requires strictly newer sequences for the same
instance/source/property: duplicates/older values are errors, not TTL refreshes.
`time_to_live_seconds: 0` maps to native infinite TTL, **not zero wire ticks**.
Finite public TTLs are whole seconds. HM expiration starts at its last-modified time,
not the source timestamp or enqueue time. With `remove_when_expired=false`,
the event retains its reported state but becomes expired and evaluates Error;
with `true`, it disappears from health query evaluation before eventual physical
cleanup. Sequence history after deletion is not promised
([health implementation](../../crates/libs/fube-runtime/src/reporting.rs#L386-L511)).

Health is one request/report with no hidden batching or retries. A timeout does
not prove the report was never applied; retry with the same explicit sequence
when preserving update identity. Both native-versioned `report_*_health2`
methods take optional borrowed `HealthReportSendOption` **after** information.
Both Immediate values directly send; true adds no reporting-interval delay,
but neither value means remote success/durability. All report methods return
synchronous `Result<()>` on the reserved execution entry. The explicit
`InstanceRuntimeIntegration::observe_report()` wrapper binds one observation
before its single call; its health sequence is matched to the HM
result; outer transport success alone is never reported as health success.
HM success is not a promise of a fresh durable transaction. Close/Abort cancels
outstanding load/health waits promptly, releasing local report/RPC work without
waiting for HM. A later old HM reply cannot acknowledge the cancelled call;
cancelling that wait does not remotely revoke an already-started report.

Fault transient/permanent map to internal native values 1/2 (the reverse of
public COM numeric values). Native stateless transient fault requests Drop and
replacement; permanent fault requests Abort. The runtime retains at most one
fault stage/worker per generation and retries its bound body every 30 seconds.
Equal faults coalesce; permanent supersedes transient. Pending transient callers
receive an explicit superseded error if their initial result is not yet fixed.
Already-completed initial results are immutable. Even an **initial write error**
can leave the stage pending for retry: inspect the explicit
`InstanceRuntimeIntegration::fault_delivery()` observation for current
attempt count, latest write error, supersession and lifecycle cancellation.
There is no fabricated fault acknowledgment or retry history collection
([fault implementation](../../crates/libs/fube-runtime/src/reporting.rs#L518-L676)).

**Fencing limit:** obsolete unsent frames are cancelled at the writer validity
boundary and never retargeted to a replacement. A frame that has passed that
boundary cannot be revoked remotely. Load/move wire data has **no incarnation**;
health has a public instance ID but no RAP incarnation. Accordingly, already-started
one-way writes may remain in native retention after closure; retained data is not
proof that a successor reported it. Native fault receiver incarnation checks
are stronger. Cancellation/timeout after frame start is uncertain delivery,
not proof of nonapplication. Closed/aborted and terminal-authority handles reject
new reports; load/move have no automatic retry.

## Finite bounds

| Resource/deadline | Limit |
| --- | --- |
| Fabric frame/object | 4 MiB each; header area separately bounded by transport |
| Strings / collections / nesting | 32,768 UTF-16 units / 4,096 elements / depth 32 |
| Pending requests / ordinary queue / reserved control queue | 32 / 32 / 32 |
| Runtime control and RAP ingress | 32 each; overflow retires the host |
| Live service units / admitted operation tasks / factories | 32 / 32 / 32 |
| Retained unit/tombstone records | 4,096; overflow retires the host |
| Registration | 30 s per request, 120 s total attach |
| Lease | Poll every 1 s after completion, request timeout 5 s; native TTL governs authority |
| Async callbacks | 30 s cooperative deadline; abort cleanup 250 ms |
| Orderly shutdown / independent hard fence | 400 ms / 500 ms after retirement or expiry |
| Native replacement acceptance | 180 s from exact application termination through usable RPC |
| Concurrent report sends/health waits | 16 runtime-wide, leaving ordinary lease-poll admission; explicit ResourceLimit on overload |
| Per-unit load/health work | One admitted call; fault coalescing has one separate bounded stage/worker |
| Report metric count / retained metric names | 64 per call / 65 per generation including move cost |
| Report source/property/description/metric names | 4096 UTF-16 units each, no NUL |
| Reporting request deadline / retained fault retry | 30 s / 30 s; no load/health retry |
| Sample retired handles / incoming RPC | 32 / 64; bounded acceptance-only fixtures |
| Artifact file / complete snapshot input | 1 MiB / 16 MiB including Current.xml stability reread |
| Configuration / data packages | 64 each; at most 4096 parameters snapshot-wide and 4096 sections per config package |
| Artifact strings / XML depth | 32,768 UTF-16 units / 32; DTD and processing instructions rejected |
| Package subscriptions / active update worker | 64 combined / 1; control ingress remains 32 |
| Package update / callback work | Receipt-relative 30 s cap; conservative 16,384 callback invocations per update |
| Sample retained package notifications | 32; overflow explicitly fails the fixture |

Queues do not silently discard required replies. Source/model limits and resource
tests are in [transport tests](../../crates/libs/fube-transport/src/connection_ipc_tests.rs),
[runtime models](../../crates/libs/fube-runtime/src/model_tests.rs) and
[lifecycle models](../../crates/libs/fube-runtime/src/lifecycle_tests.rs).

## Reproduce native acceptance

Build the [onebox image](../UbuntuOnebox.md), then:

```sh
cargo build --locked --workspace --bins --examples --all-features
python3 -m tests.onebox.shared.runtime --build-bundle build/runtime-binaries
python3 -m tests.onebox.shared.runtime runtime-check --prebuilt build/runtime-binaries \
  --full --report build/runtime-check.json
```

The explicit Cargo command builds the application and its independent observer.
`--build-bundle` packages existing outputs and locked dependency graphs from clean
source; it never compiles. Native qualification requires `--prebuilt`.
To transfer that bundle and qualify without Rust/Cargo:

```sh
tar -cf build/runtime-binaries.tar -C build/runtime-binaries .
# On the qualification runner, extract into a new directory:
mkdir build/runtime-prebuilt
tar -xf build/runtime-binaries.tar -C build/runtime-prebuilt
python3 -m tests.onebox.shared.runtime runtime-check --prebuilt build/runtime-prebuilt \
  --full --report build/runtime-prebuilt.json
```

The bundle retains executable permissions and includes the build-time dependency
graph plus SHA256 hashes for both binaries **and both versioned package templates**
(bundle format 3, profile `linux-x86_64-volatile-empty-control-v1`,
producing revision recorded). Prebuilt mode rejects missing,
extra, symlinked, nonregular, nonexecutable or modified inputs and invalid dependency evidence before
allocating a onebox; it never compiles or invokes Cargo. ELF dependencies and
live application mappings are still checked on the qualification runner. CI
produces the bundle in the Linux native-free job, then downloads/extracts it in
the onebox job without installing a Rust toolchain.

The harness uses the existing lifecycle controller, creates/destroys only its
owned allocation, mints unique app/type names, and reconciles ambiguous native
mutations. `--existing-owner OWNER` explicitly reuses a caller-owned healthy
box and never destroys it. It still cleans its exact minted application and
every provisioned type version, polling for absence. Upload staging is uniquely
owned beneath the allocation's data volume and removed after use.
Ownership drift, failed cleanup or unconfirmed late mutations produce FAIL with
recovery identities
([harness ownership](../../tests/onebox/shared/runtime.py#L102),
[cleanup](../../tests/onebox/shared/runtime.py#L421)).
Creation captures its original owner capability under the existing controller
lock; a later same-name allocation never grants cleanup authority
([create_owned](../../tests/onebox/shared/runtime.py#L471)).

The authority test needs Linux x86-64 pidfd APIs and the existing privileged
onebox to duplicate **only the verified application's IPC descriptor**, recheck
its inode, and shut that socket down. This is test-only OS fault injection, not
an application runtime helper, node modification or security fallback.
Unavailable kernel capabilities block that test.

### Recorded package-update outcome (2026-10-27)

After refreshing against main `ecd8959ce18e1ede2fadf9f0105d5f1714291a69`,
post-review full native run **`9c15deaa3c04`** passed on SF **11.8.121.4**, using the
strict prebuilt bundle produced from code revision
`1fcbd3d180d2d102f9eb0ae121846a86cc1cc8b7`.
Local evidence: `build/package-context-native-final2.json`;
bundle manifest SHA256
`840923d5db871a93e33f47cfdbed7a2e9245aec8bc1bac960856c212fa5ddcef`.
This fresh allocation/bundle supersedes pre-review run `dc9c2e7b0053`; old
evidence was not relabeled after executable fixes.

- Real unforced application 1.1→1.2 upgrade delivered native context refresh:
  Config version 1.0→2.0, config/service rollout 1.0→1.1, applied native override;
  unchanged Code version/rollout/checksum and running executable bytes.
- PID **23326**, process start ticks **14908311**, host
  `5e5373dd-79a9-4bbc-bb63-48e3d0969f05`, runtime
  `952728d4-67ba-486f-87b4-e8c52c053c5f` and native entry-point instance
  **134375486847215728** stayed unchanged. **35** independently resolved RPC
  observations during upgrade succeeded, with zero sampling errors.
- Current generation became 1; both retained old context and package handle
  still read `before-upgrade`, current read `after-upgrade`; two callbacks ran
  in registration order after publication, and the unregistered handler ran zero
  times. Old path existence was observed, not promised or pinned.
- Later native concurrent service creations both received generation 1 and new
  settings. Existing reporting/lifecycle/failure/authority/replacement checks
  passed, including independent dependency/ELF/live-map evidence.
- The application and **both** type versions were confirmed absent. The task
  controller then destroyed its original allocation; exact container, data volume
  and network absence were independently verified. No unrelated pre-existing
  onebox was reused or commandeered.

The source revision remains a research baseline, **not** binary build provenance.
These observations qualify this successful unchanged-code switch profile, not
unconditional no-restart upgrades, rolling CODE upgrade/rollback, directory
retention timing or stronger binary-wide callback guarantees. Ordinary workspace
checks/Rustdoc, 51 runtime tests and 139 Python tests passed on Linux. At that
revision, Windows and separate ignored native-client fixture groups were CI
coverage, not local Windows claims. The current repository is Linux-only.

### Recorded native outcome (2026-10-26)

Run `249e33468e62` on SF 11.8.121.4 passed the full declared suite:

- SF process replacement plus independent resolution/RPC: **8.049 s**.
- Two distinct range partitions shared PID **50881**, with **two overlapping
  Open callbacks**; native Abort replaced only its target partition and siblings
  continued serving from that PID.
- Native correlated replica-detail query succeeded; both description updates
  were acknowledged; Open failure published no endpoint; Close failure ran Abort
  cleanup before acknowledgment.
- Exact IPC loss at monotonic **128380.411824986**: the deliberately noncooperative
  old listener still answered at **+0.493 s**, but the independent watchdog
  terminated the process at **+0.505 s**. Native exit status **17920 = 70 << 8**
  confirmed `_exit(70)`.
- Fresh poll TTL **313,860,000 ticks (31.386 s)**; initial TTL was 313,910,000 ticks.
  The poll deadline was send-based; receipt did not establish a fresh TTL origin.
- Dependency tree, ELF NEEDED and both live process mapping sets contained no
  FabricRuntime/FabricCommon. Loaded libraries were libc, libm, libgcc_s and the
  ELF loader, plus the Rust executable.
- Only the minted app/type were deleted; absence was confirmed. The workflow's
  caller-owned box was retained until its final explicit teardown.

Additional fresh singleton runs recovered in 1.516 s and 4.381 s (the latter
moved nodes). Earlier failed experiments were not counted: recycled application
URI produced stale native code-package observations; sfctl's named-list
serialization failed; initial query validation rejected native default data;
the kernel rejected a test-only SOCK_DESTROY attempt. Unique identities,
operation-correct query validation and explicit pidfd-bound socket injection
resolved these without relaxing runtime safeguards.

Deterministic stale/duplicate commands, delayed/zero TTL, cancellation/panic and
blocked-stderr/executor-independent watchdog tests remain **model evidence**.
The native authority test proves IPC-loss fencing, not complete node shutdown,
upgrade, TLS or delayed-node-lease interoperability.

### Reporting outcome (2026-10-27)

Compiler-free prebuilt run `6bc5933d7071` on SF 11.8.121.4 passed the full suite:

- Independent FM query observed `RuntimeWork=731` and `_MoveCost_=3` with nonzero
  native timestamps; these were not RAP/self-echo measurements.
- HM query matched the stateless public instance, observed sequence100 Warning,
  rejected stale99 with native error `-2147017711`, retained100 unchanged, then
  accepted101 Ok. Finite TTL tests observed retained expired events evaluating
  Error, higher-sequence renewal, and remove-on-expiration disappearance.
- Runtime `report_fault(Transient)` changed instance/incarnation
  `134375374383572350` → `134375374484938620` for partition
  `1482d521-857c-9649-8839-6318066a4c13`, while PID23240/host
  `14724700-b548-42f9-8a13-0d79d096df1a` and the sibling stayed unchanged.
  Separate fube-client resolution reached usable replacement RPC.
- All four old-handle report calls returned Closed; the active replacement's
  distinct load947 subsequently appeared in FM. Retained pre-replacement load
  was not counted as successor evidence.
- Existing lifecycle/query/update/failure tests, native-free graph/ELF/live maps
  and independent IPC authority fencing (0.513s) also passed. Only owned app/type
  and onebox resources were cleaned; application absence and onebox data removal
  were confirmed.

The first two reporting experiments failed in the **observation harness** (wrong
REST replica path, then assuming nested rather than flattened health JSON) and
are not counted as passes. Reporting uses no HTTP/management send shortcut.
Deterministic queue, admission, initial-result/retry, mismatched health result and
lifecycle/authority cases are model evidence, not additional native claims.

Final tar-extracted prebuilt rerun `688940f27899` also passed with Cargo absent.
It repeated FM/HM reporting and replaced instance/incarnation
`134375379356725150` → `134375379458523610` in **1.853s**, retaining PID23334,
host `c93c1757-808d-4f07-8f35-a71dd086df2a` and the sibling. It additionally
checked all four rejected report categories after native Abort and inspected
the exact post-report process mappings (only executable, libc, libm, libgcc_s,
loader). Independent IPC-loss fencing took **0.507s**. Application/type and owned
`partition-reports-final` container/network/data cleanup all completed.
