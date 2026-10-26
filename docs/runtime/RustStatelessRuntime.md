# Rust stateless application runtime

`mssf-runtime` is an experimental **application-side** Rust hosting/RAP runtime.
Real Service Fabric nodes start the executable, authorize registration, place
stateless instances and replace failed application processes. It does not load
FabricRuntime/FabricCommon, adapt COM or call a native helper for runtime work.
The native SF node, deployment tools and OS libraries are still expected.

## Qualified profile

| Boundary | Qualification |
| --- | --- |
| Platform/node | Linux x86-64, native SF 11.8.121.4, repository unsecured onebox |
| Activation | SF-activated normal ExeHost, one code package, SharedProcess |
| Services | Singleton and concurrent Int64-range stateless partitions in one process |
| IPC | Actual node-injected loopback address, trusted clear transport; no reconnect |
| Lifecycle | Native type registration, Open, graceful Close, Abort flag 4, failure cleanup |
| Controls | Correlated stateless RAPQuery; service-description updates |
| Discovery | Separate mssf-client resolution, then real RPC to that returned endpoint |
| Recovery | Exact owned-process termination, new native host/PID/incarnation, usable endpoint |
| Authority | Fresh node lease poll before serving; terminal IPC/lease/host authority |

This is not a complete replacement for every FabricRuntime feature. Named
partition decoding has model coverage, not native qualification. Package/config
XML APIs, upgrades/context refresh, stateful services, replication/storage,
containers, activator/multicodepackage hosts, Windows, TLS and COM ABI compatibility
are outside this profile. Unsupported security is rejected, never downgraded.
The broader [design](RustFabricRuntime.md) retains these separate capability tiers.

## Owned programming model

Implement `StatelessFactory::create` and `StatelessService::{open,close,abort}`.
The API uses owned identities/initialization bytes, `Arc`, `Result` and boxed
`Send` futures rather than COM lifetimes
([api.rs:13-49](../../crates/libs/mssf-runtime/src/api.rs#L13-L49)).

1. `Runtime::attach()` consumes actual SF-injected activation identity and
   completes host/runtime registration plus a fresh lease poll.
2. `register_stateless(name, Arc<Factory>)` retains the factory for the runtime
   lifetime. There is no invented type-unregister operation.
3. Factory creation receives the service URI/type, partition GUID, public
   stateless instance ID, separate RAP incarnation, initialization bytes and
   immutable activation snapshot.
4. Open receives a generation-bound partition and cancellation token. Bind the
   real application listener **before returning** its opaque endpoint string.
5. Close asynchronously disposes resources. Abort synchronously disposes them
   before returning; neither may rely on a dropped future as proof of cleanup.
6. Explicitly await `shutdown()` for orderly unregistration.

See the actual [listener-backed fixture](../../crates/samples/rust-runtime-app/src/main.rs).
Its default routes check partition validity per RPC. `/FailOpen`, `/FailClose`,
`/Concurrent` and `/Uncooperative` suffixes are **acceptance-only failure fixtures**,
not application deployment conventions. The last intentionally ignores authority
in both listener/main to prove independent hard process fencing.

The activation snapshot intentionally contains real host/package/incarnation,
work/log/temp directories, node IP and resolved endpoint ports—not a complete
native activation/configuration API
([context.rs:7-29](../../crates/libs/mssf-runtime/src/context.rs#L7-L29)).
Node context comes from the actual Start registration reply. No missing-runtime-
access fallback discovers a node address or invents host identity.

## Wire and ownership invariants

Generic IPC is an explicit transport role, separate from the gateway notification
exception. Requests and one-way sends carry host/PID IPC identity; consuming
correlated replies do not. Lifecycle RA actor **12** is used in both directions;
Open/Close semantic replies are one-way, not `RelatesTo` responses
([connection.rs:349](../../crates/libs/mssf-transport/src/connection.rs#L349),
[connection.rs:509](../../crates/libs/mssf-transport/src/connection.rs#L509)).

Each service unit has independent operation ownership; callbacks never run on
the socket reader or under registry/state locks. Duplicate Open reuses the
stored endpoint; duplicate Close acknowledges a disposed unit. Stale epochs and
incarnations are fenced. All successful Open replies, including cached duplicates,
check terminal authority at the common publication boundary
([lifecycle.rs:346](../../crates/libs/mssf-runtime/src/lifecycle.rs#L346)).
Incompatible operations follow native retry/drop
semantics; Abort/Drop can cancel an opening instance, followed by abort cleanup.
Graceful Close errors become successful native Close only **after** cleanup,
while internal failure counters remain observable
([lifecycle.rs:252](../../crates/libs/mssf-runtime/src/lifecycle.rs#L252),
[lifecycle.rs:574](../../crates/libs/mssf-runtime/src/lifecycle.rs#L574)).

RAPQuery uses the common DTO but a **default service description**, not Open's
manifest identity. Query validation is operation-specific and replies reflect
actual local operation/state. Equal-version description updates acknowledge
without changing stored data; only newer versions apply
([lifecycle.rs:615](../../crates/libs/mssf-runtime/src/lifecycle.rs#L615),
[lifecycle.rs:651](../../crates/libs/mssf-runtime/src/lifecycle.rs#L651)).
Unknown required actions/schema fail the host, never silently succeed.

## Authority and process-wide failure

Lease renewal is anchored to monotonic **request send time plus native TTL**,
not reply receipt. Initial registration TTL is retained as diagnostic data but
does not grant serving authority; a fresh correlated poll does. Retired authority
cannot be renewed. Poll failures leave the previous deadline, not a new lease
([lease.rs:46](../../crates/libs/mssf-runtime/src/lease.rs#L46)).

**Dropping the attached runtime, failed/cancelled shutdown, IPC loss, invalidation
or lease expiry is process-wide and terminal.** An independently owned OS thread
uses atomic authority state and `_exit(70)` without stdio locks or application
cleanup. Nominal hard termination is 500 ms after retirement/expiry, checked
every 10 ms; the qualified acceptance allowance is one second. There is no
reconnect/revival. This can stop unrelated services sharing that application
process, by design
([lease.rs:88](../../crates/libs/mssf-runtime/src/lease.rs#L88),
[lib.rs:372](../../crates/libs/mssf-runtime/src/lib.rs#L372)).

Cancellation is advisory. Service callbacks must own and dispose their resources.
Blocking executor code is unsupported callback behavior; authority fencing is
independent of executor progress. Successful normal shutdown requires no active
callbacks and proven abort/disposal before watchdog disarming.

## Capability inventory

| Operation | Behavior |
| --- | --- |
| Open/Close/Abort, stateless query, description update | Implemented; real native tests |
| Lease polling and terminal IPC authority | Implemented; native poll/IPC-loss test and model lease races |
| InvalidateLease | Terminal retirement, correlated acknowledgment; model coverage |
| UpdateCodePackageContext/upgrade refresh | Explicit terminal refusal; no stale-context acknowledgment |
| Partition health/load/fault/move-cost reporting | Explicit local error; no empty batches or success logs |
| Query reported load | Empty because reporting is unsupported, not fabricated measurements |
| Named partitions | API/codec/model support; not native-qualified by this suite |
| Stateful/replication/security-required profiles | Rejected/out of scope, no insecure/native fallback |

If an unsupported operation becomes necessary for continued hosting, placement
or restart, that scenario is a blocker—not successful profile qualification.
Management `sfctl replica remove` **without force** exercises genuine native
Abort. Force-remove kills the host and is not accepted as Abort evidence.

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

Queues do not silently discard required replies. Source/model limits and resource
tests are in [transport tests](../../crates/libs/mssf-transport/src/connection_ipc_tests.rs),
[runtime models](../../crates/libs/mssf-runtime/src/model_tests.rs) and
[lifecycle models](../../crates/libs/mssf-runtime/src/lifecycle_tests.rs).

## Reproduce native acceptance

Build the [onebox image](../UbuntuOnebox.md), then:

```sh
python3 scripts/rust_runtime_test.py runtime-check --full --report build/runtime-check.json
```

The harness uses the existing lifecycle controller, creates/destroys only its
owned allocation, mints unique app/type names, and reconciles ambiguous native
mutations. `--existing-owner OWNER` explicitly reuses a caller-owned healthy
box and never destroys it. It still cleans its exact minted application/type.
Ownership drift, failed cleanup or unconfirmed late mutations produce FAIL with
recovery identities
([harness ownership](../../scripts/rust_runtime_test.py#L40),
[cleanup](../../scripts/rust_runtime_test.py#L356)).
Creation captures its original owner capability under the existing controller
lock; a later same-name allocation never grants cleanup authority
([create_owned](../../scripts/rust_runtime_test.py#L406)).

The authority test needs Linux x86-64 pidfd APIs and the existing privileged
onebox to duplicate **only the verified application's IPC descriptor**, recheck
its inode, and shut that socket down. This is test-only OS fault injection, not
an application runtime helper, node modification or security fallback.
Unavailable kernel capabilities block that test.

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
