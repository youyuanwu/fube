# Native classic replicator oracle

**Test-only native-backed executable, not the production Rust engine.** With no
arguments this is an SF-activated code package. It creates an actual native
runtime, registers only `RustStatefulRuntimeType`, and implements the published
stateful factory/replica interfaces. Its real node-supplied partition calls
`CreateReplicator` with a state-provider bridge. `EndOpen` returns that exact
native `IFabricReplicator`, not a Rust replicator wrapper. This selects classic
V1, not the transactional replicator.

The shared `fube-data::MemoryProvider` is **application payload and state only**:
versioned map history, mutations and request-result ledger. This executable
never constructs `fube_data::Engine`. HTTP is client/observer traffic only;
native V1 implements all replication, membership, configuration and quorum.
The node, not environment role hints or a test driver, assigns roles.

## Build and local validation

```sh
cargo build --locked -p native-replicator-oracle
cargo check --locked -p native-replicator-oracle --all-targets
cargo test --locked -p native-replicator-oracle
cargo clippy --locked -p native-replicator-oracle --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked -p native-replicator-oracle --no-deps
target/debug/native-replicator-oracle --abi-self-test
```

`--abi-self-test` remains an entirely separate **local ABI check**: it does not
register a runtime, load a native engine or claim native interoperability.
Unit tests exercise published COM interfaces, live asynchronous provider
bridges, exact-horizon copy, null EOS, failures, cancellation, ownership, native
operation application-before-ACK and the common request parser. Typed native
COM doubles cover the actual service replica's CreateReplicator/EndOpen identity,
reentrant callback End/Abort, native write Begin failure (including dirty
out-LSN), assigned-LSN/local-apply-before-End, delayed failed End, a dropped
HTTP waiter, and known/conflicting/copied retries. They do **not**
establish deployed native interoperation. Actual same-partition copy, writes,
swap/failover and replacement must be executed by the external qualification
harness in both Rust-primary/native-secondary and native-primary/Rust-secondary
directions. No cluster resources are allocated by the local commands above.

## Activation and endpoints

Deploy this binary through a test-only node-selecting launcher that preserves
SF's activated environment. The service package must declare
`RustStatefulRuntimeType`; a separately declared `RustRuntimeType` need not be
registered by this binary. Initialization data may be empty or `fube-data-v1`.
Activation must expose a nonzero HTTP/TCP endpoint named **`Rpc`**.

The native `NodeContext` supplies the actual IP and node name. Injected
`SF_NODE_NAME`, `SF_NODE_IP`, `Fabric_NodeName` and `Fabric_NodeIPOrFQDN`, when
present, must agree. `host` is SF's `Fabric_ApplicationHostId` GUID, not a
fabricated process identity. This fixture requires a numeric native node IP.
Native replication binds to that IP at port zero, separately for every replica.
It never uses the HTTP port as a replication address.

Every factory-created replica has a unique native-fixture incarnation and
publishes:

```text
http://<native-node-IP>:<Rpc>/data/<partition>/<replica>/<incarnation>/service
```

Sibling operations use that same owner path:

| GET operation | Parameters / semantics |
|---|---|
| `service` | Authoritative primary read; actual native read access must be Granted. |
| `diagnostics` | Observations on primary or secondary, not an authority grant. |
| `write` | Exactly `id`, `key`, `value`; percent-encoded UTF-8 supported. |
| `test` | Exactly **one** of `ack_delay_ms=0..2000` (secondary apply/ACK) or `copy_delay_ms=0..100` (source provider copy item); decimal, qualification-only. |

Requests are limited to **4096 bytes** including headers. Invalid UTF-8,
malformed percent encoding, duplicate/missing/unknown query parameters and
unsupported operations fail. Connections are bounded to 64 and have a 12-second
request lifetime. Successful writes return HTTP 200 / `outcome: committed`;
accepted but uncertain writes return 202 / `ambiguous`; invalid requests return
400, and authority/admission rejection returns 503. Write JSON includes
`request_id`, `duplicate` and, after acceptance, `sequence`.

Reads and diagnostics use the shared schema: `profile: fube-data-v1`,
`engine_kind: native-classic-v1`, `partition`, `replica`, `incarnation`,
`engine_incarnation`, `node`, `host`, `pid`, `service_address`, `native_epoch`,
actual partition `read_status` / `write_status`, `state` (the unchanged
`MemoryProvider.snapshot()`), and an `engine` diagnostic object.

`role` / `current_role` are the actual native service role callback.
`replicator_role` and `replication_address` are deliberately **null**: the
reconfiguration agent owns the native replicator's `Open` / `EndOpen`; calling
them a second time to intercept the control address is invalid. The bind
address is separately labeled `replication_bind_address`. Query the native
replica/RA through the external native observer for the actual published
replication address and replicator role. No guessed address, role or peer list
is substituted.

`native_epoch` preserves full data-loss/configuration values from the actual
provider callback, without projecting configuration bits; it is null until a
successful native epoch callback has been observed. Effective settings
are queried through native `IFabricStateReplicator2::GetReplicatorSettings`;
query failures remain explicit. Requested settings are also recorded:
**RequireServiceAck=true AND its flag**, replication/copy initial queues 64,
and bounded maximum queues 1024. `native_current_progress` is separately labeled
as the raw native control API observation, not silently equated with quorum.

`engine.progress.applied` is provider progress; `received` is native operation
receipt/local native assignment, and `assigned` is locally observed native
primary assignment. `committed` / `completed` are lower bounds from **successful
local native EndReplicate results whose identities remain in current state**.
`client_completed` counts only writes actually returned as committed. The
secondary's quorum watermark is not publicly observed and is never inferred
from provider progress or its application ACK. Unknown native peer observations
remain null. These limitations are explicitly labeled in diagnostics.

`copy_delay_ms` defaults to zero and is shared with the live provider's
`BeginGetNext` work. It delays each actual non-null source copy item, not EOS,
HTTP writes, or replication operations, inside the existing eight-second
per-callback deadline and context/replica cancellation. Cancellation or timeout
after enumeration has started makes that stream terminal; it cannot silently
skip an already enumerated item and resume.

`native_copy_horizons` is a bounded inventory of the **last 32 actual accepted
native GetCopyState calls**, not simulated HTTP copy requests. Each entry has:

- `requested_horizon`, unchanged from native `upToSequenceNumber`;
- `provider_progress_at_request`, and nullable `build_context` containing the
  same requested horizon, provider progress and full provider epoch at snapshot
  creation; progress may exceed the horizon but the snapshot never does;
- `items` (non-null items produced), `begin_count`, `end_count` (validated,
  consumed Ends, including failed results), and `callback_count` (non-null
  completion callback invocations);
- `enumeration_end` and nullable `provider_progress_at_end`, recorded at the
  first actual source EOS; repeated EOS does not overwrite that observation;
- `copy_context: native_volatile_null`.

These observations let qualification prove nonempty copy, its actual native
boundary and source advancement during enumeration. They are **not** quorum or
secondary-build-completion claims. `native_streams` separately counts received
copy/replication items and observed stream EOS on a consuming secondary.
`replication_original_epochs` remains null: the published native
`IFabricOperation` metadata does not expose an original replication epoch.

## Native provider, writes and lifecycle

Primary admission validates actual native write access and primary service
role. `BeginReplicate` assigns the LSN, then the provider applies locally
**before waiting for EndReplicate**. Completion is retained by an owned worker,
not the HTTP waiter. A five-second waiter timeout/disconnect is ambiguous, not a
rejection or cancellation of already accepted application work. Identical
known-committed retries return the original sequence without another mutation;
conflicting identities fail. Known-pending identities retain their exact
completion outcome. A copied identity without a locally observed native
completion is truthfully ambiguous, never re-replicated solely to manufacture
an answer.

Provider `GetLastCommittedSequenceNumber`, `UpdateEpoch`, `OnDataLoss` and copy
callbacks call the real shared application provider. Its progress is **not**
quorum commit knowledge. Epoch reconciliation preserves/truncates the same
versioned map and dedup ledger. The volatile copy context is an asynchronous
empty stream; `GetCopyState` accepts null context and rejects non-null context.

Copy enumeration waits up to eight seconds for local application to reach the
**exact native requested upToSequenceNumber**, covering native assignment racing
primary apply. It neither lowers the horizon nor invents progress. The snapshot
pins immutable records at that exact horizon; later writes are excluded and
remain available to native replication handoff. Unavailable history fails.

Idle-secondary pumps consume native `GetCopyStream`, then
`GetReplicationStream`; demoted primaries consume native replication. They
validate metadata and segmented payload bounds (1024 segments / 1 MiB), apply
the provider payload, then explicitly `Acknowledge`. Null EOS, non-null empty
operations, explicit EOS metadata and HRESULT failure are distinct. Empty
normal application payload is an application error, not EOS. Failed copy/apply
never ACKs or advertises successful build; a pump failure reports a native
transient partition fault. Promotion during unfinished provider copy fails.

Context work is bounded and owned; native waits propagate cancellation and
consume the native callback/End result (bounded five-second cancellation drain).
Replica Close/Abort fence HTTP admission, cancel and join pumps and application
work, drain admitted requests and release pending result captures. Cleanup
callbacks cannot be cancelled halfway through disposal.

**Synchronous native callback boundary:** the application future, its locks and
all work captures are destroyed **before** `Slot::start` publishes completion
and invokes the external COM callback. A callback may call its once-only End
and the actual service replica's synchronous `Abort`. `ReentrantAbort` marks
only currently executing external Invoke frames on that call stack. A drain
waits for all other work, but cannot join an Invoke frame currently waiting for
that same Abort to return; concurrent callback Aborts use the same narrow rule.
This is the necessary stack-order tradeoff, not permission to abandon work:
`Workers` retains those task handles, and the activated `Host.worker_owners`
retains both worker sets per replica (bounded to 128 sets / 64 replicas).
Finished sets are reaped; host shutdown uses `join_fully` outside callback
stacks to join every remaining handle. Even cancellation of a drain restores
its pending handles to their owner. No worker is detached.

During that narrowly retained callback tail, only completion metadata, the
published context and its external callback references remain; no application
future is executing or holding replica locks. A context must retain its
callback for the published `get_Callback` contract until native releases that
context; freeing that external reference from inside its own Invoke would not
be a valid stronger cleanup guarantee. Abort still clears unended result
captures and native route captures after draining the actual work. Regression
tests exercise Open, role, epoch, data-loss and copy callbacks, including
concurrent Aborts, once-only End, other admitted work disposal and an outside
full drain. Native-held callbacks that fail to respond to Cancel remain the
native engine's ownership, not detached Rust workers. Failures are errors,
never fabricated success.

## Typed ABI, provenance and dependency boundary

The following read-only reference was re-inspected before implementation:
`service-fabric-apps-rs` commit
`16914836baaae1641275de8eb92d945cd8b923a3`,
`crates/libs/mssf-ext` **0.1.0**, with core/com/pal/util **0.10**:

- `src/state_provider.rs`: narrow published state-provider bridge shape.
- `src/stream.rs`: async Begin/End and native stream proxy pattern.
- `src/state_replicator.rs`: native BeginReplicate assigned-LSN seam.
- `src/data.rs`, `src/operation.rs`: immutable segmented buffers and explicit ACK.

All factory, replica, provider, context, operation and data definitions use
published `mssf-com` **0.10.0** interfaces and typed `#[implement]` from
`mssf-pal` **0.10.0**, backed by Windows interface/core **0.100.0**. There is no
handwritten IUnknown/vtable/refcount implementation, binding fork, vendoring or
reference-repository modification. Microsoft/MIT attribution is retained; see
the root [Microsoft MIT notice](../../../LICENSE.service-fabric-apps-rs) and
[LICENSE](../../../LICENSE).

The one explicitly authorized exception is the minimal macro-generated nullable
data-stream declaration (and the analogous pre-existing test-only operation
stream fixture). It uses the published IID, method order, calling convention and
pointer ABI. Native Linux classic streams return **S_OK/null EOS**; the published
implementation trait returns a non-null interface and cannot encode that
successful result. The adapter returns `Option`, and native receive helpers call
the published raw vtable. Empty non-null data is preserved.

The reference `stream.rs:58` creates a non-null Rust interface from null; that
invalid conversion is **not copied**. The reference
`state_provider.rs` copy-context `unwrap` is also **not copied**.
Actual native waits reuse the typed `mssf_core::sync::AwaitableCallback`
Begin/End pattern with an owned `NativeReceiver`, projected HRESULTs and
explicit context cancellation. The core `fabric_begin_end_proxy` error path
calls `Error::from_thread` / `FabricGetLastErrorMessage`: a local failing COM
double concretely panicked with `cannot load lib FabricRuntime :dlopen failed`,
and the same translation inside Invoke can cross a non-unwinding COM boundary.
The local receiver avoids that unrelated native loader dependency while keeping
Begin failure and End failure distinct. `TokioExecutor` bridges synchronous
Abort; its `block_on_any` uses `block_in_place` and does not make joining the
calling task from its callback possible. `BridgeContext` 0.10 was evaluated but is
unsuitable here: it unwraps optional callbacks, unsafely downcasts End contexts,
panics on repeated End and spawns untracked inner workers. The local published
context implementation validates identity/once-only End, handles absent
callbacks and catches future panics, with tracked worker ownership.

This dependency profile dynamically loads native FabricRuntime/Common/Client
through `mssf-core`. An empty ELF `NEEDED` list does **not** make the activated
binary native-free. Native API/load/registration failure is reported; there is
no stub fallback. Production Rust runtime/data code must not depend on this
oracle. Native interoperation is **unqualified until externally executed**.
