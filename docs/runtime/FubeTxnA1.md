# Fube transaction A1: ordinary volatile execution

A1 supplies a [public service owner](../../crates/libs/fube-txn/src/runtime.rs#L79)
over **one normally constructed** FabricReplicator. The service returns the
actual transactional composite `Arc<dyn IPrimaryReplicator>` (boxed for service
Open), not the lower concrete object or a binder-selected substitute. Ordinary incremental
transactions, primary provider registry operations, native null-context Full
copy, memory-log checkpoints/truncation and owned lifecycle drains execute in
Rust. Existing public trait signatures are unchanged. This is offline,
source-derived integration at SF
`51a7703a1f83749a9234d3ceb4296d8622e42cca`, not native-binary, mixed-native,
deployment, persisted-state or ReliableCollections qualification.

## Construct in a registered stateful service

The factory captures `NativeRuntimeIntegration` using its runtime-issued
`NativeIntegrationProvider.capture(partition_id, replica_id)`. In the service
replica's `open` callback, validate `OpenMode` and construct:

```rust,ignore
let owner = fube_txn::VolatileTransactionalReplicator::create(
    partition, native, endpoint, fube_data::Settings::default(),
    provider_factory, data_loss_handler,
    fube_txn::VolatileRuntimeSettings {
        work_folder: provider_work_folder, // nonempty per-replica host location
        ..Default::default()
    },
)?;
let transactions = owner.transactional_replicator();
let control = owner.into_service_replicator();
self.owner.lock().unwrap().replace(owner); // retain through native Close/Abort
Ok(control)
```

`provider_factory` implements `IStateProvider2Factory`; `data_loss_handler`
implements `IDataLossHandler`. No test Host/private provider is necessary.
[External behavioral usage](../../crates/libs/fube-txn/tests/public_runtime.rs#L435)
shows registered construction with actual native-issued controls.
Do **not** separately open/role-change/close either layer: the returned
[composite](../../crates/libs/fube-txn/src/kernel/composite.rs#L51-L178)
delegates lower first, then upper once. The service retains the owner; a transaction
facade handle alone does not replace service/native lifecycle ownership.
Application access awaits bootstrap/role/copy readiness and native partition
access, not merely the constructor's success.

```rust,ignore
let tx = transactions.create_transaction()?;
transactions.add(tx.clone(), "fabric:/example".into(), "MyProvider".into(),
    None, None, fube_txn::Cancellation::default()).await?;
tx.commit().await?;
tx.dispose()?; // commit does not dispose the native retained registry locks
```

GetOrAdd returns `already_exists=false` for transient admission, **not** committed
creation. Add's first operation caches without LSN; Initialize/Open/Recover/role
and active visibility happen at Apply. Get excludes transient creates but includes
transient deletes. Remove commits DelayDelete; strict safe-frontier cleanup later
performs None→RemoveState→Close. Enumeration materializes root-only/all entries;
before-first/after-EOS Current is an error.

Provider writers use ordinary `Transaction.add_operation`. Direct synchronous
second/later Add requires a canonical execution entry; outside service execution,
use the factory's retained public host integration:
`native.execution().try_reserve()?.run_owned(move || { ... })`.
The same upper admission worker returns its **known disposition**, not merely
enqueued work. Async registry operations retain/resume that entry internally.
Observer Drop/timeout is not rollback. Dispose releases name/transaction locks,
including after existing GetOrAdd/read-only commit. Provider isolation, payloads
and rollback remain provider responsibilities.

## Native algorithms and unavoidable adapters

| Algorithm | Source / implementation |
|---|---|
| Ordinary lower admission, no LSN on REQueueFull | V1 ReplicationQueueManager.cpp:369–409; OperationQueue.cpp:285–365 |
| Begin/Operation-only writer/truncation throttle; End/Barrier still ordinary lower admission | LogicalLogRecord.cpp:58–65; CheckpointManager.cpp:882–930 |
| Native group retry while Primary/not-closing, cap5000ms, scheduling released before quorum; separate four-status transaction retry | CheckpointManager.cpp:1144–1239; TransactionBase.cpp:197–208 |
| Primary Add/GetOrAdd/Remove, transaction-held key locks, discovery before registration, child Initialize before root and root-first Open | StateManager.cpp:284–960,3926–4530,4797–5034; [registry.rs](../../crates/libs/fube-txn/src/kernel/registry.rs#L240) |
| Native signed 100ns/high-water provider IDs, copied/replayed advancement | StateManager.cpp:5264–5291; [identity.rs](../../crates/libs/fube-txn/src/kernel/identity.rs#L40) |
| Full state3→metadata4→log7, original copied LSN vs lower replication metadata; no custom image/Done packet | SecondaryDrainManager.cpp:906–1100; [runtime.rs:723–914](../../crates/libs/fube-txn/src/kernel/runtime.rs#L723-L914) |
| Ordered Barrier checkpoint selection; Prepare/Apply/stability/Perform/End/Complete and reader-safe truncation | [maintenance.rs](../../crates/libs/fube-txn/src/kernel/maintenance.rs#L114), [checkpoint.rs](../../crates/libs/fube-txn/src/kernel/checkpoint.rs#L120) |

### Public boundary and foundation dispatch

The old `create_with_lifecycle`/private bootstrap-rebase recipe and lower
execution/identity/snapshot/root-routing access are **withdrawn**, not renamed
public APIs. Production and tests use the same boundary:

- Normal `FabricReplicator::create` / `create_with_capabilities`, public
  partition + runtime-issued host integration, endpoint and validated `Settings`.
  A1 uses no replay extension and explicitly selects the existing R0
  `StateProviderCapabilities::supports_copy_until_latest_lsn`
  ([construction](../../crates/libs/fube-txn/src/kernel/runtime.rs#L170-L187)).
- `IStateReplicator::replicate`, copy/replication streams; `IStateProvider`'s
  progress, epoch, data-loss and copy callbacks; `IOperation` metadata/data/ACK,
  `IOperationStream::get_operation`, `IOperationDataStream::get_next`.
- Common `IReplicator` Open/role/epoch/progress/capability/Close/Abort and
  `IPrimaryReplicator` data loss/configurations/quorum/Build/Remove, with their
  ordinary admission/completion, segmented payload, error/epoch/role values.
  `get_state_replicator` supplies only the public trait view.

The public owner exposes `transactional_replicator`, `replicator` and
`into_service_replicator`
([runtime.rs:156–173](../../crates/libs/fube-txn/src/runtime.rs#L156-L173)).
The latter two share the actual composite; neither exposes lower diagnostics.
Public host execution owns synchronous admission/capture and registry resumption.
The [upper-local weak-pointer guard](../../crates/libs/fube-txn/src/kernel/runtime_lower.rs#L160-L183)
rejects same-owner publication reentry; it is not an engine ID or authority.
Provider capture occurs outside transaction-selection locks.

**FUBE ADAPTATION:** runtime internally scopes the exact issued invocation and
purpose through canonical configuration/Build/Remove delegation, including eager
Build construction and owned polling. Normal built-in construction installs no
replacement-object binder. Explicit custom framework bindings retain their
existing selected-view behavior; txn and its helper never install one
([stateful_binding.rs:724–841](../../crates/libs/fube-runtime/src/stateful_binding.rs#L724-L841)).
Original record-current/one-use, actor, revision, remote sequence, generation,
native lifetime, actual lease and deadline fences remain; no newer record is
selected to repair a stale scoped proof. This is internal host plumbing, not a
new SF contract or alternate membership/quorum authority.

### INITIAL progress, role and failure order

Normal construction starts provider supervision/transport/owner **without**
querying the provider or activating a zero primary queue. Lower Open marks
Opened; upper Open then creates and flushes the source-shaped fresh zero/one log.
Opened current-progress queries the actual upper log tail outside owner locks,
caches successful results only and retries ordinary getter errors; Opened
catch-up capability is zero. First Primary uses cached success or queries actual
progress, then creates the **INITIAL** queue at tail+1. Initial secondary instead
uses transmitted start; promotion/demotion transfer lower queue authority.
An incomplete initial secondary's progress query preserves native NotReady/fault
disposition; it is not a fabricated zero/readiness value.
There is no activated-queue rebase
([engine.rs:5088–5248](../../crates/libs/fube-data/src/engine.rs#L5088-L5248),
[actual registered bootstrap test](../../crates/libs/fube-txn/src/kernel/runtime_bootstrap_tests.rs#L797-L860)).
Native callers are Replicator.cpp:296–390,
Replicator.ChangeRoleAsyncOperation.cpp:127–252 and
ReplicationQueueManager.cpp:29–91 at the pin above; upper fresh log comes from
LogManager.cpp:246–341 **after** lower Open.

IdleSecondary→ActiveSecondary is a distinct native promotion of the existing
secondary. This lower's fixed explicit-service-ACK profile completes lower
promotion immediately, without epoch rebase, stream/capture replacement or
waiting for application ACK/live replication EOS. Upper promotion still awaits
its copy completion, not live replication completion
([engine.rs:2349–2373](../../crates/libs/fube-data/src/engine.rs#L2349-L2373),
[runtime.rs:469–487](../../crates/libs/fube-txn/src/kernel/runtime.rs#L469-L487)).
Pinned SecondaryReplicator.cpp:2110–2154 distinguishes this RequireServiceAck
branch from the unsupported no-service-ACK dispatch-drain branch.

Lower Open/role/Close failure skips the corresponding upper operation. Upper
Open failure aborts lower and returns the original error; upper role failure
fences admission/reports fault **without unconditional lower Abort**. Actual
native Close/Abort later owns terminal cleanup; Abort calls lower first.
UpdateEpoch/data loss delegate once into public provider callbacks
([composite.rs:51–218](../../crates/libs/fube-txn/src/kernel/composite.rs#L51-L218)).
Owned futures, counted disposal and finite execution bounds are Rust lifetime
adapters, not native allocation, replication or fairness algorithms.

Copy selection holds the checkpoint completion consistency lock through
descriptor selection/pinning and releases it **before** provider stream capture
(CopyStream.cpp:249–285; CheckpointManager.cpp:1473–1505). Registry metadata and
provider streams share one published checkpoint snapshot. Preserve both fences.

Native registry quirks are retained: child factory initialization parameters
but null child replicated initialization, root parent-ID for discovered
descendants; acquired root name-lock ID is adopted without recreating the
factory object; Remove refreshes the full metadata/provider under its write
lock and processes the root before children. Tokens are ignored during metadata
lock waits; retry checks token at
loop top and elapsed **>** timeout after classified failure, first16ms delay,
cap4096ms. Successfully acquired locks live until explicit transaction Dispose,
not commit. No synthetic forced unlock/rollback on Close.

## Finite profile and exclusions

| Boundary | Default / limit | Difference from native allocation |
|---|---|---|
| Shared memory log | 8 MiB default; 512 KiB–16 MiB valid | Uniform fallible logical/physical allocation; no protected termination/maintenance pool |
| Native checkpoint/min-log | 50 MiB each; factors2/4 | Default native thresholds cannot trigger within the finite maximum log; small tests explicitly use1 MiB/8 MiB, not remapped units |
| Upper owners/captures | 16 transaction owners; 32 admission captures; 64 metadata rows and root+descendant discovery captures; 128 name locks; 16 copy readers; 128 PV entries | InsufficientResources can precede MB throttle; discovery rejects before another factory capture; retained object/Dispose ownership matters |
| Lower operation | Default1 MiB/1024 segments, existing queue1024/16 MiB, captures64 | A1 record plus stage7 must fit: default record≤1 MiB−4/1023segments; earlier splitting is not native packet-size equivalence |
| Upper admission execution | At most30s and caller reserved deadline | Hard execution failure/fencing, not native timeout or retry status |

Native algorithms do **not** guarantee progress at persistently full ordinary
queues or uniform Rust allocation exhaustion. The previously invented
LogReservation/reserved Submit/recycled End/Barrier grants/protected pools were
**removed**, not relocated; withdrawal and review rounds remain in local PAW
history. Generic InsufficientResources is not native ServiceTooBusy or part of
the transaction retry whitelist. Group admission has its distinct native retry/
fault rules; pre-record Rust allocation failure is an explicit terminal adapter.
After LSN assignment, failed local write retains Activity/map/LSN/quorum/Unlock.
Physical allocation failure invents no Information record or completed drain.

None provider stream, Some(empty payload), EOS and explicit Dispose are distinct.
Active copied metadata still owns Begin/End even if its source stream is None.
Initial state EOS before first data follows native success; interrupted state EOS
after Begin/Set is Cancelled, with no successful End/replication/readiness. Lower
captures retire after actual local flush/ACK/Drop before next poll (upper commit-
Apply ACK disabled). Apply, quorum, visibility, tail and stable are not synonyms.

Unsupported paths reject explicitly: atomic/atomic-redo execution, managed and
false-progress formats, persisted Existing Open/recovery/restore, backup and
change-handler products. Providers may implement declared persistence callbacks,
but that does not enable an aggregate product. UTF-8 names are checked scheme/body
projections, **not** full KUri normalization/equality qualification. Checked
overflow/finite safety errors are Rust differences, not native allocator parity.

## Validation

[Public behavioral test](../../crates/libs/fube-txn/tests/public_runtime.rs#L702)
constructs the supported owner in a registered service, creates provider IDs,
executes incremental transactions, triggers native MB checkpoint policy, performs
actual null-context Full Build with one lower capture, distinguishes empty/absent
streams/EOS/Dispose, then replicates new increments and commits registry deletion.
It now discovers/initializes a real child, copies its state and ID through Full,
and distinguishes roots-only/all enumeration. Both direct-composite and ordinary
public-delegating return routes run these same observations.
An accepted commit observer is dropped while provider Apply is held; after gate
release the public transaction commits, Unlock occurs, explicit Dispose succeeds,
and subsequent one-capture public progress advances
([observer witness](../../crates/libs/fube-txn/tests/public_runtime.rs#L640-L698)).
Public control recording also verifies two actual completed Builds with the
native retry's Remove between them, explicit later Remove, projected epoch,
one data-loss callback/result and actual Close. A separate real incomplete
secondary getter returns native NotReady; application Add is refused, and
actual native terminal Close/Abort fallback owns cleanup
([control/error witness](../../crates/libs/fube-txn/tests/public_runtime.rs#L893-L1047)).
The post-success externally rejected Build/retry discriminator uses **normal
default Settings**; fresh Full, observer Drop and subsequent Barrier/progress
retain `provider_operations=1`. Repeated already-completed copy under a single
capture window is not a qualified progress guarantee; do not infer it from
default-profile retry success or hide it with a protected EOS/Barrier pool.
[Registry tests](../../crates/libs/fube-txn/src/kernel/phase4_registry_tests.rs#L1)
add owned observer-drop, key ordering, abort/dispose, native timeout/token/retry,
hierarchy, post-Open failure cleanup, copy/promotion high-water and Close drains.
The retained phase tests supply exact Apply-before-quorum, native queue-full,
once-only composite/delegating-public lifecycle, copy overlap, checkpoint and
truncation partial orders. With `provider_operations=1`, they observe refusal
without an LSN and later actual public progress after capture retirement.
Full separately observes successful source Build/public stream EOS while upper
Apply/copy remains pending; failed upper role observes a still-live lower before
actual native Close cleanup. All gates/readers/drains release before assertions.
The original a780dd8 CI reader/manual-checkpoint cleanup function remains byte
identical; old CI success is not acceptance of this public-boundary revision.
The controlled pre-fix unfinished-copy role rejection and post-fix public success
are observed; the older uninstrumented successful-Build/role-timeout chronology
remains **unknown**, not reconstructed.

The single [RAP test-support package](../../crates/libs/fube-rap-test-support/Cargo.toml#L8-L13)
depends only on public runtime/transport/tokio/uuid/serde, not data. Txn depends
normally on data/runtime/tokio/uuid and dev-depends on that helper; it has no data
qualification feature, private source inclusion/friend access or lower-control
reexport. The moved model retains actual registration/Open addresses and
Close-before-host-shutdown, not fabricated lower proofs.
Byte/Point payloads remain test fixtures, not collection compatibility.

Run serially using the existing toolchain:

```sh
CARGO_BUILD_JOBS=1 cargo test --locked -p fube-txn -- --test-threads=1
CARGO_BUILD_JOBS=1 cargo clippy --locked -p fube-txn -p fube-data -p fube-runtime -p fube-rap-test-support -p rust-runtime-app --all-targets -- -D warnings
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 RUSTDOCFLAGS='-D warnings' cargo doc --locked -p fube-txn -p fube-data -p fube-runtime -p fube-rap-test-support -p rust-runtime-app --no-deps
```

Repeat Rustdoc with `--document-private-items`. Local PAW Docs records exact
results, acceptance mapping, corrections and both-model review evidence.
