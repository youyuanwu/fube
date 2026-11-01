# Fube transaction A1: ordinary volatile execution

A1 supplies a [public service owner](../../crates/libs/fube-txn/src/runtime.rs#L82)
over the existing **one** FabricReplicator/native binder. Ordinary incremental
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
let lower = owner.fabric_replicator();
self.owner.lock().unwrap().replace(owner); // retain through native Close/Abort
Ok(Box::new((*lower).clone()) as Box<dyn fube_runtime::IPrimaryReplicator>)
```

`provider_factory` implements `IStateProvider2Factory`; `data_loss_handler`
implements `IDataLossHandler`. No test Host/private provider is necessary.
[External behavioral usage](../../crates/libs/fube-txn/tests/public_runtime.rs#L265)
shows registered construction with actual native-issued controls.
Do **not** separately open/role-change/close the upper owner: the existing lower
root/bound lifecycle calls it once. The service retains the owner; a transaction
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
use `lower.execution_runtime().try_reserve()?.run_owned(move || { ... })`.
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
| Full state3→metadata4→log7, original copied LSN vs lower replication metadata; no custom image/Done packet | SecondaryDrainManager.cpp:906–1100; [runtime.rs](../../crates/libs/fube-txn/src/kernel/runtime.rs#L477) |
| Ordered Barrier checkpoint selection; Prepare/Apply/stability/Perform/End/Complete and reader-safe truncation | [maintenance.rs](../../crates/libs/fube-txn/src/kernel/maintenance.rs#L114), [checkpoint.rs](../../crates/libs/fube-txn/src/kernel/checkpoint.rs#L120) |

**FUBE ADAPTATION:** common-root/bound lifecycle hooks preserve lower-first native
composition without another binder. Actual-getter one-time bootstrap rebase
bridges constructor tail0 to native lower-first bootstrap1; no caller supplies
LSNs. Owned futures, counted cleanup, finite captures, deadline/reentry checks and
execution-entry resumption implement Rust lifetime/synchronous-admission plumbing,
not new replication, queue or fairness algorithms.

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

[Public behavioral test](../../crates/libs/fube-txn/tests/public_runtime.rs#L431)
constructs the supported owner in a registered service, creates provider IDs,
executes incremental transactions, triggers native MB checkpoint policy, performs
actual null-context Full Build with one lower capture, distinguishes empty/absent
streams/EOS/Dispose, then replicates new increments and commits registry deletion.
[Registry tests](../../crates/libs/fube-txn/src/kernel/phase4_registry_tests.rs#L1)
add owned observer-drop, key ordering, abort/dispose, native timeout/token/retry,
hierarchy, post-Open failure cleanup, copy/promotion high-water and Close drains.
The earlier phase tests supply exact Apply-before-quorum, native queue-full,
root/bound lifecycle, copy overlap, checkpoint and truncation partial orders.
Byte/Point payloads remain test fixtures, not collection compatibility.

Run serially using the existing toolchain:

```sh
CARGO_BUILD_JOBS=1 cargo test --locked -p fube-txn -- --test-threads=1
CARGO_BUILD_JOBS=1 cargo clippy --locked -p fube-txn -p fube-data -p fube-runtime --all-targets -- -D warnings
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 RUSTDOCFLAGS='-D warnings' cargo doc --locked -p fube-txn -p fube-data -p fube-runtime --no-deps
```

Repeat Rustdoc with `--document-private-items`. Local PAW Docs records exact
results, acceptance mapping, corrections and both-model review evidence.
