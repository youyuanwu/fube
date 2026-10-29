# RA store P1

`fube-ra-store` implements the logical typed-record async storage seam with a
memory model and local SQLite backend. It is **not an RA**, entity state machine,
native ESE/TStore importer, runtime restart protocol or durable application
replicator. `fube-data` remains volatile; app/Hosting/FM/lease behavior is unchanged.

## Contract and use

[`KeyValueStore`](../../crates/libs/fube-ra-store/src/api.rs) exposes fail-fast
`submit(Request)` admission, caller-owned operation tickets, statistics and
async close. [`Store`](../../crates/libs/fube-ra-store/src/supervisor.rs) provides:

- `new(backend, settings)`: initialize a caller-selected concrete or boxed
  `dyn Backend` under the same mandatory supervisor.
- `memory(settings)`: same logical contract, no disk-durability claim.
- `create_new(path, identity, settings)`: atomic new-file reservation, never an
  existing-store fallback.
- `open_existing(path, identity, settings)`: requires an initialized compatible
  store; never creates/reinitializes missing, empty, foreign or damaged data.

Types and IDs are exact nonempty, NUL-free UTF-8 strings, not native enum
ordinals or normalized names. Values are opaque bytes (including empty values).
Insert creates only; duplicate typed ID conflicts without replacing content.
Update/delete of a missing typed ID is not-found. Independent types can use the
same ID. Enumeration returns an ID-ordered, complete committed snapshot of one
type or an error, never a partial vector. Returned snapshots are owned, not
lazy readers. Separate submissions are **not** a multi-record atomic batch.

```rust
use fube_ra_store::{Backend, MemoryBackend, Record, RecordId, Request, Settings, Store};
async fn example() -> fube_ra_store::Result<()> {
let backend: Box<dyn Backend> = Box::new(MemoryBackend::new());
let store = Store::new(backend, Settings::default()).await?;
let operation = store.submit(Request::Insert(Record {
    key: RecordId::new("example/v1", "entity-1"),
    value: b"opaque payload".to_vec(),
}))?;
operation.wait().await?;
let report = store.close().await?;
assert!(report.is_clean());
Ok(())
}
```

For SQLite, pass an explicit `Identity { deployment, node }`; each field is exact
nonempty NUL-free UTF-8, at most 512 bytes. The parent directory must already
exist and be private (0700); files must be private regular files without
hardlinks. Use one private local-filesystem database per RA, accessed by this
library only. A persistent 0600 `.lock` sidecar and nonblocking cooperative
filesystem lock exclude other store owners, independently of SQL locks. Never
unlink/recreate the lock inode while any owner might exist. Symlink parents are
canonicalized; database and sidecar symlinks are rejected. This is not a defense
against hostile administrators, path replacement/aliases or network filesystems.

### Caller-selected backends

Construct `MemoryBackend::new()`, `SqliteBackend::create_new(path, identity)` or
`SqliteBackend::existing(path, identity)`, then pass it to `Store::new`.
These constructors hold configuration only: initialization, filesystem ownership
and workers start inside the owned supervisor, not on the caller's Tokio runtime.
The existing Store convenience constructors delegate to this same path.
RA code can receive `Arc<dyn KeyValueStore>` independently of backend selection.

The public [`Backend`](../../crates/libs/fube-ra-store/src/backend.rs) trait is
object-safe, requires `Send` but not `Sync`, and uses boxed Send futures for
`initialize`, `execute` and `close`. Store owns one engine, initializes it once,
serializes operations, and calls close even after partial initialization fails.
The backend's futures must yield; blocking work belongs on an owned worker.
Implementations must obey the same logical commit/snapshot and configured
storage-budget contracts. This is a trusted extension point, not a sandbox for
arbitrary plugins; Store cannot prove a backend actually committed or stopped.

An opaque `BackendContext` lets an implementation retain one grouped exclusion/
resource guard independently of engine/task lifetime. Release it only after
positively witnessed cleanup, never on a cancelled wait or lost channel.
SQLite uses that single slot for its lease: delegated initialization rejects
a second registration rather than replacing an already-retained guard.
`mark_worker_lost` retains unresolved operation ownership and fences the store;
return Unknown from the affected backend call as well. A retained guard or
reported worker loss prevents a success-shaped close and causes process-lifetime
quarantine. Initialization and cleanup errors are both surfaced when both fail.
The context can also gate writes under storage pressure and record postcommit
maintenance failures without changing committed success. It does not expose
internal supervisor state, admission or a bypass around shared limits.

## Completion, cancellation and close

Admission reserves outstanding operation count **and** retained request bytes
atomically with enqueue. The bounded queue is owned by a fixed supervisor thread,
independent of the caller's Tokio runtime. Memory has one fixed execution worker;
SQLite uses **only tokio-rusqlite's existing blocking connection worker**, with
at most one submitted call. No SQLite/lock wait runs on application Tokio workers;
no task is spawned for each operation. The wrapper does not rely on
tokio-rusqlite's unbounded internal queue for backpressure.

`Operation::wait` observes ownership; it does not own execution. Dropping its
future, the ticket, or all public store handles does **not** retract accepted
work. Its count/byte charge stays held until actual worker completion and
supervisor accounting. Later work cannot overtake unresolved earlier work.
Keep the ticket if you need that operation's exact result. With no ticket, close
retains aggregate success/failure counts and the first failure, not an unbounded
per-operation history. Completed tickets/snapshots retained by callers are
outside the store's outstanding request budget.

The queue-inclusive operation deadline is supervision, not a rollback deadline.
On expiry the supervisor fences admission, reports `Unknown` for in-flight work,
finishes undispatched requests as `NotExecuted`, and keeps polling the **same**
worker call with its capacity/lease intact. `Operation::resolve(budget)` or
`status()` can expose the eventual actual result, including late committed
success. The store stays terminally fenced/closed; drain before reopening.
Do not treat cancelled or timed-out commit as rollback or launch conflicting
entity effects based on that assumption.

`close()` atomically blocks admission and drains accepted work. Its finite wait
includes cleanup and shared join observation; abandonment/concurrent closers do
not cancel cleanup. Retry close after an `Unknown` pending report. A successful
`CloseReport` accounts accepted requests as succeeded/failed/not-executed;
`first_failure` includes rejected admitted operations (even expected conflicts),
deadline fencing and maintenance failures. Inspect `is_clean()`: success of the
close API alone does not erase earlier failures. `succeeded` includes reads and
maintenance, not just committed mutations.

Constructor futures start independently owned bootstrap before their first
internal wait. Cancelling/timing out a polled create can leave an initialized
database, not rollback. It requests cleanup while ownership remains held; do
not delete/retry-create that path while cleanup is unresolved.

tokio-rusqlite 0.8 has no worker join and can report successful close after
channel loss. The wrapper therefore swaps in a private in-memory placeholder
on its worker and consumes/closes the **disk** connection, retaining ownership
until that positive close witness. It then closes the placeholder. A worker/
supervisor loss or negative disk-close witness is failure-visible: unproven
lease and bounded unresolved request ownership are intentionally retained in
process-lifetime quarantine, even if every public handle is dropped.

An OS/SQLite stall cannot be safely killed by aborting a Tokio future. Finite
reporting is not a hard IO deadline or real-time scheduling guarantee. A stalled
worker may retain its connection, snapshot reader and owned threads indefinitely;
no further work is dispatched after fencing. There is no racing replacement or
in-process quarantine recovery. Releasing a genuinely unknown owner's exclusion
requires process exit and deliberate subsequent existing-store validation.

## SQLite format, durability and pressure

The version-1 format uses strict `records(record_type TEXT, record_id TEXT,
value BLOB)` with a composite primary key, plus singleton format/version/
deployment/node metadata and SQLite application/user version markers. Existing
open validates exact recognized schema (including absence of foreign triggers),
`integrity_check(1)`, identities and record/field/logical page budgets. Lowered
limits reject oversized existing contents, never truncate them. Structural
integrity checks do not detect arbitrary valid-looking payload edits; no
cryptographic envelope or native migration is claimed. RA entity serialization/
envelopes remain P2.

[`sqlite.rs`](../../crates/libs/fube-ra-store/src/sqlite.rs) configures and queries
effective **WAL + synchronous=FULL** on the sole writer before any successful
bootstrap commit. Mutations use ordinary INSERT or affected-row UPDATE/DELETE in
an immediate transaction; success is returned only after actual COMMIT. Statement
and commit failures explicitly resolve rollback or fence uncertain outcomes.
Only a record primary-key duplicate is conflict, not every constraint or IO
error. Errors distinguish busy, capacity, IO, corruption, incompatible format,
engine failure, closed/fenced, not-executed and unknown outcome.

The engine is bundled SQLite C (currently tokio-rusqlite 0.8.0, rusqlite 0.40.2,
libsqlite3-sys 0.38.2 / SQLite 3.53.2), not a native SF library or pure Rust.
Producers need a C compiler/archiver; do not override the bundled policy with
system pkg-config/custom SQLite flags. Consumers run prebuilt executables and
need no Rust compiler, SQLite development package or SF installation.

### Default finite budgets

| Resource | Default |
|---|---|
| Record type / ID / value | 64 B / 512 B / 1 MiB |
| Stored count / logical bytes | 4096 / 32 MiB |
| Complete snapshot | 8 MiB |
| Outstanding operations / retained queued request bytes | 64 / 8 MiB |
| Queue-inclusive operation supervision | 5 s |
| Bootstrap report / public close wait (including join) | 10 s / 10 s |
| SQLite busy wait | 100 ms |
| Logical/main DB threshold / live-WAL threshold | 64 MiB / 8 MiB |

`Settings` validates nonzero finite budgets and arithmetic/conversion ranges.
Stored and snapshot accounting is type+ID+value byte lengths plus 128 B per
record. Queued accounting is **buffer capacities**, including overallocated
strings/vectors, plus 512 B per envelope; empty operations also count.
Memory copies retained records into tight private buffers before publication.
These are explicit logical accounting domains, not allocator/RSS quotas.
SQLite engine row/SQL/variable/attachment limits are separately finite.

SQL snapshots fully materialize inside one worker transaction, bounded by
count/bytes; no caller can pin a lazy reader. Database page growth is capped by
`max_page_count` (budget rounded down to whole pages). Before each mutation the
worker rechecks main/logical/WAL pressure and, when needed, attempts a finite
TRUNCATE checkpoint, inspecting its busy flag and remeasuring. After COMMIT,
known WAL pressure stops new mutation admission; `Request::Maintain` performs
bounded checkpoint/recheck through the same admission queue (no-op for memory).
Already queued mutations recheck pressure before executing.

A blocked checkpoint reports busy and leaves writes gated until successful
maintenance. Reads/maintenance/close remain available. A committed write remains
successful even if postcommit maintenance/metadata observation fails; close
surfaces that failure. Noncooperative external writers/readers are outside the
deployment profile; a pinned-reader test qualifies safe refusal, not support
for an arbitrary external writer.

**The live-WAL threshold is not a filesystem quota.** One bounded transaction
may exceed it before the postcommit gate is set; SQL page/record/value budgets
bound work but there is no claimed exact WAL amplification formula or hard
power-loss space reserve. `journal_size_limit=0` is a recycling hint, not a hard
live-WAL cap. Filesystem exhaustion can still fail an operation. Never delete
`-wal`/`-shm`, reset the database or create a replacement to recover space while
ownership/recovery is unresolved. Preserve the DB/WAL set for offline diagnosis.

## Native alignment and evidence

Reference revision: Service Fabric
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`, under
`src/prod/src/Reliability/Failover/ra/`:

- `Storage.Api.IKeyValueStore.h:34-57`: type enumeration, single-record Begin/End,
  distinct-ID concurrency and one-winner same-ID insert.
- `Storage.LocalStoreAdapter.cpp:98-181,278-343,403-460`: operation dispatch,
  commit-result completion and private complete enumeration.
- `Test.Functional.Storage.KeyValueStore.cpp:142-259`: seven shared scenarios
  (empty, insert, duplicate preserving old value, missing/existing delete and
  missing/existing update), ported into the shared Rust contract.

Deliberate differences: futures/tickets instead of COM/KTL Begin/End; UTF-8
type+ID composite namespace rather than native memory's ID-only comparison;
staged memory failure rather than immediate native-memory mutation/delayed
notification; explicit finite budgets, supervision, identity and failure-visible
close rather than ignored native timeouts/void close. Native ESE duplicate
translation differs from the shared test's conflict spelling; this store follows
the explicit logical create/conflict contract, not native byte/error-code parity.

Run `cargo test --locked -p fube-ra-store`. Shared executed tests cover CRUD,
type/ID isolation, insert race, snapshots, independent bounds, staged failures,
cancelled queued/in-flight ownership, deadlines and close. SQLite adds committed
reopen, bootstrap cancellation/timeout, identity/schema/integrity errors, actual
commit veto, contention, page/WAL pressure, post-COMMIT panic and negative
close/worker/supervisor quarantine. Quarantine child processes exercise ownership
failure only: **not** process-crash/power-loss durability qualification.

Tests and reopen evidence do not establish full native RA parity, OS/power-loss
qualification, entity publication, runtime recovery or warm adoption.
