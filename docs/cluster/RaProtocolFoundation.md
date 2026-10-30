# RA protocol foundation

`fube-ra` provides a complete native boundary **inventory**, the first
AddInstance/AddInstanceReply plus [stateless removal/reporting](RaStatelessRemoval.md)
and [opening failure/retry with node notifications](RaStatelessOpening.md)
schemas/codecs, and a bounded P2 entity-execution
seam above [P1 storage](RaStore.md). It is **not a running RA**, FM, Hosting
adapter, full P2/P3 implementation or restart protocol.

## Inventory and scope

[NativeRaProtocol.md](NativeRaProtocol.md) and its
[registry CSV](NativeRaRegistry.csv) reconcile the pinned native
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` source:
119 initialized actions, 121 declarations, 33 RA metadata entries,
66 required boundary actions and 67 actual directional contracts.
All others have explicit exclusion evidence; declaration-only legacy names,
deprecated handling, current no-ops and source endpoint gaps are classified.

Every RA–FM and RA–RA exchange remains mandatory eventual scope. Replacing
Federation delivery does not remove peer CreateReplica/GetLSN/Deactivate/Activate
or peer DoReconfiguration continuation. Catalog completeness is not complete
codec/handler/native-trace coverage. The remaining 58 actions have no enabled
Rust action codec/handler capability; even the selected eight actions' complete runtime
lifecycle/behavioral qualification remains unfinished.

## Codec evidence

[protocol.rs:41–585](../../crates/libs/fube-ra/src/protocol.rs#L41-L585)
models the selected bodies and their full nested descriptions, native defaults,
raw NodeId/GUID, enum/error widths, collections, polymorphic scaling and relevant
headers. It reuses the bounded transport Fabric codec, not a new serializer.
Native request generation and FM/FMM identity remain distinct; the actual
AddInstanceReply/RemoveInstanceReply/ReplicaUp senders do not require the
request's generation header. Remove inherits the base scope plus force;
ReplicaUp/Reply retain full normal/dropped FT lists and both flags.

[The explicit oracle](../../tests/ra_protocol/README.md) compiles unmodified
native stream/compressor/helper code and mechanically extracts native field
macros/declarations for 43 schema types/129 members. Its disclosed test platform
support replaces allocator/container/raw byte storage, **not wire algorithms**.
Schema adapters omit unrelated C++ production methods/dependencies. This is
**source-baseline native-stream conformance with schema adapters**, not execution
of the full original production RA classes/handlers or interoperability with
independently pinned SF qualification binaries.

Executed evidence includes 36 native→Rust exact-field cases and 36 Rust→actual
native decoder comparisons, default/nondefault bodies, semantic errors,
UTF-16/surrogates, high-bit identities and nonempty inherited/polymorphic fields.
Rust rejects every proper prefix of rich Add/reply and both force removal
fixtures and both rich notification bodies; native rejects six truncated bodies.
Native input hashes, generated schemas, compiler flags and
prebuilt digests are recorded in the producer bundle. Acceptance runs verified
prebuilt inputs with no compiler/native installation/build fallback.

## Entity API and admission

[`Executor::new`:441–499](../../crates/libs/fube-ra/src/entity.rs#L441-L499)
accepts `Arc<dyn KeyValueStore>`, immutable trusted session/node admission and
finite settings. The caller selects/initializes the P1 backend, including its
correct deployment/node binding. Executor does not infer a storage identity
from the trait object or recover an existing entity.

[`Command`:192–235](../../crates/libs/fube-ra/src/entity.rs#L192-L235) admits:

- Add: full native body, actual source node incarnation and generation header.
- CompleteOpen: a matching result supplied by a trusted future Hosting/RAP
  adapter; tests supply it now. It does not create application serving authority.
- RetryOpen: explicit new retry work for an existing pending open.
- Request/Capture/CompleteServiceTypeWork, separate retry timer commands and
  ServiceTypeReply: node-wide registration notifications,
  native shared reference/version semantics, throttled batching and partial
  instance-matched acknowledgments, not one fake FT reply.
- Remove, one-FT ReplicaUpReply, trusted Registered/CompleteClose/HostDown and
  due close/report retries: [native stages and adapter contract](RaStatelessRemoval.md).
  `Open` now requests registration; only a committed Registered handoff permits
  RuntimeOpen, and successful Open completion requires that witness.

[Admission/transition logic:834–989](../../crates/libs/fube-ra/src/entity.rs#L834-L989)
enforces the selected stateless InBuild/up/package profile, configured FM/local
node incarnation and generation domain. Configured-source validation is an
additional session gate, not a claim that the generic native dispatcher
cryptographically authenticates current FM. Generic native receive-generation
lower-bound admission applies before node/FT routing; generation zero is not a
bypass, and newer generations are admitted rather than requiring exact equality.

The seam distinguishes not-ready/upgrading silence, native deactivated
RANotReadyForUse, pending duplicate suppression, stale epoch/instance/outcome
suppression, and identical Ready success reply. A matching raw native-shaped
completion, **not Add receipt/store insertion**, lets RA establish Ready and
update endpoints; an adapter need not manufacture a pre-Ready replica.
Service-type-not-registered remains pending/retryable without success.

Open replacement and general epoch/service-description updates,
and FM-service/FMM execution return explicit
Unsupported results without successful wire replies. Those are cataloged
implementation gaps, not permanent omissions or alternate protocol statuses.
Selected native close/tombstone, force and report/ack stages are implemented;
their exclusions and source-derived tests are [documented separately](RaStatelessRemoval.md).
Native registration/Open error policy, returned version handling, retained usage,
final Abort cleanup and opening host-down are [implemented separately](RaStatelessOpening.md).

## State, commit and effects

[Owned supervision:435–800](../../crates/libs/fube-ra/src/entity.rs#L435-L800)
uses one independent thread/runtime and bounded per-entity workers. Same-entity
work serializes. Accepted count, tight encoded bytes and bounded action slots
(Add 2; open/report retry 1; Open completion/other lifecycle and node work 8)
are reserved atomically and held through actual completion.

[Commit/publication:992–1257](../../crates/libs/fube-ra/src/entity.rs#L992-L1257)
keeps proposed state private. Durable creation/Ready/close/tombstone updates commit one aggregate
under record type `ra/entity/v1` and a lowercase hex native-GUID ID. Format 2
stores the full selected body, phase, deletion/cleanup, tokens/mode and FM stage;
registration and retry counters remain volatile. It is an internal envelope,
not native disk migration, a durable outbox or a restart loader.
Accepted RAP Open replies/opening host-down select persistent Update;
registration retry/adoption (including terminal lookup failure) is memory-only
as selected by native source. Requested version, usage and the node ledger are
also volatile; an older stored opening envelope is not a recovery promise.
Existing-entity registration/retry/error/report bookkeeping can be memory-only. Durable transitions
cannot be downgraded by a caller flag.

On confirmed success, an immutable state snapshot and its owned action record
are published together, then the ticket completes. Definite failure discards
both. Unknown retains the **same P1 operation ticket** and fences conflicting
work until the actual outcome resolves. A P1 deadline leaves P1 terminally
fenced even after late success: executor never reopens/replaces the store.
Unrelated permitted memory work can progress; subsequent durable work still
fails against a fenced store.
Permanent observation-channel loss and terminal Unknown also retain the fence
and apply the configured minimum polling interval; immediate resolution errors
cannot turn uncertainty into a busy loop.

[`take_effects`:647–652](../../crates/libs/fube-ra/src/entity.rs#L647-L652)
transfers each action record once per accepted execution. Explicit new retry
commands and native duplicate-ready replies can legitimately emit new actions.
No arbitrary callback runs under a store lock; transport delivery, automatic
timer scheduling and crash consistency remain unimplemented. Explicit report
retry/semantic acknowledgment events exist for the bounded removal slice.

[`Ticket`:54–86](../../crates/libs/fube-ra/src/entity.rs#L54-L86) is observation,
not ownership. Cancelling its future, losing the caller Tokio runtime or dropping
all executor handles does not retract accepted work.
[`close`:654–668](../../crates/libs/fube-ra/src/entity.rs#L654-L668) closes only
executor admission and drains accepted work; it does not close the caller's
store. Finite timeout reports Unknown, never rollback. Unresolved work may retain
the supervisor indefinitely; there is no racing replacement or hard IO deadline.

### Default finite limits

[Settings:106–180](../../crates/libs/fube-ra/src/entity.rs#L106-L180):
32 entity slots, 64 outstanding operations, 256 KiB encoded command,
8 MiB queued bytes, 64 retained/reserved actions, 20 ms late-resolution polling.
Entity slots are retained for the executor lifetime. Snapshots/effects retained
by consumers are outside admission budgets. Limits are logical bounds, not
allocator/RSS quotas or real-time scheduling guarantees.
An additional 64 outstanding registration lookups per entity are retained across
removal/recreation until their real result arrives; separate acquisition IDs
make callback replay idempotent and discarded-reference release explicit.
Node notifications additionally bound 64 shared types and 256 KiB batch output
by default. Actual memory-only attachment projection/publication is atomic.
Semantic knobs are separately mutable signed native properties in
[config.rs](../../crates/libs/fube-ra/src/config.rs), not immutable resource
budgets or positivity validators. See the opening profile for read points.

## Tests and remaining qualification

```sh
cargo test --locked -p fube-ra -p fube-ra-store
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs -v
```

[55 deterministic entity tests](../../crates/libs/fube-ra/tests/entity.rs)
map to the catalog's native EntityMap/EntityJobItem ordering and selected
AddInstance preconditions plus selected removal/close/report/native scenarios.
Real public Store backends and entered/release
barriers prove no premature state/effects/reply, definite insert/update failure,
unknown late success/failure, unrelated memory progress, retained caller
ownership, per-execution single dispatch and real SQLite aggregate commit.
[14 schema tests](../../crates/libs/fube-ra/tests/protocol.rs) cover defaults,
full nested fields, errors, headers, malformed input, bounds and pointer tags.
Offline validator-model bytes are explicitly **not native fixtures**.
Additional [real-ticket fault regressions](../../crates/libs/fube-ra/src/entity.rs#L1260)
exercise permanent observation-channel loss and terminal Unknown in bounded
child processes, asserting explicit polling backoff without releasing ownership.
Coordinator/config unit tests cover native shared references/version/ack,
Unicode UTF-16-unit C-locale identity, actual report projection, dynamic properties,
emitted work/timer obligations and captured-send interleavings. Separate
[actual-native helper comparisons](../../tests/ra_protocol/README.md#separate-actual-native-helper-evidence)
execute original PAL/retry code and extracted identifier/generation methods;
this is not full original-handler execution.
New [opening scenario mappings](RaStatelessOpening.md#storage-tests-and-reproducibility)
distinguish direct SL evidence, shared/helper-derived cases and Rust-only
storage-fault adaptations.

Broader normalized native behavioral traces, all-message codecs/handlers,
live FM/Hosting/runtime sessions, actual stateless cluster qualification,
restart/replay/adoption, distributed Federation leases, process/power-loss and
transparent FM recovery are not delivered or implied by this foundation.
