# RA protocol foundation

`fube-ra` provides a complete native boundary **inventory**, the first
AddInstance/AddInstanceReply plus [stateless removal/reporting](RaStatelessRemoval.md)
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
codec/handler/native-trace coverage. The remaining 60 actions have no enabled
Rust action codec/handler capability; even the selected six actions' complete runtime
lifecycle/behavioral qualification remains unfinished.

## Codec evidence

[protocol.rs:41–539](../../crates/libs/fube-ra/src/protocol.rs#L41-L539)
models the selected bodies and their full nested descriptions, native defaults,
raw NodeId/GUID, enum/error widths, collections, polymorphic scaling and relevant
headers. It reuses the bounded transport Fabric codec, not a new serializer.
Native request generation and FM/FMM identity remain distinct; the actual
AddInstanceReply/RemoveInstanceReply/ReplicaUp senders do not require the
request's generation header. Remove inherits the base scope plus force;
ReplicaUp/Reply retain full normal/dropped FT lists and both flags.

[The explicit oracle](../../tests/ra_protocol/README.md) compiles unmodified
native stream/compressor/helper code and mechanically extracts native field
macros/declarations for 39 schema types/123 members. Its disclosed test platform
support replaces allocator/container/raw byte storage, **not wire algorithms**.
Schema adapters omit unrelated C++ production methods/dependencies. This is
**source-baseline native-stream conformance with schema adapters**, not execution
of the full original production RA classes/handlers or interoperability with
independently pinned SF qualification binaries.

Executed evidence includes 29 native→Rust exact-field cases and 29 Rust→actual
native decoder comparisons, default/nondefault bodies, semantic errors,
UTF-16/surrogates, high-bit identities and nonempty inherited/polymorphic fields.
Rust rejects every proper prefix of rich Add/reply and both force removal
fixtures; native rejects four truncated bodies. Native input hashes, generated schemas, compiler flags and
prebuilt digests are recorded in the producer bundle. Acceptance runs verified
prebuilt inputs with no compiler/native installation/build fallback.

## Entity API and admission

[`Executor::new`:334–380](../../crates/libs/fube-ra/src/entity.rs#L334-L380)
accepts `Arc<dyn KeyValueStore>`, immutable trusted session/node admission and
finite settings. The caller selects/initializes the P1 backend, including its
correct deployment/node binding. Executor does not infer a storage identity
from the trait object or recover an existing entity.

[`Command`:165–188](../../crates/libs/fube-ra/src/entity.rs#L165-L188) admits:

- Add: full native body, actual source node incarnation and generation header.
- CompleteOpen: a matching result supplied by a trusted future Hosting/RAP
  adapter; tests supply it now. It does not create application serving authority.
- RetryOpen: explicit new retry work for an existing pending open.
- Remove, one-FT ReplicaUpReply, trusted Registered/CompleteClose/HostDown and
  due close/report retries: [native stages and adapter contract](RaStatelessRemoval.md).
  `Open` now requests registration; only a committed Registered handoff permits
  RuntimeOpen, and successful Open completion requires that witness.

[Admission/transition logic:588–787](../../crates/libs/fube-ra/src/entity.rs#L588-L787)
enforces the selected stateless InBuild/up/package profile, configured FM/local
node incarnation and generation domain. Configured-source validation is an
additional session gate, not a claim that the generic native dispatcher
cryptographically authenticates current FM. Generation zero means the complete
zero GenerationNumber; counter zero with another owner is not a bypass.

The seam distinguishes not-ready/upgrading silence, native deactivated
RANotReadyForUse, pending duplicate suppression, stale epoch/instance/outcome
suppression, and identical Ready success reply. A matching trusted Ready
completion, **not Add receipt/store insertion**, permits semantic success.
Service-type-not-registered remains pending/retryable without success.

Open replacement and general epoch/service-description updates,
FM-service/FMM execution and general native open-error policy return explicit
Unsupported results without successful wire replies. Those are cataloged
implementation gaps, not permanent omissions or alternate protocol statuses.
Selected native close/tombstone, force and report/ack stages are implemented;
their exclusions and source-derived tests are [documented separately](RaStatelessRemoval.md).

## State, commit and effects

[Owned supervision:334–560](../../crates/libs/fube-ra/src/entity.rs#L334-L560)
uses one independent thread/runtime and bounded per-entity workers. Same-entity
work serializes. Accepted count, tight encoded bytes and bounded action slots
(Add 2; Open completion/open/report retry 1; other lifecycle work 8)
are reserved atomically and held through actual completion.

[Commit/publication:790–1015](../../crates/libs/fube-ra/src/entity.rs#L790-L1015)
keeps proposed state private. Durable creation/Ready/close/tombstone updates commit one aggregate
under record type `ra/entity/v1` and a lowercase hex native-GUID ID. Format 2
stores the full selected body, phase, deletion/cleanup, tokens/mode and FM stage;
registration and retry counters remain volatile. It is an internal envelope,
not native disk migration, a durable outbox or a restart loader.
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

[`take_effects`:497–502](../../crates/libs/fube-ra/src/entity.rs#L497-L502)
transfers each action record once per accepted execution. Explicit new retry
commands and native duplicate-ready replies can legitimately emit new actions.
No arbitrary callback runs under a store lock; transport delivery, automatic
timer scheduling and crash consistency remain unimplemented. Explicit report
retry/semantic acknowledgment events exist for the bounded removal slice.

[`Ticket`:49–81](../../crates/libs/fube-ra/src/entity.rs#L49-L81) is observation,
not ownership. Cancelling its future, losing the caller Tokio runtime or dropping
all executor handles does not retract accepted work.
[`close`:504–518](../../crates/libs/fube-ra/src/entity.rs#L504-L518) closes only
executor admission and drains accepted work; it does not close the caller's
store. Finite timeout reports Unknown, never rollback. Unresolved work may retain
the supervisor indefinitely; there is no racing replacement or hard IO deadline.

### Default finite limits

[Settings:95–153](../../crates/libs/fube-ra/src/entity.rs#L95-L153):
32 entity slots, 64 outstanding operations, 256 KiB encoded command,
8 MiB queued bytes, 64 retained/reserved actions, 20 ms late-resolution polling.
Entity slots are retained for the executor lifetime. Snapshots/effects retained
by consumers are outside admission budgets. Limits are logical bounds, not
allocator/RSS quotas or real-time scheduling guarantees.
An additional 64 outstanding registration lookups per entity are retained across
removal/recreation until their real result arrives; separate acquisition IDs
make callback replay idempotent and discarded-reference release explicit.

## Tests and remaining qualification

```sh
cargo test --locked -p fube-ra -p fube-ra-store
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs -v
```

[33 deterministic entity tests](../../crates/libs/fube-ra/tests/entity.rs)
map to the catalog's native EntityMap/EntityJobItem ordering and selected
AddInstance preconditions plus selected removal/close/report/native scenarios.
Real public Store backends and entered/release
barriers prove no premature state/effects/reply, definite insert/update failure,
unknown late success/failure, unrelated memory progress, retained caller
ownership, per-execution single dispatch and real SQLite aggregate commit.
[11 schema tests](../../crates/libs/fube-ra/tests/protocol.rs) cover defaults,
full nested fields, errors, headers, malformed input, bounds and pointer tags.
Offline validator-model bytes are explicitly **not native fixtures**.
Additional [real-ticket fault regressions](../../crates/libs/fube-ra/src/entity.rs#L1017)
exercise permanent observation-channel loss and terminal Unknown in bounded
child processes, asserting explicit polling backoff without releasing ownership.

Broader normalized native behavioral traces, all-message codecs/handlers,
live FM/Hosting/runtime sessions, actual stateless cluster qualification,
restart/replay/adoption, distributed Federation leases, process/power-loss and
transparent FM recovery are not delivered or implied by this foundation.
