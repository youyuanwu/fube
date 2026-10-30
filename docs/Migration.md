# Migration and provenance

## Breaking runtime and data API migration

The current runtime/data API follows SF/mssf canonical responsibilities with
explicit Rust/profile differences in the
[four-axis alignment reference](runtime/SfApiAlignment.md). Migrate coupled
workspace consumers together; there are **no backward-compatibility aliases,
old-contract-taking adapters or deprecated wrappers**.

| Removed contract | Current integration |
|---|---|
| Async `StatelessFactory::create(ServiceContext)` / `StatefulFactory::create(StatefulServiceContext)` | Sync fallible `IStatelessServiceFactory::create_instance` / `IStatefulServiceFactory::create_replica`, explicit type/URI/init bytes/partition/public ID. Factory captures activation/native integration providers separately. |
| `StatelessService`, `StatefulService`, concrete partitions | `IStatelessServiceInstance`, `IStatefulServiceReplica`, object-safe `IStatelessServicePartition` / `IStatefulServicePartition`; explicit `OpenMode::{New,Existing,Invalid}` (New only). |
| `register_stateless` / `register_stateful` async calls | Sync `register_stateless_service_factory` / `register_stateful_service_factory` with boxed factories on reserved execution; await the owned observation, not SDK Result. |
| Monolithic `Replicator`, old configuration/removal payloads | `IReplicator` + `IPrimaryReplicator`; sync applied/disposed configuration/removal, async lifecycle/build/catch-up; private native-issued metadata outside SDK descriptors. |
| Old `Epoch` fields, `Idle`/`Secondary` role vocabulary, diagnostic `ReplicaStatus` | `data_loss_number` / `configuration_number`, `IdleSecondary` / `ActiveSecondary`; SDK `ReplicaStatus::{Invalid,Down,Up}` vs `extensions::ReplicaDiagnostics`. No mechanical rename of native FM/COM/XML fields. |
| Public `Engine`/`submit`, `StateProvider`, concrete operations/streams | One `fube_data::FabricReplicator`, `IStateReplicator::replicate` early admission + separate completion, standard-only `IStateProvider`, payload `IOperationDataStream` vs acknowledged destination `IOperationStream`/`IOperation`. |
| Ambiguous `report_health`, Duration TTL/HealthSequence convenience | Target-specific synchronous health/health2, `HealthInformation` TTL seconds (0 infinite), sequence0 auto, optional borrowed send options only on health2; both Immediate values direct-send. |

Use the [compiling public registration example](../crates/libs/fube-runtime/src/lib.rs#L12-L31)
and [actual combined construction/pumps](../crates/samples/rust-runtime-app/src/data.rs#L736-L774).
No `native_binding`/`replication_runtime` discovery members exist on SDK traits.
Private issued authority cannot be constructed from descriptors or diagnostics;
out-of-host standalone primary control is not supported.

**Provenance:** SF/mssf informs canonical contracts, not whole implementation
source/ABI compatibility. Execution/native-binding/cleanup integration is newly
written fube infrastructure using standard Tokio ownership/JoinSets, not a
ported SDK executor. Replay/origin/epoch-checked behavior preexisted in fube and
was reorganized as explicit capabilities. Native `ToPrimaryEpoch` supplies the
mask rule; exposing `EpochDomains` is fube's choice.

Primary local apply precedes quorum wait; the included provider's standard-named
last-committed getter can lead quorum. Cancellation after admission is ambiguous,
not rollback. Both destination copy and replication operations need explicit
once-only service ACK; provider enumeration has no ACK. These are coordinated
contract migrations, **not replication-algorithm changes or new durability**.
Historical native runs below and in runtime guides qualify only their old exact
source/profile; current alignment deployed validation is **NOT RUN**.

## Original repository import

This repository imports the Fabric client, transport, runtime/host documentation,
standalone stateless host and native-runtime fixture from
[`youyuanwu/service-fabric-apps-rs`](https://github.com/youyuanwu/service-fabric-apps-rs).

The agreed source range is **inclusive**:

- First commit: `f06e8b28f3e6930c8f336c645843f0c634c8aa49` (initial Rust client POC).
- Last commit: `3fad8553f532589031235c66d64a652de051cf44` (manifest test host).

The first imported commit contains the selected files as they existed at the
first revision, including prerequisite transport documentation and onebox
support that predate it. All 28 commits in the range are retained with their
authors, author/committer dates, and original messages, followed by
`Source-Repository` and `Source-Commit` trailers. Filtered trees/parents change
commit IDs and invalidate original signatures; no signature preservation is
claimed. Old PR numbers in imported commit messages refer to the source repo.

The filtered history was merged without rewriting fube's existing initial
commit or license. For example:

```sh
git log --all --grep='Source-Commit: f06e8b28f3e6930c8f336c645843f0c634c8aa49'
git log --follow -- crates/libs/fube-client/src/lib.rs
```

Post-import integration removes unrelated workspace members/dependencies,
replaces app-specific CI/build recipes, and updates documentation links.
At that import checkpoint the client, transport, host and native-app source code
was otherwise unchanged; later fube development is separate from the import.
The original MIT notice is retained in `LICENSE.service-fabric-apps-rs`.

## Repository boundary

fube owns the native-wire client/transport and standalone host, their complete
tests/designs, native acceptance scripts/workflow, and a simple single-container
Linux onebox. Shared lifecycle scripts, bootstrap/probe code, and regression
fixtures are included so onebox remains self-contained; historical names and
resource identities remain stable. No multi-container Dockerfile, multi-container
CI job, kvmap, or replicator implementation is imported.

The original repository retains `kvmap`, `mssf-ext`, `mssf-rplctr`, their packaging
and Windows deployment workflow, and the multi-container cluster. After the
initial migration removed its onebox-specific entry points, the matching onebox
infrastructure, offline tests, and lifecycle CI were restored there. Retained-data
shutdown/restart qualification lives there; fube keeps its self-contained onebox
for live native-client CI without duplicating that qualification. Moved
client/host code remains in fube. No generated cluster state, native libraries,
build output, or local workflow artifacts are migrated.
