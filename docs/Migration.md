# Migration and provenance

## Explicit Endpoint, Channel and client construction

All clients now consume an established Channel. The address-taking convenience
constructors are removed, without deprecated wrappers or compatibility aliases.

| Removed API | Current composition |
|---|---|
| `FabricClient::connect_clear(address, timeout)` | `FurpcClient::endpoint_clear(address)` -> furpc Endpoint connect -> local `FurpcClient::new(channel)`; the ordinary legacy façade is removed. |
| `ImageStoreClient::connect_clear(address, timeout)` | `ImageStoreClient::endpoint_clear(address)` -> furpc Endpoint connect -> async `ImageStoreClient::new(channel, ImageStoreClientOptions).await` |
| `ServiceNotificationClient::connect_clear(address, timeout)` | `ServiceNotificationClient::endpoint_clear(address)` -> Endpoint connect -> async `ServiceNotificationClient::start(channel, NotificationClientOptions)` |
| `Channel::connect_unsecured_loopback(address, composition, limits, deadline)` | `Endpoint::unsecured_tcp(address, composition)?.channel_limits(limits).connect_until(deadline)` |
| `Endpoint::unsecured_loopback(address, composition)` | `Endpoint::unsecured_tcp(address, composition)`; general TCP now accepts non-loopback unicast addresses. |
| `fube_transport::accept_unsecured_loopback(stream, composition, limits, deadline)` | `fube_transport::accept_unsecured_tcp(stream, composition, limits, deadline)`; local runtime IPC keeps its separate loopback-only constructors. |

The first three rows use furpc; the remaining rows describe legacy transport APIs
still used by other workspace consumers.
`Endpoint::connect_timeout` governs dialing; client options govern subsequent
operations, and notification options separately govern real native startup.
Enclose furpc connection/startup in `tokio::time::timeout_at` to share one
absolute deadline; use its remaining duration for notification startup.
See the [construction and deadline examples](transport/RustFabricClient.md#uncached-resolution-and-independent-notifications).

Native client constructors install explicit authorized furpc ingress services
on dedicated Channels with no existing ingress service. Notification and
ImageStore operations capture an original-generation Peer without reconnect or replay.
ImageStore's legacy `close`/`is_closed`/`terminal_error` methods are removed.
Drop releases Channel ownership, not a physical-driver join barrier or revocation
of outside clones. Crate-root transport re-exports are removed; Error/Result now
mean fubst input/model validation. Remote errors use FurpcClientError,
NotificationResult and ImageStoreError/ImageStoreResult.
Runtime's initiating IPC and Hosting's accepted IPC both use furpc.
Data replication now also uses furpc's public raw one-way service/Peer path;
RA/node control now also uses furpc through the Node-owned FM/peer adapter.

Runtime requests now use typed furpc Request/Response and FubstCodec over the
original Peer. Host/PID identity is added only to initiating requests and semantic
one-way sends, never original-path replies. The legacy runtime transport adapter,
ordered handoff and held-completion worker are removed. Runtime authority remains
checked at domain admission/publication and completion, not at each first-write
attempt; already-admitted output can have an unknown remote outcome after
retirement. Close releases Channel ownership, not a physical-driver join.
`RuntimeEvent::QueryReplySent` becomes `QueryReplyReady` with a furpc MessageId
and means reply data was handed to the runner, not that it was written.
No furpc lifecycle or write-fence extension is introduced.

Runtime's retained lifecycle/stateful/subprocess and shared RAP model peers now
use public furpc Server/Peer/ingress contracts, including runner-owned replies.
Backpressure and partial-read checks use test-only I/O adapters. Hosting/node
integration tests retain independent, byte-preserving frame observation using
public furpc headers; they exercise the production Hosting server.
Neither fube-runtime nor fube-rap-test-support directly depends on fube-transport,
including dev-dependencies. Legacy core/tool transitive consumers remain.
Role-changing reconfiguration retries during catch-up no longer receive a
membership-only success before the service and replicator roles are applied.

Hosting's accepted IPC now composes public furpc Server/ingress services and
captures the original-generation Peer for semantic lifecycle sends. Host/PID
admission, serialized registration mutations, finite leases, non-evicting replay
history, result ownership and explicit successor admission remain in Hosting.
Replies are data handoff to furpc, not a first-positive-write authority fence or
an ingress-before-reply-write barrier. Lifecycle observers return furpc Error/Result;
authority is checked before submission and after local write completion, while
already-admitted output can race revocation with an unknown remote outcome.
Unbound/wrong-kind/capacity-rejected ingress is discarded by furpc without
granting Hosting authority; it need not retire the connection. Invalid domain
messages that reach Hosting still retire their exact session.
Hosting owns and joins connection/send tasks on its dedicated executor; dropping
each inline furpc serving future releases its accepted socket. This is a consumer
ownership guarantee, not a new furpc driver-join API. Unregister replies retain the
bounded original cleanup path after serving retirement. Fubst codec errors retain
their original source. Hosting and the owned-app sample no longer directly depend
on legacy transport, including dev edges. Independent adversarial socket fixtures
use public furpc headers and a test-only clear-frame injector; managed-client and
real runtime-child/RA/RAP coverage exercise production accepted serving.
Native SF11.8.121.4 `hosting-leases` and `hosting-registration` passed at
`0191bcc` with freshly rebuilt, source/hash/mode-verified binaries. Both production
profiles observed two finite correlated polls, actual runtime unregister and
graceful child exit/reap/group drain; registration also verified native factory
registration, same-core lookup and owned local closure decisions. All five
retained native fixture profiles passed separately. This qualifies those clear
accepted-side bootstrap/registration paths, not native deployed lifecycle,
stateful/reporting, failed-write/revocation races, or TLS/claims.

RA reliability factory/decoder APIs now exchange public furpc Messages and
HeaderRecords with furpc Error/Result; native schemas and action/actor/generation
asymmetry remain unchanged. Node control uses an owning per-socket Session and
the original Peer, a bounded domain inbox and consuming response-data slots.
FM/peer PToP incarnation/generation checks and SF-style RA/store semantic gates
remain separate from transport admission. Reader-level ordered handoffs and the
legacy aggregate allocation ledger are removed; generic furpc rejection may
discard, and committed output may race retirement. Response-slot completion
hands data to the runner, not proof of writing or remote receipt. `Link::retire`,
`closed`, `terminal_error` and `is_live` replace direct legacy Channel access;
completed close joins the owned serving task and actual socket adapter release.
No reconnect or new public furpc join observer is introduced.
The RA/node, cluster/FM and standalone-Node normal/dev legacy edges and coupled
fixtures are retired together; core, CLI and test-host edges remain.
Native-data evidence at `154b4f1` does not qualify this later RA/node migration
or native FM/Federation interoperability.

`fube-data`'s low-level `Packet::encode` now returns a public furpc Message;
`Packet::decode` accepts one. Both use core Error/Result for domain validation.
The classic native
schemas, operation segmentation, supported address profile and public SF-style
replicator construction remain unchanged. Its accepted/initiating peers use
furpc raw-only mode and a shared listener address/instance; immediate and delayed
ACKs retain original-generation Peer affinity rather than resolving a replacement.
The direct legacy normal/dev dependency is removed, including retained socket/
schema fixtures. Framing/checksums/handshake belong solely to furpc.

The data adapter's control/data queue/byte allowances remain independent domain
policies after verified body delivery, not pre-body allocation admission or
an aggregate heap guarantee. Domain-invalid input/control overflow retires its
exact session; generic furpc rejections may discard without closing. Shutdown
joins owned session/I/O lifetimes on the dedicated thread but can truncate a
committed frame; the old first-write predicate/started-suffix flush contracts
are retired. Local qualification write evidence is
`complete_local_socket_acceptance` with actual observed frame bytes, not an
explicit flush or remote ACK. Fresh native furpc replication qualification passed
all 17 `native-data` scenarios at clean source `154b4f1` against SF 11.8.121.4,
including both replication directions, copy/ACK/quorum and owned workload cleanup.
See the [exact-source record and limits](runtime/FubeData.md#furpc-native-data-qualification-2026-11-20);
historical data/Hosting receipts are not relabeled as this migration's evidence.

Public `ReplicationEndpointOptions` literals must set `advertised_ip`: use
`None` for an existing concrete listener, or `Some(ip)` for the concrete IP to
publish from a wildcard listener. The actual bound port is retained; there is
no NAT/port translation. Hosting's local `NodeContext` now also requires an
explicit `ip`, propagated through native node metadata and application
environment without changing the container-local IPC address.

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

### Peer ingress receivers

`fube_data::transport::Incoming` no longer exposes raw `control`/`data` receiver
fields. Migrate low-level consumers together; no compatibility side queues exist.

| Previous use | Explicit payload receive |
|---|---|
| `incoming.control.recv()` | `incoming.recv_control()` |
| `incoming.data.recv()` | `incoming.recv_data()` |
| Selecting both receiver fields | One `incoming.recv()`, then inspect `Packet.body` |

The combined method prefers ready control. Each method is cancellation-safe and
releases transport byte accounting before returning a `Packet`; holding the
payload does not retain that lease. Dropping `Incoming` releases both queued
classes. The bounded queues and class-specific admission/drop policies remain.

Class-specific receives drain their buffered class before returning `None`.
Combined receive preserves control-first closure: empty closed control returns
`None` even with buffered data; `recv_data()` can drain that remainder. One
socket's EOF does not close these transport-wide queues. Calls borrow all of
`Incoming` mutably, so use the combined method rather than selecting two class
methods simultaneously. Public payload consumers receive no source/reply grant;
the private engine handoff retains only original weak session identity, with
independent engine-retention accounting.

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
