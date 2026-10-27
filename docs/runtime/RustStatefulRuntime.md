# Rust volatile empty-state runtime

`mssf-runtime` now hosts owned stateful replicas with custom replicators, beside
stateless instances in the **same executable, code package and Runtime**.
The qualified implementation is an **empty-state lifecycle/control-plane
milestone, not replicated storage**. There is no application write API, data
copy/replication stream, persistence or durability claim.

## Supported profile and ownership

The executed profile is Linux x86-64, native SF **11.8.121.4**, normal activated
ExeHost, one Code package, SharedProcess, trusted clear loopback IPC. The
application loads no FabricRuntime/FabricCommon/COM/native replication library
or native runtime helper. Native nodes still own membership, placement, epochs
and reconfiguration; peer requests cannot elect a primary or alter membership.
See the [shared host/lease profile](RustStatelessRuntime.md#qualified-profile).

The mixed package registers `RustRuntimeType` (stateless) and
`RustStatefulRuntimeType` (**HasPersistedState=false**); its default stateful
service starts with target/minimum **1/1**, then qualification changes it to
**3/2**. Both share Rpc; the Internal Replication endpoint is a **different
control listener/address**, not a client endpoint or native FabricReplicator
wire implementation
([manifest](../../crates/samples/rust-runtime-app/pkg/RustRuntimeService/ServiceManifest.xml),
[application](../../crates/samples/rust-runtime-app/pkg/ApplicationManifest.xml)).
The package reader validates both manifest and native digested type declarations;
persisted/implicit/group types and unrelated host-profile expansions remain
rejected.

## Owned programming model

1. Attach `Runtime`, then call `register_stateful(name, Arc<Factory>)`.
2. `StatefulFactory::create` receives owned service/partition identity, public
   replica ID, **separate RAP incarnation and node incarnation**, initialization
   bytes and activation snapshot.
3. `StatefulService::open` receives a generation-bound `StatefulPartition` and
   returns an owned `Arc<dyn Replicator>`. Replicator Open returns its control
   address; service ChangeRole independently returns its client address.
4. Implement service/replicator Close and Abort, role/epoch changes, progress,
   data loss, configurations, catch-up, build and removal. There is deliberately
   **no default all-success replicator**.
5. Use `partition.status()`, `access_status()` and `ensure_valid()` for owned
   diagnostics and authority/lifetime checks. A published address or a cached
   role alone is not permission to serve. Shutdown is explicit and awaited.

API: [stateful_api.rs:18-191](../../crates/libs/mssf-runtime/src/stateful_api.rs#L18-L191),
[partition handles:346-382](../../crates/libs/mssf-runtime/src/stateful_api.rs#L346-L382),
[registration:405](../../crates/libs/mssf-runtime/src/lib.rs#L405).
Stateful reporting/write/storage APIs are not supplied by this seam.

Callbacks execute outside protocol locks/readers. Cancellation is advisory,
**not proof that work or callback destruction completed**. Owned operations are
supervised through actual completion/disposal; uncooperative callbacks remain
subject to the independent OS watchdog. Its 64 atomic callback-deadline slots
are separate from the send-relative native host lease. Healthy lease polls
cannot renew a stuck callback deadline; pending ownership prevents normal
watchdog disarm
([lease.rs:14-113](../../crates/libs/mssf-runtime/src/lease.rs#L14-L113),
[disarm:158-173](../../crates/libs/mssf-runtime/src/lease.rs#L158-L173)).
Shared process authority loss fences **both** service kinds; replica disposal
removes only that generation's routes/operations.

## What “empty” proves

The actual [sample policy](../../crates/samples/rust-runtime-app/src/stateful.rs)
keeps initialized application progress **first=last=0**. Epochs and control
counters never become fabricated log sequence numbers. `on_data_loss=false`
means the permanently empty application state did not change, not that native
data-loss epochs cannot advance.

Build and catch-up use bounded real HTTP peer-control exchanges. They validate
the native-selected partition/node/replica/incarnations, role, allowed native
epoch and zero frontier. Missing, foreign, stale, removed, wrong-role or nonempty
peers do not prove readiness. Native configuration quorum, ALL and required
target obligations are distinct, including the previous configuration and
whether it actually contains the local replica. Removal tombstones, control
revisions, cancellation and reconfiguration fence late receipts; removal does
not shrink native membership to manufacture success
([policy checks:216-259](../../crates/samples/rust-runtime-app/src/stateful.rs#L216-L259),
[catch-up:437-536](../../crates/samples/rust-runtime-app/src/stateful.rs#L437-L536)).

Native demotion runs **ALL → UpdateEpoch → ALL → role changes**. Both ALL checks
may encounter a real peer still in the explicitly native previous epoch,
because demotion precedes target promotion. That epoch is accepted only during
native-directed Secondary / still-Primary-replicator demotion in PreWriteRevoke
or CatchUp, not ordinary Primary quorum readiness
([regression](../../crates/samples/rust-runtime-app/src/stateful_peer_tests.rs#L3-L59)).
This is empty-state readiness, **not an acknowledgment of replicated writes**.

## RAP, access and endpoint semantics

The adapter implements ordered Open/ChangeRole/Close/Abort, UpdateConfiguration,
build/remove, quorum/ALL catch-up/cancel, progress/update-epoch/data-loss,
endpoint notification/retry and stateful RAP query observations. Applied
configuration receipts are separate from mere native observations, retaining
down/Dropped descriptors in proof keys. Stale epochs/incarnations, removed peer
receipts and canceled overlap cannot reinstall success
([adapter](../../crates/libs/mssf-runtime/src/stateful_reconfiguration.rs),
[review regressions](../../crates/libs/mssf-runtime/src/stateful_review_tests.rs)).

The native RA envelope is actor **12 in both directions**. Lifecycle semantic
replies are one-way, not ordinary correlated replies. UC/build/remove use native
default service/runtime fields; admission binds the existing replica owner,
not stateless-era defaults. Endpoint notifications retain the original body
until an exact incarnation/role/location acknowledgment or disposal. Volatile
access revokes locally; persisted-only read/write-revoked notifications are not
invented.

`ReplicaStatus` retains full RAP current/previous epochs and service versus
replicator roles. Access uses native role/stage/minimum/PC/CC availability rules,
then the partition overlays lifetime and host authority. Pre-write revocation
cannot be undone by a same-epoch retry. Granted diagnostics **do not introduce
an application write operation**
([access:234-342](../../crates/libs/mssf-runtime/src/stateful_api.rs#L234-L342)).

The sample's service route requires live authority, matching desired/service/
replicator Primary roles and granted read access; its diagnostic route is
lifetime-only. Secondary service requests return 503. Addresses include a local
generation token so reopened native identities/new processes cannot revive old
routes. Stateless route policy is unchanged.

**Epoch observation caveat:** the native FM partition-list query returns
`ToPrimaryEpoch()`, not the complete RAP epoch: mask the low 32 configuration
bits when correlating them. The observer retains both, not false full equality.
Native source evidence is read-only baseline
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:
`Reliability/Failover/fm/QueryHelper.cpp:1075-1085`,
`ra/ProxyActionsList.cpp:130-142`. This is not a claim that the deployed binaries
were built from that revision; see [native architecture](FabricRuntime.md).

## Reproduce compiler-free native qualification

Install the repository toolchain on the **build** machine, build/reuse the
[onebox image](../UbuntuOnebox.md), and choose new bundle/report paths and a
uniquely owned allocation name:

```sh
cargo test --locked -p mssf-runtime -p rust-runtime-app -p mssf-transport -p mssf-client
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/rust_runtime_test.py --build-bundle build/stateful-binaries
# Transfer the complete bundle preserving executable modes when using another runner.
python3 scripts/rust_runtime_test.py runtime-stateful-check \
  --prebuilt build/stateful-binaries --stateful three --full \
  --report build/stateful-native.json
```

`--stateful one --full` runs the initial singleton milestone without the
three-replica scenarios. Three requires `--full` before allocation, never a
reduced suite labeled full. The native runner **requires --prebuilt** and has
no Cargo fallback. Version **3**, profile
`linux-x86_64-volatile-empty-control-v1` bundles contain exact source revision,
dependency graph, both executables and both versioned package templates with
strict hashes/inventory/schema/modes. Build source must be clean.

The independent `runtime_observer` uses native mssf-client partition/replica/
deployment/code-package queries, primary discovery and actual HTTP. Its
constraint-respecting native `MovePrimary` query has **no force/ignore option**;
it never implements runtime membership. RA departure queries make no fixture
contact. Host termination uses the corroborated native PID/executable/identity
and pidfd exit proof, not a process-name kill. Original creation capabilities
govern cleanup; application/type/upload and each original container/volume/
network absence are checked independently
([harness](../../scripts/rust_runtime_test.py),
[observer](../../crates/libs/mssf-client/examples/runtime_observer.rs)).

## Executed evidence and limits

Local ignored evidence `build/stateful-empty-native-three6.json` records actual
SF 11.8.121.4 **PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE**,
errors=[], executable-tested source
**`a29b2aff265e35f1444ac38640f97e41f34ce240`**:

| Executed scenario | Independent outcome |
| --- | --- |
| Initial singleton, then target=3/min=2 | Native Ready 1P+2S, discovery/HTTP/control identity and role/access agreement |
| Planned Primary move | Previous Primary retained as Secondary in the same process, old service HTTP 503; primary configuration epoch 8589934592 → 12884901888 |
| Nonforce replica removal | Actual Abort, old service/control HTTP 404, old ID absent from native RA inventory, colocated stateless identity unchanged; epoch 17179869184 |
| Auxiliary volatile deletion | Graceful Close without Abort fallback, native RA inventory empty, retired routes, unrelated stateless process/owner unchanged |
| Intentional mixed-host SIGKILL | pidfd exit; PID 24232 → 26618 with new native host/entry-point identity; both types recover, old routes 404; epoch 21474836480 |
| Preserved legacy suite | Native config-only update, reporting, query/update, callback failures, independent authority hard fence and restart pass; stateful 3 revalidated afterward |
| Cleanup | Minted app/type versions 1.1/1.2/uploads absent; exact owned container, volume and network destroyed and individually absence-confirmed |

| Evidence | SHA256 |
| --- | --- |
| Report | `bdfd20d56a7312bffd77b6fce2725d4d3f89b60dbab496c8f31c851c9b806e65` |
| Bundle metadata | `83dce91fff9a6f41a2da7c0e485dd50a0adf70d2f894d195b1a79080891c31a3` |
| Application | `9a8242c80c90c4d5160daf36f88530cfde6dff70f9edd060f414cc4fbed5485d` |
| Observer | `9df0ffd68526967618d063eeb9605feead6bcbaf68e3492d5ac8ed06067c9878` |

Earlier failed probes were not promoted to passes. The later harness-only
missing-`--full` rejection and documentation do not alter these tested Rust
executables. Evidence is local, not checked into the repository; CI runs the
same strict prebuilt/native tiers and publishes its own results.

**Refreshed-main repeat:** latest-main fetch/rebase was a no-op onto
`c2601c382cfcdfe7c05cdc5c103005e74c8822af`. A new clean bundle and full native
run at **`2b3179b6e14b2cafa097b438deb8ca94a16f50ab`** passed every scenario again,
including the required-full guard and independent RA departure checks:
`build/stateful-empty-native-final.json`, SHA256
`78d66aa6c0e4349ab63cb153a4f4a89faf5f63632745133eb6d4ab5eada4ae53`;
bundle metadata SHA256
`20257fdf5ef0be0caf9fca619ca96e496c75fcad87592c7dc31402d597b715d4`.
Both executable hashes are identical to the table above. This repeat replaced
shared PID 25971 → 26727; all minted app/type/upload and original
`stempty-dab15f41-final` container/volume/network resources are absence-confirmed.
Subsequent evidence-note commits are documentation-only.

No application-state writes, replication data plane/storage, persisted recovery,
durability, native FabricReplicator peer wire compatibility, Reliable Collections,
rolling code upgrade/rollback, TLS/reconnect, Windows runtime or expanded host
profiles are qualified. This is a bounded **subset of custom stateful hosting**,
not completion of the broader C/N/T capability tiers.
