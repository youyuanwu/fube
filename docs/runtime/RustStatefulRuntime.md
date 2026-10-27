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

API: [stateful_api.rs:18-202](../../crates/libs/mssf-runtime/src/stateful_api.rs#L18-L202),
[partition handles:357-393](../../crates/libs/mssf-runtime/src/stateful_api.rs#L357-L393),
[registration:405](../../crates/libs/mssf-runtime/src/lib.rs#L405).
Stateful reporting/write/storage APIs are not supplied by this seam.

`Replicator::remove_replica` receives an owned `ReplicaRemoval` purpose:
`Idle` for native idle-owner removal, or `BuildRetryCleanup` for disposal of
partial/previous resources before another node-authorized Build. Cleanup
success is never Build success. Failed Build/Remove retains a cleanup obligation
even for equal-incarnation retries; final native removal remains a distinct
terminal tombstone. The sample fences pre-cleanup network receipts with per-peer
build tokens, permits a verified retry after cleanup, and still rejects a
genuinely removed incarnation.

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
Final runtime-owned service/replicator references, including local callback
clones and failed-Open disposal, are released on supervised blocking workers
**before** semantic disposal replies. A blocking destructor remains deadline
fenced while a different executor worker renews the host lease.
Close/Abort also acquire the compatible-configuration dispatch gate: clearing a
callback flag alone is insufficient while a dispatcher retains participants
behind a queued reply. The whole dispatcher reference lifetime must drain
before disposal can acknowledge, including after cancellation or backpressure.

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
([regression](../../crates/samples/rust-runtime-app/src/stateful_peer_tests.rs#L83-L139)).
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

Successful still-current configuration application transfers matching Ready
idle-build records to retired bookkeeping according to native CC/applicable PC
selection. A delayed same-incarnation RemoveIdle then acknowledges without
calling custom removal or invalidating the live member's readiness; it does not
silently remove that configured member. A newer native incarnation still
requires cleanup of its older owned resources before Build retry.

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
([access:245-353](../../crates/libs/mssf-runtime/src/stateful_api.rs#L245-L353)).

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
The controller publishes the newly committed original owner capability before
fallible startup/readiness; both the harness and CI retain it for teardown on
partial creation failure. Rejected pre-existing/colliding names grant no
capability, and owner drift never authorizes destroying a replacement.

## Executed evidence and limits

The records below identify pre-remediation executables precisely. They are not
silently relabeled as qualification of subsequent final-review semantic fixes.

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
The `2b3179b` → `278de9a` evidence-note change was documentation-only; subsequent
final-review remediation requires its own executable-qualified record.

**Post-remediation qualification:** all four final-review fixes were followed by
a new clean bundle and actual full native PASS at
**`0a0a897d0562fdd0e00f7e943ef6a2fb1da1c185`**, after another latest-main
fetch/no-op rebase onto the same base. Report
`build/stateful-empty-native-reviewed.json` SHA256
`c01e8d8adc8c53346242da89309b9c88b27ac23ec0abb2a2f5ab526f0497e652`;
bundle metadata SHA256
`104ece2dda813d5b3a4ec1d7b168faa305376db259a7789e32571fbc43241bc1`.
New application SHA256
`288db31885ed504e5e7a19fd06938364931fee9832cc8a8c41c435fbce3d036a`;
observer SHA256 remains
`9df0ffd68526967618d063eeb9605feead6bcbaf68e3492d5ac8ed06067c9878`.
All singleton/three-replica swap/Close/Abort/mixed-host and preserved full
legacy scenarios passed again, errors=[]; shared PID 25525 → 26253.
Auxiliary Close has empty native RA inventory, removed old replica is absent
despite a live replacement, and all old service/control routes are fenced.
Minted app/type1.1&type1.2/uploads and exact original
`stempty-dab15f41-reviewed` container/volume/network are absence-confirmed.
Refreshed-main checks pass: 542 workspace tests, 10 doctests, 153 Python tests,
fmt/check/clippy and strict four-library Rustdoc; 5 explicitly native-tier tests
are ignored locally, not counted as passes. Focused new tests first reproduced
each reviewed bug, including positive blocking-destructor entry under healthy
lease renewal and actual CI-script partial-creation failure handling.

No application-state writes, replication data plane/storage, persisted recovery,
durability, native FabricReplicator peer wire compatibility, Reliable Collections,
rolling code upgrade/rollback, TLS/reconnect, Windows runtime or expanded host
profiles are qualified. This is a bounded **subset of custom stateful hosting**,
not completion of the broader C/N/T capability tiers.
