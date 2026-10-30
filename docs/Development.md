# Development

Use the toolchain in `rust-toolchain.toml` and Python 3.11+. The workspace does
not require `protoc`, app packaging tools, or a sibling repository.
The workspace and Python test infrastructure support Linux only; non-Linux
targets fail explicitly instead of compiling portability fallbacks or skipping
Linux tests. Native Service Fabric qualification remains Linux x86-64 only.
Windows-named COM/PAL interfaces and native GUID byte ordering are retained
because the Linux Service Fabric ABI/protocol requires them.

## Test layout and CI partitions

Python test code lives under `tests/`; reusable cluster lifecycle commands stay
under `scripts/`. Run module commands from the repository root, without a
`PYTHONPATH` override. Rust unit/integration tests remain with their crates.
[`tests/suites.toml`](../tests/suites.toml) defines suite selectors, profiles,
timeouts, report inventories and ordered onebox partitions. Python's standard
`tomllib` reads it; no additional framework or package is required.

```text
tests/
  suites.toml                      suite/partition and expected coverage definitions
  suites.py                        strict manifest validation
  run.py                           suite runner and CI matrix output
  offline/
    cluster/                       lifecycle, topology, diagnostics
    native_runtime/                standalone harness models
    onebox/
      client_runtime/              runtime harness models
      native_data/                 common and mixed-native data harness models
      shared/                      CI receipts, aggregation, partition routing
  native_runtime/runner.py         clusterless native acceptance
  onebox/
    client_runtime/__main__.py     client-runtime partition alias
    native_data/__main__.py        native-data partition alias
    shared/
      data.py                     replication harness and bundle producer
      runtime.py                  runtime harness and bundle producer
      ci.py                       input validation, cleanup, evidence aggregate
      fixture.py                  partition-owned onebox startup and finalization
```

The two onebox directory names map to the existing `client-runtime` and
`native-data` CI workers. The Linux producer emits the worker matrix from this
file. CI invokes `python3 -m tests.run --partition "$WORKER_ROUTE" ...`; YAML
retains prerequisites, pinned image building, artifact transfer and an
`always()` emergency-cleanup step, not normal cluster lifecycle or suite lists.

The manifest selects supported runners (`unittest`, archived `nextest`, data,
strict legacy, standalone native runtime), not arbitrary shell commands.
Unknown options/references, duplicate or unassigned onebox suites, invalid
resource scopes and empty unittest selections fail. Each onebox suite must be
assigned exactly once. Move a **whole suite** between partition lists to change
distribution; shared-state scenario implementations and their order remain in
Python. This is not automatic per-test sharding.

Each partition acquires one onebox fixture. Before allocation the runner checks
output/config paths and validates the strict current-attempt producer receipt,
bundle tar, extracted binaries and any native-client archive. It requires the
pinned image to be built already; there is no compiler or image-build fallback.
The existing guarded controller creates the cluster, publishes its immutable
original owner before fallible startup, and waits for native readiness. A fresh
ownership/health check gates suite execution.

Partition execution is ordered and non-fail-fast: a failed/timed-out suite stays
failed while later suites still run. Suites continue owning only their minted
applications/services, not the cluster. In `finally`, the fixture collects
credential-safe diagnostics under the original-owner guard and destroys its
owned allocation, independently verifying resource absence. Partial startup,
suite interruption and diagnostic failure still attempt teardown. Replacement
owners and colliding allocations are never adopted or deleted.

`--existing-owner OWNER` (also accepted as `--owner`) explicitly reuses a
caller-owned healthy allocation and **retains it**, including after failure.
It publishes no emergency-destroy capability. Such retained runs cannot satisfy
the CI aggregate's original-allocation teardown requirement. Normal partition
commands require `--name`, `--bundle-tar`, `--prebuilt`, `--producer` and a new
`--report-dir`; native-client partitions also require their archive, private
config directory and fixture owner. The shared whole-profile commands below
remain available for local qualification.

In CI, `--github-output` publishes the masked original capability immediately;
`cleaned=true` is emitted only after verified teardown. An `always()` emergency
step retries cleanup with that exact capability when normal cleanup did not
complete, including cancellation that prevents Python finalization. Its receipt
is separate and cannot overwrite/heal a failed normal run. Runner loss can still
prevent all cleanup; no in-process fixture guarantees recovery from host loss.

The five native-client tests remain crate-local and execute
through nextest, with `--no-tests fail`, before Rust data and legacy in their
current partition. Private fixture configs/journals stay outside uploaded proof
directories. The aggregate independently revalidates typed reports, exact
manifest-derived coverage, `suite-outcomes.json`, its manifest hash, actual
`fixture.json` readiness/ownership/phase evidence and original-resource cleanup.
A command-success receipt alone cannot pass.

Offline groups mirror these responsibilities and are selected by suite name:

```sh
python3 -m tests.run --suite offline
python3 -m tests.run --suite cluster_models
python3 -m tests.run --suite runtime_models
python3 -m tests.run --suite data_models
python3 -m tests.run --suite ci_models
python3 -m tests.run --suite native_runtime_models
python3 -m tests.run --matrix
python3 -m tests.run --partition client-runtime --help
```

Direct `unittest discover -s tests/offline -t .` remains available for targeted
Python debugging. The shared harness commands below retain local whole-profile
qualification and upstream bundle production.

## Rust producer and compiler-free consumers

The [Rust producer](../.github/workflows/rust-build.yaml) builds all workspace
binaries/examples and a workspace-wide, all-target/all-feature nextest archive.
It uses Ubuntu 22.04 x86-64 so its native executables also run on the oldest
acceptance runner, rather than depending on Ubuntu 24.04's newer glibc.
It publishes `rust-tests`, `fube-data-binaries`, `rust-runtime-binaries` and
`native-runtime-binaries`. Tar bundles preserve executable permissions.

The current-attempt producer receipt binds the archive and native bundles to
the actual checkout/tree, PR identity, run/attempt, target, debug profile,
Rust/Cargo/nextest versions, lockfile and toolchain file. Archive-only Rust tests,
standalone native acceptance and onebox workers depend only on this producer
and run without building Rust. Standalone acceptance verifies the bundle before
native preflight; missing, changed or wrong-source binaries fail without a
compiler fallback.

Python offline tests and Rust quality checks run independently of the producer.
Quality covers formatting, production-only native-free library checks,
workspace Clippy/docs and doctests. Nextest does not include doctests.
The existing **Linux onebox native client** aggregate also requires successful
archived Rust tests, Rust quality, Python tests and standalone acceptance;
failed, skipped or cancelled checks cannot pass the final gate.

The Python `--build-bundle` commands below now only package existing Cargo
outputs and record dependency graphs with `cargo tree --locked`; they never
compile. Build the required binaries/examples explicitly first.

## Native-free checks

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked -p fube-transport -p fube-client -p fube-test-host
cargo test --locked -p fube-ra-store
cargo test --locked -p fube-ra
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo doc --locked -p fube-transport -p fube-client -p fube-test-host --no-deps
python3 -m unittest discover -s tests/offline -t . -p 'test_*.py'
```

The [RA protocol foundation](cluster/RaProtocolFoundation.md) adds ordinary
native-free Rust tests and offline catalog/input-validator tests to these same
workspace/offline selections. Its separate [native source-baseline codec
oracle](../tests/ra_protocol/README.md) has explicit Clang/Rust fixture production
and digest/source-bound, compiler-free consumption. It needs no running SF
installation or full native build and does not qualify a deployed native FM/RA.

The live tests are ignored by ordinary `cargo test`. Offline model tests do not
establish native wire compatibility. Unsupported host platforms fail before
Python test discovery.

The [RA P1 store](cluster/RaStore.md) uses bundled SQLite C through
`tokio-rusqlite`; producers need a C compiler and archiver (no SF or system
SQLite development package). Do not override the bundled build with
`LIBSQLITE3_SYS_USE_PKG_CONFIG` or custom `LIBSQLITE3_FLAGS`. Its nonignored
workspace tests are automatically included in the existing `rust-tests` archive
and resource-free `rust_tests` suite. Acceptance consumers execute that archive
with standalone nextest; they do not compile Rust, install SQLite/SF, or start SF.

## Standalone native runtime

Follow [native ABI prerequisites and commands](runtime/RustTestHost.md#22-prerequisites-and-commands):

```sh
cargo build --locked --workspace --bins --examples --all-features
python3 -m tests.native_runtime.runner package --output build/native-runtime-binaries
python3 -m tests.native_runtime.runner prepare
python3 -m tests.native_runtime.runner bootstrap --prebuilt build/native-runtime-binaries
python3 -m tests.native_runtime.runner close --prebuilt build/native-runtime-binaries
python3 -m tests.native_runtime.runner abort --prebuilt build/native-runtime-binaries
python3 -m tests.native_runtime.runner manifest-close --prebuilt build/native-runtime-binaries
python3 -m tests.native_runtime.runner manifest-abort --prebuilt build/native-runtime-binaries
```

The helper extracts SHA256-pinned libraries under ignored `build/`; it does not
install/start a node or cluster. The app still needs compatible OS ABI libraries.
Use `--dependency-directory PATH` for private ABI dependencies.
The reusable [native workflow](../.github/workflows/native-runtime.yaml), called
by the main CI after artifact production, qualifies all five scenarios on Ubuntu
22.04 without installing a Rust toolchain.

## Live native client

Create a [onebox](UbuntuOnebox.md), install the pinned nextest version from
[the Rust producer](../.github/workflows/rust-build.yaml), and follow the
[client qualification instructions](transport/RustFabricClient.md).
CI builds the test archive once, then runs the explicitly owned fixture/setup
tests and `group(onebox)` against the real native gateway. The two
[onebox workers](#parallel-onebox-ci) depend only on artifact production; the
aggregate additionally gates all independent checks.
Each worker always attempts diagnostics and original-owner teardown.
Retained-data shutdown/restart
qualification runs in
[service-fabric-apps-rs](https://github.com/youyuanwu/service-fabric-apps-rs/blob/main/docs/UbuntuOnebox.md),
not in this job; offline lifecycle regression tests and local down/up commands
remain available here.

## Rust application runtime (Linux x86-64)

This is separate from the native-backed `stateless-test-app` and standalone
node-side test host. It deploys a normal ExeHost to real SF nodes:

```sh
cargo test --locked -p fube-runtime -p rust-runtime-app -p fube-transport
cargo clippy --locked -p fube-runtime -p rust-runtime-app --all-targets -- -D warnings
cargo doc --locked -p fube-runtime --no-deps
python3 -m unittest discover -s tests/offline -t . -p 'test_*.py'
just onebox-build
cargo build --locked --workspace --bins --examples --all-features
python3 -m tests.onebox.shared.runtime --build-bundle build/runtime-binaries
python3 -m tests.onebox.shared.runtime runtime-check --prebuilt build/runtime-binaries --full --stateful three
```

Use new bundle/report paths and a unique allocation name. The last command
creates and destroys only its exact owned onebox, using the
existing lifecycle controller. It verifies native placement, independent
fube-client resolution/RPC, exact pidfd-based application termination/replacement,
native lifecycle/query/update/failure traffic, concurrent shared-process
partitions, a real config-only application upgrade, retained old package/settings
snapshots and ordered subscription/unregistration behavior, runtime
load/health/fault/move-cost reporting, and bounded fencing of
an intentionally non-cooperative application. Reporting qualification reads
native FM load/move retention and HM health sequence/expiration results, then
proves runtime transient fault replaces only its target instance with usable
independently resolved RPC. It also checks stale handles after Close and Abort;
models cover queued-write and terminal-authority races.
It requires Linux pidfd APIs and the existing privileged development container
for the exact-owned-IPC shutdown test; unavailable capabilities fail qualification.
There is no security fallback.

The explicit Cargo step builds the app and independent observer with `--locked`;
packaging requires clean source and existing outputs. Native qualification always
requires `--prebuilt`. CI builds them in the Rust producer and transfers a tar bundle containing
both executables, both versioned package templates, their SHA256 hashes and the
dependency graph (bundle format 3, `linux-x86_64-volatile-empty-control-v1`). Its native consumer
uses `--prebuilt DIRECTORY`: it verifies the bundle and ELF dependencies, then
deploys/tests without installing Rust or invoking Cargo. Missing or changed
artifacts fail; there is no compilation fallback. See the
[separate build/run commands](runtime/RustStatelessRuntime.md#reproduce-native-acceptance).

Use `--report NEW_FILE` for a new structured evidence file. To reuse an already
owned healthy allocation, explicitly supply `--existing-owner OWNER`; the
harness cleans up only its minted application and all its provisioned type
versions with absence checks, never the caller's box.
Failed or ambiguous cleanup is reported with exact recovery identities.
See the supported [stateless](runtime/RustStatelessRuntime.md) and
[volatile empty-state stateful](runtime/RustStatefulRuntime.md) profiles before
interpreting a test result. Delayed/stale commands and lease-expiry races also
have offline model tests; native IPC-loss proof is not a node-failure/TLS claim.

The stateful tier first proves target/minimum=1, then target=3/minimum=2,
constraint-respecting Primary movement, native role/epoch/access observations,
per-replica graceful Close and nonforce Abort with surviving stateless owners,
and intentional shared-host termination/replacement affecting both types.
RA departure inventory, discovery, deployment PIDs and real HTTP are independent
observations; counters alone do not authenticate success. `--stateful three`
requires `--full` so reporting/control/authority regressions cannot be omitted
from a full-profile PASS. This qualifies empty-state lifecycle/control-plane,
not replicated writes, persisted recovery or durability.

The shared lifecycle engine and its regression fixtures retain their historical
`linux_cluster*` names and `docker/linux-cluster` paths. They are support for
onebox, not a separately shipped multi-container image or CI topology.

## Volatile classic V1 replication

The opt-in [`fube-data-v1` profile](runtime/FubeData.md) is separate from the
default empty-state fixture above. It uses real Fabric TCP peer replication;
HTTP is only its client/observer interface.

```sh
cargo test --locked -p fube-data -p fube-runtime -p rust-runtime-app -p native-replicator-oracle
# Upstream build and packaging from clean source; use a NEW bundle directory:
cargo build --locked --workspace --bins --examples --all-features
python3 -m tests.onebox.shared.data --build-bundle build/data-binaries
# Compiler-free consumer; use unique allocation/report paths:
python3 -m tests.onebox.shared.data data-rust-01 --prebuilt build/data-binaries --profile rust --report build/data-rust-01.json --summary
python3 -m tests.onebox.shared.data data-full-01 --prebuilt build/data-binaries --profile full --report build/data-full-01.json --summary
```

| Profile | Complete scope |
|---|---|
| `rust` | 14 Rust-to-Rust data scenarios; Rust replicas on the five-node allocation. |
| `mixed` | 17 mixed-native data scenarios; mixed-native auxiliary data-loss topology. |
| `full` | 17 mixed-native data scenarios with constrained three-Rust auxiliary topology, then the hardened full empty-state/stateless legacy suite. |
| `native-data` | Exactly `full`'s 17 data scenarios and auxiliary policy, **without legacy**; not a full-profile claim. |

Dependent scenarios stay sequential within a profile. `mixed` is not simply
`full` minus legacy: its data-loss auxiliary policy differs. `--summary` bounds
console output only; the complete structured report is still written.
The version-2 strict data bundle includes exact executable, package, launcher,
source and dependency metadata. It is not interchangeable with the version-3
empty-state bundle. Missing/tampered inputs fail before allocation; qualification
never invokes Cargo. CI builds the native-backed oracle upstream. Linux
quality checks run independently; standalone acceptance consumes the same producer.

Only the named oracle loads native SF libraries; `fube-data`, `fube-runtime` and
the Rust app do not. Packaging validates exact fixed-name regular non-symlink
files and hashes. Already-executable files run under actual SF application
credentials without ownership/mode changes; adding needed owner-execute is
restricted to an exact owned artifact. There is no RunAs, ACL or trust fallback.
Each run retains immutable original allocation and minted deployment ownership,
verifies absence independently, and reports cleanup errors instead of adopting
same-name replacements. `--existing-owner` explicitly retains the caller's
allocation; its caller must perform final original-capability teardown.

## Parallel onebox CI

The [main workflow](../.github/workflows/ci.yaml) runs two non-fail-fast workers
on **different hosted runners**. They never share a mutable cluster, daemon,
application or port namespace:

- **client-runtime**: all five archived native-client tests, complete Rust14
  data, then the existing hardened full legacy empty-state/stateless suite.
- **native-data**: the complete baseline 17-case mixed-native data chain,
  including both replication directions, held copy-ACK negatives, no-write
  moves, stale-frame ingress, failures and the original full-data auxiliary
  policy. No dependent scenario is sliced across workers.

The existing **Linux onebox native client** check name now belongs to the strict
aggregate: **Rust14 + native-data17 + hardened legacy1 =32 namespaced profile
assertions**, plus five archived native tests. A partial report cannot label
itself full. Native-client reports retain their actual heterogeneous schemas;
seven internal clear-client cases are not seven native tests.

The upstream producer publishes a current-attempt receipt outside the immutable
bundle. [`shared/ci.py`](../tests/onebox/shared/ci.py) verifies actual checkout/tree,
PR head versus nullable merge, run/attempt, tar/archive/manifests and executable
hashes before allocation. Each worker retains its immutable original creation
capability through partial failure, verifies logical app/type/upload absence,
then publishes a structured original container/volume/network absence receipt
**after teardown**. The aggregate revalidates report bytes and typed evidence;
missing/failed/skipped work or cleanup cannot become green.
The final aggregate also requires all independent Rust/Python checks and
standalone native acceptance to succeed.

Legacy dispatch reuses the strict outer/nested loaders and
`NativeDataDeploymentPolicy`, including both package versions. The direct
nested-bundle `tests.onebox.shared.runtime` CLI is not a substitute for this hardened
CI adapter. Use `--profile full` above for local combined qualification.
`python3 -m tests.onebox.shared.ci --help` describes the CI-only receipt commands.

Artifact names include run and attempt: `onebox-inputs-*`,
`onebox-worker-<partition>-*`, `onebox-aggregate-*`. Worker artifacts retain their
partition subdirectory so the aggregate can download all workers without a
second hardcoded partition list.
Each worker artifact includes full reports, `validated-input.json`,
`suite-outcomes.json`, `fixture.json`, `worker-proof.json`, post-teardown
`cleanup.json` and credential-safe diagnostics. Emergency cleanup, when needed,
adds a separate `emergency-cleanup.json`. No bootstrap, owner state, journals or
raw logs are required.
Do not mix attempts: after partial reruns/missing inputs, start a **fresh
complete workflow dispatch**, rather than reuse an earlier-attempt receipt.

[Measured run40781117241](https://github.com/youyuanwu/fube/actions/runs/40781117241)
passed at source1414adca with all32+5. Compared with successful sequential
[baseline40771194035](https://github.com/youyuanwu/fube/actions/runs/40771194035)
at4e92d27, identical app/observer/oracle ELF bytes qualified in both:
native-stage wall time **22m01s →12m59s**, including aggregate (**41.0% less**);
workflow API envelope **25m11s →15m22s** (**39.0% less**). This is one observed
comparison, not an SLO; setup/cache/scheduling/artifact and bounded-console
logging differences are included, not attributed solely to parallel execution.
