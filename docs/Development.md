# Development

Use the toolchain in `rust-toolchain.toml` and Python 3.11+. The workspace does
not require `protoc`, app packaging tools, or a sibling repository.

## Native-free checks

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked -p mssf-transport -p mssf-client -p mssf-test-host
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo doc --locked -p mssf-transport -p mssf-client -p mssf-test-host --no-deps
python3 -m unittest discover -s scripts -p 'test_*.py'
```

The live tests are ignored by ordinary `cargo test`. Offline model tests do not
establish native wire compatibility. Linux lifecycle tests skip on Windows.

## Standalone native runtime

Follow [native ABI prerequisites and commands](runtime/RustTestHost.md#22-prerequisites-and-commands):

```sh
python3 scripts/native_runtime_test.py prepare
python3 scripts/native_runtime_test.py bootstrap
python3 scripts/native_runtime_test.py close
python3 scripts/native_runtime_test.py abort
python3 scripts/native_runtime_test.py manifest-close
python3 scripts/native_runtime_test.py manifest-abort
```

The helper extracts SHA256-pinned libraries under ignored `build/`; it does not
install/start a node or cluster. The app still needs compatible OS ABI libraries.
Use `--dependency-directory PATH` for private ABI dependencies.
The separate [native workflow](../.github/workflows/native-runtime.yaml)
qualifies all five scenarios on Ubuntu 22.04.

## Live native client

Create a [onebox](UbuntuOnebox.md), install the pinned nextest version from
[the native-client workflow](../.github/workflows/native-client.yaml), and follow the
[client qualification instructions](transport/RustFabricClient.md).
CI builds the test archive once, then runs the explicitly owned fixture/setup
tests and `group(onebox)` against the real native gateway. The onebox job depends
only on the Linux native-free checks; Windows checks run independently and do
not delay or block it. The onebox job always attempts diagnostics and confirmed
teardown. Retained-data shutdown/restart
qualification runs in
[service-fabric-apps-rs](https://github.com/youyuanwu/service-fabric-apps-rs/blob/main/docs/UbuntuOnebox.md),
not in this job; offline lifecycle regression tests and local down/up commands
remain available here.

## Rust application runtime (Linux x86-64)

This is separate from the native-backed `stateless-test-app` and standalone
node-side test host. It deploys a normal ExeHost to real SF nodes:

```sh
cargo test --locked -p mssf-runtime -p rust-runtime-app -p mssf-transport
cargo clippy --locked -p mssf-runtime -p rust-runtime-app --all-targets -- -D warnings
cargo doc --locked -p mssf-runtime --no-deps
python3 -m unittest discover -s scripts -p 'test_*.py'
just onebox-build
python3 scripts/rust_runtime_test.py --build-bundle build/runtime-binaries
python3 scripts/rust_runtime_test.py runtime-check --prebuilt build/runtime-binaries --full --stateful three
```

Use new bundle/report paths and a unique allocation name. The last command
creates and destroys only its exact owned onebox, using the
existing lifecycle controller. It verifies native placement, independent
mssf-client resolution/RPC, exact pidfd-based application termination/replacement,
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

The explicit build step builds the app and independent observer with `--locked`
from clean source; native qualification always requires `--prebuilt`.
CI builds them in the Linux native-free job and transfers a tar bundle containing
both executables, both versioned package templates, their SHA256 hashes and the
dependency graph (bundle format 3, `linux-x86_64-volatile-empty-control-v1`). The onebox job
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
cargo test --locked -p fube-data -p mssf-runtime -p rust-runtime-app -p native-replicator-oracle
# Upstream build from clean source; use a NEW bundle directory:
python3 scripts/fube_data_test.py --build-bundle build/data-binaries
# Compiler-free consumer; use unique allocation/report paths:
python3 scripts/fube_data_test.py data-rust-01 --prebuilt build/data-binaries --profile rust --report build/data-rust-01.json
python3 scripts/fube_data_test.py data-full-01 --prebuilt build/data-binaries --profile full --report build/data-full-01.json
```

`rust` qualifies Rust-to-Rust data; `mixed` adds both native classic directions;
`full` additionally executes the preserved empty-state/stateless full suite.
The version-2 strict data bundle includes exact executable, package, launcher,
source and dependency metadata. It is not interchangeable with the version-3
empty-state bundle. Missing/tampered inputs fail before allocation; qualification
never invokes Cargo. CI builds the native-backed oracle upstream, then runs
archived native clients, Rust-only data and full mixed qualification in the
compiler-free onebox consumer. Linux/Windows portable checks and the separate
standalone native job remain independent.

Only the named oracle loads native SF libraries; `fube-data`, `mssf-runtime` and
the Rust app do not. Packaging validates exact fixed-name regular non-symlink
files and hashes. Already-executable files run under actual SF application
credentials without ownership/mode changes; adding needed owner-execute is
restricted to an exact owned artifact. There is no RunAs, ACL or trust fallback.
Each run retains immutable original allocation and minted deployment ownership,
verifies absence independently, and reports cleanup errors instead of adopting
same-name replacements. `--existing-owner` explicitly retains the caller's
allocation; its caller must perform final original-capability teardown.
