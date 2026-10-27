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
cargo test --locked -p mssf-runtime -p mssf-transport
cargo clippy --locked -p mssf-runtime -p rust-runtime-app --all-targets -- -D warnings
cargo doc --locked -p mssf-runtime --no-deps
python3 -m unittest discover -s scripts -p 'test_rust_runtime_test.py'
just onebox-build
python3 scripts/rust_runtime_test.py runtime-check --full
```

The last command creates and destroys only its exact owned onebox, using the
existing lifecycle controller. It verifies native placement, independent
mssf-client resolution/RPC, exact pidfd-based application termination/replacement,
native lifecycle/query/update/failure traffic, concurrent shared-process
partitions and bounded fencing of an intentionally non-cooperative application.
It requires Linux pidfd APIs and the existing privileged development container
for the exact-owned-IPC shutdown test; unavailable capabilities fail qualification.
There is no security fallback.

Local qualification builds the app and independent observer with `--locked`.
CI builds them in the Linux native-free job and transfers a tar bundle containing
both executables, their SHA256 hashes and the dependency graph. The onebox job
uses `--prebuilt DIRECTORY`: it verifies the bundle and ELF dependencies, then
deploys/tests without installing Rust or invoking Cargo. Missing or changed
artifacts fail; there is no compilation fallback. See the
[separate build/run commands](runtime/RustStatelessRuntime.md#reproduce-native-acceptance).

Use `--report NEW_FILE` for a new structured evidence file. To reuse an already
owned healthy allocation, explicitly supply `--existing-owner OWNER`; the
harness cleans up only its minted application/type, never the caller's box.
Failed or ambiguous cleanup is reported with exact recovery identities.
See [the supported runtime profile](runtime/RustStatelessRuntime.md) before
interpreting a test result. Delayed/stale commands and lease-expiry races also
have offline model tests; native IPC-loss proof is not a node-failure/TLS claim.

The shared lifecycle engine and its regression fixtures retain their historical
`linux_cluster*` names and `docker/linux-cluster` paths. They are support for
onebox, not a separately shipped multi-container image or CI topology.
