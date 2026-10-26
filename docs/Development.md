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
[CI](../.github/workflows/ci.yaml), and follow the
[client qualification instructions](transport/RustFabricClient.md).
CI builds the test archive once, then runs the explicitly owned fixture/setup
tests and `group(onebox)` against the real native gateway. The onebox job always
attempts diagnostics and confirmed teardown. Retained-data shutdown/restart
qualification runs in
[service-fabric-apps-rs](https://github.com/youyuanwu/service-fabric-apps-rs/blob/main/docs/UbuntuOnebox.md),
not in this job; offline lifecycle regression tests and local down/up commands
remain available here.

The shared lifecycle engine and its regression fixtures retain their historical
`linux_cluster*` names and `docker/linux-cluster` paths. They are support for
onebox, not a separately shipped multi-container image or CI topology.
