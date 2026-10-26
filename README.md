# fube

Experimental native-wire Service Fabric client, transport, and standalone test
host in Rust.

| Component | Purpose |
| --- | --- |
| [`mssf-client`](crates/libs/mssf-client) | Native clear-TCP FabricClient queries, naming, health, resolution, and notifications; no native SF library dependency. |
| [`mssf-transport`](crates/libs/mssf-transport) | Bounded Fabric framing, serialization, and concurrent TCP messaging. |
| [`mssf-test-host`](crates/libs/mssf-test-host) | Node-side stateless test-host subset, including manifest-based SharedProcess activation. |
| [`stateless-test-app`](crates/samples/stateless-test-app) | Acceptance fixture using the **real native FabricRuntime** through published `mssf` bindings. |

The host is not a pure-Rust application runtime or a full Service Fabric node.
Current native qualification is Linux x86-64, SF 11.8.121.4, trusted loopback,
without TLS. See the [exact supported manifest profile](docs/runtime/ManifestTestHost.md).

## Build and test

Install the pinned Rust toolchain and Python 3.11+. `just` is optional. No
Protocol Buffers compiler or sibling checkout is needed.

```sh
cargo build --locked --workspace
cargo test --locked -p mssf-transport -p mssf-client -p mssf-test-host
python3 -m unittest discover -s scripts -p 'test_*.py'
```

[Development and test tiers](docs/Development.md) describe native-free tests,
standalone native-runtime acceptance, and live onebox qualification. CI runs
native-free tests on Linux and Windows, a separate Linux standalone-native job,
and a Linux onebox lifecycle/client job. Windows does not qualify the native host.

## Simple Linux onebox

Use a local Linux x86-64 Docker daemon with Compose v2+, Python, and iproute2:

```sh
just onebox-build
just onebox create dev
just onebox status dev
just onebox down dev
just onebox up dev
just onebox destroy dev --confirm dev
```

Without `just`, build with
`docker build -t sf-apps-onebox:11.8.121.4 -f docker/onebox/Dockerfile docker`
and run `python3 scripts/onebox_ctl.py ...`.

This is one privileged Ubuntu container with five local SF nodes, retained data,
and **unsecured loopback-only** gateways at `127.0.0.1:19000` (native TCP) and
`http://127.0.0.1:19080` (HTTP/Explorer). No app is implicitly deployed.
See [onebox prerequisites, lifecycle, and recovery](docs/UbuntuOnebox.md).

## Documentation

- [Rust FabricClient API and native qualification](docs/transport/RustFabricClient.md)
- [Native transport/TCP/TLS specification](docs/transport/Transport.md)
- [FabricClient behavior](docs/transport/FabricClient.md) and [message formats](docs/transport/FabricClientMessageFormat.md)
- [Native FabricRuntime](docs/runtime/FabricRuntime.md) and [node-side host responsibilities](docs/runtime/FabricRuntimeHost.md)
- [Standalone test host](docs/runtime/RustTestHost.md) and [manifest execution](docs/runtime/ManifestTestHost.md)
- [Pure-Rust application runtime design](docs/runtime/RustFabricRuntime.md) (not implemented)
- [Migration provenance and repository boundaries](docs/Migration.md)

## License

MIT. The existing [fube license](LICENSE) is retained alongside the
[original imported-code notice](LICENSE.service-fabric-apps-rs).
