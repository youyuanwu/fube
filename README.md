# fube

Experimental native-wire Service Fabric client, transport, application runtime,
and standalone test host in Rust.

| Component | Purpose |
| --- | --- |
| [`mssf-client`](crates/libs/mssf-client) | Native clear-TCP FabricClient queries, naming, health, resolution, and notifications; no native SF library dependency. |
| [`mssf-transport`](crates/libs/mssf-transport) | Bounded Fabric framing, serialization, and concurrent TCP messaging. |
| [`mssf-test-host`](crates/libs/mssf-test-host) | Node-side stateless test-host subset, including manifest-based SharedProcess activation. |
| [`mssf-runtime`](crates/libs/mssf-runtime) | Owned async stateless application runtime; real SF placement/restart without native SF libraries in the application. Linux x86-64 only. |
| [`rust-runtime-app`](crates/samples/rust-runtime-app) | Listener-backed pure-Rust ExeHost fixture deployed by real SF nodes. |
| [`stateless-test-app`](crates/samples/stateless-test-app) | Acceptance fixture using the **real native FabricRuntime** through published `mssf` bindings. |

The node-side `mssf-test-host` is not an application runtime or a full SF node.
`mssf-runtime` is a separate application-side component. Native qualification is
Linux x86-64, SF 11.8.121.4, trusted clear IPC, without TLS. See the
[application runtime profile](docs/runtime/RustStatelessRuntime.md) and
[standalone host profile](docs/runtime/ManifestTestHost.md).

## Build and test

Install the pinned Rust toolchain and Python 3.11+. `just` is optional. No
Protocol Buffers compiler or sibling checkout is needed.

```sh
cargo build --locked --workspace # Linux; runtime profile is Linux-only
cargo test --locked -p mssf-transport -p mssf-client -p mssf-test-host
python3 -m unittest discover -s scripts -p 'test_*.py'
```

[Development and test tiers](docs/Development.md) describe native-free tests,
standalone native-runtime acceptance, and live onebox qualification. CI runs
native-free tests on Linux and Windows, a separate Linux standalone-native job,
and a Linux onebox native-client/runtime job. On Windows, build the existing
client/transport/test-host packages explicitly; neither native runtime profile
is qualified there.

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
- [Rust stateless application runtime: API, limits and native evidence](docs/runtime/RustStatelessRuntime.md)
- [Rust volatile empty-state stateful runtime: control-plane semantics and native evidence](docs/runtime/RustStatefulRuntime.md)
- [Broader pure-Rust runtime design and capability tiers](docs/runtime/RustFabricRuntime.md)
- [Migration provenance and repository boundaries](docs/Migration.md)

## License

MIT. The existing [fube license](LICENSE) is retained alongside the
[original imported-code notice](LICENSE.service-fabric-apps-rs).
