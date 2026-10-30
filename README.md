# fube

Linux-only experimental native-wire Service Fabric client, transport, application runtime,
and standalone test host in Rust.

| Component | Purpose |
| --- | --- |
| [`fube-client`](crates/libs/fube-client) | Native clear-TCP FabricClient queries, naming, health, resolution, and notifications; no native SF library dependency. |
| [`fube-transport`](crates/libs/fube-transport) | Bounded Fabric framing, serialization, and concurrent TCP messaging. |
| [`fube-test-host`](crates/libs/fube-test-host) | Node-side stateless test-host subset, including manifest-based SharedProcess activation. |
| [`fube-runtime`](crates/libs/fube-runtime) | Owned async stateless/stateful application runtime; real SF placement/reconfiguration without native SF libraries in the application. Linux x86-64 only. |
| [`fube-data`](crates/libs/fube-data) | Owned volatile classic V1 replication, explicit acknowledgments, copy streams and native Fabric TCP peers. |
| [`fube-ra-store`](crates/libs/fube-ra-store) | Bounded typed-record RA store seam with memory and bundled SQLite; no RA runtime/recovery integration. |
| [`fube-ra`](crates/libs/fube-ra) | Native reliability codecs, bounded stateless opening/removal and node registration notifications; trusted simulation, not a running RA. |
| [`rust-runtime-app`](crates/samples/rust-runtime-app) | Listener-backed pure-Rust ExeHost fixture deployed by real SF nodes. |
| [`stateless-test-app`](crates/samples/stateless-test-app) | Acceptance fixture using the **real native FabricRuntime** through published `mssf` bindings. |
| [`native-replicator-oracle`](crates/samples/native-replicator-oracle) | Isolated test-only native classic V1 reference peer; not a production dependency. |

The node-side `fube-test-host` is not an application runtime or a full SF node.
`fube-runtime` is a separate application-side component. The supported profile is
Linux x86-64, SF 11.8.121.4, trusted clear IPC, without TLS. Historical native
qualification covers its recorded source/profile only; the current breaking
SDK alignment has offline verification, **no new deployed qualification**. See the
[application runtime profile](docs/runtime/RustStatelessRuntime.md) and
[standalone host profile](docs/runtime/ManifestTestHost.md).

## Build and test

Install the pinned Rust toolchain and Python 3.11+. `just` is optional. No
Protocol Buffers compiler or sibling checkout is needed.
Non-Linux Rust builds and Python tooling entrypoints are rejected. Native
qualification additionally requires Linux x86-64.

```sh
cargo build --locked --workspace
cargo test --locked -p fube-transport -p fube-client -p fube-test-host
python3 -m tests.run --suite offline
```

[Development and test tiers](docs/Development.md) describe native-free tests,
standalone native-runtime acceptance, and live onebox qualification. CI runs
native-free tests on Linux, a separate Linux standalone-native job,
and two independent compiler-free Linux onebox workers with a strict
[complete-coverage aggregate](docs/Development.md#parallel-onebox-ci).

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
- [Fube Data: volatile classic V1 replication, provider contract and executed native profile](docs/runtime/FubeData.md)
- [Broader pure-Rust runtime design and capability tiers](docs/runtime/RustFabricRuntime.md)
- [Current SF/mssf API alignment: per-method shape, values, completion and limits](docs/runtime/SfApiAlignment.md)
- [Basic pure-Rust cluster design: FM, RA/Hosting, authority and qualification milestones](docs/cluster/PureRustCluster.md)
- [RA store P1: API, SQLite profile, cancellation and operational limits](docs/cluster/RaStore.md)
- [RA protocol foundation: catalog, native codec evidence and minimal P2 execution](docs/cluster/RaProtocolFoundation.md)
- [Native-derived RA stateless removal: close/report stages, tests and limits](docs/cluster/RaStatelessRemoval.md)
- [Native-derived RA stateless opening: retries, ownership and node notifications](docs/cluster/RaStatelessOpening.md)
- [Native-derived RA stateless replacement: Drop, FM resend and direct description helpers](docs/cluster/RaStatelessReplacement.md)
- [Migration provenance and repository boundaries](docs/Migration.md)

## License

MIT. The existing [fube license](LICENSE) is retained alongside the
[original imported-code notice](LICENSE.service-fabric-apps-rs).
