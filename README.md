# Service Fabric Rust Apps
Various service fabric rust apps.

# Apps and libs
* [kvmap](crates\samples\kvmap) an key value store using SF default replicator. Still work in progress.
* [kvstore](crates\samples\kvstore) app using default SF key value store implementation in FabricRuntime.
* [ext](crates\libs\mssf-ext) lib that extends mssf-core crate with various support including default replicator.
* [Native runtime test host](docs/runtime/RustTestHost.md#2-implemented-mvp-and-usage) runs a real stateless FabricRuntime app without a cluster (experimental Linux loopback profile).

# Build
Install Rust (using the toolchain pinned in `rust-toolchain.toml`), [just](https://just.systems), Python 3, and Protocol Buffers (`protoc`).

Service Fabric dependencies use the published `mssf` 0.10 release family.
Tokio-based Service Fabric executors require a multi-threaded Tokio runtime.

```ps1
just build
```

`just` also runs the build by default. It compiles the workspace and packages `kvmap`
under `build/kvmap_root`, ready for the deployment script below.
Use `just build-rust` to compile without packaging, or `just package` to repackage
an existing debug build. Packaging uses only the Python standard library.

# Run Example on SF-onebox
```ps1
.\scripts\kvmap_ctl.ps1 -Action Add
```

# Documentation
* [Runtime test tiers and CI](docs/runtime/RustTestHost.md#ci-test-tiers-and-diagnostics) - native-free regressions, standalone Linux native-library acceptance without a node, and full cluster tests.
* [FabricRuntime architecture](docs/runtime/FabricRuntime.md), [node-side RA/Hosting](docs/runtime/FabricRuntimeHost.md), and the separate [pure Rust runtime design](docs/runtime/RustFabricRuntime.md).
* [Ubuntu single-container onebox](docs/UbuntuOnebox.md) - five local nodes in one container, with retained lifecycle and localhost HTTP/native client gateways.
* [Linux container-cluster lifecycle](docs/LinuxContainerCluster.md) - build, create, resume, inspect, stop, and explicitly destroy a local three-node cluster.
* [Joining an existing Windows Service Fabric cluster](docs/WindowsClusterJoin.md) - standalone PowerShell workflow, source trace, security, and operational caveats.
* [Linux multi-container cluster design](docs/LinuxContainerClusterDesign.md) - one node per Docker container, Windows orchestration adaptation, and dynamic-join validation gates.

# License
MIT
