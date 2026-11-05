# Minimal local Hosting application

This is the explicit stateless, single ExeHost/SharedProcess fixture for
[owned local activation](../../../docs/cluster/HostingLocalActivation.md).
It links `fube-runtime`, not native FabricRuntime/FabricCommon. Its factory can
create an empty-endpoint stateless instance, but this registration-only Hosting
profile does not send RAP Open/Close or directly invoke callbacks. The separate
[production local RA/RAP owner](../../../docs/cluster/StatelessRaRap.md) now
authorizes actual lifecycle traffic on this same activated session.

Build and stage a trusted local package without changing the source templates:

```sh
cargo build --locked -p hosting-local-app
mkdir -p build/hosting-local-package/LocalService/Code
cp crates/samples/hosting-local-app/pkg/ApplicationManifest.xml build/hosting-local-package/
cp crates/samples/hosting-local-app/pkg/LocalService/ServiceManifest.xml build/hosting-local-package/LocalService/
cp target/debug/hosting-local-app build/hosting-local-package/LocalService/Code/
env -u LD_LIBRARY_PATH -u FabricConfigFileName \
  cargo test --locked -p hosting-local-app --test activation --test lifecycle -- --test-threads=1
```

Supply the staged directory to `fube_hosting::local::Activation::start`; see the
guide for retaining ownership on error, explicit H2 clock driving and cleanup.
There is no new deployed-node CLI. The integration tests perform the actual
launch themselves rather than relying on a previously staged package.

Default execution attaches/registers the declared `LocalStatelessType` and stays
resident. The `Arguments` manifest parameter supports explicit qualification
fault modes: `--exit-before-attach`, `--no-register`, `--delay-ms N` and
`--crash-after-ms N` and `--shutdown-after-ms N` (orderly Runtime/session detach,
process stays alive). These are fixture controls, not Hosting recovery policy.
`--open-delay-ms N`, `--block-dispose` and `--close-error-block-dispose` exercise
real callback/disposal timing, not an alternate lifecycle path. An actual
instance-owned `local-instance.resource` file lives under application work;
the bounded destructor barrier waits at most five seconds for
`local-dispose.release`. Tests continue driving while held and independently
observe the resource and zero log eviction. A hard exit is not a destructor
receipt; physical directory cleanup is checked separately.
The existing mixed `rust-runtime-app/pkg` is not a substitute for this profile.

## Stateful registration-only fixture

`pkg-stateful/` is a separate single-type volatile singleton package: target/
minimum 1/1 and explicit `HasPersistedState="false"`, without config/data packages.
Stage its two manifests with the same executable and set
`fube_hosting::local::Options::profile` to
`Profile::VolatileStatefulRegistration`. The default remains stateless.

Its default `--stateful-register` argument registers `LocalStatefulType` through
the ordinary runtime API and real actor-9 Hosting IPC. Registration emits
`LOCAL_STATEFUL_REGISTERED` only after successful completion. The stateful
factory emits `LOCAL_CALLBACK create_replica` and fails explicitly if called;
this profile must never create/open a replica. Local Node composition and
lifecycle sends reject; finite local leases and owned teardown remain real.
The existing activation/lifecycle test commands above include this profile's
registration, lookup, authority-loss, prelaunch rejection and lifecycle-gate
tests. No stateful primary, data or recovery behavior is claimed.

## Stateful Idle lifecycle fixture

Reuse `pkg-stateful/`, set `Options::profile` to
`Profile::VolatileStatefulLifecycle`, and compose the original local `Node`
before launch. Override the `Arguments` parameter with `--stateful-lifecycle`
(optionally followed by the existing callback/disposal fault mode). Register,
then authorize `Node::open_stateful` and `Node::close`; the registration-only
profile and its callback-failing factory remain unchanged.

The fixture owns a real service resource and loopback control listener through
the public stateful/custom-replicator interfaces. It only supports IdleSecondary
and cleanup-to-None, returns no serving endpoint, observes denied read/write
access, and explicitly rejects data, quorum, copy/build, configuration and Primary
controls. It is not a data-replicator implementation. Final service/control
disposal is independently observed before semantic Close and H2 release.
The existing lifecycle test command includes replay, disposal/close-error and
Opening/Open lease-loss qualification for this separate profile.

### Opt-in real-data Idle lifecycle

Use the same profile/package and set `Arguments` to
`--stateful-lifecycle --data-replicator`, optionally followed by the existing
callback/disposal fault mode. Without this flag the custom control above remains
unchanged; the registration-only manifest default never selects data.

The factory captures the runtime's normal `NativeIntegrationProvider`, and service
Open calls `NativeRuntimeIntegration::create_replicator` with the public partition
and an observed `MemoryProvider`. RAP uses the actual service-returned
`RuntimeReplicator`; there is no engine/test-hook binder or direct role setter.
IdleSecondary observes the real engine as non-ready and attempts two valid writes
through `IStateReplicator`. Both fail as NotPrimary without changing progress or
owning operations. No serving endpoint, Primary, configuration, copy/build,
FM/peer control or recovery capability is added.

The lifecycle target verifies the complete native-shaped replication endpoint,
actual listener reachability/release, replay and exact H2 use. Graceful Close,
close-error Abort and direct ForceAbort join actual provider/service disposal
before semantic completion. `--block-provider-dispose` holds the provider's
destructor for at most five seconds until `local-provider-dispose.release`
appears; its independently observed resource is `local-provider.resource`.
The existing `--block-dispose` holds the service separately. Lease loss during
construction/Open or Idle hard-fences the process and removes its engine socket,
without fabricating destructor receipts; owned physical cleanup removes the
remaining files. This is local native-free Idle engine evidence, not replicated
writes, native stateful interoperability or M2 completion.
