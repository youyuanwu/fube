# Owned local Hosting activation

**Status:** bounded H4 local package/process activation is implemented. This is
not a deployed Rust node, production RA/RAP service lifecycle or serving-readiness
qualification. Native source semantics use Service Fabric
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; release-native regression uses the
separate Linux x86-64 SF **11.8.121.4** profile, not binaries asserted built from
that source. See [Hosting design](RustHosting.md) and
[registration/events](HostingRegistrationEvents.md) for the adjacent boundaries.

## Profile and package

The public [`local::Activation`](../../crates/libs/fube-hosting/src/local.rs#L92-L185)
owns one trusted local application, one SharedProcess service package, one
stateless singleton service/type with InstanceCount=1, and one ExeHost code
package. Linux trusted numeric-loopback/no-TLS IPC is required. There is no
cross-owner placement catalog: callers must not activate competing hosts for the
same native application/package/type/activation key. Multiple owners are not
permission to violate that placement limit.

The supported package is an ordinary caller-supplied application directory:

```text
package/
  ApplicationManifest.xml
  LocalService/
    ServiceManifest.xml
    Code/
      hosting-local-app
```

The [explicit minimal fixture](../../crates/samples/hosting-local-app/README.md)
uses `fube-runtime` and no native SF libraries. Its package templates are separate
from `rust-runtime-app/pkg`: the latter has stateful/config/data and other
unsupported content and is rejected, never silently filtered.

The [shared validator](../../crates/libs/fube-local-host/src/manifest.rs#L199-L446)
retains the [manifest profile](../runtime/ManifestTestHost.md#exact-supported-xml-subset):
allowlisted XML/attributes/namespaces, bounded literal parameters and whitespace
argv, Work or CodePackage working folder, optional fixed internal TCP endpoint
metadata, safe relative identifiers, executable regular Program and no code-tree
symlinks/nonregular files. Config/data, stateful, multiple imports/code packages,
exclusive activation, setup/guest/DLL/container, unsupported policies/topology,
ImageStore downloads and resource governance are not enabled. Endpoint metadata
does not reserve ports. Inputs must stay trusted and immutable through copy;
this is not a hostile filesystem or malicious-same-user sandbox.

## Ownership and usage

Construction validates options/capabilities without filesystem/process side
effects. `start` validates before reserving a unique root, materializes matching
native-shaped deployment files, records fresh host GUID/code and package
sequences/version instance, and launches the exact executable with cleared
environment. Shared mechanics live in `fube-local-host`; Hosting still forbids
unsafe code and has no production dependency on test-host, Runtime, Node or RA.
Native preflight/environment/scenarios remain test-host adapters
([launch](../../crates/libs/fube-hosting/src/local.rs#L186-L355),
[shared execve](../../crates/libs/fube-local-host/src/process.rs#L127-L243),
[materialization](../../crates/libs/fube-local-host/src/deployment.rs#L26-L153)).
Implicit ENOEXEC shell fallback is disabled; explicit trusted shebang
interpreters remain ordinary executable behavior.

**Keep the owner even on error.** `start`, registration observation and
`terminate` borrow it; failures never consume it. This example returns both owner
and observation, including partial launch failure:

```rust,ignore
fn start_local(
    options: fube_hosting::local::Options,
    package: &std::path::Path,
) -> Result<(fube_hosting::local::Activation,
             Result<(), fube_hosting::local::LocalError>),
            fube_hosting::local::LocalError> {
    let mut owner = fube_hosting::local::Activation::new(options)?;
    let observed = owner.start(package, "fabric:/Local", &[])
        .and_then(|()| owner.wait_registration(std::time::Duration::from_secs(5)));
    Ok((owner, observed))
}
```

`ObservationTimeout` is not command cancellation or rollback. The process,
admission and root remain accessible for continued observation or termination.
An actual configured registration deadline/failure revokes authority and retains
cleanup ownership. Do not return an error from your wrapper while inadvertently
dropping the owner. Drop is best effort, not a successful cleanup receipt or
durable recovery protocol
([observation](../../crates/libs/fube-hosting/src/local.rs#L384-L478),
[cleanup](../../crates/libs/fube-hosting/src/local.rs#L551-L659)).

The dedicated IPC listener withholds acceptance/dispatch until actual spawned
PID and complete package context are admitted. Host/runtime/type completion must
match that PID, package sequence, runtime and claimed single session. A second
connection cannot adopt it. These are trusted-loopback claims, **not OS peer
authentication**. Registration never authorizes service Open. Only future
production RA `RuntimeOpen`/RAP integration can do that; no new direct factory,
Open, Close or Abort calls exist in Hosting
([barrier](../../crates/libs/fube-hosting/src/ipc.rs#L583-L692),
[completion](../../crates/libs/fube-hosting/src/local.rs#L403-L461)).

After registration, `acquire_usage` returns H2's complete `UsageIncrement`,
including any exact unresolved owner. Retain the nonclone acquisition and release
it through `release_usage`; dropping it does not decrement. Known terminal
process/session failure rejects new acquisitions without erasing previously
accepted owners; those must still be released. Supply monotonic 100ns observations
to `drive_deactivation` and periodic `scan_unused` explicitly.
There is no background scheduler. Admission starts unused without a timer;
last usage release schedules package idle teardown, and unused scans select the
application first. Current H2 sequence/usage guards, not each service Close reply,
decide idle teardown
([usage/driver](../../crates/libs/fube-hosting/src/local.rs#L480-L550)).

## Cleanup boundaries

`process_status`, `session_status`, `cleanup_status`, `residency`, `root`, `logs`
and `take_outputs` expose separate observations. `terminate` explicitly requests
teardown; `drive_deactivation` executes only admitted H2 final work. Both use the
same retained cleanup path:

1. Revoke lease authority and stop/join the dedicated IPC service.
2. Terminate/drain the owned group, preserving the waitable leader before any
   numeric PGID signal, then reap the direct child and join bounded log readers.
3. Check logical host/runtime retirement; failed logical cleanup remains an
   error, not assumed successful because the process died.
4. Remove exact declaration sources and release the exact admission.
5. Complete only current zero-usage H2 final work using exact cleaned premises.
   Package-first completion separately schedules application grace;
   application-first completion requires the whole owned package cascade.
6. Release the exclusive run directory only after those boundaries are known.

Explicit termination can kill/reap a process with live H2 usage, but returns
`Busy` and retains acquisition accounting and directory until the original
acquisition is released and cleanup retried. Directory failure retains the path.
Physical failure leaves H2 finals/entries intact. `finish_local_deactivation` is
an explicit trusted owner's completion premise, not H2 independently proving OS
cleanup. Failed/foreign/stale/duplicate completion cannot erase a current owner
([cleanup implementation](../../crates/libs/fube-hosting/src/local.rs#L559-L650),
[H2 completion](../../crates/libs/fube-hosting/src/residency.rs#L1357-L1599),
[process reservation/drain/reap](../../crates/libs/fube-local-host/src/process.rs#L302-L431)).

`take_outputs` transfers bounded local registration/status/obligation/operation
records. Transfer is not downstream acknowledgment; this profile has no live
RA/FM/HM consumer. Required health/application reporting, FM-status completion
or deployed lifecycle reject at construction. Process termination is not
runtime/host unregister traffic, nor does an empty output lane mean serving
readiness. No native recovery or RA retry/incarnation policy is introduced.

## Budgets and limits

| Boundary | Bound |
|---|---|
| Registration observation / configured registration | Positive, at most 30s each; observer timeout does not retract launch |
| Startup dispatch barrier | 5s after listener creation |
| Runtime local lease | 2–30s, default 30s; Runtime polls every 1s |
| Physical process cleanup | Positive, at most 10s per attempt; IPC stop/join is a separate step |
| Launch argv/environment | At most 128 arguments plus argv0, 64 explicit environment values, 128KiB total |
| Logs | 4096 retained records / 1MiB; 16KiB line chunks, eviction counter; stopped readers drain at most another 1MiB each |
| XML / code | 1MiB per manifest; 4096 tree entries / depth64 / 512MiB |
| IPC | One admission/session, 5MiB logical scratch, 64KiB frames/replies, 256KiB decode/projection; 16 operation results |
| H2/core outputs | Finite existing default count/byte budgets; explicit capacity errors, no silent lost work |

These are logical payload/operation budgets, not RSS or real-time scheduling
guarantees. Mutations serialize through `&mut Activation`; there is no unbounded
activation command queue. Caller-owned output/acquisition storage needs its own
bounds. See [options](../../crates/libs/fube-hosting/src/local.rs#L51-L158),
[supervisor](../../crates/libs/fube-local-host/src/process.rs#L18-L206) and
[H2 limits](../../crates/libs/fube-hosting/src/residency.rs#L75-L119).

Group cleanup does not guarantee escaped descendants, helper-zombie adoption,
privilege-changing applications or abrupt owner/machine-death recovery.
Parent-death SIGKILL protects the direct child only. Directory cleanup assumes
trusted callers do not replace/move the owned root concurrently.

## Verification and qualification boundary

From the repository root:

```sh
env -u LD_LIBRARY_PATH -u FabricConfigFileName \
  cargo test --locked -p hosting-local-app --test activation -- --test-threads=1
cargo test --locked -p fube-local-host -p fube-test-host -p fube-hosting
cargo test --locked -p fube-node
cargo test --locked -p fube-ra --test hosting_events
cargo test --locked -p fube-runtime --lib
```

The [real fixture tests](../../crates/samples/hosting-local-app/tests/activation.rs#L84-L387)
run actual owned package/process/IPC registration, independently inspect native
library absence and verify reap/root absence. They cover fresh identity,
observation timeout, registration failure, launch error, crash, live lease
renewal, H2 package/application teardown, Busy ownership and directory retry.
[H2 completion tests](../../crates/libs/fube-hosting/tests/residency_completion.rs#L1-L254)
and [physical-failure tests](../../crates/libs/fube-hosting/src/local.rs#L661-L758)
cover retained finals and acquisition guards. Production capability gates stay
closed. No successful Rust service callback execution is claimed by this
registration-only fixture.

Run the seven [separate native regressions](../Development.md#standalone-native-runtime)
from fresh clean-source binaries after shared-mechanics changes. Bootstrap,
Close, Abort, manifest Close/Abort, leases and registration have distinct
unregister/forced-cleanup evidence; none substitutes for this native-free
package owner, production RA/RAP or deployed service readiness.
