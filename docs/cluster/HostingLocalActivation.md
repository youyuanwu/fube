# Owned local Hosting activation

**Status:** bounded H4 local package/process activation is implemented. This is
not a deployed Rust node or serving-readiness qualification. The separate
[production local RA/RAP composition](StatelessRaRap.md) drives actual service
lifecycle through this same owned session. Native source semantics use Service Fabric
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; release-native regression uses the
separate Linux x86-64 SF **11.8.121.4** profile, not binaries asserted built from
that source. See [Hosting design](RustHosting.md) and
[registration/events](HostingRegistrationEvents.md) for the adjacent boundaries.

## Profile and package

The public [`local::Activation`](../../crates/libs/fube-hosting/src/local.rs#L92-L185)
defaults to one trusted local application, one SharedProcess service package, one
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
symlinks/nonregular files. Config/data, stateful replica lifecycle, multiple imports/code packages,
exclusive activation, setup/guest/DLL/container, unsupported policies/topology,
ImageStore downloads and resource governance are not enabled. Endpoint metadata
does not reserve ports. Inputs must stay trusted and immutable through copy;
this is not a hostile filesystem or malicious-same-user sandbox.

The existing run parent must resolve outside the source Code directory.
Canonical path checks reject equal or nested paths, including symlink aliases,
before reserving a root, and recheck separation before materialization.

### Opt-in volatile stateful registration

Set `Options::profile` to
`fube_hosting::local::Profile::VolatileStatefulRegistration` before constructing
the owner. This admits exactly one `StatefulService` with `SingletonPartition`,
explicit target/minimum **1/1**, and one matching `StatefulServiceType` with
explicit `HasPersistedState="false"`. The remaining package, host/security and
process ownership restrictions above are unchanged. The default profile and
native test-host `Package::load` remain stateless-only; this is not automatic
profile detection or mixed-package admission.

Materialization preserves the stateful declaration in both the service manifest
and digested service-package XML. The real application attaches normally and
registers its stateful factory through actor 9 on the exact owned PID/package/
runtime/session. Lookup is a registration fact, not replica readiness. Native
type-registration messages carry the type identity/context, not a new
stateful-kind field; no wire schema is added.

The [separate minimal fixture](../../crates/samples/hosting-local-app/pkg-stateful)
registers `LocalStatefulType`; its `create_replica` records an observation and
returns an explicit unsupported-profile error if unexpectedly invoked.
Registration never calls it. This profile rejects the stateless local Node
composition and lifecycle sends on its returned `HostLease`, before any RAP
Open/Close is submitted. Lease renewal/withdrawal and explicit owned cleanup
remain available. No stateful RA/RAP lifecycle, primary role, application data,
config/data packages, replication or recovery is enabled.

The native-free `hosting-local-app` activation tests execute actual registration,
same-runtime lookup and renewal across multiple finite lease intervals, reject
undeclared registration and persisted admission, verify no replica callback, and
independently check reaping/group drainage/root removal. A separate authority
withdrawal test observes terminal expiry and cleanup. These are not deployed
native stateful qualification or M2 completion.

### Opt-in volatile stateful local lifecycle

`Profile::VolatileStatefulLifecycle` admits the same strict package, but requires
an original consumer gate before launch. Compose an unstarted `Activation` with
`fube_node::local::Node::new`, start/register it, then call `open_stateful` with
the matching package/service/version/node identity, a valid current epoch,
native Idle role **2**, InBuild state **1**, and target/minimum **1/1**.
This is trusted local authorization, not an implemented FM or peer request.
`add`, `add_from_fm` and `fm_work` do not enable stateful network control.

The original committed RA lookup/acquisition and `RuntimeOpen` effect drive
actor-12 `ReplicaOpen` on the admitted Hosting session. Existing application RAP
creates the replica, opens its service-returned public replicator control, and
selects IdleSecondary. The semantic one-way reply completes Open after the state update
commits. The RA's internal `Phase::Ready` means this Open completed; the native
replica stays **Idle/InBuild**, with no serving endpoint or write authority.
Only a local `LocalStatefulOpenReply` observation is produced, never a fabricated
`AddInstanceReply`, `AddPrimaryReply` or `CreateReplicaReply` network send.

Ordinary `close` drives the same committed actor-12 Close path. Runtime control
and service cleanup/final disposal precede semantic completion; the original H2
acquisition is released exactly once. Drop/Abort remain bounded cleanup options.
Duplicate/stale traffic cannot recreate the lifetime or release usage twice.
Replacement, reopen/adoption, primary/configuration/build/data-loss work and
persisted replicas reject. Finite leases, original store uncertainty fences and
independently verified process/group/reap/directory cleanup remain unchanged.
Local terminal state retains a typed `StatefulClosePending` reporting obligation
in `external_pending`, not a stateless `ReplicaUp` send or native FM/peer
completion. Those network handlers/choreography remain gated P4 work, not
synthesized ACKs; local quiescence/disposal does not acknowledge that escrow.

Native semantic authority remains pin
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`:
`Reliability/Failover/ra/FailoverUnit.cpp:5250-5330` selects InBuild for Idle Open;
`ReconfigurationAgent.cpp:883-930` distinguishes stateless, Primary and peer
completion; `FailoverUnit.cpp:5472-5551` also owns terminal reporting. This local
profile reuses the existing proxy schemas/runtime, not
those network processors or a new wire protocol. The default lifecycle fixture
implements public SF-style stateful/control interfaces with a real owned loopback listener and
resource file; all data, quorum, copy/build and primary-only controls explicitly
fail. It is not a replication engine or M2 qualification.

The lifecycle tests observe native-free real Open/Close, public access denial,
control-listener release, a held service destructor, close-error Abort/disposal,
duplicate/stale traffic, original usage, lease loss during Opening/Open, and
physical cleanup. RA model tests separately hold actual commits and reject
foreign/Primary completions. Registration-only tests still prove no callback
and rejected lifecycle sends.

#### Opt-in real-data Idle lifecycle

The same package/profile also supports the explicit fixture argument
`--stateful-lifecycle --data-replicator`. It captures the normal runtime integration
at factory creation and constructs the existing `fube-data` engine during service
Open through `NativeRuntimeIntegration::create_replicator`, with the public
partition and a minimal observed in-memory provider. RAP invokes the actual
service-returned `RuntimeReplicator`; neither RA nor Hosting links application
objects or obtains private data-engine control.

The replica remains Idle/InBuild with no serving endpoint. Two valid public
`IStateReplicator` writes fail as NotPrimary and leave the engine's sequence
progress and owned-operation budgets unchanged. Tests observe the real engine's
Idle/non-ready state, exact native-shaped replication endpoint and actual
listener, duplicate/stale traffic and one original H2 acquisition.
Graceful Close, close-error Abort and direct ForceAbort release the listener and
join provider/service disposal before semantic Close and exact H2 release.
Separate held provider and service destructors retain the original Closing/use
obligation; the application remains locally leased while held.

Opening/Open lease-loss tests construct the actual engine before withdrawing
renewal. They independently observe its socket disappear and the waitable process
exit, without asserting a graceful provider/service destructor receipt. Physical
process-group drainage, reap and root removal remain separate checks.
The custom-control and registration-only modes stay unchanged. This adds no
Primary/configuration/build/copy, acknowledged data, FM/peer choreography,
recovery or native stateful/M2 qualification.

### Opt-in initial singleton Primary lifecycle

`Profile::VolatileStatefulPrimaryLifecycle` is a separate trusted-local profile
with the same strict volatile stateful 1/1 package constraints and mandatory
original-consumer-before-launch gate. The fixture argument `--stateful-primary`
selects normal real-data construction. `Node::open_stateful_primary` authorizes
one fresh Primary/InBuild lifetime with an invalid/absent previous configuration,
not promotion of an existing Idle replica. Registration-only and Idle admission
remain unchanged; FM AddPrimary, peer control, replacement, reopen/adoption,
configuration changes, persisted replicas and recovery still reject.

The original committed lookup/acquisition and `RuntimeOpen` drive the existing
RAP initial-Primary sequence through the service-returned public replicator.
At native source pin `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`,
`Reliability/Failover/ra/ProxyActionsList.cpp:44-54` defines OpenPartition,
ReconfigurationStarting, OpenReplica, OpenReplicator, ChangeReplicatorRole,
ChangeReplicaRole, ReportAnyDataLoss and ReconfigurationEnding.
`ReconfigurationAgentProxy.cpp:300-449` selects that distinct New-mode path;
`FailoverUnit.cpp:5250-5330` makes the successful initial Primary Ready.
No blanket UpdateConfiguration, direct engine role setter or fabricated
data-loss completion substitutes for that sequence. This local profile rejects
state-changed-on-data-loss completion instead of enabling its upstream protocol.

RA accepts only matching original identity, role, epoch, native-shaped InBuild
reply and nonempty service/replication endpoints, then commits Primary/Ready
before publishing its local `LocalStatefulOpenReply`. This is not a native
AddPrimaryReply or FM acknowledgment. Model tests hold real commits, veto the
initial insert, reject foreign/late/invalid completions and verify exact usage
release after committed Close.

The fixture owns an actual joined numeric-loopback application listener.
Public writes before service role completion fail as ReconfigurationPending
without sequence consumption. Application requests additionally require runtime,
service, replicator and engine Primary roles, matching epoch, readiness and
public partition access. An independent client submits one fixed nonzero public
`IStateReplicator` mutation; the application applies the admitted sequence to its
provider before waiting for the separate quorum result. The client verifies
assigned/applied/committed progress and reads back the value. This is singleton
volatile acknowledgment, not a replicated secondary ACK or durability claim.

Real-process tests hold provider and service disposal independently, observe
access denial during retirement, join both listeners/resources before semantic
Close and exact H2 release, and retain `StatefulClosePending` external reporting.
Duplicate/stale traffic cannot reopen or resurrect endpoints. Opening/serving
lease loss independently checks both sockets and the waitable process exit,
then owned group drainage/reap/root removal; hard fencing does not fabricate
provider/service destructor receipts. Request-ledger retry/ambiguity semantics,
mixed/config/data packages, FM/peer choreography, copy/build, recovery and native
stateful/P4/M2 qualification remain outside this slice.

### FM-authorized fresh Primary

`Profile::VolatileStatefulFmPrimary` separately admits the same strict volatile
1/1 package and original-consumer-before-launch gate. Local
`open_stateful_primary` does not authorize this profile; only
`add_primary_from_fm` accepts its matched fixed FM source/generation and issues
the distinct RA `AddPrimary` command. Registration-only, Idle and local Primary
profiles still reject FM Primary control.

The [minimal FM cluster](MinimalFmCluster.md#initial-stateful-fm-control) now
drives this path over actual native-shaped `AddPrimary`/`AddPrimaryReply` frames.
The RA retains original network reply responsibility across registration, Open,
retry and unknown store completion; a local observation is never serialized as
an FM reply. Native initial deactivation info is `(current configuration epoch,
0)`. Matching Ready duplicates reply from the retained FT without recreating
the lifetime or replacing its epoch. Closed/stale traffic retains typed dropped
reporting rather than synthesizing an ACK. Stateful deletion/reconfiguration,
replacement/reopen, FMM and recovery remain gated.

Real foreground/channel tests exercise exact semantic completion, a lost reply
and original-lifetime retry, public acknowledged write/readback and physical
cleanup. A separate real-process lease-loss test observes hard fencing and
retained terminal reporting without claiming graceful destructor receipts.

## Ownership and usage

Construction validates options/capabilities without filesystem/process side
effects. `start` validates before reserving a unique root, materializes matching
native-shaped deployment files, records fresh host GUID/code and package
sequences/version instance, and launches the exact executable with cleared
environment. Shared mechanics live in `fube-local-host`; Hosting still forbids
unsafe code and has no production dependency on test-host, Runtime, Node or RA.
Native preflight/environment/scenarios remain test-host adapters
([launch](../../crates/libs/fube-hosting/src/local.rs#L200-L372),
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
([observation](../../crates/libs/fube-hosting/src/local.rs#L444-L539),
[cleanup](../../crates/libs/fube-hosting/src/local.rs#L610-L712)).

The dedicated IPC listener withholds acceptance/dispatch until actual spawned
PID and complete package context are admitted. Host/runtime/type completion must
match that PID, package sequence, runtime and claimed single session. A second
connection cannot adopt it. These are trusted-loopback claims, **not OS peer
authentication**. Registration never authorizes service Open. Only the
[production local owner's](StatelessRaRap.md) committed RA `RuntimeOpen` permits
the actual RAP request; no direct factory,
Open, Close or Abort calls exist in Hosting
([suspended listener](../../crates/libs/fube-hosting/src/ipc.rs#L730-L735),
[acceptance barrier](../../crates/libs/fube-hosting/src/ipc.rs#L825-L839),
[actual-PID admission](../../crates/libs/fube-hosting/src/local.rs#L348-L372),
[completion](../../crates/libs/fube-hosting/src/local.rs#L444-L521)).

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
([usage/driver](../../crates/libs/fube-hosting/src/local.rs#L540-L609)).

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
([cleanup implementation](../../crates/libs/fube-hosting/src/local.rs#L610-L712),
[H2 completion](../../crates/libs/fube-hosting/src/residency.rs#L1357-L1599),
[process reservation/drain/reap](../../crates/libs/fube-local-host/src/process.rs#L302-L431)).

`take_outputs` transfers bounded local registration/status/obligation/operation
records. Transfer is not downstream acknowledgment; bare Activation has no live
RA/FM/HM consumer. The separate local Node owns its required RA/H2 consumers,
not FM/HM delivery. Required health/application reporting, FM-status completion
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
bounds. See [options](../../crates/libs/fube-hosting/src/local.rs#L51-L166),
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
and [physical-failure tests](../../crates/libs/fube-hosting/src/local.rs#L720-L818)
cover retained finals and acquisition guards. Production capability gates stay
closed. No successful Rust service callback execution is claimed by this
registration-only fixture.

Run the seven [separate native regressions](../Development.md#standalone-native-runtime)
from fresh clean-source binaries after shared-mechanics changes. Bootstrap,
Close, Abort, manifest Close/Abort, leases and registration have distinct
unregister/forced-cleanup evidence; none substitutes for this native-free
package owner, production RA/RAP or deployed service readiness.
