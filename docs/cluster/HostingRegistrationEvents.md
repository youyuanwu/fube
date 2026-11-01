# Hosting factory registration and owned local events

**Status: bounded production actor-9 registration and nonactivating equal-version
lookup, with an owned local node/RA event bridge, are implemented. Native-ready
RA/FM/HM and deployed package/process/replica lifecycle remain capability-gated.**
This is not a running Service Fabric node or serving/deployment readiness.

**Authorities:** native semantics pin
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; release-native protocol profile
11.8.121.4 Linux x86-64. The release binaries are not claimed built from that pin.
Earlier [H1/H2 and bootstrap-only H3 evidence](RustHosting.md) keeps its original
scope. New registration receipts do not replace the six existing native actions.

## 1. Profile and composition

[`fube-node`](../../crates/libs/fube-node/src/lib.rs#L311) owns a listener,
exclusive Hosting delivery owner, worker and RA consumer lifetime. Dependency
direction is Node → Hosting/RA; Hosting does not depend normally on RA, runtime,
test-host or native SF libraries. Applications remain across IPC.

```text
actual stateless factory client -- actor 7/9/15 --> production Hosting
                                                  |
                               native-derived type registry / same-core lookup
                                                  |
                           owned registration, status and obligation lanes
                                                  |
                    fube-node worker -> serialized current-state RA decisions
                                     -> separate local status processor
```

The admitted slice is trusted loopback, expected actual host/PID and complete
prepared code/package context, activated single-code-package stateless
SharedProcess, one current runtime/session and declared managed types.
Preparation does not activate/download a package or launch a process.
The existing bootstrap-only `Server::admit` remains separate; it does not inherit
factory permission.

| Capability | Admission/result boundary |
|---|---|
| Declared factory, equal-version nonactivating lookup | Implemented over the actual IPC-owned registry. Registration is a fact, not permission to serve. |
| Registered/RuntimeClosed/HostClosed | Owned delivery to current-state consumers; readiness/nonmatch and required-but-gated lifecycle decisions are explicit. |
| Enabled/Disabled | Owned local staleness/coalescing/pending processing; activation is not the status key. No FM send/ACK API is exposed. |
| Node managed health | Explicit construction-time reporting-disabled profile with real bounded entity/source/report/removal bookkeeping; no enabled processing/HM delivery. |
| Application health/load/fault reports, required FM status completion, deployed lifecycle | Required profiles rejected before preparation; no successful dummy replies. |
| Activating/mismatched-version operational lookup | Activating lookup is unsupported. Registered mismatch returns an inner UnsupportedCapability with request-spelled candidate/sequence intact, without invented H2 scheduling/deactivation outcomes. |
| Exclusive/adhoc/multicode/stateful/container/remote/TLS/upgrade/distributed leases | Unsupported; no field stripping or fabricated activation identities. |

Native `FabricClient/IsHealthReportingEnabled=false` is an explicit
construction-time setting, not “the app sends no reports”
(`client/ClientConfig.h:41-42`, `HealthReportingComponent.cpp:94-115,475-481` at
the pin). Native managed source membership and report construction still happen
before disabled processing. The Rust owner retains package-entity sticky report
history and independent health sequences; property/entity removal has distinct
kinds/TTL/instance provenance
([health.rs:177-280](../../crates/libs/fube-hosting/src/health.rs#L177)).
No enabled-health terminal local store is substituted for HM.

## 2. Registration and identity

The actual factory producer emits the ordered complete runtime context and
type-instance identity. The production handler
([ipc.rs:1400](../../crates/libs/fube-hosting/src/ipc.rs#L1400)) checks the full
admitted wire context before H1 projection, declared type and exact current
session/runtime ownership. Native semantic CI/UTF-16 identity is not replaced
with blanket ASCII/Unicode folding. Transport route identity, semantic identity,
session incarnation and event sequence remain separate facts.

Native tentative/valid-Finish/invalid-Finish cleanup, same-runtime duplicate
`0x80071d31`, other-runtime duplicate `0x80071bd1`, invalid type `0x80071c21`
and primary-versus-cleanup outcomes remain distinct. Accepted results are retained
before mapping/serialization/write; failed output is not rollback.
The post-lock deadline path uses the held state guard, never a recursive lock.
Type requests have a finite 30-second node cap and propagated remaining timeout
when that producer profile requires it. Native ClientLocal is explicit, not
missing-header fallback.

Registered snapshots use retained declaration identity and Finish context;
lookup uses request spelling and valid runtime-table operational fields,
including the numeric package—not code—sequence. Lookup shares the same core,
not an independent modeled registry.

## 3. Actual consumers and completion

[`HostingRegistered`, `HostingRuntimeClosed`, `HostingHostClosed`](../../crates/libs/fube-ra/src/hosting_events.rs#L36)
are producer events, not the existing issued `RegistrationCompletion`.
Routing inventory is not final validation: serialized workers reread committed
FT type/local full version/activation/readiness and exact host/runtime.
Normal RuntimeClosed vetoes already-closing/down/closed replicas; it is never
HostDown. Matching excluded reopen/retry/close work is reported as Required/gated
without fabricated lookup, acquisition, RAP completion or usage release.

[`LocalStatusConsumer`](../../crates/libs/fube-ra/src/hosting_events.rs#L131)
uses native type CI keys, ten-minute sequence history with hourly cleanup,
lower/equal-sequence staleness, opposite-status coalescing, Disabled priority
and a separate downstream message counter. Local processing is not FM
acceptance; pending status remains owned without an ACK.

Default composition readiness is closed, not inferred. Qualification tests use
explicit trusted readiness and actual committed RA/store state to test matching
branches; issued runtime/acquisition premises remain test-only. New native
factory execution observes readiness-gated **IgnoredNotReady** local root
decisions, not native-ready RA/FT processing. Source inspection, actual Rust
consumer execution, release-native producer protocol and deployed native
interoperability are different evidence classes.

`TypesUnregistered`/health-source package obligations remain explicitly owned
and gated where an Opened package/timeout owner is absent. They are not physical
cleanup or H2 release. No new production H2 acquisition bridge is implied.

## 4. Ownership, bounds and shutdown

Native queues' effectively unbounded defaults are not inherited. Logical
payload limits are not allocator/RSS guarantees. Default node bounds
([Config](../../crates/libs/fube-node/src/lib.rs#L39)):

| Owner | Default bound / lifetime |
|---|---|
| Hosting producer lanes | 256 events and 1MiB per registration/status lane; separate 256/1MiB obligations and core budgets. |
| Node transferred payload | 256 items/1MiB per each of the three lanes. Capacity leaves the original source head intact. |
| Node retained fanout/results | Separate 8MiB per lane, conservative `128 + max_entities*512` per registration event; follows pending/unknown/receipt/shutdown ownership. |
| Node routing | 32 current entity candidates by default, final validation still serialized. |
| Node commands/results | 64 slots, input256KiB; aggregate4MiB includes worst-case retained output until collection. Lookup reserves projection bytes plus request-spelling/input and fixed-result bytes, not encoded UTF-16 frame size. Checked arithmetic precedes enqueue. |
| Status history/pending | 64 types/256KiB; admission reserves possible keys before type preparation. |
| Decision observation | At most30 seconds; Unknown retains accepted tickets and never resubmits them. Worker turns sleep5ms; uncertainty is not cancellation. |
| IPC | Existing64KiB frames, finite read/write/scratch/result reservations and ≤30-second atomic local authority. |

Receipt/result transfer frees its reservation once; it is not RA/FM/HM ACK.
Terminal RA lifetime is read-only, lock-free and bound into Hosting admission;
ordinary Complete+sender closure is distinct from terminal noncomplete owner
loss ([Ticket observation](../../crates/libs/fube-ra/src/entity.rs#L84)).
Living unknown-store proposals retain their original ticket/fence and publish
no postcommit effects before definite resolution.

`Node::stop` withdraws admission, joins its worker and returns resources through
`Handle::take_shutdown`; residuals own the stopped Hosting core, source owner,
RA, pending tickets and status state. Poison recovery permits inspection/transfer
only, never resumed admission. Drop does not claim physical cleanup or H2 release.
The default-off RA `qualification` feature's deliberate-termination command is
an internal test hook, not enabled IPC/control catalog.

Atomic finite actor-15 renew/stop/revoke and the application's independent
send-relative watchdog do not wait for RA/store/event progress or destructors.
Node local leases deliberately replace the Unix node Federation lease source;
they are not per-application distributed lease-agent parity.

## 5. Execution and evidence

Focused introduced/preserved checks:

```sh
cargo test --locked -p fube-hosting -p fube-ra -p fube-ra-store -p fube-node -- --test-threads=1
cargo test --locked -p fube-runtime --lib hosting_registration_tests:: -- --test-threads=1
cargo test --locked -p fube-runtime --lib hosting_tests:: -- --test-threads=1
python3 -m unittest discover -s tests/offline/native_runtime -p 'test_*.py'
```

Fresh actual release-native qualification (choose new directories each time):

```sh
cargo build --locked --workspace --bins --examples --all-features
python3 -m tests.native_runtime.runner package --output build/registration-NEW
python3 -m tests.native_runtime.runner hosting-registration --prebuilt build/registration-NEW --diagnostics-directory build/registration-NEW-receipts
```

Also retain bootstrap, close, abort, manifest-close, manifest-abort and
hosting-leases as six separate required regressions. The original five are
fixture lifecycle evidence, hosting-leases is production bootstrap-only;
neither qualifies new production factory registration or H4.
New native registration qualifies actual factory/lookup and owned Registered,
RuntimeClosed and HostClosed decisions before stop. Runtime unregister is
observed; automatic native host-unregister was not observed, so HostClosed
is identified as connection-loss cleanup. One gated package obligation remains
owned. Fixture parent launch/drain/reap is not Hosting lifecycle implementation.

Every native bundle binds a clean exact source revision, binary inventory/modes/
hashes and pinned mapped release libraries. Documentation-only changes also
require a new current-source bundle. Do not overwrite/relabel prior receipts.
See [native profile commands/limitations](../runtime/RustTestHost.md#28-owned-registration-events-qualification)
and [development gates](../Development.md#standalone-native-runtime).
