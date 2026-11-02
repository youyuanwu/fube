# Rust node-side Hosting design

**Status: H1 registration/lookup, H2 admitted accounting/scheduling and the narrow
bootstrap H3 IPC/local leases and the bounded registration-events slice are
implemented, together with bounded H4 owned local package/process activation.
Broader H3 and deployed RA/RAP composition remain gated.**
[`fube-hosting`](../../crates/libs/fube-hosting/src/lib.rs) provides
a native-derived, deterministically driven core plus a separately owned local
IPC service, not a complete running node. H1's original model evidence is not
new interoperability qualification. See [implemented H1](#implemented-h1-model)
for the exact API/source and evidence boundary.
See [implemented H2](#implemented-h2-accounting-and-scheduling) for actual
residency and the separate real-accounting RA test profile.
See [implemented H3](#implemented-h3-bootstrap-only-local-ipc) for the newly
executed Rust/native bootstrap profiles and their non-deployed/non-distributed
qualification limits.
See [supplied port contracts](#supplied-port-contracts) for the distinction
between interface definitions, H1 model implementations and gated live work.
See [factory registration and owned local events](HostingRegistrationEvents.md)
for the separate production actor-9/same-core lookup/node-owner profile, exact
native readiness/health gates and new Rust/release-native execution boundaries.
See [owned local activation](HostingLocalActivation.md) for the implemented
registration-only package/process owner, H2 teardown and exact qualification limits.

**Baselines:** repository `0748b9ab3a39530a57387fda23b5889e6e7a442c`;
native Service Fabric `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`, inspected
2026-10-31. Native references below are relative to `src/prod/src/` at that pin.
Source behavior is not proof that the SF 11.8.121.4 qualification binaries were
built from this revision.

## 1. Decision and relationship to native Hosting

Create `fube-hosting` as a native-SF-library-free Rust library implementing the
selected **node-side `Hosting2` contracts**. Native behavior is the authority;
the existing test host supplies reusable mechanics, not the specification.
Port supported state transitions, identity/version rules, error results and
event ordering. Adapt C++ ownership, Begin/End scaffolding, process activation
and external infrastructure explicitly.

The crate is neither another RA nor another application runtime:

| Component | Responsibility |
|---|---|
| `fube-hosting` | Package residency/activation, owned processes, host/runtime/type registrations, node-side Hosting IPC and finite local lease service. |
| `fube-ra` | Failover-unit decisions, native reliability admission, Open/Close/replacement/retry policy, FM/peer choreography and commit-before-effects. |
| Node executable | Compose RA and Hosting; bridge effects, results and events; own node identity, sessions, shutdown and supervision. |
| `fube-runtime` | Application-side Hosting client, RAP, factories, service callbacks and runtime authority/watchdog. Reuse it rather than implement a second RAP. |
| `fube-test-host` | Bounded standalone scenarios and native-runtime qualification using the shared Hosting mechanics as they are extracted. |

This is the Hosting companion to the [cluster architecture](PureRustCluster.md)
and [RA port/store design](RustRaPort.md). The [native node-side architecture](../runtime/FabricRuntimeHost.md)
describes the complete surrounding system. A bounded port is not a replacement
for every native Hosting subsystem, FabricHost or native SF node.

### Initial execution profile

Target Linux, initially x86-64, trusted same-machine loopback IPC, activated
single-code-package ExeHost, stateless services and prevalidated local packages.
Begin with SharedProcess activation and one active replica per owned app process;
allow multiple independently admitted records without singleton global state.
That placement restriction does not permit competing hosts for the same native
service-type/activation key. Reject unsupported placement collisions rather than
inventing private activation identities to force separate processes.
Do not silently enable exclusive activation, multiple code packages, config/data
packages or stateful deployment because their identity types already exist.

Remote/TLS profiles, containers, nonactivated hosts, guest/DLL hosting, native
image-store downloads, executable upgrades and resource governance are separate
capabilities. Unsupported execution features fail capability admission before
launch; they do not get stripped from manifests or receive success-shaped stubs.
These profile limits do not remove any eventual RA-FM/RA-RA obligations from
the [native reliability catalog](NativeRaProtocol.md).

## 2. Existing implementation and extraction boundary

The [RA opening](RaStatelessOpening.md), [removal](RaStatelessRemoval.md) and
[replacement](RaStatelessReplacement.md) slices use trusted simulated adapters.
There is no deployed Hosting/RAP adapter or running Rust SF node executable.
The bounded [`fube-node`](HostingRegistrationEvents.md) library now owns actual
local registration/closure/status consumers and production Hosting IPC; its
qualified matching premises do not enable RAP deployment or FM/HM completion.
The [application runtime](../runtime/RustStatelessRuntime.md) and
[standalone native test host](../runtime/RustTestHost.md) exercise opposite sides
of the boundary; neither establishes the combined Rust RA-to-Rust RAP path.
Dated native evidence qualifies its exact source/profile, not every later change.

The local validator/materializer/supervisor now live below both Hosting and the
native adapters in [`fube-local-host`](../../crates/libs/fube-local-host/src/lib.rs#L1-L30).
Native preflight, environment and callback parsing stay in test-host. The
following extraction map retains its original source provenance:

| Existing source | Reuse/adapt | Keep outside the shared core |
|---|---|---|
| [`process.rs`](../../crates/libs/fube-test-host/src/process.rs) | Child ownership, process-group reservation, bounded logs, exit observation, group drain and reap. | Fixture stdin commands, `MSSF_APP` parsing and mandatory native-library launch environment. |
| [`lib.rs`](../../crates/libs/fube-test-host/src/lib.rs) registration handlers | Native request/reply shapes and host/runtime/type validation patterns. | Hardcoded node identity, one-runtime/type fields, `Scenario`, `RunReport` and complete-scenario pump. |
| [`ipc.rs`](../../crates/libs/fube-test-host/src/ipc.rs) | ListenInstance handshake, accepted IPC session, bounded framing/ingress, correlated replies and one-way sends. | Whole-scenario timeouts and a single synchronous pump as the long-running node scheduler. |
| [`manifest.rs`](../../crates/libs/fube-test-host/src/manifest.rs), [`deployment.rs`](../../crates/libs/fube-test-host/src/deployment.rs) | Strict supported-package validation, owned layout and matching activation artifacts. | Fixture deployment constants and native Runtime/Common preflight as a universal requirement. |
| [`wire.rs`](../../crates/libs/fube-test-host/src/wire.rs), [`lifecycle_wire.rs`](../../crates/libs/fube-test-host/src/lifecycle_wire.rs) | Verified Hosting/RAP schema constituents. | Narrow empty-array substitutions or test assumptions as general schema definitions. |
| [`controller.rs`](../../crates/libs/fube-test-host/src/controller.rs) | Wire/send examples only. | Its miniature RA, fixture probes and stricter test reply predicates. Production policy stays in `fube-ra`. |

Keep ping/refusal probes, native binary/library verification and scenario reports
in the test harness. Native-backed and native-free Rust launches are separate
explicit profiles; the latter must not inject native SF loader paths.
Do not make production Hosting depend on the test-host crate.

## 3. Native porting map

| Native authority | Behavior to preserve | Delivery |
|---|---|---|
| [`Hosting2/IHostingSubsystem.h`][native-interface] | RA-facing lookup, usage count, termination and registration/closure event boundary. Not the entire interface in one milestone. | H1-H4 |
| [`ApplicationHostRegistration.cpp`][native-host-registration], [`ApplicationHostManager.cpp`][native-host-manager] | Start/finish registration, valid/expired state, unregister and host termination handling. Preserve activated versus nonactivated distinctions; only the declared activated profile is enabled initially. | H1 core, H3/H4 adapters |
| [`FabricRuntimeManager.cpp:117-249`][native-lookup] | Activation-scoped service-type lookup, request-derived type identifier, sequence number, activation request and native pending/error outcomes. | H1 decisions, H2 package dependency |
| [`FabricRuntimeManager.cpp:383-458`][native-version] | Returned candidate/version and deactivation-if-unused decision. Version mismatch is not always an unconditional rejection. | H1 decision seam, H2 implementation |
| [`FabricRuntimeManager.cpp:501-902`][native-runtime-registration] | Tentative runtime/type registration, host/runtime validity checks, cleanup on failure and closure invalidation. | H1 |
| [`ServiceTypeStateManager.cpp`][native-type-state], [`ServiceTypeStatus.h`][native-type-status] | Declared-type entries, registration transitions, disable/recovery state, native sequence checks and events. | H1 selected state-machine closure |
| [`ApplicationManager.cpp:1079-1109`][native-unused], [`Deactivator.cpp`][native-deactivator] | Package usage, idle deactivation, cancellation and sequence-fenced timers. | H2 |
| [`ApplicationManager.cpp`][native-application-manager] | Selected service-package activation/deactivation lifecycle; external download/activation dependencies remain explicit adapters. | H1 modeled obligations, H4 local execution |
| [`ServiceTypeStateManager.cpp:923-1115,1581-1641`][native-type-state] | Separate runtime/host closure effects and registered/enabled/disabled events with their identity/sequence fields. | H1, then node bridge |
| [`HostingSubsystem.cpp:2124-2156`][native-lease] | Native actor-15 lease-reply shape and TTL meaning. Its Federation lease source is deliberately replaced, not ported. | H3 |
| [`ra/Hosting.HostingAdapter.cpp`][native-ra-adapter], [`ra/ServiceTypeRegistrationWrapper.cpp`][native-ra-wrapper] | RA owns lookup-error policy, registration attachment and usage commit/cancel ordering. | Existing RA plus H2 integration |

The `ra/` paths in this table are under `Reliability/Failover/`.
Each implementation slice must record the exact native method/consumer closure
and corresponding tests. Finding a similarly named native class is insufficient.

## 4. Architecture and interface shape

```text
fube-test-host -----------------> fube-hosting ---> fube-transport
       |                              |
       +------> fube-local-host <------+

future node executable ---------> fube-hosting
                       ---------> fube-ra --------> fube-ra-store
                         |
                    effect/event bridge
                         |
          node IPC <----> fube-runtime in each owned app
```

`fube-hosting` must not depend on `fube-ra`, `fube-runtime`, `fube-test-host` or
native SF libraries. The application runtime remains across IPC, not linked
into the node to invoke service callbacks directly. The node bridge maps between
Hosting records and RA commands/effects without changing native semantics.

Start with one crate and internal modules for registration/state, lookup/events
and dependency adapters. Add package usage, IPC/leases and process/deployment
modules as their stages land. Do not introduce a separate protocol crate,
daemon or universal plugin framework merely for this extraction.

The [supplied port contracts](#supplied-port-contracts) now define request-only
registration lookup, package residency, lookup-triggered activation/scheduling,
and two typed owned event lanes. Host/runtime/type registration retains the
existing H1 inherent/split-step APIs; production RA termination/RAP stay gated rather than
receiving success-shaped wrapper traits. PackageUsage and narrow PackageScheduling
have deterministic H2 implementations; physical PackageLifecycle remains
definition-only. Native lookup errors, optional returned registration
and sequence number remain independently representable; an error must not
automatically erase its candidate.

Use explicit injected observations, clocks and controlled dependency outcomes
for model tests. A test input certifies only its stated premise. A model
registration is not evidence that a process exists or holds serving authority.

### Identity and records

Keep separate records for application/service-package activation, code-package
launch, application host, runtime and service-type registration. Carry native
service/package versions, activation context/public activation identity,
host/runtime identifiers, owned PID and applicable sequence numbers.
Reference acquisition IDs and process/session handles are additional Rust
ownership metadata, not fields invented in native messages.

A service type is not keyed only by its short name. Preserve native comparison
and ordering for each identifier, including the pinned Linux string behavior
already researched by [RA opening](RaStatelessOpening.md#shared-node-registration-notifications).
Do not replace it with Rust Unicode case folding, blanket ASCII restrictions
or normalized wire strings. Reconcile reusable identifier helpers without
introducing a dependency from Hosting back to RA.

Registration snapshots preserve **producer-specific field provenance**, not a
universal stored spelling. Native [synchronous lookup][native-lookup] passes
the requested `versionedServiceTypeId.Id` into
[`CreateServiceTypeRegistration`][native-lookup-registration]; the
[`ServiceTypeRegistered` event][native-registration-event] instead passes the
stored entry's service-type identifier and activation identity.
The [shared constructor helper][native-registration-fields] obtains the version,
host/PID, runtime, code-package name and service-package sequence from the selected
runtime context. Do not copy the whole requested description into those fields,
or canonicalize the returned type identifier to the stored spelling.

For example, with a stored `Widget` type and a comparison-equivalent `widget`
request, lookup returns `widget` while the registration event returns `Widget`.
The bridge preserves whichever registration it receives; the
[RA type map][native-ra-type-map] retains its first attached identifier for
notifications. Matching equivalence does not make these spellings interchangeable
in emitted messages.

## 5. H1 registration and lookup semantics

H1 is a testable in-memory port with no sockets or process launch. Port the
selected host/runtime/type registration closure, not just public method names.

Host registration has native start/finish/timeout states. Runtime registration
creates a tentative entry, checks the host and validates or cleans the entry.
Type registration starts a tentative transition, verifies runtime and host
validity, then finishes or cancels that transition. Managed types must be
declared by the admitted service package; do not create arbitrary type entries
merely because an app sends registration.

Preserve `NotRegistered_Enabled`, `InProgress_Enabled`, `Registered_Enabled`,
`NotRegistered_DisableScheduled`, `NotRegistered_Disabled`,
`InProgress_Disabled` and manager closure where applicable. These are not a
generic Ready/Faulted state machine. Port registration-related disable/recovery
transitions and timer obligations needed by this closure; broader failure
sources may remain capability-disabled. Preserve source-specific duplicate
results: duplicate service-type StartRegister differs by owning runtime, and
service-type FinishRegister has its own state-dependent behavior.

### Host Finish and timeout are separate contracts

Port the [host state method][native-host-registration] and
[public Finish processor][native-host-finish] separately. After a successful
registration-table Find, the native processor ignores
`OnFinishRegistration()`'s error, replies Success, then invokes
`OnApplicationHostRegistered`. That invokes the activation proxy's registration
callback only if the proxy is found; it is not itself a host-closed event.
[`IsRegistered`][native-host-validity] independently requires a Completed entry.
A successful Finish reply must not manufacture that state or grant runtime/lease
authority.

| State when public Finish resolves its table lookup | State-method result | Public reply | Validity and activation-proxy notification |
|---|---|---|---|
| Activated host InProgress; Finish wins before timeout | Success; becomes Completed | Success | Registered while the entry remains; callback if proxy exists. A later timeout state check fails without removing this Completed entry. |
| Duplicate Finish on Completed | InvalidState; remains Completed | Success | Still registered; callback is attempted again if proxy exists. |
| Timedout but entry not yet removed | InvalidState; remains Timedout | Success | Not registered; callback can still run if proxy exists. Timeout can subsequently remove the entry. |
| Entry already removed by timeout | Method not called | HostingApplicationHostNotRegistered | No registered entry or registration callback. |
| Registration table closed | Method not called | ObjectClosed | Table lookup fails; no registration callback. |

The [timeout processor][native-host-timeout] cancels its timer, finds the entry,
marks it Timedout and then removes it, stopping on each operation's failure.
It does not itself call ordinary host-unregistered/host-closed processing.
Keep the native table/state operation boundaries observable to deterministic
tests, including a pause between timeout marking and removal; an atomic model
of the entire processor must not erase the Finish race.

The test host's `started && !finished` duplicate-Finish rejection is a fixture
restriction, not this public native contract. Preserve the native reply/state
distinction while keeping additional terminal local-launch/lease rules explicit
as separate H3/H4 ownership adaptations.

### Lookup is not activation success

Lookup uses versioned service type, immutable service-description inputs and
native activation context. Keep the lookup sequence and distinguish:

| Native condition | Required behavior |
|---|---|
| Registered/enabled with usable runtime | Construct the actual candidate, then apply native version/deactivation policy. |
| Not registered/enabled or disable-scheduled | Request applicable activation; preserve its immediate result. Synchronous success still becomes type-not-registered; asynchronous work can return deployment-in-progress. |
| Registration in progress | Return the native type-not-registered outcome; do not issue RAP Open. |
| Disabled | Return the native disabled error for RA's policy to interpret. |
| Package/runtime/state dependency failure | Preserve the specific native result; do not collapse everything into pending or success. |

`EnsureReturnedServiceTypeVersion` calls
`ApplicationManager::ScheduleForDeactivationIfNotUsed`. Its reported
`hasReplica` decision selects registration-version-mismatch versus `NotFound`;
the candidate can already be populated. Port that actual collaborator behavior,
not an inferred rule that every mismatch is fatal or every nonzero count alone
decides the result. RA can accept a returned mismatch candidate under its
existing upgrade-version rules.

This is a real H1/H2 dependency. H1 uses an explicit controlled package adapter
for activation and deactivation-if-unused decisions; tests supply the result
and observe its obligations. There is no default "unused", fake usage success
or ignored activation request. H1 alone cannot claim operational lookup parity.
H2 replaces that dependency with the ported package-usage/deactivation logic.

### Events and closure

Publish native registered/enabled/disabled and runtime/host-closed events with
their own sequences, activation scope and affected type information. Mutate the
relevant registration state before making its notification observable; deliver
owned notifications without invoking consumers under registration locks.

Runtime closure invalidates the affected runtime's registrations, not every
runtime in its host. Host closure removes its runtime registrations and applies
the native service-type closure path. Preserve the actual consumer checks for
late registration/timer work rather than adding a blanket "latest sequence
wins" rule. Distinguish logical unregister/closure from observed OS exit.

The local launch/lease authority has additional terminal retirement rules in
H3/H4. Those rules prevent reviving an old owned launch; they do not redefine
every native registration duplicate or re-registration result.

## 6. RA integration and package ownership

The current [RA effect boundary](../../crates/libs/fube-ra/src/entity.rs)
already exposes registration lookup, runtime Open, Close, description update,
host termination and reference release. The node bridge must:

| Existing RA seam | Hosting/node obligation |
|---|---|
| `Open` / `FindRegistration` | Resolve the native-scoped lookup; preserve candidate/version/error and acquire or retain package usage according to the existing ownership contract. `Open` here is lookup work, not permission to send RAP Open. |
| `Registered` | Supply one owned completion per issued lookup through the existing `RegistrationCompletion` contract. Keep retained usage distinct from a new acquisition. |
| `RuntimeOpen`, `Close`, `UpdateServiceDescription` | Route the captured native-shaped command to the selected host/runtime. Sending is not lifecycle completion. |
| `CompleteOpen`, `CompleteClose`, `CompleteServiceDescription` | Decode actual RAP semantic replies and let RA apply its existing admission/consumer rules. No stronger test-controller predicates or synthesized successful cleanup. |
| `ReleaseRegistration`, `ReleaseUsage` | Release the exact acquired references once. They are different resources; neither means terminate the process immediately. |
| `TerminateHost`, `HostDown` | Issue owned termination separately from an appropriately matched observed host event. A termination-request callback cannot fabricate `HostDown`. |

The Hosting snapshot is richer than today's bounded RA registration projection.
Retain PID, activation and process/session ownership in the adapter's records;
do not discard them or add them as new native reply equality requirements.
Existing lookup/acquisition IDs remain private adapter bookkeeping.

H1 preserves all selected native event types even where the current RA lacks
a consumer. In particular, its per-entity `HostDown` command is not a substitute
for every native runtime-closed or service-type event. Before enabling a live
path, port the missing native RA consumer and routing checks as a dependent
slice, or reject that execution capability. Never silently ignore the event
or translate runtime closure into physical host death.

H2 implements package usage and deactivation, not a second RA retry engine:

- Keep package usage separate from the registration snapshot. RA can lose a
  registration while retaining package residency during opening recovery.
- Preserve native first-use/last-release behavior, grace policy and cancellation
  of old deactivation work using its source-defined sequence checks.
- Honor RA's persist/cancel ownership. Native increment can precede persistence
  with cancellation compensation; decrement is a post-commit action. The Rust
  bridge must reconcile this with the existing acquired-usage seam and unknown
  store outcomes, not simply perform both operations whenever a future ends.
- Bind release to the actual acquired package/activation. Semantic description
  changes cannot retarget an outstanding reference.

H1 integration tests retain explicitly simulated usage/RAP adapters. H2's
separate [real-accounting profile](../../crates/libs/fube-hosting/tests/ra_residency.rs)
uses actual residency and RA/store completion ownership; RAP observations remain
controlled. Required live event consumers are still absent, so live admission
remains rejected even though H2 accounting is implemented.
Unknown P1 writes retain their original proposal/ticket/fence; Hosting cannot
replace a fenced RA executor or infer rollback from observer cancellation.

## 7. IPC, leases and process activation

### H3: long-running local protocol service

Reuse `fube-transport` framing and serialization. Reconcile the test-host,
runtime and RA schema definitions against native source before sharing them;
do not generalize a narrow codec by accepting arbitrary missing fields.

Preserve the application-initiated IPC connection, ListenInstance handshake,
host/PID admission and reverse routing. Actor 7 handles host registration,
actor 9 runtime/type registration, actor 15 lease polling and actor 8 supported
host controls. RA/RAP actor 12 is a separate dispatcher, not a Hosting state
machine; lifecycle commands/replies are semantic one-way traffic while
registration and lease polls use correlation. Maintain their actual
headers/actions/schemas.

Route supported fault/load/health traffic to its actual consumer with explicit
completion and errors. A generic successful reply is not a substitute for an
unimplemented report handler. Capability admission must account for the traffic
an enabled app/runtime can emit.

Native Hosting's lease reply obtains TTL from the Federation lease agent.
The local cluster intentionally replaces that authority source. Keep finite
actor-15 polling and existing runtime send-relative expiry/terminal retirement;
initial TTL is not permission to serve without a fresh poll. Start with the
existing 30-second local profile, not an infinite grant. Host/session authority
loss must stop renewal or revoke it through the supported protocol.

Lease service and the runtime's independent callback/destructor watchdog must
not wait on RA commits, process teardown, diagnostics or application callbacks.
FM isolation alone does not revoke a live local app lease. An FM heartbeat
timeout is not evidence that an app has expired or been fenced.
Closed/retired local authority cannot resume on an old connection.
This is a documented local-authority adaptation, not native distributed lease
parity.

**Implemented H3 subset:** [the bootstrap-only local service](#implemented-h3-bootstrap-only-local-ipc)
now accepts external host/PID/package admission and supplies host/runtime
registration plus finite local polling. Broader report/control remains gated;
the separately owned H4 local activation API does
not enable live RA/RAP deployment.

### H4: owned activation and real stateless lifecycle

**Delivered runtime-alignment prerequisite:**
[`fube-runtime` Open][runtime-empty-open] accepts a successful empty opaque
endpoint without treating it as failure. Native [stateless EndOpen][native-stateless-end-open]
accepts null/empty endpoint results, and its
[Open consumer][native-stateless-open-consumer] becomes Opened on success.
The [Rust lifecycle regression][runtime-empty-tests] executes actual
application-side RAP messages with a test peer: empty success without Abort,
matching duplicate reuse without another factory/Open, and successful graceful
Close with independently observed destruction before ACK. Genuine failure
cleanup and authority checks remain unchanged. This bounded prerequisite is
delivered. [H4 owned local activation](HostingLocalActivation.md) is now delivered
separately; production RA/RAP and combined-path deployed qualification below
remain gated. Hosting/test-host acceptance alone is not
Rust lifecycle evidence.

Port the selected native activation lifecycle with a trusted local-package
adapter instead of image-store download or a FabricHost dependency. Validate
immutable supported package inputs before side effects, materialize matching
native-shaped deployment artifacts, then launch with fresh host/code/package
identities and explicit process/session ownership.

Keep native-library and native-free launch environments separate. Neither path
weakens the runtime's host/security checks. Registration success is not process
identity authentication: peer-provided PID/host data must be checked against the
owned launch, package and admitted session. The initial trusted-loopback profile
is not a malicious-same-user sandbox.

Only registered types can satisfy lookup; only RA's `RuntimeOpen` permits the
RAP command. Preserve actual replies, endpoints and failures. Endpoint strings
may be opaque or empty; application-specific probes belong in acceptance tests.
There is no direct service callback or role setter on the node.

The delivered runtime prerequisite proves that a successful empty-endpoint Open stays
Open without Abort, a matching duplicate Open returns the same empty endpoint
without another factory/Open callback, and subsequent Close disposes the
instance normally after successful graceful Close. A later deployed H4 slice must exercise this
through its owned activation and production RA/RAP traffic using
independent callback/resource observations, not a connection to the returned
endpoint string. Nonempty samples or native-runtime test-host success do not
qualify the Rust runtime's empty-endpoint path.

Track service Close/Abort, runtime unregister, lease retirement, termination
request, observed exit, group drain, direct-child reap and directory release
separately. Do not reuse package directories or claim cleanup merely because
one observation occurred. Preserve the test host's waitable-leader reservation
before numeric process-group signaling and its bounded log/cleanup ownership.

A host may outlive one replica. Package usage/deactivation policy, not every
Close reply, decides idle process teardown. Hosting's native activation recovery
policy must remain distinct from FM-authorized replica replacement; retries
must not replay retired work or invent a new RA replica incarnation.
Escaped descendants, abrupt owner-death cleanup and fresh-deployment fencing
require explicit qualification and remain limitations until proved.

## 8. Execution, bounds and failure reporting

H1 may use a synchronous serialized core driven by deterministic tests. Later
stages add supervised execution around that same state model, not an unbounded
task per callback or a global scenario loop. Keep external I/O, process waits
and consumer callbacks outside state locks. Reserve queue/event capacity before
publishing mutations that require those outputs.

Configure finite counts and byte budgets for packages, hosts, runtimes, types,
outstanding operations, timers, events, IPC frames, logs and retained tombstones.
Reject excess admission explicitly. Slow consumers cannot silently drop required
closure/release events, extend authority indefinitely or cause unlimited retries.
Do not block lease service behind a full lifecycle queue; reserve a separate
control path and supervision budget.

Once work is accepted, observer cancellation does not prove execution stopped.
Retain ownership until its outcome/cleanup is established. Expose operation
failure, unknown outcome and cleanup failure distinctly; cleanup success never
turns a failed activation or lifecycle into success. Explicit shutdown drains
owned work and reports unresolved resources. Drop can initiate cleanup but is
not a successful shutdown receipt.

Use bounded typed diagnostics carrying identity, native error/detail, sequence,
phase and cleanup observations. Redact bootstrap capabilities and secrets.
Protect local directories/listeners according to the cluster's trusted profile;
do not bypass existing permissions, trust controls or unsupported TLS rejection.
These are Rust execution/ownership adaptations, not extra native wire fields.

## 9. Incremental delivery and qualification

| Slice | Deliverable | Completion boundary |
|---|---|---|
| **H1: registration and lookup core** | New crate; native-derived host/runtime/type records, registration transitions, scoped lookup decisions, events and explicit package/clock adapters. | Deterministic source-derived scenarios and RA registration-bridge tests cover producer-specific spelling and separate host Finish/timeout reply/state outcomes. No IPC, process launch, real usage or deployed-parity claim. |
| **H2: usage and deactivation** | Implemented admitted package/application accounting, exact residual ownership, deterministic grace/sequence scheduling and real-accounting RA test composition. | Tests cover mismatch disposition, exact release, retention and definite/unknown-store ordering. Physical final work remains owned; required live event consumers are still gated. |
| **H3: IPC and local leases** | Extract/adapt accepted sessions and Hosting handlers; long-running bounded dispatch and finite local authority. | Actual protocol/lease tests and standalone native-host regression qualify the enabled profile. H3 is not yet a real Rust RA deployment. |
| **H4: process activation and RA/RAP** | Bounded [owned local activation](HostingLocalActivation.md) and empty-endpoint Runtime prerequisite implemented. Production RA/RAP Open/Close/Drop/resend remains future work. | Local owner tests qualify real package/process/IPC registration and H2 teardown, not callbacks. Future combined-path qualification must prove actual RA/RAP replies, independent callback/resource observations and exit/drain/reap, including crashes and lease loss. |

Each slice may require several PRs. Publish its source mapping, enabled
capabilities, remaining dependencies and actual qualification separately.
Do not move whole files merely to make the crate appear complete.

### Test and evidence policy

H1 must cover registration order, tentative-entry cleanup, declared-type
validation, same/different-runtime duplicates, package/activation isolation,
candidate/version/error combinations, runtime-versus-host closure and late
completion/timer races. Use controlled barriers/clocks, not timing sleeps as
the evidence for ordering. Port matching native tests where they exist; label
processor/helper-derived combinations and Rust ownership tests separately.
`ServiceTypeStateManager.Test.cpp` alone is not a complete native state-machine
oracle.

Include lookup-versus-event type casing with runtime-derived operational fields
and first RA notification spelling. Exercise public host Finish separately
from the state method: ordinary completion, duplicate Completed Finish, timeout
marking before removal, Finish after removal and closed-table failure. Assert
the reply, table membership/validity, activation-proxy notification and absence
of fabricated host-closed events independently.

H2 adds first/last usage, shared residency, reacquisition, stale deactivation,
version mismatch with both collaborator decisions, definite failure, unknown
commit and exactly-once disposal. Test registration loss without incorrectly
releasing retained package usage.

H3/H4 require native-produced codec fixtures or real-runtime protocol execution
for newly enabled IPC, malformed/truncated-input rejection, lease retirement,
queue pressure, blocked cleanup, crashes during registration/Open/Close and
delayed replies after replacement. Run the existing standalone native test
profile after extraction rather than relabel its historical evidence.

For the combined path, prove real Rust-app registration and successful Open,
then verify serving independently for endpoint-bearing services. For empty
endpoints, prove the lifecycle/resource observations above without inventing
a listener requirement. Also prove resource cleanup after Close/Abort,
old-identity Drop followed by a later FM-authorized Add, and no stale
resurrection. Use a test driver for the existing FM input seam if a real FM is
absent; identify it as such. Process exit is separate from endpoint shutdown
and from a successful semantic Close.

Bind qualification to the actual implementation revision, native source/binary
profile and artifact identities. Rust-only round trips and mocked process
observations do not establish native interoperability. Complete-handler
execution, wire compatibility and deployed process behavior are distinct
evidence classes. Existing development/test tiers are described in
[Development](../Development.md); no new build command or CI job exists merely
because this design names H1-H4.

### Implemented H1 model

The core is single-owner synchronous Rust with no consumer callback or I/O
under registration locks. Each native table/state operation uses bounded
projection-before-publication; public Start/Finish/timeout and tentative
registration steps remain separately drivable. The H1 core introduces no production
dependency on RA, runtime, test-host or native SF. The crate now additionally uses
transport/Serde/Tokio for the separate bootstrap-only H3 service; socket2 is
test-only ([manifest](../../crates/libs/fube-hosting/Cargo.toml)).

| Implemented surface | Rust source | Native closure |
|---|---|---|
| Host records, held Start timer/state, Finish find/state/reply/proxy receipt, timeout cancel/find/mark/remove | [host.rs:5–379](../../crates/libs/fube-hosting/src/host.rs#L5-L379) | ApplicationHostRegistration.cpp:38–145; ApplicationHostManager.cpp:1292–1299,1766–1816,1966–2053,2266–2286 |
| Tentative runtime Add/host check/Validate, duplicate errors and original/cleanup receipt | [runtime.rs:4–163](../../crates/libs/fube-hosting/src/runtime.rs#L4-L163) | RuntimeRegistrationTable.cpp:53–266; FabricRuntimeManager.cpp:501–679 |
| Managed declaration/component/failure entries; six selected statuses, unfenced Finish and grace callback checks | [types.rs:5–624](../../crates/libs/fube-hosting/src/types.rs#L5-L624) | ServiceTypeStateManager.cpp:128–299,425–838,1142–1557 |
| Candidate/error/optional sequence; native rendered public-map keys; activation and deactivation-if-unused controlled outcomes | [lookup.rs:5–237](../../crates/libs/fube-hosting/src/lookup.rs#L5-L237), [collaborators.rs:5–95](../../crates/libs/fube-hosting/src/collaborators.rs#L5-L95) | FabricRuntimeManager.cpp:117–249,296–458; ApplicationManager.cpp:1079–1093; HostingSubsystem.cpp:165–180 |
| Owned Registered/RuntimeClosed/HostClosed and separate Enabled/Disabled enqueue lanes; timeout-tracking/source-release obligations | [events.rs:9–294](../../crates/libs/fube-hosting/src/events.rs#L9-L294) | ServiceTypeStateManager.cpp:923–1117,1581–1636; EventDispatcher.cpp:39–197 |
| Logical count/byte admission, complete output reservation, explicit remaining-work closure | [core.rs:78–220](../../crates/libs/fube-hosting/src/core.rs#L78-L220), [model.rs:94–159](../../crates/libs/fube-hosting/src/model.rs#L94-L159) | Rust ownership adaptation, not native limits or physical shutdown |

**Behavior and ownership:** host public Finish can return Success while its
state method returns InvalidState or the held record is no longer in the table.
Proxy presence is an explicit modeled input; the returned callback obligation
is not invocation of an activation proxy or a lease grant. A timer can cancel
before Start's state method, and held Start/Finish records survive removal.
Timeout never invokes ordinary HostClosed processing. Runtime and host
unregister produce different events; an externally supplied termination input
is not a measured OS exit.

Lookup uses requested type/package spelling with runtime-table operational
fields. Registered enqueue uses stored type spelling with Finish-context
operational fields. Matching `widget` does not rewrite stored `Widget`.
Scheduling-error observations remain exposed even where native
ApplicationManager intentionally maps them to NotFound with a retained
candidate. `LookupModel` requires every applicable outcome; missing premises
are explicit local failures. Pending activation End consumes its owned result,
including after core closure, without rewriting a prior lookup or reviving a
factory. H1 controlled observations alone imply neither H2 accounting nor
successful deployment.

The public-activation map is a native **case-insensitive flat string map** over
`<ServicePackageIdentifier.ToString()>_<activation GUID text>`, unlike the
structured package/type tables. Rendering keeps embedded NUL and delimiters;
comparison can therefore alias different GUIDs after an earlier NUL or
different structured package identities with the same rendered string.
TryGet/GetOrAdd preserve the first mapping's public spelling on those aliases;
no second assignment/private identity is fabricated. Retained rendered keys
are byte-accounted ([identity.rs:105–149](../../crates/libs/fube-hosting/src/identity.rs#L105-L149)).

The core exposes **enqueue observations**, not native FifoLifo worker execution.
Each lane can be drained independently; no global cross-event delivery order
is promised. Required events and modeled obligations must fit before a
transition publishes. Public destructive unregister reserves the whole
closure projection before publishing host/runtime removal. Declaration batches
instead preserve native per-entry partial admission: a later modeled
health-source failure retains the admitted prefix and reports its failed index.

Default finite budgets are 128 public package-activation mappings, 64 retained
host records (including held removed records), 128 runtimes, 256 types, 128
failures, 1,024 component associations, 256 timers, 128 total held host/pending
activation operations, 256 events **per lane** and 256 external obligations.
Logical retained state, each event lane and obligations each have a separate
1 MiB byte budget. These are logical payload limits, not RSS/allocator
guarantees. Drained snapshots/jobs become the caller's bounded delivery
responsibility; there is no durable outbox or observer-cancellation cleanup
receipt. `close_core` retains accepted outputs/late-End ownership and reports
remaining work rather than certifying external resources absent.

**Explicit safety adaptations:** fatal native assertions/undefined paths are
reported as local `NativeInvariant` without inventing native recoverable
error/success parity:

- Public tentative-runtime unregister would dereference the table's null
  registration (FabricRuntimeManager.cpp:657–663,896–900). Table cleanup still
  safely returns Success with no valid snapshot.
- InProgress_Disabled failure removal has contradictory null-timer assertion
  predicates (ServiceTypeStateManager.cpp:1515–1524). Its apparent successful
  body is not implemented as native-proven recovery.
- Disable-scheduled Start with only the failure source would erase the last
  component then fail outer Start's postcondition (469–473,583–602). This input
  is rejected before mutation; the defined disabled-Finish/failure-unregister
  held-entry erase and event sequence behavior is preserved.
- Empty tentative runtime/host owners and conflicting public activation
  provenance are source invariant boundaries, not new character restrictions.
- Empty application type name with nonzero application number, or a nonempty
  application with empty package name, violates native constructor assertions
  (ApplicationIdentifier.cpp:27–34; ServicePackageIdentifier.cpp:42–49).
  Core admission/consumption rejects these structures before publication while
  raw comparator probes remain usable. Valid non-ASCII/NUL data is not rejected.
- Empty component removal is an invariant failure before membership testing.
  Empty managed failure is an invariant failure after closed/blocklisting
  checks but before the unknown-failure ignored row
  (ServiceTypeStateManager.cpp:251–259,1142–1167).

Managed prevalidated declarations are the enabled model producer. Adhoc failure
creation/recovery and real health/activation producers are not implemented.
Exclusive GUID records test **identity isolation only**, not exclusive execution.
Native Linux comparison preserves UTF-16/NUL/signed-short behavior and raw
non-ASCII text ([identity.rs:1–176](../../crates/libs/fube-hosting/src/identity.rs#L1-L176));
GUID validation accepts native canonical hyphen/hex shape without normalizing
accepted spelling.

**Original H1 evidence (unchanged):** the merged H1 delivery recorded 72
unit/integration cases plus its model-only Rustdoc example. Test headers
separate narrowly adapted native sort/query/registration/version/disable
observations from processor-derived exact matrices and Rust admission/ownership
tests. The source scenario inventory is:

- [host/runtime](../../crates/libs/fube-hosting/tests/host_runtime.rs): reply/state/membership/validity/proxy distinctions, held Start/Finish, timeout interleavings and tentative cleanup.
- [type state](../../crates/libs/fube-hosting/tests/type_state.rs): declaration prefix failures, scoped stored query observations, independent registration during grace, Start/Finish matrices, sources, disabled recovery, missing/late work and invariant boundaries.
- [events/bounds](../../crates/libs/fube-hosting/tests/events_bounds.rs): runtime versus host closure (including disabled-in-progress failure retention), shared sequence/first scope, independent lanes and complete rollback under queue/byte pressure.
- [lookup](../../crates/libs/fube-hosting/tests/lookup.rs): full candidate/error/sequence and package-outcome matrices, casing/provenance, scoped identities, pending End and shutdown admission.
- [RA bridge](../../crates/libs/fube-hosting/tests/ra_bridge.rs): actual H1 completion candidates through issued RA lookup IDs, diagnostic NotFound (unacquired), mismatch policy, exact simulated disposal/replay/recreation, retained simulated usage and actual first-spelling notification payloads.
- [identity differential](../../crates/libs/fube-hosting/tests/identity.rs): all 65,536 production comparison operands and representative structured pairs agree with RA's already-evidenced helper. This is Rust-to-Rust regression linkage, **not new independent native H1 execution**.

The test-only event-derived candidate still fulfills an issued RA completion;
it does not port unsolicited native Hosting event routing. Missing live RA
RuntimeClosed/Enabled/Disabled consumers remain gated, and RuntimeClosed is
never silently translated to HostDown. `require_live_composition` rejects live
admission. H2 now supplies actual admitted acquisition/release and scheduling
in its separate ledger below. This H1 delivery's historical evidence predates the
separate registration-events and [owned local activation](HostingLocalActivation.md)
profiles; neither enables production RA/RAP.
The H1 delivery ran no complete native
Hosting handler execution, new codec oracle, wire interoperability or deployed
process qualification was run. Existing test-host/native evidence is unchanged;
it is not relabelled for H1.

### Supplied port contracts

The [public contract Rustdoc](../../crates/libs/fube-hosting/src/contracts.rs)
is the semantic reference, with pinned **implementation and test** links,
supported-profile and ownership distinctions. Native source inspection is not
a new native execution PASS. The original H1/RA/native-helper/test-host evidence
above remains evidence at its original revisions.

| Contract | Supplied implementation/profile | API and semantic comments |
|---|---|---|
| `RegistrationLookup` | Borrowed H1 controlled adapter or H2 actual scheduling adapter; activation still controlled, no live adapter. | [contracts.rs:94–215](../../crates/libs/fube-hosting/src/contracts.rs#L94-L215) |
| `PackageUsage` | Actual admitted `Residency`; exact nonclone full/residual acquisition. | [contracts.rs:219–328](../../crates/libs/fube-hosting/src/contracts.rs#L219-L328), [residency.rs:1341–1358](../../crates/libs/fube-hosting/src/residency.rs#L1341-L1358) |
| `PackageScheduling` | Narrow H2 implementation and closed bounded disposition; separate from physical activation. | [contracts.rs:414–491](../../crates/libs/fube-hosting/src/contracts.rs#L414-L491) |
| `PackageLifecycle` | Definition-only activation/End, extends scheduling; **not** implemented by H2. | [contracts.rs:333–412](../../crates/libs/fube-hosting/src/contracts.rs#L333-L412) |
| `HostingEventSource` | Direct `Hosting` implementation, two owned H1 lanes. No subscriptions, live status/runtime-close routing or delivery ACK. | [contracts.rs:493–600](../../crates/libs/fube-hosting/src/contracts.rs#L493-L600) |

Lookup keeps outer admission versus inner native result/candidate/sequence,
request identity versus producer runtime fields, full-version/scheduling-derived
mismatch/NotFound and unwritten sequence. The H1 adapter borrows explicit
observations and invokes the unchanged core path; it does not call external
package code under projected mutation or treat missing outcomes as success.
See the [runnable boundary example](../../crates/libs/fube-hosting/src/lib.rs).

Usage's owned acquisition/reconciliation type is a **local ownership adaptation**,
not a wire/RA-issued lookup ID. Increment distinguishes managed acquisition,
explicit adhoc no-residency, proven rejection and unresolved partial mutation.
Release returns exact unresolved ownership; verified local release is stronger
than native public void. Dropping an observer/token is not async cleanup, and
retrying an uncertain decrement can underflow. RA still owns reference attachment,
retained residency, precommit compensation and postcommit disposal policy.

Lifecycle distinguishes synchronous End from owned pending activation and
consuming final observation, returning ownership on noncompletion. Scheduling
eligibility, timer admission and physical deactivation are different milestones;
source-specific grace/sequence checks are not universal event fences. No
observer-timeout-to-success or cancellation rollback is supplied.

Events preserve all five typed variants, sequences, first affected scope and
producer spelling; lanes can drain independently. Native subscription removal
does not drain already snapshotted callbacks, native shutdown may drop callbacks,
and H1 finite admission is a separately documented safety adaptation. No
RuntimeClosed-to-HostDown translation or silent Enabled/Disabled drop is enabled.

**Contracts test inventory:** four reusable actual-adapter lookup cases and two
actual-Hosting event cases are added in existing
[lookup.rs:10–250](../../crates/libs/fube-hosting/tests/lookup.rs#L10-L250) and
[events_bounds.rs:8–138](../../crates/libs/fube-hosting/tests/events_bounds.rs#L8-L138).
They exercise trait dispatch, provenance/candidate-error combinations, absent
premises/unwritten sequence, pending End, atomic admission and typed owned
transfer—not mock-success or compile-only conformance. Comments distinguish
native-test adaptations, processor-derived combinations and Rust ownership/
admission checks. The original contracts delivery recorded **78 ordinary cases** (original 72
preserved) and **two model doctests**. These are native-free contract regressions,
not H2/H3/H4, native-handler, codec, wire or deployed qualification.

**Original contracts validation (historical):** affected H1 tests and examples passed. Locked
all-feature workspace check, Clippy (warnings denied), binary/example build,
warning-denied Rustdoc and serial tests passed: 1,040 ordinary cases, five live
cases ignored, and 38 doctests (including H1's two). The separate doctest gate
also passed; those counts overlap, not additional unique tests. No new native
or live execution was performed. The known intermittent demoted-role-stream
sample test passed on the first workspace run here; it was not modified or
claimed fixed by this work.

### Implemented H2 accounting and scheduling

H2 starts from merged H1/contracts base
`f3bb6a045e4b01770563fc925953c4fe5e72ff4d`. Native source/test bodies remain
pinned to `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; they were inspected,
**not executed**. This is real in-memory accounting, not package materialization,
physical activation or a running RA/RAP node.

| Implemented surface | Rust source | Pinned native authority |
|---|---|---|
| Admitted application/package/context identity, first public-map metadata, frozen full version/numeric instance | [residency.rs:490–621](../../crates/libs/fube-hosting/src/residency.rs#L490-L621) | Deactivator.cpp:293–328,1148–1178; HostingSubsystem.cpp:22–180; ServicePackageInstanceIdentifier.cpp:74–109 |
| Package-first split increment and exact residual, application-first pair release, locally verified package-only compensation | [residency.rs:657–910](../../crates/libs/fube-hosting/src/residency.rs#L657-L910) | Deactivator.cpp:37–103,330–394; HostingSubsystem.cpp:1349–1392 |
| Full current grace, timer escrow, strict newer replacement and ≤ removal; independent scan/guards/final ownership | [residency.rs:913–1223](../../crates/libs/fube-hosting/src/residency.rs#L913-L1223) | Deactivator.cpp:113–166,445–527,735–815,912–969,1078–1100,1264–1394 |
| Actual scheduling-derived mismatch/NotFound with pre-reserved closed result | [lookup.rs:253–305](../../crates/libs/fube-hosting/src/lookup.rs#L253-L305), [contracts.rs:466–491](../../crates/libs/fube-hosting/src/contracts.rs#L466-L491) | FabricRuntimeManager.cpp:381–429; ApplicationManager.cpp:1079–1093 |
| Real acquired-reference **test composition**, original completion/tickets, exact correction/withdrawal and retained disposal batches | [residency_bridge.rs:139–619](../../crates/libs/fube-hosting/tests/support/residency_bridge.rs#L139-L619) | ServiceTypeRegistrationWrapper.cpp:78–162; Hosting.HostingAdapter.cpp:83–203; EntityJobItem.h:228–278; local ownership adaptation |

**Supported profile:** driver-admitted managed application/package entries begin
unused at count/sequence zero and injected Now. Shared and exclusive partition
contexts support **accounting**, not exclusive execution. Increment never
creates a missing record. Different full versions or numeric instances cannot
replace an admitted record. Native package/context comparisons, rendered-CI
public-map aliases, UTF-16/NUL and first spelling remain intact. Adhoc usage is
explicitly unmanaged. Admissions are not physical deployment certificates.

Package increment changes count/sequence before application increment. The split
begin/finish seam exposes application closing after package mutation:
`Unresolved` retains the exact package-only owner, rather than pretending a
mutation-free rejection or rollback. Normal release verifies application-first
pair decrement. Explicit package-only compensation requires that residual's
unchanged sequence, nonclosing package and verified ownership counts; otherwise
the same owner remains unresolved. These are **local verification/ownership
rules**, not native opaque tokens or a native reconciliation algorithm. Public
native decrement is void; internal diagnostics and local `Released` are distinct.
Drop never performs asynchronous release; shutdown retains unresolved owners.

Shared/application grace defaults to 60 seconds, exclusive package grace to one
second, unused scan to 600 seconds. Each is explicitly finite or infinite.
First use sets unused-since MaxValue (represented by None); last use records
Now. Ordinary last application release does not schedule application work.
Independent scan selects old-enough unused applications first, otherwise
packages; matching application work can enter closing while packages remain.
Changing grace/debug observations affects new attempts, not old deadlines or
callback guards. Debug suppression applies to package scheduling only.

Eligibility is not timer admission: zero usage with infinite grace gives
successful ineligibility/mismatch; finite eligibility plus debug/closed-queue/
budget suppression still gives NotFound with a candidate. Closing accounting
returns an error, not successful ineligibility. Strictly newer timer sequences
replace; removal/cancellation accepts stored sequence ≤ incoming. Dequeued
callbacks separately require exact current sequence, zero usage and not closing;
package access additionally requires an open/nonclosing application. Successful
guard retains **final-deactivation obligations only**; the accounting scheduler
does not execute physical cleanup. The later
[local owner and explicit completion seam](HostingLocalActivation.md#cleanup-boundaries)
now retire exact package/application finals after owner-supplied cleanup premises,
without claiming H2 independently observes processes or implements native recovery.

`H2LookupAdapter` reserves the maximum fixed-payload scheduling outcome and all
H1 outputs before one external call. No fallible publication follows that call.
Controlled H1 lookup remains available; H2 does not fake an entire lifecycle to
obtain scheduling. First request ServiceName/ApplicationName/PublicActivationId
are byte-accounted and preserved; explicit bindings without names leave them absent.

**Finite ownership:** defaults are 128 applications, 256 packages, 1,024
acquisitions, 256 timers, 256 callbacks, 512 final obligations and 4 MiB logical
state. Timer admission reserves downstream callback/final capacity and work
bytes conservatively. Latest-attempt diagnostics occupy preallocated entry
slots, not an audit log/retry worker. Shutdown stops new accounting/work admission
without erasing earlier tokens, callbacks or finals. Logical budgets are not RSS
or allocator promises; composition separately budgets drained work.

The new [RA residency target](../../crates/libs/fube-hosting/tests/ra_residency.rs)
uses actual `Residency`, `Executor`, `RegistrationCompletion` and `Store` with
real `MemoryBackend` under controlled gates. Its bounded owner reserves original
targets/snapshots/completions/tickets, terminal IDs and downstream effects before
acquisition/submit/drain. Fresh issued lookups acquire once; retained reopen
usage reuses its exact owner. Ticket success alone is not adoption: committed
usage and exact RA disposal are authoritative. Consumed-issuance evidence survives
pre-collection replay; delayed disposal cannot be locally abandoned. Failed memory
publication retains correction ownership; explicit abandonment withdraws the same
offer before exact compensation. Drained actions stay owned even on classification
or release errors.

FSR attachment remains **memory-only**, unchanged RA policy/store format.
Definite/unknown **durable close** tests use original CompleteClose work and an
already adopted owner: late Written releases postcommit; late definite failure
retains usage. Observer cancellation cannot infer rollback. This is not failed
durable-attachment evidence or a production node adapter. RAP/host observations
remain controlled, live event consumers remain gated, and the original
[ra_bridge](../../crates/libs/fube-hosting/tests/ra_bridge.rs) still simulates usage.

**Current H2 scenario inventory:** 24 [residency](../../crates/libs/fube-hosting/tests/residency.rs)
cases plus two private sequence-overflow cases, six
[actual lookup](../../crates/libs/fube-hosting/tests/lookup_residency.rs) cases and
17 real-accounting RA cases. Headers distinguish native-test adaptations
(wrapper add/down/close assertions) from processor-derived partial/race/scheduling
combinations and local Rust ownership/admission/unknown-store tests. No native
handlers, new codecs, live IPC/leases, process launch or deployed interoperability
are qualified. H3/H4, successful-empty-endpoint runtime alignment and live
consumer routing remain prerequisites, not H2 successes.

**Qualification-blocker correction:** a separately approved narrow correction
preserves the oracle's direct synchronous COM Invoke/once-only End while
[handing off its owned multithread worker](../../crates/samples/native-replicator-oracle/src/async_context.rs#L340-L357).
The bounded [data model host](../../crates/libs/fube-data/tests/support/rap_model.rs#L264-L417)
now retains original pending callback results, then observes exact ReplicaClose
before unchanged guarded runtime shutdown. Its causal test holds an actual
provider epoch callback, not a cached-status read or sleep. Archived baseline
execution reproduced the oracle hang; the original SDK selector passed in that
baseline, so a preexisting SDK failure or H2 noncausation is not inferred.
Earlier failures remain part of local validation history. Neither correction
enables physical Hosting, live RA/RAP or new native interoperability.

The first callback/shutdown correction's Close sequencing was necessary but
insufficient: independent acceptance reproduced the original SDK target failure.
Its isolated first panic was loop-end guarded shutdown returning Closed, after
the bootstrap/replication/copy assertions, not an assumed earlier bootstrap
assertion. A subsequent [runtime dispatcher correction](../../crates/libs/fube-runtime/src/lifecycle.rs#L274-L349)
preserves unregister IPC when exact Closed from already-selected admission/
owned completion races the requested retirement. It keeps the original owned
drain, authority retirement, other errors and active-callback fencing intact.
Controlled old/new race tests and original full/repeated SDK evidence qualify
that closure; old candidate passes remain historical, not parent acceptance.

### Implemented H3 bootstrap-only local IPC

[`ipc::Server`](../../crates/libs/fube-hosting/src/ipc.rs) is a separately owned,
dedicated-executor service for externally prepared activated single-code-package,
SharedProcess, noncontainer/nonactivator ExeHosts on clear loopback TCP. The caller
owns process launch, actual PID admission and validated package/node inputs.
Claimed IPC headers are not authenticated OS peer credentials or a same-user sandbox.

The implemented route is application-initiated ListenInstance → actor-7
Start/Finish → actor-9 runtime registration → actor-15 empty-action/body correlated
polls. Full native Start/runtime scopes are preserved in
[`wire`](../../crates/libs/fube-hosting/src/wire.rs), with explicit projection
into H1 and operation-specific native errors. Runtime/host unregister retain
primary/state/cleanup outputs. Replies remain on the original accepted stream;
there is no enabled general reverse host-control dispatcher.

`Admission::deadlines` explicitly distinguishes Rust's positive propagated
Timeout header from native IpcClient's caller-local deadline (no Timeout header).
The latter was observed on actual native frames and traced to
`Transport/IpcClient.cpp:201–239` at the pinned native source. It is not a default
for malformed Rust requests: present expired/malformed headers always fail.
Both profiles retain finite frame/write, registration and local authority bounds.

The default local duration is **30 seconds**, configurable only to a finite
1 ms–30 s duration. Initial TTL is informational: completed host, owned valid
runtime and a fresh correlated poll are required. Polls bind a producer GUID and
increasing index, permit gaps and reject replay/GUID changes. Atomic terminal
authority checks precede renewal and frame start. `HostLease::stop_renewal` permits
only the remaining prior grant; `revoke` closes authority/I/O without waiting for
registration cleanup, callbacks, diagnostics, RA or process teardown. Expiry/EOF/
registration timeout/replacement cannot revive an old session. Already-started
or buffered frames are not retroactively recalled; the runtime retains its
request-send-relative TTL and independent terminal fence.

One-use incarnation admission, timer disposal and per-operation target checks
prevent old/foreign host or runtime IDs from mutating a successor. Release/new
admission is explicit and fails closed while old work/results/cleanup failures
remain. Connections reserve 4 MiB of logical scratch and admissions 512 KiB;
decode/projection/reply budgets and slot counts are checked before excess work.
`take_outputs` synchronously transfers owned H1 results/events/obligations.
Transfer is not live RA delivery ACK, process cleanup or Residency release.
Full closure capacity can retain membership and a cleanup failure while authority
is already terminal. The H1/H2 semantics and live-composition rejection remain.

**New qualification (2026-11-01, `1a1a51fd215d0b237f1f0d63902e5ef3d02a951f`):**
23 [Hosting socket cases](../../crates/libs/fube-hosting/tests/ipc_leases.rs)
exercise malformed/truncated requests, identity/target/freshness rejection,
deadline/write pressure, expiry/revocation, EOF/replacement and owned closure.
Four [actual Rust runtime child profiles](../../crates/libs/fube-runtime/src/hosting_tests.rs)
observe real frames: bootstrap with three correlated polls and orderly shutdown,
finite stop-renewal, blocked executor and blocked capture destructor with exit 70.
The [new native `hosting-leases` profile](../runtime/RustTestHost.md#27-production-hosting-lease-qualification)
observes two finite replies, actual runtime unregister and graceful reaped/group-
drained exit through production Hosting. All five original native fixture profiles
also ran anew from the verified exact-head bundle; their lifecycle evidence still
belongs to the fixture, not production Hosting.

In this bootstrap-only profile, type/factory registration, health/load/fault,
lifecycle, context-update/reconnect/security and other host profiles reject rather
than return generic Success. The later factory-registration and local-activation
profiles are documented separately; production RA/RAP and stateful Hosting remain
out of scope. Actual native
application frames are not complete original native node-handler execution,
deployed interoperability or distributed Federation lease parity. Native expiry/
hard-process fencing is not inferred from the short native bootstrap run.

## 10. Deferred work and invariants

This design adds no database, loader, durable Hosting outbox or restart/adoption
protocol. Initial recovery follows the cluster's fresh-deployment/fencing
policy; reconstructing a registration from disk is not permission to trust a
live process or resume its lease. It does not change `fube-data` volatility.

Stateful Hosting integration, all required RA runtime-event consumers, upgrades,
broader package/activation profiles, health aggregation, remote security and
owner/process-loss qualification remain explicit staged work. Full native
Hosting and deployed SF interoperability are not implied by crate extraction,
native-derived unit tests or the eventual stateless end-to-end milestone.

The invariant through every slice is the same: **Hosting reports actual
registration/process/package facts and performs owned operations; RA decides
replica lifecycle under the native reliability contract; the application RAP
executes service callbacks under finite local authority.**

[native-interface]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/IHostingSubsystem.h#L34-L168
[native-host-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostRegistration.cpp#L39-L158
[native-host-manager]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1715-L2090
[native-host-finish]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L2011-L2054
[native-host-validity]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1292-L1300
[native-host-timeout]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1966-L2002
[native-lookup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L117-L249
[native-lookup-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L431-L458
[native-registration-fields]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeRegistrationHelper.h#L8-L23
[native-registration-event]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L1581-L1591
[native-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L383-L458
[native-runtime-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L501-L902
[native-type-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp
[native-type-status]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStatus.h#L10-L29
[native-unused]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L1079-L1109
[native-deactivator]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp
[native-application-manager]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp
[native-lease]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingSubsystem.cpp#L2124-L2156
[native-ra-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingAdapter.cpp#L47-L203
[native-ra-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ServiceTypeRegistrationWrapper.cpp#L78-L156
[native-ra-type-map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.ServiceTypeMapEntity.cpp#L16-L79
[native-stateless-end-open]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ComProxyStatelessService.OpenAsyncOperation.cpp#L16-L35
[native-stateless-open-consumer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.OpenInstanceAsyncOperation.cpp#L134-L174
[runtime-empty-open]: ../../crates/libs/fube-runtime/src/lifecycle.rs#L845-L911
[runtime-empty-tests]: ../../crates/libs/fube-runtime/src/lifecycle_tests.rs#L1054-L1161
