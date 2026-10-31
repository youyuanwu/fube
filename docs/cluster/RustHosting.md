# Rust node-side Hosting design

**Status: proposed design. `fube-hosting` does not yet exist.** This document
defines an incremental port, not an implemented API, running node or new
interoperability qualification.

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
There is no production Hosting adapter or running Rust RA+Hosting executable.
The [application runtime](../runtime/RustStatelessRuntime.md) and
[standalone native test host](../runtime/RustTestHost.md) exercise opposite sides
of the boundary; neither establishes the combined Rust RA-to-Rust RAP path.
Dated native evidence qualifies its exact source/profile, not every later change.

Extract selectively, preserving the existing standalone profile:

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

The API shape is a small set of typed operations, results and owned events:
register/unregister, lookup, package acquisition/release, activation and
termination obligations, and closure/registration observations. These are
conceptual roles, not committed Rust signatures. Native lookup errors,
optional returned registration and sequence number remain separately
representable; an error must not automatically erase its candidate.

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

H1 integration tests may reuse the RA suite's explicitly simulated usage/RAP
adapters. They qualify the registration bridge only. No new live RA attachment
is enabled until H2 proves real usage ownership and the required event consumers.
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

### H4: owned activation and real stateless lifecycle

**Dependent runtime-alignment prerequisite:** at the repository baseline,
[`fube-runtime` Open][runtime-empty-open] rejects a successful empty endpoint
with `InvalidInput("open returned empty endpoint")`, aborts the instance and
replies with failure. Native [stateless EndOpen][native-stateless-end-open]
accepts null/empty endpoint results, and its
[Open consumer][native-stateless-open-consumer] becomes Opened on success.
H4 therefore includes a dependent `fube-runtime` alignment slice before its
combined-path qualification; Hosting changes or test-host acceptance alone
cannot satisfy this contract. Reuse the existing RAP and preserve genuine
failure cleanup and authority checks. This design does not implement that fix
or claim the current runtime already supports it.

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

The runtime prerequisite must prove that a successful empty-endpoint Open stays
Open without Abort, a matching duplicate Open returns the same empty endpoint
without another factory/Open callback, and subsequent Close disposes the
instance normally. H4 must also exercise this through real RA/RAP traffic using
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
| **H2: usage and deactivation** | Native package usage/residency, deactivation-if-unused results, grace/cancellation/timer behavior and RA acquired-reference integration. | Tests prove version-mismatch decisions, exact release, retained usage and failed/unknown-store ordering. Required native event consumers are present before live use. |
| **H3: IPC and local leases** | Extract/adapt accepted sessions and Hosting handlers; long-running bounded dispatch and finite local authority. | Actual protocol/lease tests and standalone native-host regression qualify the enabled profile. H3 is not yet a real Rust RA deployment. |
| **H4: process activation and RA/RAP** | Dependent empty-endpoint `fube-runtime` alignment; shared validated deployment/process mechanics, native-free launch and selected native activation policy; real stateless Open/Close/Drop/resend. | Empty-endpoint Open/duplicate/Close passes runtime and real RA/RAP qualification. Independent app/resource observations, actual replies and separate exit/drain/reap evidence qualify the combined path, including crashes and lease loss. |

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
[runtime-empty-open]: ../../crates/libs/fube-runtime/src/lifecycle.rs#L783-L849
