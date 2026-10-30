# Native-derived stateless opening

**Status:** bounded source-derived `fube-ra` execution, with trusted simulated
Hosting/RAP/health/transport adapters, not a running native RA/FM or process
proof. Source pin: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
[Removal](RaStatelessRemoval.md) and original P1 ownership/fencing remain.

## Native dispatch and raw replies

Generic dispatch is part of the contract. `RA/MessageHandler.cpp:237–244,815–822`
checks generation **before** node/FT routing;
`RA/GenerationStateManager.cpp:29–40` admits generation >= receive generation.
GenerationNumber orders signed number then native unsigned high/low NodeId
(`Reliability/Failover/GenerationNumber.cpp:43–65`). Add, Remove, ReplicaUpReply
and ServiceTypeReply apply that lower bound. Zero is not a bypass, newer is not
rejected for failing equality. Configured source/owner admission remains a
trusted transport/session boundary, not Federation authentication or membership.

[Raw Open results](../../crates/libs/fube-ra/src/opening.rs) carry native
unit/replica/error/detail/flags, not an extra cleanup boolean or acquisition
packet witness. RAP copies the request replica and changes its endpoint
(`RA/ReconfigurationAgentProxy.ActionListExecutorAsyncOperation.h:33–35`,
`.cpp:346–357`); **RA** establishes Ready and updates service/replicator endpoints
(`RA/FailoverUnit.cpp:5302–5368`). It does not replace local ids, roles, LSNs or
package versions from arbitrary reply fields.

Actual generic IPC and feature guards remain: existing/open/up, allowed pending
action, nonlower epoch, matching instance, current registration presence and no
StandBy reply for an InCreate. Close additionally checks replica id/instance.
No extra incoming node/package/token/acquisition equality predicates are added
(`RA/infrastructure.FailoverUnitEntityTraits.cpp:288–410`,
`RA/ReconfigurationAgent.cpp:3782–3828,3903–3945`).

At current count=drop−1, a non-Abort reply is ignored before failure processing.
The reverse case is not symmetric: an old valid Abort echo after policy retuning
invokes native `TESTASSERT_IF`, which is disabled by default, then continues
into the helper. Rust follows that production path, not a new unconditional
rejection (`Common/macro.h:55`, `Common/AssertWF.cpp:45–49,116–134`).

The raw trusted RAP result supplies its actual native cleanup premise: RAP
aborts/cleans before emitting terminal failures and can suppress incompatible
results (`RA/ReconfigurationAgentProxy.cpp:1313–1383`). This does **not** prove
a live adapter/process executed cleanup. Termination completion still is not
observed host-down; Obliterate still waits for the matching observed event.

## Registration, retry policy and ownership

Native paths below use `RA/` = `src/prod/src/Reliability/Failover/ra/`;
`FC/` = `src/prod/src/Reliability/Failover/common/`.

| Native authority | Behavior |
|---|---|
| `RA/Hosting.FindServiceTypeRegistrationErrorList.cpp:35–94` | New-instance retry allowlist: deployment/activation in progress, Hosting type-not-registered, NotFound, application/package/registration version mismatch and runtime-not-registered. Disabled type, deleted application and unknown errors terminate creation. Existing/drop registration retries unknown/disabled errors; deleted application is fatal. The old callback retryable bool is not a semantic override. |
| `RA/ReconfigurationAgent.cpp:986–1033`; `RA/FailoverUnit.cpp:5104–5130,5919–5924` | Returned registration-version mismatch may become success if no upgrade entry exists or its instance is <= returned instance. Service/local replica versions update before work/reporting, also on Close reacquisition. Original requested version is retained for unchanged Add retransmission/closed recreation, not general service-update support. |
| `RA/RetryableErrorState.cpp:21–85` | Failure increments then equality-tests warning/drop/restart/error in that order. Warning wins ties. Success-clear uses **current** warning/error thresholds, even if no warning was remembered; raising thresholds can suppress an old warning's clear. Consumers ignore helper Restart/Error actions rather than inventing host restarts or health errors. |
| `RA/RetryableErrorStateThresholdCollection.cpp:21–47,63–95`; `FC/FailoverConfig.h:540–627` | Per-property dynamic signed config: independent Open/Close/Delete policies; registration-at-drop W/D separate, restart/error shared with registration-at-open. Threshold reads are separate property accesses, not one immutable policy transaction. Zero/negative native values are not rejected by a Rust positivity rule. |
| `RA/FailoverUnit.cpp:5199–5248,5610–5657` | Terminal failure captures opening description, closes, releases registration then retained usage, emits Close health, then original-error AddInstanceReply with the captured replica. No substitute ReplicaUp/success-shaped status. Ready success replies with current Ready endpoints/version and retains resources. |
| `RA/ReconfigurationAgent.cpp:1138–1183`; `RA/FailoverUnit.cpp:5258–5279` | Opening host-down increments Open failure count; below drop it removes registration, retains usage and resets registration retry state without immediate new work/FM reply. At drop it closes/replies with ApplicationHostCrash, independently of an Abort packet. Valid previously issued replacement registrations are not universally invalidated. |
| `RA/FailoverUnit.cpp:4760–4778` | Close request uses canonical replica fields (invalid default package version) plus current selected service version. Existing removal modes/reply timing are preserved. |

[NativeConfig](../../crates/libs/fube-ra/src/config.rs) separates mutable semantic
properties from immutable finite resource budgets. Defaults: Open W10/D40;
Close W10/D40320; Delete W10/D40; registration-at-open four INT_MAX;
registration-at-drop W240/D1920 with shared registration restart/error.
`Executor::config().set(state, threshold, signed_value)` changes a property
without resetting entities/counters. Failure, success, last-attempt and
notification checks read at their source-defined points. Undefined native signed
overflow is not emulated: checked wider Rust arithmetic retains finite safety.

Issued lookup IDs and usage leases are Rust adapter ownership bookkeeping, not
native reply fields. Exactly one acquired result belongs to each issued lookup.
An unused candidate releases its own registration/usage; replay cannot release
again. Across opening host-down, usage is retained once and a native-valid
replacement can attach. Extra offered usage is disposed without rejecting its
valid registration. Failed usage increment does not increment lookup failures or
clear health. Rejection/capacity veto leaves the offered callback and issued
obligation with its adapter for correction/retry. Unissued acquisitions are never
silently adopted. No live Hosting/health transport exists; original error,
message and API identity remain explicit owned health obligations.

## Shared node registration notifications

The [coordinator](../../crates/libs/fube-ra/src/notification.rs) aggregates native
service-type references, not one report per partition:

- Comparison uses actual pinned Linux startup **C-locale** PAL behavior:
  UTF-16 code units, NUL termination, `towupper` mappings established by an
  exhaustive original-source probe, and signed-short mismatch ordering.
  Non-ASCII names are accepted; Rust Unicode folding is not substituted.
  Application number precedes name; both-empty applications omit package
  comparison (`ServiceTypeIdentifier.cpp:78–118`, `ApplicationIdentifier.cpp:86–123`,
  `ServicePackageIdentifier.cpp:91–123`, `pal/wc16.c:131–150`).
  Wire strings retain exact fields. Arbitrary external locale initialization and
  malformed raw NUL-app triples that violate native ordered-map preconditions
  are not valid-manifest/deployed parity claims.
- The first **returned registration's** actual identifier/spelling is retained.
  Different full versions, including downgrades, replace only version/code name
  and start a round. Same-version additions change references but ignore incoming
  payload/spelling. Partial removal does not fall back to another owner's version;
  zero erases without a disabled notification (`Hosting.ServiceTypeMapEntity.cpp:16–80`).
- Generic receive-generation admission precedes node ack. Then only listed keys
  with matching **version instance number** acknowledge. Different rollout/code
  names do not strengthen that guard; sending is never acknowledgment.

`NotificationClock` observes native signed 100ns ticks; default Linux clock is
CLOCK_MONOTONIC_RAW, tests inject controlled clocks. A stale/backward observation
is evaluated by native elapsed-time logic, not rejected as a protocol error.
Drive these distinct obligations:

1. `RequestServiceTypeWork {work, after_ticks}`: zero enqueues immediately;
   otherwise arm only the remaining node interval. Requests coalesce while
   pending/executing. Honor it with `CaptureServiceTypeWork`.
2. `ServiceTypeWorkCaptured {work, body}`: send the captured optional real body;
   always complete the work (also for empty/count-zero passes) with
   `CompleteServiceTypeWork`. Captured old data may be sent after replace/remove.
   Old completion cannot throttle a new round or recreate an entry.
3. `ArmServiceTypeRetry` / `CancelServiceTypeRetry`: a separate periodic timer,
   honored with `ServiceTypeRetryTimer`. Set while already armed does not rearm;
   native sequence checks protect cancellation. MaxValue disables; zero/negative
   due-times are clamped to one native tick, not an infinite synchronous loop.

These are source-derived BGM/RetryTimer/EntityRetryComponent contracts
(`Infrastructure.BackgroundWorkManager.cpp:53–225`,
`BackgroundWorkManagerWithRetry.cpp:47–68,120–129`,
`RetryTimer.cpp:46–149`, `EntityRetryComponent.h:96–133`), not live scheduler proof.
Dynamic defaults: 100 infos, 5s/node, 45s/type, 30s retry. Count zero retains
pending without send; negative native int count selects all finitely due entries.
Changes affect actual check/arm read points, not already armed timer deadlines.

Resource bounds remain: 64 shared types, 256 KiB output batch and existing
command/queue/effect limits by default. Capacity projects the **actual retained
report**; a same-version ignored payload fitting inbound bounds is not falsely
charged as an enlarged report. All enabled attachments are memory-only and
project/commit node ledger, entity snapshot and effects under one publication
lock with no await or separately fallible enqueue. Durable detach keeps old
ownership until actual P1 outcome. Captured work/selected contexts remain bounded.

## Storage, independent evidence and limits

Internal aggregate format 2 under `ra/entity/v1` is unchanged, with no loader/
migration/recovery contract. Native FSR retry/adoption (including terminal lookup
failure) is memory-only; accepted RAP Open replies/opening host-down select persistent
Update even for intermediate failures (`RA/ReconfigurationAgent.cpp:846–882,3837–3847`).
Unknown keeps the same original ticket/fence/proposal; later configuration cannot
rewrite that proposal. P1 is never reopened/replaced after terminal fencing.
Effects transferred with `take_effects` still need a delivery owner: no outbox.

```sh
cargo test --locked -p fube-ra -p fube-ra-store -- --test-threads=4
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs tests.ra_protocol.test_helper_producer -v
```

Current tests: 15 RA units, 55 entity scenarios, 14 protocol tests plus unchanged
P1 tests. Direct SL source scenarios, shared/stateful helper derivations and
Rust-specific fault/ownership adaptations are labeled in tests. No arbitrary
sleep controls ordering. The original C++ RA handlers were not executed.

[Independent evidence](../../tests/ra_protocol/README.md) retains the unchanged
36 bidirectional wire cases/43 types/129 fields/40 bundle files, with no consumer
build/install fallback. Separate actual PAL/identifier/retry/generation helper
evidence compares 501 trace lines and all 65,536 units against production Rust,
with native inputs/method hashes and disclosed type/locking/config adapters.
That selected helper/dispatch execution is not complete handler/runtime parity.
Fresh final evidence must bind the clean reviewed correction HEAD/source digest;
old/development bundles do not qualify it.

Live Hosting/RAP/health/Federation, dynamic node/upgrade/service-description
protocols, FM-service/FMM, open replacement, broad stateful/peer features, full
initial/last ReplicaUp upload, physical GC, replay/adoption/outbox and process/
power-loss/hosted-CI qualification remain unfinished. The data engine remains
volatile. All RA–FM/RA–RA contracts remain required eventual scope in the
[catalog](NativeRaProtocol.md); eight enabled codecs are not whole-RA parity.
