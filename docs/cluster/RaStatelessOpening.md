# Native-derived stateless opening

**Status:** bounded `fube-ra` execution with **trusted simulated Hosting/RAP**.
Not live processes, original native-handler execution, a running RA/FM,
deployed interoperability or whole-RA parity. Native source pin:
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
[Removal](RaStatelessRemoval.md) and P1 ticket/fencing guarantees are preserved.

## Adapter and ownership contract

The [executor commands/effects](../../crates/libs/fube-ra/src/entity.rs#L190-L348)
separate registration lookup (`Open`), committed runtime handoff (`RuntimeOpen`)
and semantic `AddInstanceReply`. An issued lookup ID identifies exactly one
callback obligation, independently of lifecycle token and host/runtime strings.
`Open.usage`/`FindRegistration.usage` identifies retained package usage, or asks
for a new lease when absent.

[RegistrationCompletion](../../crates/libs/fube-ra/src/lifecycle.rs#L19-L39)
contains the acquired registration, returned version, upgrade-map observation,
usage-increment outcome and a separately transferred usage acquisition. Its
successful result certifies that the candidate matches the requested service
type/activation and that required package usage was acquired or retained.
These are trusted adapter assertions, not a live Hosting implementation.

New usage is owned by that lookup ID. Overlapping lookups can return distinct
acquisitions even with identical host/runtime strings; an unused acquired
candidate releases its **own** registration and any own usage. Replaying a
consumed callback cannot release active resources or change node reference
counts. Failed usage increment releases the candidate but does not increment
the lookup failure count or clear an existing warning. Registration and package
usage have separate lifetimes: opening host-down releases registration but
retains usage until final closure.

Submission is not unconditional acquisition transfer. Rejected/failed callbacks
(including capacity veto) leave the issued lookup and adapter-owned offered
acquisition intact for correction/retry. Once consumed, accepted work owns its
release/publication even if its observer is cancelled. Unissued callback
acquisitions remain the caller's responsibility. Outstanding lookup obligations
remain bounded (default 64/entity), including across removal/recreation.

[OpenCompletion](../../crates/libs/fube-ra/src/entity.rs#L190-L200) carries
current registration provenance, flags, error/message/API identity and a trusted
cleanup witness. A terminal negative Abort result requires proxy cleanup proof,
including queued/in-flight Open, not merely a failed user API or a termination
callback. Observed matching host-down is separate cleanup evidence. Close
success retains its existing trusted cleanup contract; termination completion
still does **not** mean observed host-down.

## Native policy, not a generic failure status

`RA/` below is the pinned `src/prod/src/Reliability/Failover/ra/`;
`FC/` is `src/prod/src/Reliability/Failover/common/`.

| Authority | Ported behavior |
|---|---|
| `RA/Hosting.FindServiceTypeRegistrationErrorList.cpp:35–65` | New-instance retry allowlist: deployment/activation in progress, Hosting service-type-not-registered, NotFound, application/package/registration version mismatch and runtime-not-registered. Disabled type, deleted application and unknown errors are immediately terminal. [Exact HRESULTs/policy](../../crates/libs/fube-ra/src/opening.rs#L5-L113). Caller `retryable` applies to the preserved Close seam, **not** this Open policy. |
| `RA/ReconfigurationAgent.cpp:986–1033`; `RA/FailoverUnit.cpp:5104–5130,5919–5924` | Returned registration-version mismatch may become success if no upgrade entry exists or its instance is no greater than the returned candidate. Both service/local-replica versions update before runtime work and node reporting, also on Close reacquisition. Original admitted package is remembered separately so a lost Ready reply followed by the unchanged Add can still return current Ready success. Unrelated service/epoch updates remain unsupported. |
| `RA/RetryableErrorState.cpp:21–85`; `RA/RetryableErrorStateThresholdCollection.cpp:21–26,42–47`; `FC/FailoverConfig.h:542–557,599–621` | Independent counters advance on accepted failures, not sends/time. Equality precedence: warning, drop, restart, error. Registration/Open consumers ignore restart/error actions; they do not invent host restarts or error-health reports. Success clears when old count >= warning **or** error, then resets. |
| `RA/FailoverUnit.cpp:5143–5197,5250–5369`; `RA/ReconfigurationAgent.cpp:3782–3828` | RAP negative results all retry at this layer, unlike the registration allowlist. At count=drop−1 requests use Abort=4; only exact Abort replies are accepted there. Wrong flags, including success, do not advance counters or emit effects. Repeated accepted negative replies can count again; no invented per-send native ticket. `RAProxyStateChangedOnDataLoss` follows the success path and normalizes to Success (its native named scenario is stateful, not stateless data-loss qualification). |
| `RA/FailoverUnit.cpp:5281–5369`; `RA/ReconfigurationAgent.cpp:3837–3899` | Ready success updates endpoints, clears applicable Open health and replies using Ready/current version. Registration and usage remain held. |
| `RA/FailoverUnit.cpp:5199–5248,5610–5657` | Terminal failure captures the opening description, closes, releases registration then usage, emits Close health, then AddInstanceReply with the **original error and captured opening replica**. No substitute ReplicaUp/dropped success reply. Internal retry membership is represented by phase; closed/closing due retries cannot reopen. |
| `RA/ReconfigurationAgent.cpp:1138–1183`; `RA/FailoverUnit.cpp:5258–5279` | Opening host-down invokes the Open failure helper before ordinary stateless close logic. Below drop: optional Open warning precedes registration release, usage retained, registration counter reset, no immediate new lookup/Open/FM reply. At drop it closes/replies with ApplicationHostCrash, without requiring an Abort packet. |
| `RA/ServiceTypeRegistrationWrapper.cpp:78–163,221–233` | Replaceable registration/map reference and retained package usage are distinct. The Rust session additionally changes its open token after host crash to fence pre-crash lookups; late runtime replies must match the new committed acquisition. This is stronger adapter provenance, not added native wire fields. |
| `RA/FailoverUnit.cpp:4760–4778` | Close sends a canonical replica description (default invalid package version) plus the **current selected service version**. Do not copy the full local replica into that request or weaken prior removal guards. |

Defaults: Open warning **10**, drop **40**, restart/error `i32::MAX`;
new-registration all four thresholds `i32::MAX`. Close keeps its separate prior
policies. Settings are immutable per executor, positive finite native-int
thresholds; tests use small values. Explicit retry inputs simulate due work,
not automatic timers or a count of lost replies.

Opening health preserves original HRESULT and ProxyErrorCode message/API
identity (`api`, `interface`, `metadata`), separately from registration warnings.
Close health denotes entity deletion, not a successful create. Effects are owned
obligations; no live health service or descriptor/TTL delivery is implemented.
The foundation's legacy CompleteOpen internal RA-service-not-registered input
only records pending status; it is **not** a RAP reply or an issued lookup
result. Actual Hosting service-type-not-registered has its distinct HRESULT.

## Shared node registration notifications

The [coordinator](../../crates/libs/fube-ra/src/notification.rs#L7-L232) is one
volatile executor-owned ledger, **not one report per partition**.

- Native identity: application number then case-insensitive application/package/
  type names; when both application identifiers are empty, package name does
  not distinguish types (`ServiceModel/ServiceTypeIdentifier.cpp:78–118`,
  `ApplicationIdentifier.cpp:86–123`). Execution supports non-NUL **ASCII key
  names** only, matching the pinned PAL `wc16.cpp:137–155` bounded comparison
  profile. Non-ASCII keys explicitly return Unsupported; wire strings remain
  lossless UTF-16 and are independently tested. No unverified Unicode fold.
- Every attached registration increments shared references. The first id spelling
  remains. A different **full version**, even a downgrade, replaces version/
  code name and starts a fresh pending round; same-version code-name changes do
  not. Removing a reference does not fall back to another owner's version;
  zero removes the entry without a disabled notification
  (`RA/Hosting.ServiceTypeMapEntity.cpp:28–80`).
- Retry sends actual `ServiceTypeNotification`. Sending never acknowledges.
  Replies clear only listed native keys whose **version instance number**
  matches, even if rollout or code name differs. Unknown/removed keys are
  ignored; partial replies retain the rest
  (`RA/Hosting.ServiceTypeMap.cpp:57–145`).
- Defaults: 100 infos/batch, 5s/node minimum, 45s/type minimum, 30s retry
  scheduling (`FC/FailoverConfig.h:412–422`). Trusted monotonic `now_ms` inputs
  drive work; backward observations reject. All-clear work stops scheduling.
  Version replacement clears the per-type throttle. This implementation
  serializes snapshot/send-record generation under the publication lock; it
  does not claim native asynchronous captured sends cannot outlive removal.
- `ServiceTypeReply` validates the configured normal-FM source, not a newly
  invented FT-generation ack predicate. Factories/decoders preserve real actions,
  request FM actor 11/no generation, reply RA actor 12/actual FM generation,
  and **false** priority. No generic successful status/transport ACK replaces
  the body. Federation idempotence/timeout routing is not implemented by this seam.

Shared capacity is finite: default 64 live/reserved types and 256 KiB batch
bytes, alongside existing command/queue/effect budgets. Reservation conservatively
checks the possible retained first identifier plus replacement payload across
in-flight same-key candidates; a later commit cannot create an unsendable
entry. Codec-ceiling/byte overflow ends the batch at its valid prefix.
Precommit reservations and
[atomic node/state/effect publication](../../crates/libs/fube-ra/src/entity.rs#L1044-L1148)
avoid a second fallible enqueue. Definite failure publishes nothing; unknown
retains the original P1 ticket, node ownership and reservations until resolution.
Node work uses a separate command route, not a fake FT/partition. Node effects
use a zero entity field as an envelope marker; their type—not that field—routes
them, so a real all-zero partition still has its own entity worker.

## Storage, tests and reproducibility

Internal aggregate **format 2**, record type `ra/entity/v1`, is unchanged and
has no loader/migration/restart contract. Counters, requested package,
registrations, usage and node ledger are session data, not durable recovery.
Native storage selection is preserved: lookup retry/adoption (including terminal
registration retry failure) is memory-only; accepted RAP replies and opening
host-down select persistent Update even for intermediate failures
(`RA/ReconfigurationAgent.cpp:846–882,3837–3847`). Thus a terminal lookup can
leave an older stored opening envelope; **no recovery support is implied**.
Rejected/stale/wrong-flag callbacks do not write. P1's terminal fence is never
reopened/replaced after late success. Effects transferred with `take_effects`
still require an external delivery owner; this is not a durable outbox.

[47 entity tests](../../crates/libs/fube-ra/tests/entity.rs), five coordinator
unit tests plus two existing uncertainty tests, and 14 protocol tests cover the
selected execution surface. Counts are Rust tests, not counts of native tests.
Direct SL scenario authority is `RA/Test.StateMachine.cpp:2572–2852,3082–3194`:
lookup retries/fatal errors/usage failure/warning/clear, runtime retry/final
Abort/success/warning/terminal captured reply. True retryable lookup exhaustion
uses `RA/Test.Unit.ServiceTypeRegistrationWrapper.cpp:613–621`; the named
ExceedThresholdAtFSR test actually uses a fatal error. Host-down/open/removal
compositions are handler-derived stateless translations; native late-open
Close tests are stateful. Node notification native scenario tests
(`RA/Test.StateMachine.ServiceTypeNotification.cpp:158–320`) use SP1, with
state-independent shared helpers—not direct native stateless end-to-end proof.
Rust-only entered/release barriers and real store journals cover definite/
unknown outcomes, caller cancellation, capacity and immutable snapshots.

```sh
cargo test --locked -p fube-ra -p fube-ra-store -- --test-threads=4
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs -v
```

Independent [native-stream qualification](../../tests/ra_protocol/README.md)
retains the prior 29 cases and adds seven notification cases: **36
bidirectional cases, 43 schema types/129 fields, 40 bundle files**. Actual native
stream/helper/field macros with disclosed schema/platform adapters—not full
native handlers. Final evidence requires fresh production at a clean reviewed
HEAD/source digest and compiler-free consumption with independently expected
identity; older/development bundles do not qualify the delivered head.

## Remaining unsupported scope

Live Hosting/RAP/health/Federation, runtime-closed event integration, dynamic
activation/upgrades/service descriptions, non-ASCII execution keys,
FM-service/FMM, open replacement, broad stateful/peer features, initial/last
ReplicaUp upload, physical tombstone GC, restart/replay/adoption/outbox,
process/power-loss and CI-oracle qualification remain unsupported.
The fube-data engine remains volatile. All remaining RA–FM/RA–RA contracts
remain required eventual scope in the [catalog](NativeRaProtocol.md); eight
enabled action codecs are not whole-contract/runtime parity.
