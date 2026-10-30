# Native-derived stateless removal

**Status:** bounded, deterministic `fube-ra` execution with trusted simulated
Hosting/RAP events. Not a running RA/FM, live Hosting adapter, native-handler
execution, deployed SF interoperability or full algorithm-parity claim.
Native baseline: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.

## Interface and runtime boundary

The [existing executor](../../crates/libs/fube-ra/src/entity.rs#L160-L270)
now admits RemoveInstance and a one-FT ReplicaUpReply, plus Registered,
CompleteClose, HostDown, RetryClose and RetryReport events. Protocol types and
distinct factories/decoders are in
[protocol.rs:319–364,418–539](../../crates/libs/fube-ra/src/protocol.rs#L319-L539).
RemoveInstance uses inherited `ReplicaMessageBody` scope followed by its own
force-bool scope. RemoveInstanceReply uses ReplicaReplyMessageBody. ReplicaUp
and its reply retain both FT lists, last and fromFMM flags.

FM commands retain configured source/local NodeInstance and generation admission.
These are additional trusted-session gates, not native cryptographic
authentication. Stateful/persisted/group execution and FMM remain disabled.
Remove does **not** apply Add's upgrade/deactivation veto.

The [registration handoff](../../crates/libs/fube-ra/src/lifecycle.rs#L478-L541)
is explicit: `Open` requests registration only. The adapter may perform runtime
Open **only after** a matching successful Registered event publishes RuntimeOpen.
Success means its registration reference was acquired and both host/runtime IDs
are nonempty. Removal before registration can finish synchronously; late acquired
registration is released without issuing runtime Open. Active opening removal
instead exposes Close or TerminateHost work. Open completion during/after close
is ignored and cannot recreate the entity.

Close success is a **trusted cleanup proof**, including cancellation/draining of
queued/in-flight Open, proxy closure and resource cleanup—not just a successful
callback. TerminateHost means native RemoveReplica termination reason; its
callback is **not** host-down. Obliterate completes only on a matching observed
host-down. Tests simulate those facts; no adapter currently proves them against
real runtime processes. Health/release effects are explicit obligations, not
implemented health/Hosting services.

## Source decisions, not a generic removal machine

`N/` below is the pinned native `src/prod/src/`; `RA/` is
`N/Reliability/Failover/ra/`; `common/` is `N/Reliability/Failover/common/`.
Links to original files are in the
[complete catalog](NativeRaProtocol.md); line ranges are pinned-source evidence.

| Native authority | Ported decision / Rust mapping |
|---|---|
| `RA/ReconfigurationAgent.cpp:1726–1729,777–829`; `RA/infrastructure.FailoverUnitEntityTraits.cpp:204–219,259–275` | Real entry is RemoveInstanceMessageProcessor → DeleteLocalReplica. Generic Remove epoch/instance checks are exempted; helper rejects lower epoch only when nonforce and requires exact replica ID. **Lower instance is accepted**; impossible newer same-ID instance on an open FT is rejected, not successful. [lifecycle.rs:365–476](../../crates/libs/fube-ra/src/lifecycle.rs#L365-L476). |
| `RA/ReconfigurationAgent.cpp:4279–4293`; `RA/FailoverUnit.cpp:48–74,366–391`; `common/FailoverUnitDescription.cpp:19–30` | Missing FT constructs from ID, consistency and service, not full incoming FT fields. Closed epoch advances and higher ID updates ID+instance **before** helper guards; same-ID instance is retained. A silent request can still commit those earlier updates. |
| `RA/ReconfigurationAgent.cpp:821–827`; `RA/FailoverUnit.cpp:4787–4848,4916–5069` | Closed always selects Delete, even force. Open nonforce selects Delete; force selects **Obliterate**, not ForceDelete. Duplicate ordinary close cannot enqueue conflicting work; force may escalate another close. [close helpers:191–257](../../crates/libs/fube-ra/src/lifecycle.rs#L191-L257). |
| `RA/FailoverUnit.cpp:4760–4778,4976–5068`; `RA/ReplicaCloseMode.h:150–171` | Delete marks deleted, suppresses FM report pending work, cancels opening, enters InDrop/Closing; active runtime gets canonical RAP Close with Drop=8. No-registration InCreate finishes synchronously. Obliterate does not newly mark deleted and requests host termination, no RAP Close. |
| `RA/FailoverUnit.cpp:5372–5418`; `RA/RetryableErrorState.cpp:21–68` | Close failure remains pending. Warning/drop compare equality (warning wins ties). Internal drop escalation is ForceAbort; unlike duplicate command handling, it calls StartClose directly and waits for next Abort=4 retry when runtime active. A later ForceAbort completion does not invent an original Delete reply. [completion:565–622](../../crates/libs/fube-ra/src/lifecycle.rs#L565-L622). |
| `RA/ServiceTypeRegistrationWrapper.cpp:245–332`; `RA/FailoverUnit.cpp:5072–5140,5198–5209` | Host-crash close error releases registration; retry requests lookup. Retryable lookup stays pending, warning/drop policy applies; fatal/exhausted lookup finishes through native close helper. Lookup failure is not interchangeable with failed RAP Close. |
| `RA/FailoverUnit.cpp:5472–5544,5610–5657`; `RA/ReconfigurationAgent.cpp:671–706,3903–3945` | Cleanup releases registration, then schedules native reporting if applicable, then Close health; only captured **Delete** produces RemoveInstanceReply after completion. Closed/deleted cleanup-pending tombstones are retained, not physically erased. [finish/reply:128–162](../../crates/libs/fube-ra/src/lifecycle.rs#L128-L162). |
| `RA/ReconfigurationAgent.cpp:1138–1181` | Stateless host-down during Delete can finish as AppHostDown without original Delete reply. Deleted suppresses down/dropped reporting; later Remove retry on Closed replies synchronously. |
| `RA/FailoverUnit.cpp:4015–4046,4111–4123,5798–5854`; `RA/FMMessageState.cpp:99–176,232–289` | Fresh Obliterate finishes Closed/not-deleted with ReplicaDropped pending; RequestReportRetry schedules due timer work, RetryReport emits **ReplicaUp** dropped singleton. Sending does not clear pending. IC invalid epoch is `(0,0)`, native default, not `(-1,-1)`; canonical dropped fields and wire localDeleted=false are preserved. [report:89–126](../../crates/libs/fube-ra/src/lifecycle.rs#L89-L126). |
| `RA/ReconfigurationAgent.cpp:3325–3408`; `RA/FailoverUnit.cpp:5734–5788` | Ack checks outer replica ID. Dropped-stage ack requires wire Dropped, no invented exact-instance guard. Down-stage closed ack requires exact instance/down. Processed-list Closed/Dropped selects Delete (can reply); FM-deleted list selects ForceDelete (no direct Delete reply). [ack:669–719](../../crates/libs/fube-ra/src/lifecycle.rs#L669-L719). |
| `RA/ReconfigurationAgent.cpp:1687–1721`; `RA/FailoverUnit.cpp:189–258,5926–5929,5962–5973`; `N/ServiceModel/reliability/failover/Epoch.h:13–16`, `epoch.cpp:11` | Closed Add with higher instance recreates, clearing deleted/cleanup/report work; equal reports dropped, older reports then StaleRequest. Deleted tombstones suppress config reports. Both initial/recreated creation normalize previous epoch to native invalid `(0,0)` and target/minimum from service. [creation:299–363](../../crates/libs/fube-ra/src/lifecycle.rs#L299-L363). |

## Store ordering and finite work

The [same P1 ticket loop](../../crates/libs/fube-ra/src/entity.rs#L788-L886)
owns each write through unknown outcomes; no replacement ticket/backend/reopen.
Proposals and ordered effects publish atomically only after commit. Definite
failure publishes neither. A client observation deadline can precede the P1
supervisor fence; tests observe the public Lifecycle before claiming that fence.
P1's terminal fence is not lifted by RA after late success.

[Admission:372–473](../../crates/libs/fube-ra/src/entity.rs#L372-L473) reserves
two effects for Add (closed report + stale reply), one for Open completion,
RetryOpen/RetryReport, eight for other lifecycle commands. The actual count is
checked before persistence; retained effects and all outstanding reservations
share the finite budget. Cancellation does not return accepted work's reservation.

Internal aggregate **format 2**, still under `ra/entity/v1`, adds lifecycle,
deleted/cleanup, close token/mode and FM stage fields
([entity.rs:777–831](../../crates/libs/fube-ra/src/entity.rs#L777-L831)).
Registration, retry counters and dispatch bookkeeping are volatile session data.
There is **no loader/migration/adoption support** for either envelope version;
the record type is not a recovery compatibility promise or native disk format.
This is not a durable outbox; effects transferred with take_effects still need
an external owner to deliver/retry them.

Default retry policies: close warning 10, Delete drop 40, other close drop 40320,
registration-at-drop warning 240/drop 1920
(`common/FailoverConfig.h:559–597`). Settings permit finite test thresholds.
RetryClose/RetryReport are **trusted due timer inputs**, not automatic loops or
proof that delivery succeeded. Tombstone retention is unbounded in this session;
physical retention-time GC is not enabled.

## Translated scenario evidence

[The real-executor suite](../../crates/libs/fube-ra/tests/entity.rs) has 30 tests,
with native-source comments and a real Store/MemoryBackend operation journal.
This count includes existing addition/storage invariants, not 30 native C++
tests. Selected scenarios:

| Rust test / scenario | Native scenario authority |
|---|---|
| `native_ready_delete_duplicate_retry_and_close_completion_order` | `Test.StateMachine.Close.cpp:367–384`; `ReplicaLifecycle.cpp:1747–1766,1920–1942` |
| `native_missing_closed_tombstone_identity_epoch_ordering` | `Test.StateMachine.cpp:7210–7218`; `DeleteReplica.cpp:81–94,214–275` |
| `native_open_remove_guards_accept_lower_instance_but_reject_impossible_newer` | `DeleteReplica.cpp:347–386`; entity traits above |
| `native_obliterate_requires_host_down_reports_replica_up_and_ack_list_selects_reply` | `ReplicaLifecycle.cpp:1715–1731,1769–1857,1976–1987`; `ReplicaUpReply.cpp:233–249,336–352` |
| `native_delete_open_races_before_and_after_registration_are_explicit` | `DeleteReplica.cpp:388–400`, shared helper: despite test names, those inputs use DeleteReplica and are nonforce; actual Remove/force racing cases here are helper-derived |
| `native_close_failures_warning_forceabort_and_captured_reply_mode` | `FailoverUnit.cpp:5372–5418`; `RetryableErrorState.cpp:21–51`; shared Close tests `759–830` |
| `native_lookup_failure_and_apphostdown_are_not_generic_close_success` | `ReplicaLifecycle.cpp:1769–1797`; shared persisted-context `Close.cpp:413–464`; actual stateless host-down handler above |
| `native_closed_add_reports_then_stale_reply_and_tombstone_equal_suppresses_report` | `Test.StateMachine.cpp:3567–3589`; source recreation normalization regression uses helper lines above |

Additional tests cover trusted close token/node/identity/epoch fencing, force
escalation while opening/deleting, registration retries, exact stored tombstone
payload, multi-effect reservation, definite/unknown removal and completion
failures, cancellation and immutable old snapshots. Controlled barriers—not
arbitrary sleep—determine ordering. These are **translations and helper-derived
tests**, not execution of original C++ state-machine tests.

## Reproduce and remaining obligations

```sh
mkdir -p build/ra-stateless-removal-tmp
export TMPDIR="$PWD/build/ra-stateless-removal-tmp"
cargo test --locked -p fube-ra -p fube-ra-store -- --test-threads=4
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs -v
```

Independent wire evidence is [29 bidirectional source-baseline cases](../../tests/ra_protocol/README.md):
39 schema types/123 members, actual native stream and field macros, disclosed
schema/platform adapters. Fresh bundle HEAD/source identity must be supplied
independently to compiler-free acceptance. Old `build/ra-native-final-v2` is
previous-work evidence, **not** production of this slice.

Disabled unfinished capabilities: general open/host-down error handling outside
the selected removal race; open higher-ID replacement Drop; service/package
description update; stateful/StandBy/reopen/queued-delete; FMM/system-host
termination; multi-FT/last-upload acknowledgment and initial upload bookkeeping;
physical tombstone GC; live registration/health/runtime transport; restart,
replay/adoption, durable outbox and process/power-loss guarantees.
Unsupported command profiles return explicit errors, not successful no-op stubs.
Only the selected native no-op/ignore branches are successful ignored outcomes.
Every remaining RA–FM/RA–RA contract stays an explicit eventual obligation in
the catalog. `fube-test-host` is prior lifecycle/process art, not already the
general adapter needed here, and `fube-data` remains volatile.
