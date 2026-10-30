# Native-derived stateless replacement

**Status:** bounded source-derived `fube-ra` execution over trusted simulated
Hosting/RAP/health adapters. Not a running RA/FM, live IPC integration,
original native-handler execution or deployed-parity claim.
Native pin: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
[Opening](RaStatelessOpening.md), [removal](RaStatelessRemoval.md) and original
P1 ownership/fencing remain. This slice enables old-identity Drop and FM resend,
not a second replacement state machine.

## Native admission and replacement

`RA/` below is pinned `src/prod/src/Reliability/Failover/ra/`; `FM/` is the
sibling `fm/`. Exact native paths/lines identify source evidence, not executed
C++ tests.

Generic processing-time admission is part of AddInstance. The FM receive
generation lower bound remains before routing, including generation zero.
Under the entity lock, pending Open/Close rejects Add **before** epoch/identity,
description updates and processor upgrade/deactivation checks
(`RA/MessageHandler.cpp:237–244,815–822`;
`RA/MessageContext.h:99–148`;
`RA/infrastructure.FailoverUnitEntityTraits.cpp:17–35,153–184,253–275`).
Configured source/local-incarnation checks are additional trusted-session
assumptions, not Federation authentication. The bounded profile remains
stateless/nonpersisted/nongroup, InBuild/up, normal FM.

After generic admission and existing node prechecks:

| State / incoming request | Native and Rust outcome |
|---|---|
| Opening or Closing | Ignore without description mutation, semantic reply, additional Close or retained replacement payload. |
| Ready, lower current epoch or lower instance | Ignore as stale. Deleted Closed create-intent bypasses ordinary epoch comparison. |
| Ready, greater replica ID, nonstale instance | Drop the **old** identity; incoming higher epoch does not replace old open epoch. Equal instance can start Drop. |
| Ready, non-greater ID, equal instance | Success with **current local** FT/Ready replica/endpoints/version. Lower replica ID is not independently stale. |
| Ready, same ID and greater instance | Native coding assertion; bounded Rust API returns explicit Admission error, never replacement success. |
| Ready, lower ID and greater instance | No replacement/reply; independently eligible description work still applies. |
| Closed, greater instance | Ordinary create/open from the **resent request**, even if its ID is lower; clear deleted/cleanup/FM report state. |
| Closed, equal instance | Dropped report if reportable; no success or recreation. |
| Closed, older instance | Dropped report if reportable, then StaleRequest with local FT/requested replica. Deleted suppresses the report. |

Authority: `RA/ReconfigurationAgent.cpp:1639–1724`;
`RA/FailoverUnit.cpp:189–258,4720–4740,5926–5973`.
Rust: [entity.rs:870–971](../../crates/libs/fube-ra/src/entity.rs#L870-L971),
[lifecycle.rs:315–390](../../crates/libs/fube-ra/src/lifecycle.rs#L315-L390).

Drop is not Delete/Obliterate. It retains old registration/usage during close,
sends canonical old-ID Close with Drop=8, and does not newly set deleted.
Ordinary errors remain pending; existing dynamic Close retry can escalate to
ForceAbort, with Abort=4 on a later retry. Successful finish releases old
registration then retained usage, schedules dropped reporting and emits Close
health. It sends neither AddInstanceReply nor RemoveInstanceReply and does not
reopen (`RA/ReplicaCloseMode.h:45–116,150–168`;
`RA/FailoverUnit.cpp:4760–4778,4916–5069,5372–5418,5472–5544`;
**actual finish consumer** `RA/ReconfigurationAgent.cpp:644–706`).

FM's existing stateless InBuild pending task resends Add; RA does not own a
replacement timer (`FM/PendingTask.cpp:174–190`). Creation does not wait for old
dropped-report acknowledgment. It uses the later request, not the earlier
greater-ID payload. Creation assumes an explicitly **absent application-upgrade
table entry** in this bounded profile; the native nonempty upgrade-map/lock
protocol is not implemented (`RA/ReconfigurationAgent.cpp:750–774,3014–3094`).
Normal subsequent Hosting lookup/returned-version selection still applies.

Ordinary queued jobs are not a semantic replacement queue. Add waiting behind
an unresolved Open/Close commit is evaluated against the actual resulting state
at processing time, not the phase visible when submitted.

## Directly required service descriptions

The [description helper](../../crates/libs/fube-ra/src/description.rs#L16-L100)
mirrors `RA/FailoverUnit.cpp:5660–5699,5862–5973`:

- Exact service name and service instance, strictly higher update version.
  Ineligibility skips only this helper; it does not reject the enclosing Add.
- Copy all description fields and synchronize FT target/minimum. While Open,
  preserve actual local package version. Do not invent extra service-type,
  package or activation-mode equality predicates.
- Ready update captures `UpdateServiceDescription` **before** Drop or duplicate
  reply. Close clears pending, not the already captured action. Duplicate Add
  success does not wait for the RAP update acknowledgment.
- `RetryServiceDescription` resends current data while pending; sending is not
  acknowledgment. Accepted raw completion clears only the volatile flag.
  Generic IPC requires existing/open/up and no pending Open/Close, nonlower
  epoch. Consumer requires pending, success, exact replica ID/instance and a
  **nonlower** description update version. A greater version TESTASSERT is
  diagnostic, not equality-only rejection. Reply name/type/node/package are
  not extra guards; reply data is not copied into the description.

The already-enabled **one-FT** ReplicaUpReply consumer reuses this helper
before acknowledgment/close (`RA/ReconfigurationAgent.cpp:3334–3418`).
A delayed older description is a helper no-op, not an unsupported ack.
An opening replica with runtime may therefore have description work pending;
its raw update reply is ignored until native generic IPC admission permits it.
After opening host loss, pending can remain but actual send suppresses a null
registration, preserving native's real helper consumer
(`RA/ReconfigurationAgent.cpp:1041–1060,3582–3614`). Terminal open failure and
all close paths clear pending (`RA/FailoverUnit.cpp:4989,5610–5638`).

These are trusted local commands/effects, like existing Open/Close obligations,
**not new enabled IPC wire codecs**. Live RAP, NodeUpdateServiceRequest and
broader description protocols remain unfinished.

## Crashes, late replies and ownership

Observed matching host-down during stateless Ready/Closing finishes via
AppHostDown, with ReplicaDown reporting and no old Add reply. During Opening,
existing Open failure policy releases registration but retains package usage
below drop; no immediate new Open or Add reply. At terminal failure it closes
and replies using the captured opening replica/error
(`RA/ReconfigurationAgent.cpp:1138–1181`;
`RA/FailoverUnit.cpp:5258–5279,5422–5424,5490–5529`).
Termination completion is not observed host-down.

Raw Open/Close guards remain [unchanged](RaStatelessOpening.md#native-dispatch-and-raw-replies):
late Open cannot resurrect Closing/Closed; stale old instance after recreation
is ignored. Close uses exact ID/instance and pending state, not an invented
registration/provenance witness. Trusted successful Close still certifies RAP
cleanup of queued/in-flight Open and proxy resources; no live process proves
that premise here.

Semantic description changes cannot change the identity of acquired references.
The existing registration object and opaque usage acquisition own exact old
references, including returned spelling. Drop completion releases those once;
new creation acquires a new residency. Unused acquired callbacks release their
own references; consumed replays do not acquire/release again. Outstanding
lookup obligations survive recreation. These IDs, immutable snapshots, finite
queues and the explicit trusted completion/effect seam are Rust ownership
infrastructure, not stronger native wire predicates.

Add still reserves at most **two effects**: update+Close or update+duplicate
reply; Closed report+stale reply. In this executor Ready always owns a runtime,
so Add cannot synchronously release a no-runtime Ready residency. All effects
are bounded before persistence. Original P1 writes, unknown tickets/private
proposals, terminal fencing and commit-before-effects are unchanged; no
backend reopen, recovery/adoption or outbox is added.

## Tests and independent evidence

```sh
cargo test --locked -p fube-ra -p fube-ra-store -- --test-threads=4
python3 -m unittest tests.offline.cluster.test_ra_catalog \
  tests.offline.cluster.test_ra_protocol_inputs tests.ra_protocol.test_helper_producer -v
```

Current targeted suite: 16 RA units, 69 entity tests, 14 protocol tests,
33 store units, 8 backend tests and one store doctest. Test labels distinguish:

- Direct SL transitioning/staleness constituents:
  `RA/Test.StateMachine.cpp:3470–3482,3552–3604`.
- Shared close/description helper-derived stateless combinations:
  `RA/Test.StateMachine.ReplicaLifecycle.cpp:1342–1357,1747–1761,1914–1929`;
  `RA/Test.StateMachine.UpdateserviceDescription.cpp:393–485,529–590`.
  No dedicated original greater-ID Add→Drop→resent-Add scenario was found;
  combined tests derive from the processor and actual consumers above.
- Rust fault-injection/ownership tests: controlled store barriers for Add,
  close and recreation definite/unknown outcomes, original-ticket retention,
  queued processing-time admission, key-changing descriptions and finite effects.

The [entity tests:1450–2415](../../crates/libs/fube-ra/tests/entity.rs#L1450-L2415)
exercise these combinations. Existing acquisition/fencing/notification tests
remain; the original C++ handlers are not executed.

The enabled RA–FM/RA–RA wire surface is **unchanged**: eight selected codecs,
36 bidirectional wire cases, 43 types/129 fields, six native truncation rejects.
Separate native helper evidence remains 501 trace lines plus all 65,536 UTF-16
units, not Add/Drop/description-handler execution. Follow the
[producer and compiler-free acceptance policy](../../tests/ra_protocol/README.md)
at a **clean reviewed implementation HEAD/source digest**, with new output
paths. Old `build/ra-vanilla-final-260a805` and
`build/ra-vanilla-helper-final-260a805` are previous-HEAD evidence, not this slice.

Live Hosting/RAP/health/Federation, finite lease, upgrade/deactivation protocols,
FMM, stateful/peer choreography, full initial/last upload, physical tombstone
GC, recovery/adoption/outbox and process/power-loss/CI qualification remain
unfinished. Every eventual RA–FM/RA–RA action remains required in the
[catalog](NativeRaProtocol.md); capability-disabled does not mean omitted.
