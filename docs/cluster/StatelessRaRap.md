# Owned local stateless RA/RAP

**Status:** bounded production local composition, not a deployed Service Fabric
node or FM/HM serving-readiness claim. This extends
[owned activation](HostingLocalActivation.md), using its actual native-free
ExeHost and the existing application-side [runtime](../runtime/RustStatelessRuntime.md).
Native semantic authority is source pin
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; separate SF11.8.121.4 release
regressions are not binaries asserted built from that pin.

The same owner has a separately admitted
[volatile Idle stateful Open/Close profile](HostingLocalActivation.md#opt-in-volatile-stateful-local-lifecycle).
The stateless default and this guide's native FM/replacement behavior are
unchanged; stateful local authorization does not enable those network handlers.

## Composition and admission

[`fube_node::local::Node`](../../crates/libs/fube-node/src/local.rs#L225-L308)
takes an **unstarted** `Activation`, a store and trusted local RA admission.
It creates a dedicated serialized RA executor and binds its original terminal
consumer lifetime **before** activation admission. Hosting remains independent
of RA/runtime/test-host dependencies. There is no second listener, parallel
Connection or direct service callback executor in Node/Hosting.

The profile is Linux numeric-loopback/no-TLS, SharedProcess, one owned package,
declared type and stateless singleton. Start/drive registration first or authorize
an Add while registration is pending. Registration itself does not invoke a
factory. Add preflights exact package/type/version/service, stateless InBuild
identity, immutable description and a **60KiB** canonical-body limit, reserving
4KiB for RAP/RA/frame overhead. Unsupported updates/impossible incarnations
reject before RA submission or acquisition
([admission](../../crates/libs/fube-node/src/local.rs#L477-L562)).
This trusted local input is not an implemented FM network authority.

## Actual lifecycle and ownership

The owner drives committed RA lookup effects against the **same** Activation
registry and retains nonclone H2 acquisitions under issued RA lookup IDs.
Retries may reuse their exact target; a numeric ID cannot reconstruct ownership.
Only committed `RuntimeOpen`/`Close` effects send actor12 `ReplicaOpen`/
`ReplicaClose`. Drop uses flag8, Abort flag4 and ordinary Close flag0. Lifecycle
replies are semantic **one-way** traffic; only RAPQuery uses correlation.
Transport write success is not service success
([lookup/release/effects](../../crates/libs/fube-node/src/local.rs#L791-L1025)).
Accepted IPC uses public furpc Server/ingress services. Hosting retains admission
and lease authority, plus one serialized domain handoff per accepted socket.
Its connection task owns the inline serving future and original-generation Peer;
retirement cancels that future and joins owned send work, releasing the socket
before publishing disconnected state. No listener/advertised address is used to
retarget replies, reconnect or adopt a successor. Local write and socket release
are not RA/RAP semantic acknowledgments. Furpc alone reads and correlates replies.
Authority is checked before lifecycle submission and after local completion, not
at the first positive write; already-admitted output can race revocation.

Hosting privately constructs reply provenance from the exact admitted host/PID/
connection. The adapter checks that source and captured runtime, then RA applies
native epoch/incarnation/state admission, not full packet equality. Empty
endpoints remain successful. Duplicate/stale work cannot recreate an instance or
release a current use. Replacement Drop retires the old identity and requires a
new authorized Add resend
([reply](../../crates/libs/fube-node/src/local.rs#L1026-L1126),
[combined replay](../../crates/samples/hosting-local-app/tests/lifecycle.rs#L324-L417)).

Required local Registered retry, exact RuntimeClosed ordinary Close and
HostClosed crash consumers execute inside serialized RA transitions without
setting `fm_ready`. The old registration-only/deployed consumer profile retains
its gates. Local package unregistration timestamps, already-performed
disabled-health source removal and local status sequence/pending ownership are
consumed separately. FM notification/report work stays visibly `ExternalPending`;
that is **not** an FM ACK. Required health/application reports, FM completion and
deployed lifecycle still reject at construction; unsupported report/control
traffic has no generic successful fallback
([events](../../crates/libs/fube-ra/src/hosting_events.rs#L112-L175),
[local obligations](../../crates/libs/fube-node/src/local.rs#L1128-L1191)).
Native upgrade/reactivation registration-timeout orchestration is not enabled.

## Driving, bounds and errors

Call `drive` repeatedly and drain command/effect receipts. Adapter progress and
15-second monotonic lifecycle retries are **caller-driven**; lease, IPC, RA/store
and process/watchdog supervision remain independently owned. Observation timeout/
drop never cancels accepted work. There are 32 retained command, effect/receipt
and acquisition slots each, and 32 external pending slots; saturation retains
work upstream rather than dropping a drained batch
([progress](../../crates/libs/fube-node/src/local.rs#L662-L789)).

Accepted Hosting connections use bounded furpc frame/handler/send policies within
the existing 4MiB logical connection reservation. One retained lifecycle observer
leaves bounded ingress/response queue headroom. Fixed session GUID plus non-evicting ID
history rejects replay while allowing reserved semantic output to overtake an
older lease RPC. History exhaustion retires the session. Invalid domain input
retires that exact connection, but furpc-discarded unbound/wrong-kind/capacity
input grants no domain authority and need not close it. Reply-data handoff is
neither a write-order barrier nor a first-write fence. Terminal retirement drops
the whole accepted socket, including an uncertain partial reply; that socket is
never reused ([furpc ingress](../../crates/libs/fube-hosting/src/ipc/transport.rs),
[Hosting policy](../../crates/libs/fube-hosting/src/ipc.rs)).

Retain this owner on error/Busy/Unknown. The first accepted RA ticket and original
store operation remain authoritative. A Store timeout **terminally fences that
store incarnation**, even if original accepted work later commits; it does not
reopen admission. An unresolved earlier lifecycle write can therefore quarantine
usage and require recovery outside this profile. Do not replace the worker/store,
resubmit accepted work or manufacture release. The final-Close Unknown regression
proves late original completion/exact release without another durable write or
Store recovery
([regression](../../crates/samples/hosting-local-app/tests/lifecycle.rs#L764-L840)).

## Service retirement versus physical cleanup

Successful Close is accepted only after real runtime disposal. A graceful callback
error follows the existing runtime Abort/disposal path before semantic success;
actual failed/absent semantic completion instead retains RA retry/ownership.
Service Close alone leaves a healthy shared host resident.

[`instance_disposal`](../../crates/libs/fube-node/src/local.rs#L421) exposes a
bounded positive observation for that exact retired instance. It requires a
successful original-runtime Close reply admitted through the original IPC
Entry/lease, its Applied RA command, and release of the original paired
acquisition. Command, RA operation, lookup and acquisition IDs are distinct
lineages, not interchangeable authority. It rechecks current committed identity,
unfenced original owner, drained local work and live unchanged host/session.
A Closed snapshot, Remove response, zero count or PID disappearance cannot
construct it; successor admission invalidates it. Caller-owned external
replica/report escrow and semantic deletion remain separate prerequisites.
The [foreground cluster](MinimalFmCluster.md) joins those prerequisites before
recreation without restarting the FM, Node or app host.

`terminate` is a separate explicit physical cleanup request. With live usage it
can kill/drain/reap yet return Busy, retaining exact accounting and the directory.
Drive actual HostClosed/RA/exact-use work and retry cleanup. If termination
interrupts an acquired Open, matching host retirement leaves its use owned;
the existing owner may authorize **only Drop cleanup** after withdrawal, without
reopening Add/retry/serving authority or decrementing directly on exit.
`quiescent` includes upstream Hosting deliveries, not only adapter/RA escrow.
`close_executor` additionally requires stable producer stop (session stopped and
declaration/source removal complete), so a momentary empty snapshot cannot race
new closure production. External pending FM work is not an ACK. Exit, waitable-leader reservation, group drain, direct-child
reap and directory release remain separate observations
([cleanup APIs](../../crates/libs/fube-node/src/local.rs#L1200-L1212)).
Abandoned owners/escaped descendants have no durable recovery guarantee.

## Native-free qualification

```sh
mkdir -p build/stateless-ra-rap-validation
env -u LD_LIBRARY_PATH -u FabricConfigFileName \
  TMPDIR="$PWD/build/stateless-ra-rap-validation" \
  cargo test --locked -p hosting-local-app --test lifecycle --test activation \
  -- --test-threads=1
```

The actual fixture owns a filesystem resource independently of callback markers.
Its bounded destructor barrier proves **while driving continues** that callback
return alone cannot retire RA or release H2 use. Tests separately inspect native
library/environment absence, no log eviction, real callback counts, resource
existence/removal, empty endpoint, duplicate/stale traffic, replacement resend,
Abort/error fallback, runtime unregister, opening/Ready crashes and lease loss,
original pending/Unknown work, and physical cleanup
([fixture](../../crates/samples/hosting-local-app/src/main.rs#L13-L96),
[qualification](../../crates/samples/hosting-local-app/tests/lifecycle.rs#L180-L1214)).
Hard exit is **not** a destructor receipt; physical directory cleanup is distinct.
Focused response-side tests separately exercise malformed/stale/failed replies,
native-admissible non-full equality and foreign Entry identity. Explicit
`qualification`-feature seams retain opaque real session provenance and cannot
execute callbacks; they are fault injection, **not** combined positive lifecycle
evidence. Actual wire host/PID rejection remains tested at Hosting ingress.
The first rejected semantic frame remains bounded diagnostic ownership while
authority retires and required closure consumers continue.
The [development guide](../Development.md#owned-local-stateless-rarap) lists
changed-boundary and separate release-native gates. No ImageStore, stateful
activation, FM/HM delivery/readiness or deployed-cluster qualification is implied.
