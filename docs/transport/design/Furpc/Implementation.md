# furpc: Implementation, migration, and evidence

**Status:** proposed; see the [design summary](README.md).

Maps existing gaps to owners and describes staged delivery, consumer
migration, acceptance, and unresolved qualification work. These are proposed
milestones, not a record of delivered implementation or qualified support.

## Generic adapter follow-up

**Status:** generic furpc APIs and runtime consumer migration implemented.
The runtime's typed client/server migration exposed reusable body and one-way
adapter gaps, not missing SF-specific transport policy.

The [public body helpers](Codecs.md#public-body-helpers) now share the
whole-body implementation used by RPC codecs. The
[typed one-way client and original-Peer service adapters](Requests.md#one-way-client-and-peer-service-adapters)
are also implemented. PeerSendService provides the native byte service for
OneWayClient; PeerRequestService provides request-only consumer deduplication.
Runtime retains method profiles, authority/admission checks and frozen retry
wrappers above furpc. HostingClient uses PeerRequestService; RapClient uses
OneWayClient over runtime middleware and PeerSendService. Prepared retry wrappers
retain shared Body rather than copied Vec bytes, not a replay cache.

Focused public API checks cover custom non-Serde codecs, segmented-body consumption and
explicit suffix skipping, zero/finite byte limits, and codec source/resource
classification. Typed-send cases exercise non-Clone service readiness witnesses, unused
decode types that need not be Send, encoding failure before submission,
request-local deadline shortening, on-time writes observed later, and no
correlated response entry, including sends while the only RPC slot is occupied.
Original-Peer adapters reject missing deadlines and incompatible managed headers
and do not switch generations after failure.
Cancellation cases distinguish uncommitted retraction from committed
driver-owned output without inventing confirmed cancellation on the typed future.

Runtime migration checks retain exact fault/endpoint bytes across retry attempts
with fresh message identities and verify shared prepared-byte storage,
reserved stateless reply admission versus ordinary stateful/report admission,
local encoding/authority failure isolation, decode-after-owned-admission ordering,
and usable unregister IPC after late one-way commands during orderly retirement.
These focused checks establish API/composition behavior, not new native
interoperability, deployed runtime support, TLS, or FM/Federation qualification.

## Current implementation

The crate now includes native frames/ordered headers, shared segmented messages,
incremental admission-gated framing, and a private clear-connection driver.
Initiating and accepted owned Tokio I/O use the same driver. It exchanges and
verifies ListenInstance before opening application submission/delivery, accepts
verified ListenInstance updates, and keeps transmit/receive wire contexts
separate. Clear transport does not authenticate the advertised address.

Endpoint and ServerBuilder now support native-style listen_address/instance
configuration through that same establishment engine, without exposing
ListenInstance or allowing live instance mutation. Explicit listener addresses
use native default instance zero; omitted settings preserve the existing
initiating empty/zero and accepted adapter-derived address/instance behavior.
Empty explicit addresses, instance-without-listener and configured hello-frame
overflow fail before connector readiness or acceptance. The advertisement
never changes the remote URI, actual IoInfo, nonce generation or Peer authority.
Offline public API cases cover both correlation modes, full native u64 instances,
strings beyond the legacy codec limits, fresh accepted/zero initiating nonces,
actual wildcard-listener TCP metadata, Endpoint clone/reconnect snapshots and
original-generation retirement. This is listener API evidence, not native
replication qualification; the fube-data migration is described below.

The driver owns cooperative preparation, scalar/vectored partial output,
queued serialized-frame bytes, precommit cancellation/deadlines, local-write
receipts, partial-frame/discard/committed-write watchdogs, and drain/abort.
Queue bytes remain charged until the whole unbatched frame is consumed or
removed, independently of surviving body aliases. Zero disables the native
queue-byte limit; disabling outgoing frame-size policy preserves the native
empty-queue exception. Pending-operation counts are explicit execution policy,
not construction or retained-allocation quotas.

One scope-owned poll loop consumes private verified-message/discard events;
there is no built-in application inbox, per-frame task, or second I/O owner.
The native classifier handles controls first, routes authorized native
notifications separately from ordinary ingress/correlation, diagnoses unmatched
replies and unconfigured delivery, and checks discarded-frame integrity. Unsupported
security/send-target controls and transport actions fail explicitly.
Send expiry/cancellation retracts only uncommitted output; a committed suffix
remains owned until full write or connection retirement. Drain rejects new
submission, flushes accepted output, and shuts down under one fixed deadline.

Deterministic I/O cases, backpressured in-memory exchange, and a loopback clear-TCP
exchange exercise this foundation. They are not native interoperability
qualification.

The generic `fube-client::FurpcClient` now qualifies Ping, NameExists,
GetNodeList, application/service lists and node health through RpcClient and
byte-facing Tower services against a fresh Service Fabric 11.8.121.4 onebox.
The existing baseline compares three cloned
Pings and full native node identities with independent HTTP observations.
It also checks naming true/false, node filters,
real server paging, native invalid-token failure and original-generation
healthy reuse, and mixed-method concurrency. Required `furpc_ping` and
`furpc_management` report fields retain those observations.
The gateway adapter captures each method's selected reply validator before
success-body decoding; shared native schemas do not route through legacy
Messages, channels or retained-allocation budgets.
All migrated queries use a generic typed query codec with shared declarations and
transport-independent list/item/empty reply profiles. Profile-specific actor,
action, payload-tag and paging checks remain distinct. The existing owned
list/health scenarios also compare nonempty application/service fields, real
continuations, parameter exclusion and all 17 health policy/filter cases against
independent HTTP. Their evidence checks native errors,
original-generation reuse and concurrent clones without new codecs or fixtures.
Partition and replica list methods now reuse that codec with strict List tags
5/6 and continuation-only paging, without MaxResults or automatic paging. Their
legacy methods, typed-request adapters and input accounting are retired.
The existing owned list scenario now compares canonical furpc topology against
HTTP, retains actual fields and generation/error evidence, and observes
MovePrimary through furpc replicas. The mutation was still legacy in that
topology qualification and is now migrated below.
The rewritten archived scenario passed against a fresh SF11.8.121.4 onebox:
six partitions and 22 replicas/instances matched independent HTTP observations.
The four node-local deployment queries also use the same codec: deployed
applications are paged, while service packages, code packages and deployed
replicas are unpaged. Deployed replicas retain the RS Actor 13 reply profile.
Their legacy methods, request adapters and allocation accounting are retired.
Current native scenarios keep one furpc read client across list, topology and
deployment observations, with explicit clones for concurrent calls and source
generation checks on every successful response.
The same native run qualified actual deployment paging, ten service packages,
ten ExeHost code packages and 22 deployed replicas, including full supported
REST projections, selectors, native missing-node errors and original-generation
healthy reuse. The actual report passed the independent CI evidence validators.
Synthetic wire/model-peer tests in fube-client are retired; non-wire tests,
native scenarios and report validators remain.
All twelve legacy read methods and their typed-request adapters are retired;
remaining legacy operations keep their own transport. Current proof gates use
canonical furpc results and independent HTTP/source-derived expectations, not
retained legacy implementations as comparison oracles.
CreateName/DeleteName now use the same stub with `Request<NamingUri>` and
`Response<()>`. The standard FubstCodec requires an empty success body after
NamingGateway/NameOperationReply validation, and deletion alone adds native
explicit-delete header `0x8033`. Both preserve native failures, absolute deadlines,
source/settlement metadata and no-replay/unknown-write-outcome semantics.
Their legacy methods, NameRequest/NameOperation/NameResponse and input accounting
are retired.
The existing owned name lifecycle now uses one furpc generation for writes and
reads, independently observes HTTP presence/absence and checks duplicate/missing
errors plus concurrent cloned Ping/NameExists reuse. Its required receipt is
validated independently and contributes a `furpc_naming_writes` time window.
The targeted archived lifecycle passed on a fresh five-node SF11.8.121.4 onebox:
both writes and all reads retained generation 1, duplicate/missing failures were
native domain errors, independent HTTP agreed and owned cleanup completed.
The actual receipt passed `validate_furpc_names`.
MovePrimary now uses the generic query codec with public unpaged
MovePrimaryQueryDescription/MovePrimaryQueryParameters and its existing
RS13/QueryReply empty-QueryResult profile. Nil partitions and invalid explicit
node names fail before submission. There is no force/ignore option, paging,
retry or replay; an acknowledged request is not an observed placement transition,
and an abandoned submitted mutation can still execute.
Its legacy method, request adapter, input accounting and the last legacy query
encode/decode entry points are retired. The existing owned list scenario now submits one movement on the
original furpc generation, independently observes the native/HTTP role swap
and checks concurrent replica/partition/Ping reuse on clones. The existing
movement validator requires the actual mutation response generation to match
the read-client generation.
The updated targeted archive passed against the retained SF11.8.121.4 onebox:
one request moved the owned primary from N0020 to its existing ready secondary
on N0040, with unchanged replica identities independently observed through
native reads and HTTP. The mutation and subsequent cloned mixed reads retained
generation 1. Actual list/topology/deployment/movement receipts passed the
independent validators and owned cleanup completed; the onebox was left running.
Resolution now uses its own native FubstCodec rather than the query envelope,
with checked `ResolutionRequest` selection and private previous-result metadata.
Service/partition/version binding, signed/named keys and endpoint roles remain
validated without a local endpoint cache, hidden description query or replay.
The native reply model converts without a legacy Message or transport-error
conversion and preserves furpc response headers/source/settlement.
The final ordinary FabricClient/GatewayClient façades, sealed Client/ClientRequest
contracts, options and typed allocation accounting are retired. Notification and
ImageStore clients kept their separate existing owners/transports and shared models
in that resolution slice.
The existing resolution/notification scenario now qualifies eight resolution and
refresh cases, independent HTTP partition/endpoint-role multisets, missing-service
failure and concurrent resolution/Ping/node reuse on the original furpc generation.
Its required receipt is independently validated as `furpc_resolution`.
The updated targeted archive passed on the retained SF11.8.121.4 onebox: all
eight resolution/refresh cases matched independent HTTP partition and complete
endpoint-role inventories. Missing-service failure and concurrent mixed calls
retained generation 1. Existing endpoint-count changes, primary replacement,
version advancement, real notification ACK retry and independent teardown also
passed. Actual resolution/retry receipts and owned cleanup were independently
validated; the default-Docker onebox was left running.
ServiceNotificationClient now owns an independent furpc Channel and captures its
original Peer. Startup installs an authorized notification IngressService before
the native session exchange. Typed server::Rpc/FubstCodec handlers commit page
updates/version state atomically and return ACK data; furpc alone supplies native
ACK correlation, flags, capacity and original-generation submission. Session and
filter RPCs reuse the gateway adapter over a Peer-backed byte service, with no
reconnect/replay. Filter guards retain bounded asynchronous cleanup.
The legacy notification ChannelComposition, pull/staging receiver, ReplyContext
plumbing, Message decoders and retained-allocation ledger are removed.
There is no new furpc lifecycle API: notification close joins its own worker and
releases Channel ownership, not a physical-driver barrier; outside clones are
not revoked, and idle disconnection need not wake local recv.
The same targeted native archive passed on the retained SF11.8.121.4 onebox.
Ordinary calls used generation 1 and three independent notification sessions
used generations 2/3/4. Four explicit unregisters, endpoint/primary changes,
empty invalidations and independent closes passed. A dropped successful native
ACK caused an actual gateway retry with a fresh request ID and a correlated
furpc ACK without duplicate application delivery. Actual notification,
resolution and retry receipts passed independent validators after owned cleanup.
The default-Docker onebox was left running.
ImageStoreClient now also owns an independent furpc Channel and captured Peer.
Async construction installs authorized Actor 25/27 ordinary one-way ingress;
semantic ACKs are not native notifications or ordinary RPC responses.
FileCreate/content/commit/commit-ACK use Peer::send, while the initial routed
root Delete uses Peer::request. Shared native Serde schemas preserve protocol
1.2, Activity/sequence checks, negotiated chunks, postorder markers and bounded
stale-reply handling. There is no reconnect or replay, legacy Channel
composition, retained-allocation ledger or standalone 4 MiB frame ceiling.
Cancellation/failure releases upload ownership and fences reuse; legacy
close/is_closed/terminal_error APIs are removed rather than emulated.
The last production fube-transport dependency and transport re-exports are
retired from fube-client. Shared model validation uses fubst Error/Result;
ImageStore errors retain FurpcClientError or the original filesystem source.
The remaining test-only dependency is also retired: the native notification
loss relay preserves original frame bytes and uses public furpc headers for
inspection. Legacy transport-model JSON checks were retired with fube-transport.
The existing native upload journey passed on the retained SF11.8.121.4 onebox:
one real SDK predecessor copy and furpc CLI replacement agreed on inventory,
removed stale content, and matched all 14 file/marker entries, including seven
zero-byte directory markers. Provision/create and independent native application
contact succeeded; all three deployed Data file SHA256 hashes matched source.
Owned application/type/upload/temp cleanup completed and the onebox remained
ready/running. The actual optional report retains inventories, hashes, executable
identities and confirmed cleanup. This does not qualify upload cancellation
over a real failed connection or repeated native SDK copy reliability.
Together with the earlier runs, this is initiating clear-TCP evidence for the
thirteen migrated reads, two naming writes, MovePrimary, notification receiving/ACKs
and native ImageStore v1.2 upload.
It does not qualify accepted-side serving, native notification origination, TLS,
or the rest of the gateway API. See the
[client slice](../../RustFabricClient.md#separate-furpc-backed-client).

The initiating fube-runtime IPC client also uses a dedicated Channel and original
Peer, with the same typed RpcClient/FubstCodec and byte-service structure.
Its outbound HostingClient<T> retains RpcClient<IpcService<T>> and provides
typed registration, unregister, lease-poll and health-report methods rather than
caller-selected actors/actions or reply types. Independent calls clone the stub,
not its original-generation authority; IPC identity, remaining timeout and reply
validation stay in the byte service. Cleanup exemptions and runtime retirement
remain domain-owned.
The local PeerService is retired in favor of furpc's original-Peer
PeerRequestService. Outbound RapClient<T> retains
OneWayClient<RapSendService<T>>; its typed proxy/service reply and load-report
methods select native actions and use FubstCodec. RapSendService retains IPC
identity stamping, authority checks, runtime admission and transport-error
retirement above the native PeerSendService. Independent calls clone the typed
stub without sharing mutable readiness across an await.
FaultReport::prepare and ReplicaEndpointUpdate::prepare use the public
codec::encode helper to freeze the native body once in opaque shared Body wrappers.
A private pass-through encoder adopts those existing bytes without another native
serialization or payload copy on retries; PeerSendService/Peer still allocate
fresh native message identities for each authorized attempt.
Stateless replies keep reserved admission; stateful replies and reports keep
ordinary admission. These calls still await original-Peer local writes, not RPC
responses or semantic acknowledgment.
HostingMethod and RapMethod provide the runtime-owned outbound envelopes used
both by typed methods and the byte-service submission guard. Admission lanes
remain selected by the typed operation, not inferred from shared native actions.
RuntimeServer separately composes host-control, RAPQuery and RAP one-way routes
in ipc_server.rs. Its bounded handoffs carry opaque, schema-typed invocations,
not a domain-visible actor/action/Vec envelope or an optional response slot.
The original Request<Body> stays private until the serialized executor reaches
the existing admission boundary. Host-control services accept Request<()> or
Request<UpdateContext> and return Reply<ErrorReply>; the query service accepts
Request<ProxyRequest> and returns Reply<QueryReply>. The stock server::Rpc adapter
and FubstCodec now decode requests and encode replies on that executor.
Response headers are fixed by the admitted route, and the original runner still
owns correlation and writing. A retired RPC invocation cannot call its retained
typed service. RPC execution completion means encoded response-data handoff,
not local-write completion; subsequent write failures remain serving errors.
RAP one-way handlers accept Request<ProxyRequest> and return (). Their serving
scope still completes at bounded enqueue, preserving late-command shutdown and
unregister behavior. After owned dispatch admission, public codec::decode uses
the Fubst decoder and the retained route body limit before calling the typed
domain service. Using
server::Rpc::one_way at that later point would incorrectly borrow the already
completed runner invocation. Route-selected RapAction values replace untyped
action selection in the command handoff; native actions and schemas are unchanged.
Runtime IPC uses shared native subsystem actor names from
fube_core::protocol::actor; that catalog stays above furpc and grants no routing
authority.
Runtime owns host/PID headers, real loopback I/O validation, registration, lease,
RAP semantics and domain retirement; furpc owns ingress correlation/reply writes.
Runtime's thin Session composes Channel establishment and captures the original
Peer before building the typed client stubs. Its connector permits only one I/O
acquisition and rejects retired sessions, so an attachment race cannot demand a
replacement socket under old registration authority. Generic Channel reconnect
behavior is unchanged; runtime retains first-cause retirement and passive I/O
release observation without new furpc lifecycle APIs.
The legacy channel adapter, second reader and held-completion worker are retired.
This intentionally does not emulate its first-write predicates, ordered ingress
barrier or physical-driver join. Domain authority checks and the independent
watchdog remain; already-admitted output can outlive a local check. The consumer
I/O wrapper observes stream release, not a new furpc lifecycle contract.
Data replication uses the separately owned adapter described below.
Retained runtime lifecycle/stateful/subprocess and shared RAP model peers also
use furpc's public accepted Server, original Peer and ingress reply data.
Test-only I/O gates preserve queued-write and partial-read coverage without
exporting the private codec. Production Hosting/node tests independently observe
and forward original frame bytes using public furpc headers. The direct legacy
dependency is removed from runtime's normal/dev edges and shared RAP support;
core/tool legacy edges remain; data and RA/node use the separate adapters below.
The existing native upload/provision/contact/hash journey passed with the new
runtime executable on the retained SF11.8.121.4 onebox. Native registration,
stateless activation and independent application RPC succeeded; owned workload
cleanup completed and the onebox was retained. This is not a fresh full runtime
reporting/upgrade/stateful/recovery qualification.

Fresh qualification on 2026-11-21 tested clean source
`d40b95f384401dad41332785bba5b6b5dae0a5cf`, including the typed client/server,
one-way infrastructure adoption, Session composition and outbound profiles.
Native run `092de2b36dc7` passed
`PASS_FULL_EMPTY_STATEFUL_CONTROL_PLANE_AND_STATELESS_PROFILE` on the retained
five-node SF 11.8.121.4 onebox with a verified version-3 prebuilt bundle.
Actual native observations covered registration and finite fresh lease authority,
49 same-process/host/runtime configuration-refresh samples, stateless load,
move-cost, HM sequence/expiry/error behavior, transient fault replacement,
RAP query/description updates, empty-stateful singleton and three-replica
role/configuration/failover, isolated Close/Abort and mixed-host recovery.
Exact application IPC loss fenced the deliberately noncooperative process in
0.503 seconds. Receipt verification checked eight stateful snapshots and eleven
live-process executable-hash observations against the bundle.

Ignored local evidence is retained at `build/furpc-runtime-proof/d40b95f/`:
`native-report.json`, `verification.json`, `bundle/` and `retry-model-tests.log`.
The report redacts the allocation-owner capability. Independent observations
confirmed absence of the minted application, both type versions, both native
ImageStore uploads and remote/local staging; all five nodes remained Up and the
caller-owned onebox was retained.

| Evidence | SHA256 |
| --- | --- |
| Native report | `2cd9d4465e929de8204f6cdee3e5b9ae292c0756a4dd64f61dae469721267452` |
| Bundle manifest | `f75498ed08615c1c05e22118d062ac2533e84fadaf4afcd80120e6d0ae4751e1` |
| Runtime application | `0446056b14a82e19710f1568a813d367f2eb41849dc6317dac6ce22c0ba1b5cd` |
| Independent observer | `6afd9967f59b01503f2476781e5de5d1dd460fa63bb9fa973865fee66c67d338` |

Forced fault retries and endpoint semantic-ACK loss, including retry
byte/message-ID identity, remain separate model evidence (three focused cases
passed), not native qualification.
Explicit InvalidateLeaseRequest and unregister wire exchanges were not
independently traced. This empty volatile control-plane profile does not qualify
data replication/durability, TLS, or a new physical-retirement/join guarantee.
The receipt remains bound to the tested revision, not later changes.

Hosting's production accepted IPC also uses the existing public furpc Server,
Actor-specific ingress services and original Peer. Each connection task owns
one inline serving future and a bounded serialized domain handoff; there is no
second reader, private codec export or new furpc lifecycle API. Hosting retains
actual loopback validation, exact host/PID and one-use admission, registration/
lease/replay policy, semantic reply provenance and retained cleanup/results.
Original-path reply correlation and writes belong to furpc. The former first-write
predicate and ingress-before-reply-write ordering barrier are intentionally not
emulated: already-admitted output can race revocation, and unbound/wrong-kind/
capacity-rejected traffic can be discarded without closing the connection.
Lifecycle authority checks surround submission/local completion. Hosting itself
owns cancellation and joins its connection/send tasks on its dedicated executor,
releasing the accepted socket before publishing disconnected state. Registration
retirement still permits bounded original-path unregister exchanges.
Hosting registration and lease RPCs now retain the original Request<Body>
through domain admission and use server::Rpc for typed reply encoding and
original-runner handoff. A private schema-selected codec retains Hosting's
DTO allocation budget: it borrows contiguous input, copies only fragmented
input and checks domain admission under the core lock before decoding.
Mutation reacquires that lock and rechecks authority, cleanup-only admission,
factory capability, deadlines and retained-result capacity. Reply DTOs stay
inline and retain the existing pre-clone/pre-encoding allocation checks.
Lease polls remain independent of core state locking once registration completes.
Admitted semantic RAP replies still expose the existing owned byte API; their
contiguous copy occurs only after domain identity/readiness/inbox admission.
Lifecycle sends use OneWayClient<PeerSendService> on the captured original Peer,
with shared prepared-body encoding and one method profile inventory for envelope
construction and the outbound guard. Public action/body inputs, send admission,
authority checks, retained local-write observations and joined shutdown remain
Hosting policy. This refactor adds no furpc API and does not update the
exact-source native Hosting qualification recorded below.
Private HostingServer and LifecycleClient facades now separate protocol
composition from the connection/domain executor. HostingServer owns actor-wide
ingress bindings, method dispatch and opaque deferred RPC/semantic invocations;
the executor inspects immutable headers for identity/replay admission before
routing. Header-action errors retain their original validation point, while
unsupported methods are rejected only after source attachment admission, so
they retire that exact owner instead of being silently discarded or leaving a
reusable admission. RPC invocations retain the original request/context and
response handoff and reject a mismatched typed handler before decoding.
Semantic completion remains bounded enqueue, not execution or an ACK.
LifecycleClient owns outbound profiles, opaque prepared requests and byte
submission on the original Peer. Node retains RA payload/effect interpretation;
Hosting retains send admission, authority checks and joined task ownership.
The transport adapter owns only accepted socket validation, ready observation
and inline furpc serving. No new generic layer, public Hosting API, retry/reconnection
policy or native qualification is introduced.
Hosting's wire Actor identifiers, including Empty replies, reuse the pinned
fube-core::protocol::actor definitions through a direct core dependency.
The numeric wire values are unchanged; SF identifiers remain outside furpc.
The Hosting and owned-app sample direct legacy dependency edges are retired;
RA/node control uses the separate migrated adapter below. Offline managed
furpc-client and independent adversarial socket fixtures cover the accepted path;
real runtime-child, owned activation and local RA/RAP suites remain qualification
owners.

On 2026-11-20, both native `hosting-leases` and `hosting-registration` passed at
`0191bcc` with newly built test-host, native SDK producer and manifest-acceptance
executables in one exact-source/hash/mode-verified bundle. Pinned SF11.8.121.4
Runtime/Common mappings were verified from private native/ABI inputs; no node
service was installed or used by these clusterless profiles. Each production
Hosting profile observed two positive finite correlated polls, actual runtime
unregister, graceful exit0 and owned direct-child reap/process-group drain.
Registration additionally verified the native stateless factory, same-core
equal-version lookup, reporting-disabled bookkeeping and owned Registered/
RuntimeClosed/HostClosed local decisions. FM readiness remained closed; no native
host-unregister frame was observed, and HostClosed came from connection-loss
cleanup. One gated package obligation remained owned, not treated as release.
All five retained native fixture profiles passed separately with their existing
graceful/forced-cleanup distinctions; their lifecycle evidence is not relabeled
as production Hosting lifecycle. The retained onebox stayed healthy/running.
Ignored evidence is under `build/furpc-hosting-proof/`: `binaries-0191bcc/`,
`native-receipts-0191bcc-r2/` and `native-regressions-0191bcc/`.
This qualifies the selected accepted clear-TCP bootstrap/registration paths, not
native deployed/stateful/reporting lifecycle, failed-write/revocation races,
TLS/claims, or full original native node-handler parity.

**Fresh typed-service qualification (2026-11-21):** both `hosting-leases` and
`hosting-registration` passed against clean source
`c2776ac883186b2ad2cb99f21982e9c063a82d6a`, tree
`d396347aa151a3f112298611dd288d53b2e5502d`. A fresh version-1 standalone bundle
contains the rebuilt test-host, native SDK application and manifest-acceptance
executables; the two selected profiles execute the test-host and application.
The qualified source includes typed Hosting RPC serving, the private
HostingServer/LifecycleClient composition and shared native Actor definitions.
These clusterless runs used the unchanged private SF 11.8.121.4 Runtime/Common
pins and compatible cached ABI inputs; no node service was installed or used.

Each profile independently observed two correlated positive lease replies,
with TTLs 299,999,991 and 299,999,992 ticks, actual runtime unregister,
graceful exit0, direct-child reap and absent owned process groups.
Independent observation checked four live process identities across the runs:
each host executable matched its bundle hash, and each actual application's
`/proc/<pid>/exe` matched the bundled native SDK producer. Both applications
mapped the pinned Runtime/Common libraries at their verified private paths.
Post-run verification checked unchanged native/ABI inputs, bundle bytes/modes,
actual wire actions and receipts, copied application bytes and process absence.

Registration additionally observed the actual native stateless factory,
same-core equal-version lookup and disabled-health bookkeeping. Registered,
RuntimeClosed and HostClosed were completed owned local decisions with
`IgnoredNotReady`, not FM/RA deployment acknowledgments. No native host-unregister
frame was observed; HostClosed came from connection-loss cleanup.
One gated package obligation remained owned, not treated as release.
The qualification launcher reaped its native child; this is not evidence of
production Hosting package/process residency release.

Ignored evidence is retained at `build/furpc-hosting-proof/c2776ac/`, a
convenience link to the session's physical evidence directory; the runner used
the physical bundle path because it rejects symlink parents. It contains
`preflight.json`, `bundle/`, `receipts/`, per-profile live-process observations,
`verification.json`, build/console logs and retained owned run directories.
Both native profiles and all owned process groups were stopped; the unchanged
retained onebox remained running with all five nodes Up.

| Evidence | SHA256 |
| --- | --- |
| Bundle manifest | `6e1eebdde73d92ac66683e2ab63faed3c008494ce153514fe92cdd9b9d61f326` |
| Hosting leases receipt | `4799f18eb736541fecc90e9ce208a13046d457ea09030e5500ee1861a8602709` |
| Hosting registration receipt | `7e3e5c75cdb704a923775b85aaaa195d98feb35570102566a1aaa8db240f108f` |
| Independent verification | `d5acfeaadce8e04261543a4224e73cb8e391c5b1767fac1974eb59829d882476` |
| Test-host executable | `9156dfae8c75ca92d40b735e6097a15611590920d1820bc46ceb702bcb45762c` |
| Native SDK application | `9194c42a048da99ec5f5e47d4cdcc6049fb79f39bab2c1cc50697c7fdc6cd055` |

This receipt remains bound to `c2776ac`, not subsequent documentation changes.
The original five standalone fixture profiles were not rerun; their earlier
receipts remain historical. Native outbound RAP/lifecycle, stateful/reporting
behavior, failed-write/revocation races, TLS/claims and broader native node
parity remain unqualified by these two selected profiles. No generic furpc
physical-retirement/join guarantee is introduced or qualified.

The public facade supports managed byte RPC by default and explicitly selected
`ConnectionOptions::raw_only()` for caller-owned one-way IDs. Endpoint owns inert URI,
limit and service configuration; connect returns an owning Channel. Channel
clones share one retained URI-input Tower connector and demand-driven reconnect
attempt, without replay. Peer, Source, native Operation and Attempt values never
own that logical lifetime or retarget another physical generation.
Raw-only mode rejects RPC/readiness explicitly rather than enabling correlation.

The managed core allocates process-scoped GUID/index IDs, rotating the GUID
namespace before index reuse. Its generation-bound request tables contain only
pending entries, installed before queued output can commit. Request expiry is
driver-owned, not dependent on continued observer polling; cancellation,
generation retirement and successful completion remove correlation without
lifetime tombstones or replay. Replies are matched by final effective RelatesTo,
admitted before body storage independently of HandlerLimits, and settled only
after complete-frame integrity. Unknown/late replies are diagnosed/discarded
without a consecutive-reply terminal cutoff.

`Peer::request` returns an inert native Operation and Channel is the sole
`Service<Request<Body>, Response = Response<Body>>`. Each ready Channel instance
owns one generation-bound operation/result reservation; clones copy neither a
ready permit nor a failed-attempt outcome. Transient reconnect/capacity outcomes
are delivered by the next call without native submission. Byte-service calls
require local deadline metadata and retain a Channel owner guard, while native
Peer operations remain nonowners. Response parts/body mapping preserve original
source and settlement metadata; synthetic responses have neither. On-time native
settlement remains on time when observed after the deadline or generation exit.

Native nonzero Faults are returned as RemoteFault with header/effective codes,
raw UTF-16 diagnostic text and the shared received body. As in native RequestTable,
a valid RejectFaultBody overrides error details only when its message is nonempty;
malformed bodies retain the header failure and expose their decode error rather
than creating a successful empty response. Application bodies are never decoded
by this layer.

Application-body adaptation is now available above the byte service through
public Codec/Encoder/Decoder traits, fallible segmented EncodeBuf/DecodeBuf
buffers, and FubstCodec. Custom codecs need no Serde, Clone or Sync bounds.
Encoding can adopt shared body segments without copying; decoding must consume
the whole body. FubstCodec uses the shared native Serde implementation, borrows
contiguous input directly and copies fragmented input only when needed by that
implementation. Its optional byte/depth settings are independent of native
header/control/Fault decoding and add no default depth/work ceiling.

RpcClient<T> adapts any conforming Tower byte service, borrowing the exact
transport for readiness and call without cloning, retry or replay. It consumes
one codec per call and preserves editable envelope parts and native provenance.
One original absolute deadline covers encoding, readiness and receipt; native
settlement time remains authoritative when an on-time result is observed late.
Synthetic results use observation time. ClientCodecLimits optionally narrow
encoded output and response input, default to native body capacity, and allow
zero for empty bodies. Codec errors retain encode/decode phase, original source,
and resource-limit classification without becoming connection-protocol failures.
No decoded-allocation ledger or transport-budget handoff is introduced.

Typed ingress methods now use `server::Rpc<C>::unary` or `one_way` inside the
same byte-facing services. `Reply<T>` supplies response data and ordered headers;
only the existing runner owns reply tickets, correlation and original-generation
submission. Adapters validate live core-created context, original exchange kind,
current native headers, and the admitted input/frame policy before decoding.
They use the actual scope response-output limit and shared readiness-waiter
capacity rather than independent method pools or construction-time defaults.
One-way adaptation never constructs an encoder or requires unused encode
types/state to be Send.

Invocation context retains only a weak link to private state, not handler/reply
permits or a scope owner. Queued application workers preserve the original
deadline and are notified when the owning runner retires their invocation.
Typed readiness/handler futures are dropped on that cancellation or deadline;
no method task or work queue is created by the crate. Application codec work
remains trusted and cooperative. Admitted typed replies can complete during
graceful drain without a general-purpose Peer.

TCP/custom connectors, Accept, TCP/stream acceptors and
`Server::serve_with_shutdown` use the same physical engine. Canonical
IngressService bindings admit bounded work at the header gate, before body
storage, and run readiness/handlers outside the driver. Header authorization
and HandlerLimits are shared by all connections in a server or generations in
one Channel. Handler deadlines begin at that admission, including partial-body
waiting. Dynamic Channel service installation survives reconnect. Received
Request parts preserve their original read-only source/deadline/context through
header edits and body mapping. Only an independently configured SendPolicy
supplies a source-bound Peer; policy is rechecked at the commitment gate.
`IngressOptions::requests(RequestPolicy, ...)` separately authorizes request and
response envelopes. Private runner-owned tickets capture the original request
ID, generation, response policy and deadline before editable Request publication.
Only those tickets can send IngressResponse::Reply; missing responses and replies
without a ticket are diagnosed handler failures and generate no automatic Fault.

Ordinary ingress now selects one explicit raw binding or multiple disjoint
whole-Actor/exact-action bindings. Header-stage matching uses sorted native
Actor/UTF-16 Action keys before body retention, policy checks, handler permits
or reply tickets. Unknown actors/actions are diagnosed and discarded with full
frame integrity rather than becoming connection-fatal errors or falling back
to raw delivery. Correlation/control classification remains earlier and separate
from application routing, including in raw-only mode.

The codec-free Router installs immutable exact-action bindings and distinct
RequestRouteOptions/OneWayRouteOptions. Each invocation retains its original
private binding and route identity: edited keys and independently configured
router tables cannot borrow another route's permissions. Whole-Actor services
may rewrite Action within their original Actor; explicit raw services remain
actor-less when appropriate. Routed and typed services share the existing
readiness allowance, original deadline and runner-owned reply completion.
Configuration rejects reserved control actors, duplicate/overlapping bindings
and raw/Actor mixtures without adding route-count or action-storage quotas.
Router clones share configuration, not prepared services; selected services
are cloned before readiness and called on that same instance.

The optional accepted-peer hook runs once after establishment, with its own
timeout and no handler-slot dependency. Shutdown drops acceptance, rejects
new sends/dispatch and waits for admitted handlers, hook work, pending RPC and
output under one drain deadline. Native shutdown is held until original-path
replies finish; general Peers have no drain exemption. Reply count credit is
reserved from handler capacity and follows the driver's queued frame after
commitment, independently of a timed-out handler or dropped observer. Native
queue-byte accounting still applies. Finite accept-source exhaustion is not a
shutdown signal. Dropping initial connect or the last Channel owner cancels
owned work; retaining Endpoint configuration or a Peer does not keep it alive.
Runtime errors are cloneable and retain original sources, so Error is no longer
Copy. Automatic diagnostics are rate-limited static categories/phases, not
arbitrary handler/connector error text or custom URI paths/queries.

Current TransportLimits defaults use native u32 frame capacity, a disabled
queue-byte limit, and no retained-allocation accounting. Execution defaults are
explicit Rust policy: 1,024 pending sends, 1,024 pending RPC/results per logical
scope, 128 server connections, 16 simultaneous
handshakes, 128 peer waiters, 64 KiB/32-step cooperative turns, 30-second frame
and committed-write watchdogs, and a 5-second discard watchdog. Connect, server
handshake, peer-ready and server drain default to 30 seconds; HandlerLimits
retain their documented 8 in-flight/4 readiness-waiter/30-second defaults.
These are not native constants or performance qualification.

Native notifications are implemented through byte-facing Peer::notify,
NotificationPolicy, IngressOptions::notifications, and
Router::notification/NotificationRouteOptions. They share managed pending-call
and result capacity with RPC, and keep native correlation installed before
commitment. Both initiating and accepted Peers can originate notifications
under their captured send policy; raw-only mode rejects origination.

Ordinary and notification bindings select raw or Actor dispatch independently.
Notification classification precedes ordinary RelatesTo lookup; native controls
still take precedence. Matched ACK policies run at header admission before
retention. A rejected ACK is diagnosed/discarded with full integrity and leaves
the pending operation awaiting an authorized response or expiry.

The existing service runner reserves notification handler/reply capacity and
owns the original acknowledgment ticket. IngressResponse::Reply(Message) or
typed unary Reply<T> supplies application-defined ACK data; only the ticket
supplies native RelatesTo and ordinary reply flags. Edited flags cannot change
the original route class or turn notification context into one-way authority.
NoReply, failure, cancellation and dropped work send no automatic ACK or Fault.
Admitted ACK writes retain the ordinary protected completion/drain behavior.
No public notification receiver/grant, hidden queue or extra execution pool
was introduced. Typed callers reuse RpcClient::call over an explicit byte
service forwarding Peer::notify and the original deadline.

Source qualification at the pinned native baseline covers BeginNotification,
DuplexRequestReply's special-first dispatch, and ReceiverContext::Reply.
In-memory exchanges cover both directions, typed ACK data, class-local routing,
policy rejection/commit checks, shared capacity, cancellation, Faults and drain;
the fube-client migration above additionally qualifies native gateway-to-client
notification receiving/ACKs on SF11.8.121.4, including actual ACK loss/retry.
Native notification origination and accepted-side native interoperability remain
unqualified. TLS remains unimplemented.

## Data replication migration

**Implemented; native-data qualification passed at `154b4f1`.** This separate
consumer slice replaces `fube-data`'s normal/dev legacy transport use, including
its native payload/header codecs and directly coupled fixtures. Runtime/RAP
and transactional callers keep their SF-style public replication boundary;
RA/node control traffic and global legacy-crate retirement are not included.

The selected approach is **redesign on furpc**, not preservation of every
legacy raw-session guarantee. Its furpc prerequisite is the now-implemented
[native-style listen_address/instance settings](Connections.md#native-style-listener-configuration)
on Endpoint and ServerBuilder, without a public ListenInstance value.
Other requirements compose existing public
Endpoint/Channel, Server/Accept, raw IngressService, Request context, Peer,
Operation/Attempt and native header/message APIs. Do not export the private
frame codec, add another socket reader, or retain a compatibility backend.

### Ownership and data path

The replication adapter retains its dedicated, joined network thread, actual
listener, bounded outgoing/incoming control and data queues, route table,
and combined initiating/accepted connection cap including handshakes. A bound
listener supplies one validated transport listen address/instance for those
settings on both roles; actual socket diagnostics remain independent.
The current equal listen/publish-address profile remains a domain restriction,
not a universal native rule. Different-address native producers require a
separate explicit publication/membership mapping and qualification.

Initiating connections retain an owning Channel and its original Peer.
Accepted connections use an owned serving future and an explicitly restricted
peer-ready hook. A per-socket finite Accept source permits the domain supervisor
to retire just that connection. Both roles install the same raw one-way ingress
service before application traffic. Raw-only mode preserves caller-owned
one-way MessageIds; no synthetic Actor header or Actor-0 binding is introduced.

The pre-body header policy validates the clear replication envelope.
Furpc retains control classification, integrity and body verification.
The byte-facing service then validates ReplicationFrom/ReplicationActor schemas
using public Headers and the original Source, decodes the native packet, checks
the remote listener/source identity and operation bounds, and tries the
appropriate bounded domain handoff. It returns NoReply promptly, without
waiting for provider application, copy completion or quorum. Malformed domain
input and terminal control-handoff failures notify the supervisor to retire
that exact session; data-capacity drops are diagnosed without admitting or
acknowledging the rejected operation.

Each private engine ingress item carries the packet, its domain byte lease,
and its restricted original-generation Peer/session grant together. Consumer
transfer releases the adapter lease; engine retention keeps its existing
independent charge. Fully delivered input keeps its historical source identity
after EOF, but cannot write through a retired target. Membership registration,
same-address duplicates, route relearning and reconnection cannot recreate or
retarget this grant. Ordinary authoritative membership sends may select a
current route; immediate and delayed secondary ACKs must use their original
source Peer.

ACK/retry, provider/application acknowledgment, copy/copy-context streams,
operation queues, epochs, configuration/quorum and replica removal remain
fube-data policy. Successful queue admission or local writing never fabricates
a semantic ACK or advances replication completion.

### Approved contract changes

| Area | Implemented migration contract |
| --- | --- |
| Body admission | Furpc's private frame/header/binding/HandlerLimits gates still precede body retention. Fube-data's control/data byte and queue accounting occurs after verified delivery, before domain handoff, not inside that private gate. |
| Resource isolation | Existing global control/data queue/byte allowances remain independent domain policies, including retained ingress and outgoing captures. They do not account for furpc's body backing or in-flight handlers and do not imply a total heap bound. ACK/RequestAck is ordinary native traffic, not privileged transport control. |
| Scheduling | Select ready control before queued data in the domain dispatcher. Do not promise priority over data already admitted to furpc, preemption of a started frame, or independence from a large earlier frame on the same TCP stream. |
| Rejection | Furpc may diagnose and discard unbound, wrong-kind or header-policy/capacity-rejected ingress without closing. Input actually reaching the domain still follows its strict sender/schema/control-capacity retirement policy. No rejected data receives a new ACK. |
| Send authority/cancellation | Keep one admission-started absolute deadline through route selection, connect, Peer acquisition and send. Use original Peer and Operation/Attempt cancellation; do not recreate custom first-positive-write predicates. Admitted or committed output can race domain retirement and have an unknown remote outcome. |
| Shutdown | Stop new domain work, cancel owned operations and retire Channels/serving futures. A started suffix may be cut short by cancellation/connection retirement; the legacy guarantee of flushing every started frame before joined shutdown is removed. |
| Physical retirement | The domain still owns and joins its network thread/tasks and observes release of its actual I/O adapters before publishing completed shutdown. Channel drop alone, a furpc error, or an Attempt disposition is not join evidence. No new public furpc join/closure observer is introduced. |
| Write evidence | Furpc WriteReceipt proves complete local socket acceptance, not an explicit flush, remote parse or ACK. Qualification receipts must use that classification. Exact frame-byte evidence, when required, must come from an independent observer of actual bytes, not a reconstructed frame labeled as written. |

Domain sends retain their existing finite queue captures/permits until local
completion or disposal. A consumer-owned cancellation signal can retract
uncommitted furpc work; committed work is not a rollback. Retiring a route
must preserve another authoritative actor's shared socket and already-owned
captures under the existing bounded policy. Old session teardown must not
remove a newer route or admit a late send task.

### Delivery and qualification

The adapter in `fube-data/src/transport/connection.rs` uses the common hello
engine on both roles and bounded send futures sized by the existing combined
domain operation/byte allowances. Those futures retain original queue permits.
Qualification-only observed sends drain prior native work and use an exclusive
actual-I/O capture; no reconstructed frame is claimed as written.
Packet encoding/decoding uses public furpc Messages with unchanged native
schemas, segmentation and domain limits. Private mutable header/body scratch
belongs only to that domain codec; it implements no framing or connection.
The legacy normal/dev dependency edges and raw-session implementation are removed.

Retained independent native-source schema/adversarial socket fixtures and
consumer cases cover duplex accepted reuse, exact source/actor
validation, duplicate listener affinity and delayed application ACKs,
independent control/data saturation, operation metadata accounting, shared
address route removal, combined connection limits, fragmented/expired
handshakes/frames, and terminal joined socket/task disposal under backpressure.
Retired universal rejection closure and guaranteed started-suffix completion
assertions are replaced with the documented discard/retirement contracts;
semantic ACK/queue/affinity checks remain. No private furpc codec or txn
engine/qualification hook is exposed.

Offline validation passed 153 data cases and 12 data doctests, 223 txn cases
plus its two public-runtime integration cases, 31 coupled runtime replication
cases and the furpc package suite. The updated Python evidence-contract suite
passed 170 cases with one skip. The retained multi-megabyte public-construction
txn full-copy case initially hit its unchanged five-second bound with the
single-send/bitwise-CRC composition under repeated large-frame traffic.
Bounded native send pipelining and accelerated incremental `crc32fast` preserve
native checksum bytes and cooperative work quanta, without disabling integrity,
loosening limits/deadlines or changing copy/ACK/retry policy. The same case
passes with that original bound. Evidence logs, including the failed diagnostic
runs, are ignored under `build/furpc-data-proof/`.

Fresh native qualification on 2026-11-20 passed all 17 scenarios in the maintained
`native-data` profile against SF 11.8.121.4, using clean source
`154b4f13902263a5ef61876cf88276c48fc35a07` and a newly built strict bundle.
Both Rust/native replication directions, nonempty copy, held application/final-copy
ACK negatives, quorum loss/recovery, interrupted copy/epoch changes, identified
retries, stale-wire probes and owned workload cleanup passed with zero errors.
Six stale-wire receipts pair complete local socket acceptance with independently
captured destination ingress; they do not prove native parse/rejection or flush.
The minted application/type/ImageStore upload and original staging reservation
were removed with absence checks; the caller-owned onebox remains intact.
See the [pinned qualification record](../../../runtime/FubeData.md#furpc-native-data-qualification-2026-11-20).

This qualifies that volatile clear mixed-native data profile, not the separate
Rust-only14, full legacy/runtime three-replica profile, every stateful/reporting
path, different listen/publish addresses, durability or TLS/claims. Historical
V1/Hosting receipts remain historical. Future changed-source native work requires
another fresh exact-source bundle and independently observed owned cleanup.

## RA/node control migration

**Implemented; offline-qualified, native FM/Federation interoperability not run.**
The selected design is a redesign on public furpc APIs, not a legacy Channel
backend. RA factories/decoders use public immutable Messages and native header
records; native FABRIC_FIELDS, PToP source/recipient incarnation and generation
schemas remain unchanged. The Node composition owns one finite admitted socket
and original-generation Peer for each FM or RA peer relationship. It never
reconnects, adopts another session or treats transport success as FM/RA completion.

Endpoint/Server use the same domain ingress service before application traffic.
A finite Node-owned inbox separates verified delivery from caller-driven
admission; requests retain a bounded response slot returned to the furpc runner,
which alone adds correlation and sends the reply. A reply handoff is not a
first-write authority fence or write receipt. Capacity/invalid domain input
retires its exact owner; generic furpc policy/binding/handler rejection may
discard without closing. The former reader-level ordered-handoff gate and
aggregate legacy retained-allocation ledger are not preserved. PToP validation,
older peer-generation drops, frozen LFUM retry identity, domain admission order,
store/RA commit gates and semantic acknowledgment remain domain responsibilities.
Per session, the complete frame remains capped at 64 KiB, headers at 16 records/
4 KiB, the domain inbox at eight messages, handlers at eight and pending
requests/sends at eight each. Native send-queue bytes are finite independently;
these bounds are not an aggregate heap ledger. Initial TCP source-IP binding,
actual I/O metadata and one admitted connection per relationship are retained.

The owner cancels its operations and serving lifetime and observes actual
driver-task and socket-adapter release before completed close. Dropping a
Channel/Peer or observing a terminal error alone does not prove physical join.
Already-committed output may race retirement or be truncated; no suffix-flush
or remote rollback guarantee is introduced. One absolute deadline bounds
establishment and each admitted request/send.

Coupled cluster/FM and standalone-Node launchers/fixtures migrate in the same
slice, including the Hosting residency fixture's native body accounting. Their
normal/dev legacy transport edges are removed; the final core/CLI/test-host edges
are retired in the subsequent slice below. RA source-oracle provenance now hashes furpc rather than
the retired transport, including its transitive Rust sources and manifest.

Affected offline suites passed: 257 RA unit/entity/Hosting-event/protocol cases,
nine Node unit/control and eight bridge cases, 157 cluster model/real-process
cases plus four CLI and six foreground cases, 34 standalone-Node cases and
17 coupled Hosting residency cases. Final focused checks also exercise concurrent
joined close/retained reply revocation, concurrent receive deadlines, exact inbox
overflow retirement and five
standalone generation/LFUM process scenarios. All-target/all-feature workspace
checking, strict affected-crate Clippy, warning-denied Rustdoc, formatting and
34 source-bundle/schema validator cases passed. The initial malformed-header
fixture failure was a stale post-construction assertion: public native header
construction already rejects that invalid record; no production gate was relaxed.

These are source/schema and real Rust-process results, not native handler or
Federation interoperability. No fresh native-stream producer/consumer bundle or
native FM/RA run was executed. The earlier exact-source `154b4f1` native-data
receipt does not qualify this changed RA/node code. TLS and broader routing stay
unqualified.

## Final legacy retirement

The final core, standalone test-host and CLI fixture dependencies are migrated;
`fube-transport` and its workspace/lockfile/CI entries are removed. Core retains
the complete original furpc error, including native Fault codes and bodies,
while mapping SDK kinds explicitly. Transport/handler/codec errors can capture
user code and therefore retain supervised destruction. Fubst maps directly to
SDK error categories. The unused legacy-only `transport_fault` API is removed,
not replaced with fabricated native Fault objects.

The approved test-host redesign uses public Server/Peer and immutable Message
APIs, a finite one-socket acceptor and a bounded domain inbox on an owned thread.
Furpc consumes establishment and owns correlation. The ready hook requires a
native client-only advertisement; host/PID, registration and semantic lifecycle
checks remain in the fixture. Reply submission consumes an original response
slot and means data handoff, not local write completion. One-way sends still
await local writing. Shutdown cancels inline serving, joins its thread/socket
and disposes queued messages; no reconnection or public join observer is added.
The frame cap remains 4 MiB, inbox/handler admission 64, command admission eight,
handshake/frame/handler budgets 120 seconds and write completion five seconds.
Generic furpc rejection may discard traffic without closing.

The native qualification proxies reuse the independent bounded frame observer
and forward original bytes, including checksums, unchanged. Observer failures
remain owned results rather than being silently lost on task cleanup. CLI upload
tests reuse the existing independent injector and public native header schemas.
Legacy-only model tests retire with their implementation; this is not a new
native qualification receipt. All earlier receipts retain their exact-source
boundaries. Fresh RA/native-stream and native FM/Federation interoperability
remain separate; TLS implementation is still deferred.

## Gap ownership and implementation order

| Existing gap | Primary owner in the new design |
| --- | --- |
| Fixed frame and serializer ceilings | `wire`, `fubst`, explicit resource configuration. |
| Duplicate common headers, Retry/Idempotent | Ordered header scan and common schemas. |
| Missing trailing defaults and unknown suffixes | `fubst` plus schema-specific defaults. |
| Lost correlated fault-body details | `messaging::request_reply`. |
| Lifetime ID history and unmatched-reply cutoff | Managed allocator and pending-only correlation. |
| Unknown-actor terminal behavior | Native demux plus separately named domain/resource policy. |
| Close discarding queued accepted work | Connection drain state and owned shutdown. |
| Missing notification origination | Authorized Peer notification operations and runner-owned service acknowledgments. |
| Body flattening and repeated copies | Immutable storage, segmented preparation, vectored writer. |
| Listener/connection/recovery duplication | `transport` supervision over Endpoint values, connector services, and Accept, with domain recovery above it. |
| TLS/claims and unsupported controls | Explicit driver security/control phases; TLS/claims implementation and qualification are deferred. |

Deliver the initial clear-TCP implementation in this order:

1. Establish the source-to-behavior inventory and ownership contracts. Record
   native rules, deliberate Rust adaptations, limits, and unsupported features.
2. Extend `fubst` and implement pure frames/headers/messages. Native
   header/control/Fault decode paths use one internal context derived from
   `TransportLimits`; no higher layer compensates for broken shared decoding.
   Add standalone message/header constructors with native width/schema checks;
   send/reply admission must still validate the actual scope policy.
   Add the public body-codec contract and default `FubstCodec` with separate
   body-specific operation budgets, not inherited native decode settings.
3. Implement a clear physical connection for both initiating and accepted
   sockets, with staged admission, receipts, segmented output, drain, and abort.
   Establish the security transition design early rather than retrofitting a
   `TlsStream` abstraction later.
4. Implement URI-based Endpoint configuration for destination, native options, finite
   limits, timeout, and reverse-ingress descriptors; default TCP connect,
   reusable connector injection through `connect_with_connector`, public Accept, TCP/stream adapters, and
   bounded raw listeners/connections; connect a replication adapter.
   Endpoint establishes an owning Channel through that same dialed engine;
   physical connections and supervision stay private. Expose Peer only for
   native/accepted-peer capabilities, never another client transport service.
   Add optional `on_peer_ready` Tower work with explicit SendPolicy so an
   accepted connection can initiate bootstrap before application ingress.
   Publish it after establishment/security release, not at native raw-accept
   timing, with bounded connection-owned execution outside the driver.
   Include coalesced demand-driven reconnect for later calls, without copying
   failed native attempts/correlation or granting new per-generation capacity.
   Demonstrate that raw consumers need neither gateway composition nor RPC
   and custom I/O does not require another connection engine.
   Reuse http::Uri and standard URI-input Tower services without HTTP framing,
   retaining original logical URIs across reconnect rather than pinning the
   first DNS result or introducing another public address enum.
5. Add native byte request/reply, the sole Channel client Tower service, and
   canonical IngressService execution through Endpoint::service,
   Channel::register_service, and ServerBuilder::service. Share native bindings,
   IngressResponse, HandlerLimits, and runner-owned reply completion across
   dialed reverse ingress and accepted connections. All application ingress
   exposes services only; bounded application work queues retain runner-owned
   replies/acknowledgments. Notification ingress uses explicit bindings within
   that same service, without a separate receiver or public completion grant.
   Include explicit raw ingress and optional Actor dispatch, notification
   origination, and gateway/IPC adapters. `RpcClient<T>` adapts any conforming byte service through normal
   readiness and mutable async calls, with bounded application-owned codecs
   and native admission at transport handoff. Peer notifications retain
   byte-facing native operations; typed notifications reuse RpcClient over an
   explicit notify byte-service adapter. Each typed call consumes its codec; domain convenience
   methods supply `FubstCodec`. Preserve domain/capability boundaries.
   Keep application dispatcher selection independent of managed/raw-only
   correlation mode. Qualify actor-less replication in initiating and accepted
   roles without synthesizing ordinary Actor headers or installing Actor 0.
6. Add the optional codec-free service router and separate `server::Rpc<C>`
   ordinary request/one-way and notification method adaptation inside byte-facing
   services. Qualify shared
   bounded readiness, handler lifetime/response completion, and graceful
   shutdown without forcing raw peers through typed adaptation.

TLS and claims implementation are deferred follow-ups, not a required final
step of the initial implementation. When scheduled, deliver each security
profile against its own native acceptance requirements and the phase design in
[security phases](Security.md#security-phases). Any later domain-recovery profile likewise needs its own native acceptance
requirements. Advanced handler ergonomics remain separate from the initial
minimal server service/router and are not required to retire the old raw core.

Vectored correctness and ownership belong in the initial writer. Multi-message
batching and tuning can follow without changing the public completion contract.

## Migration and acceptance

Add `furpc` as a new explicit workspace member when implementation begins.
Do not rename `fube-transport` in place or make the new core depend on it.
Move/reimplement shared responsibilities against this design; reuse proven
algorithms and source evidence without inheriting accidental policy.

Migrate consumers individually through narrow adapters outside the core.
An adapter may implement existing channel traits while its consumer migrates,
but their cancellation, close, ID, and error contracts must be reconciled
explicitly. Do not claim compatibility because method signatures match.
Keep old types and new types from circulating interchangeably inside one
connection/session.

Preserve independent gateway/notification owners, IPC host/PID and domain
admission, native replication operation-queue/ACK semantics, and original-source
ACK/reply affinity. Semantic replication ACKs and retransmission stay in `fube-data`;
runtime/RAP, service lifecycle, and transaction logic do not move into `furpc`.
See the [source-qualified flow-control boundary](Resources.md#replication-flow-control-boundary).

Acceptance evidence has distinct levels:

- Pure codec cases for native boundaries, duplicate semantics, schema evolution,
  malformed input, and native-permitted messages beyond old fixed ceilings.
- Private framing-contract cases for both admission gates, retained/discarded
  integrity, partial-frame EOF, segmented write cursors, and commitment
  boundaries, as specified in [framing contracts](Framing.md#required-evidence).
- Deterministic driver cases for partial I/O, cancellation/commit races, bounded
  retention, slow peers, drain/abort, and actual task/resource release. Internal
  cleanup tests do not imply a public post-drop retirement barrier.
- Consumer cases for raw replication, correlated requests, accepted-server
  traffic, IPC identity, and native notification acknowledgment without competing readers.
- Native-aligned replication service flow control: dispatch ACK/RequestAck
  independently of replicated-operation queues, keep service readiness free
  of data-queue-space waits, and return NoReply promptly after a bounded domain
  admission/drop decision instead of holding a handler until durable/quorum
  completion. Deferred operation processing, ACK scheduling, and retry have
  explicit domain ownership and shutdown. An intentional queue-full data drop
  is diagnosed and cannot advance acknowledgment for unadmitted operations;
  truthful ACKs for earlier progress remain valid. Shared buffers survive
  retention outside a completed handler. Exercise ordinary frame/queue-limit
  exhaustion with its documented error/drop/retirement outcome. Preserve internal native
  control, managed RPC-completion, and authorized-reply protection.
- Public header views/builders and raw/Actor-dispatched receive, including
  preserved unknown records, rejection of conflicting managed fields, bounded
  registrations, and no authority gained by setting Actor or RelatesTo.
  Exercise standalone construction without a facade/runtime, native-width encoding,
  shared backing ownership, sticky builder errors, and combined native-header
  limits including managed-field overhead. A successfully built message must
  still reject under stricter submission limits without committing output.
  Reused received storage stays shared through construction and submission.
  Verify body-first request construction, generic ordered Actor/Action
  insertion on messages/requests/replies, typed helper encodings against native
  schemas, and first-visible versus effective duplicate lookup. Action views
  preserve raw UTF-16; constructing a header does not select a handler.
- Optional application dispatch with a missing ordinary Actor header.
  Exercise raw Endpoint/Channel and accepted-server service paths
  using native classic ReplicationActor/ReplicationFrom/Action envelopes,
  including data and semantic ACKs. Preserve absent Actor records and native
  effective Empty semantics without inserting a header or requiring Actor 0.
  Domain endpoint demultiplexing remains bounded and outside the driver.
  Malformed recognized headers still fail; Actor-routed unknown actors retain
  diagnosed drops, and route misses do not fall back to raw ingress.
  Raw checks, body admission, integrity, captured source, response grants,
  and restricted original-generation ACK sends retain existing contracts.
  Verify managed RPC plus raw ingress and raw-only plus Actor dispatch to
  prove dispatcher selection and correlation mode are independent.
  Class-local raw/Actor mixtures, duplicate raw targets or Actor bindings,
  repeated service installation, and dispatcher replacement reject
  atomically. Cross-class raw/Actor composition is allowed without fallback.
  No configuration means no implicit raw catch-all. Notification bindings and
  on_peer_ready are independently authorized; native controls and
  unmatched replies never leak into application delivery. Check before-traffic
  service installation, binding/byte bounds,
  stable dispatcher selection/registry across reconnect, and no new queue,
  decoder, or reader beneath the existing admission path.
- Canonical ingress services on both Channel and Server, using the same raw
  and Actor/Router adapters without native ordinary receiver or manual receive
  loops, hidden forwarding workers, or another correlation table. Cover
  IngressResponse::Reply and NoReply with runner-owned original reply authority,
  actual admitted typed-codec budgets, and source-bound semantic sends.
  Endpoint configuration/unpolled connect must not poll service readiness or
  invoke handlers; setup installs before early reverse traffic. Its cloned
  service prototypes preserve Endpoint Clone + Send + Sync without requiring
  S: Sync, cloning a ready service, or polling a future under a lock.
  Separate explicit connects have independent runners/budgets even when the
  application intentionally shares service state.
  Test live/disconnected Channel registration, one explicit service per scope,
  atomic binding conflicts, and ownership of all Actor bindings by the one
  installed service. Dynamic installation cannot recover previously dropped traffic.
  HandlerLimits is one logical Channel/server allowance, not a fresh pool per
  clone, binding, or physical generation. Old-generation ingress futures and
  reply tickets cancel without replay while configuration survives reconnect.
  Inbound readiness/handler futures retain no public Channel owner guard;
  last-owner/failed-connect cancellation must retire unpublished/active work
  without adding a client drain API or post-drop join promise.
  Application work-queue composition preserves Request context, absolute
  deadlines, and shared storage while the runner retains reply authority.
  Enqueue/worker/oneshot waits stay inside existing bounded handler capacity;
  no second transport queue or waiting pool is created. Closed queue/worker
  channels surface as service errors. Timeout, disconnect, and shutdown races
  invalidate the original ticket; late output cannot resurrect it, forge a
  NoReply handoff, or send a duplicate response. Retained queued storage stays
  alive through ordinary references. Workers are application-owned and must remain usable
  during admitted service drain, then be explicitly stopped/disposed.
- http-shaped request decomposition and reassembly, including mutable headers
  on received requests and public parts headers while trusted context stays
  read-only. Exercise append, remove-all, explicit replacement, failed-edit
  atomicity, current duplicate lookups, copy-on-write metadata, and
  shared ownership without mutating raw messages. Repeated edits do not
  accumulate hidden tombstones. Header swaps and edited records
  must pass original-scope validation before routing/adaptation;
  edits cannot retarget an admitted route, change classification/correlation,
  mint a reply grant, widen policies/limits, or extend the original deadline.
- http-shaped response body mutation, decomposition, reassembly, and mapping
  for native and synthetic responses. Verify public bounded parts headers and
  read-only optional source, preserved original settlement timing, and
  explicit propagation of fallible body-conversion errors. Header/body edits
  must not change native Fault classification or mint provenance/reply authority.
  Splitting/reassembling responses preserves metadata and ordinary decoded-value
  ownership without hidden charges. Shared byte views retain their backing
  independently. Body replacement cannot silently grant native authority.
- Channel byte service composition beneath `RpcClient<T>`, including
  middleware and an external non-Clone/non-Sync transport that constructs
  `Response::new` without private Source access. Generic clients must not
  require physical connection access, a custom transport trait, or hidden admission
  handles. Verify conditional client Clone, mutable calls, same-instance
  readiness/call, deadline metadata through mapping/reassembly, bounded waiter
  registrations, and no duplicated ready permits, queues, or progress tasks.
  Check stricter native byte policy can reject an encoded request after
  count readiness without partial output or leaked reservations.
- Endpoint TCP/custom connects share one native establishment path and finite
  timeout, with no second public owner or Channel connect API. Channel
  clones/submitted service futures retain their private dialed scope;
  extracted Peer/source/request/response values and native operations do not.
  Last-owner release requests cancellation without an idle driver ownership
  cycle or a post-drop join promise. Peer cannot implement the built-in
  transport service, dial, widen captured policy, or become an owning Channel.
  Dropping an unpublished connect retires its scope/result, ingress-service
  work, including all configured notification-service execution.
- Accepted-side initiation through `on_peer_ready` without any incoming
  application message, including finite single-stream acceptance and the
  RA-node harness's NodeUp/NodeUpAck then ReplicaUp/semantic-reply workflow.
  Verify same-generation submission and domain validation without another
  socket reader, driver, owning Channel, or a new connection direction.
  Hook readiness/call cannot block native completion or exhaust request-handler
  capacity needed by bootstrap replies. Check explicit restricted policy,
  unchanged ingress permissions, raw-only request rejection, and no fabricated
  reply grant or authentication. Each established accepted generation admits
  at most one hook; failed establishment, dialed reconnect, repeated readiness
  polls, or hook failure must not create another call.
  Verify same-instance Tower readiness/call, non-Sync service support, finite
  connection/shared metadata accounting, fail-fast admission failure, and one
  nonrenewable readiness-plus-execution deadline. Check the 30-second default
  and rejection of zero/unrepresentable timeout configuration.
  Success leaves the connection usable without an automatic domain ACK.
  Readiness/call/timeout failure preserves its phase/source and retires only
  that connection; escaped Peers cannot remain usable or dial replacements.
  Shutdown races prevent new hook admission, bound tracked hooks by drain,
  grant no new-send exemption, and cancel work on connection/forced-scope
  failure without a post-drop join promise. Record that post-establishment
  timing is deliberately later than native's connection-accepted callback.
- URI Endpoint construction from shared text/bytes, parsed http::Uri, and
  optional SocketAddr conversion. Verify pure/fallible construction,
  parser errors, absolute scheme/authority requirements,
  userinfo rejection, and from_shared fragment rejection before parser truncation.
  Built-in TCP accepts explicit valid ports, hostnames, IPv4, and bracketed
  supported IPv6 with empty/root path and no query. Missing/zero/overflow ports,
  malformed authorities, unsupported paths/queries, and non-TCP schemes fail
  before dialing without HTTP port/scheme defaults or component stripping.
  Socket conversion must never silently discard IPv6 scope IDs.
  Custom URI services explicitly validate their own components; URI path/query
  never implicitly chooses Actor/Action, credentials, or native TLS.
  Initial/reconnect attempts receive the same original URI and can redo DNS
  within each unchanged establishment budget; resolved SocketAddr/source is
  not logical identity. TCP listener binding retains SocketAddr.
- Stable logical Channel reconnection through one retained reusable connector.
  Concurrent RPC readiness and Peer acquisition must
  coalesce into one attempt; idle disconnected handles must not redial in a
  loop. Verify eager initial failure, fresh per-attempt connect budgets,
  unchanged caller deadlines, shorter-waiter cancellation without aborting
  shared work, and last-owner cancellation/publication races.
  Transient reconnect/capacity errors pass through a ready instance's next
  call without permanently poisoning the service. They are never RPC success.
  Cached failure outcomes, zero-waiter fail-fast, diagnostics, and repeated
  failed attempts must remain bounded and preserve original causes/phases.
- Disconnect races with held readiness, unpolled native operations, pending
  correlation, and complete settled replies. Readiness reserved for one
  generation cannot silently target another; admitted requests never replay,
  including uncommitted ones. Peer/Source/Attempt/SendPermit provenance,
  notification operations and private completion tickets stay generation-bound,
  while an existing RpcClient
  can issue a later independent call. Accepted capabilities must never dial.
  Preserve old settled results and reject cross-generation late replies.
- Stable ingress-service configuration, including notification bindings,
  survive physical reconnect with the current registry, not a replay of the
  initial Endpoint descriptors or failed-generation invocations. Dynamic service
  installation survives; there is no notification receiver or registration epoch
  to recreate. Shared HandlerLimits cover all classes/generations. Old
  application-queued requests retain source/storage but cannot keep cancelled
  acknowledgment tickets live or ACK on a replacement automatically.
  Revalidate native setup/security and per-delivery policies on each generation
  without fabricating IPC registration, notification, or replication readiness.
  Connection-bound domain initialization/submission must use the same Peer,
  even if Channel changes generation between separate application awaits.
  Reconnect/current/retiring work keeps its actual owners; old results retain
  byte storage without occupying allocation-budget or new-generation queue credit.
- Preconfigured Endpoint services install ordinary/notification bindings and
  bounded execution before application traffic. Both classes invoke canonical
  services without a transport-owned notification queue or receiver handoff.
  Endpoint clones/connects never share live invocation state. Exercise
  configuration limit lowering, dynamic service installation, shared readiness
  exhaustion, last-owner drop, and application-owned bounded notification queues.
  Notifications use distinct route-option types and class-qualified keys:
  equal Actor/Action values may coexist with ordinary routes without policy
  borrowing, fallback, or retargeting through edited flags/headers.
- Peer::notify requires managed mode, explicit notification/
  acknowledgment checks and captured send restrictions. Verify native special
  flags and managed IDs, pending correlation before commitment, header-stage
  admission, acknowledgment policy/integrity, native Faults, timeout/cancellation
  and generation failure without replay. Notification services use Request<Body>
  and return IngressResponse::Reply; typed handlers reuse unary. NoReply, wrong-kind
  adaptation, failed/dropped handlers and stale queue output must never fabricate
  an acknowledgment. Admitted acknowledgments share reply-completion counts
  and drain with the original ticket; general notification sends gain no drain
  exemption. Installed notification services and application queue waits do not
  demand reconnect or own a Channel.
- End-to-end consumers of the public API sketches, including client/server ownership,
  repeated service rejection, notification classification/policies, connection/source affinity, explicit capacity
  exhaustion, client-owner drop, and signal-and-await server shutdown without
  separate closure observers.
- Awaitable server execution with no hidden startup, concurrent handler work,
  signal-driven drain, active-runner cancellation, and finite accept-source
  exhaustion that does not lose its final establishment.
- Unit-success normal serving completion and structured serving errors;
  ordinary request/peer errors do not poison normal shutdown. Distinguish a
  configured drain timeout from an outer timeout dropping the execution.
  Neither errors nor cancellation imply every background task has exited;
  retained work and byte views follow their ordinary owners.
- Raw service bindings or optional router Actor/Action selection, conflicting
  or duplicate class-local bindings,
  request versus one-way reply rules, byte-facing services, bounded readiness,
  and handler-aware drain without fabricated ACKs or generic error replies.
  Routing has no application serialization bounds or format defaults. Verify
  chosen-service readiness/call on the same clone and bounded waiter reuse
  through outer, routed, and typed services without another admission pool.
  Separate method adapters must use actual configured body budgets and reject
  stale/wrong-kind invocation state before decoding or calling typed services.
  Original admitted replies can drain; context-derived Peers do not
  exempt new application sends from shutdown.
- Reverse client ingress and server-originated sends over the same shared
  messaging components, explicit received-context send grants that cannot be
  widened through typed adapters, and independent native queue/frame limits
  for initiating and accepted roles.
- External connector/acceptor implementations and stream input, including
  a non-Clone/non-Sync reusable URI-input connector transferred once into Channel,
  repeated serialized readiness/call on that same retained instance without
  transport cloning, including later reuse after error/timeout/cancellation;
  default/custom connect sharing one unchanged budget within each attempt,
  cancellation, clean exhaustion with pending establishment, source-preserving
  accept errors including `BoxError`, absent
  socket metadata, and the same framing/handshake/retirement contracts as TCP.
- Default `fubst` and externally implemented non-Serde body codecs over the same
  driver, including distinct encode/decode types, bounded output, malformed
  payloads, and call-local codec failures without connection corruption.
  RPC calls and typed notifications through RpcClient byte-service adapters must
  support unrelated per-call codecs and
  body types, including a non-Clone/non-Sync client codec. Cloneable RPC
  transports support concurrent calls through independent client clones;
  codec state must not be shared.
  Server method services must likewise support non-Clone/non-Sync codecs and
  per-invocation typed services through `server::Rpc`; shared route-service
  bounds cannot leak onto values created inside its future. Exercise unary and
  decoder-only one-way adaptation with both fubst and external formats.
  Unpolled calls must not execute codecs or acquire new transport admission.
  Byte-service futures may only transfer a readiness slot already reserved by
  their own instance and must release it if dropped unpolled.
  Generic typed RPC/notification calls return ordinary futures without Attempt
  guarantees; byte request/notification operations retain synchronized retraction
  receipts. Notification byte-service adapters must preserve the Request deadline,
  native Response parts/source/settlement, error categories, and cancellation
  without adding replay or a hidden native pump.
  Client codecs may use explicit per-call input/output restrictions outside T's
  scope, while Channel applies native frame/send-queue policy on submission.
  Preserve shared backing through decoding and native settlement timing through response mapping.
  Synthetic responses have no source/settlement receipt and use service-result
  observation; result-holding middleware may cause its own wait timeout.
  Verify ordinary decoded-value ownership and shared backing for zero-copy views,
  with no decoded-allocation ledger or constructor-only depth/work ceilings.
- Native limit changes leave body-codec settings unchanged and vice versa;
  default/custom body codecs satisfy their invocation budgets while fixed
  native serialization continues to enforce its own resource ceilings.
- Independent native exchanges for each claimed runtime/role/security profile.
  Internal round trips alone cannot establish native interoperability.

For the deferred TLS/claims implementation, security cases must cover final
wrapped-flight ordering, coalesced wire-phase transitions, authorization
release, and idle TLS output. These are future security release gates, not
initial clear-TCP acceptance gates. Initial acceptance requires explicit
rejection of unsupported security configuration and no downgrade behavior.

Measure allocations, retained bytes, copies, throughput, and tail latency under
representative frame sizes and contention. Segmented buffers alone do not prove
native-equivalent performance. Keep verification resource-bounded and reuse
existing repository tooling.

Retire the old backend only after its consumers have migrated and their
documented contracts have an accepted replacement. Avoid permanent
legacy/native branches throughout the new codec and driver.

## Decisions still requiring evidence

Before claiming production support for a role, settle its concrete finite
defaults and native close/control details, and qualify connection retirement
and explicit re-establishment behavior. Resolve TLS/provider and trust profiles
when the deferred security work is scheduled. Every implementation milestone
must resolve its relevant items rather than hide them behind successful
defaults.

Deferred capabilities include TLS and claims implementation, caller-selected
RPC IDs, alternate protection formats, TLS session renewal, general connection
pools, generated stubs, and a complete actor36 service API. None is implied by
implementing the common raw transport.
