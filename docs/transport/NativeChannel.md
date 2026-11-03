# Operational native channel composition

## Supported composition

`fube_transport::native_channel` implements the existing
[`MessageChannel`, `RpcChannel`, `MessageReceiver` and consuming `ReplyContext`
contracts](../../crates/libs/fube-transport/src/channel.rs) for one dedicated
**explicitly unsecured-development, numeric-loopback connection**, composed for
NamingGateway clients or activated-host IPC.
This is native Fabric TCP messaging, not `IFabricTransport`, UDP, actor36,
a typed client, router or adapter around `Connection`.

`Endpoint::connect()` or `Endpoint::connect_until(deadline)` establishes TCP and
the clear ListenInstance exchange inside one absolute deadline. Endpoint setup
rejects non-loopback/zero-port addresses; connection setup rejects invalid
limits. Clear loopback assumes trusted local peers; it does not authenticate
hostile local processes and does not qualify secure production clusters.
Existing permissions, security controls and concrete connection roles are not
disabled or widened.

### Endpoint, Channel and adapters

`fube_transport::{Endpoint, Channel, ChannelLimits}` exposes the existing backend:
`Channel` and `native_channel::GatewayChannel` are the **same implementation**,
not two transports. `Endpoint::unsecured_loopback(address, composition)` requires
explicit trusted authorization. Adapter factories such as
`FabricClient::endpoint_clear` and `ImageStoreClient::endpoint_clear` choose that
policy outside core; there is no default business profile or profile query.
Construction, setters and bounded configuration clones perform no I/O. The
default connection duration is 30 seconds, independent of the frame watchdog.
`connect_timeout` and `channel_limits` are validated before dial; each borrowed
`Endpoint::connect()` establishes a fresh connection, identity and ID history
within one absolute deadline. Channel clones instead share the original owner.
`Endpoint::connect_until(deadline)` uses the same private connection path with a
caller-owned absolute budget instead of the configured connection timeout.
Channel has no public dialing constructor; clients consume an established Channel.

`Channel::target()` clones the original captured actual-peer target after a
terminal sample. It does not parse an address, consult advertised ListenInstance,
connect, grant permission or promise lasting liveness. All submit-time affinity,
session, policy and terminal checks still apply. This single-connection
capability is not a new default-target requirement on generic multi-target
`MessageChannel` implementations.

Outgoing IPC still uses `native_ipc` to instantiate actual host/PID session state
per connection; accepted IPC uses accepted-socket setup, not an Endpoint dial.
Both return the same Channel, and runtime/Hosting consume its neutral target.
Accepted targets refer to the actual ephemeral peer, never the listening or
advertised address. Session envelopes/adoption remain private. Registration,
leases, upload sequences and admission are domain operations, not stub handshakes.

Shared abstraction does **not** make arbitrary sessions interchangeable or permit
competing protocol pumps. Repeatable receiver acquisition, receiver Clone and
concurrent distinct-message receive remain supported; receivers share one queue,
not subscriptions or an exclusive claim. Upload privately retains one mutable
serial inbox; runtime clones share one adapter driver; Hosting retains one
accepted handler. Raw clones neither duplicate those owners nor prevent explicit
upload/runtime retirement or Hosting whole-socket disposal from closing them.
See [ordinary stub composition](RustFabricClient.md#endpoint-channel-and-local-stub).

### Shared raw sessions, distinct adapters

The channel and replication peers now use the same operational
[`raw_session`](../../crates/libs/fube-transport/src/raw_session.rs) establishment,
staged reader, fenced frame writer and joined I/O lifetime. Raw sessions support
initiating sockets (zero ListenInstance nonce) and accepted sockets (fresh
listen-side nonce). One absolute establishment budget includes connect and both
hello directions. This does **not** make `GatewayChannel` an arbitrary peer
server: its numeric-loopback and composition checks remain in force.

The raw reader validates fixed lengths before allocation, then offers a bounded
header-stage admission/drain decision before body allocation. Idle waiting has
no inactivity timeout; a started frame has one finite progress watchdog.
Adapters retain finite queue/byte ownership and choose priority. Their fence
is sampled around every actual socket poll until the first positive write;
selection and Pending do not commit visibility. After that write, the complete
suffix and its charge stay owned until completion or independent frame failure,
even on caller cancellation, operation expiry or ordinary close. Owners stop
then join; they must not cancel/drop that writer future while retaining a
reusable stream. The channel's explicit terminal whole-socket disposal below
is a different operation, not observer cancellation. Complete local write is never
remote receipt, acknowledgment or commit.

Gateway correlation/history, request/reply grants and IPC identity remain above
raw I/O. Replication codecs, routes, semantic ACK/retry/copy/reconfiguration and
independent control/data admission remain in `fube-data`; neither profile is
forced through the other's protocol.

### Internal established connection engine

Channel's crate-private
[`raw_session::engine`](../../crates/libs/fube-transport/src/raw_session/engine.rs)
owns the established socket/task scope, bounded raw frame progress, priority
write jobs, local-write receipts, common envelope/controls, first terminal cause
and ordinary-close versus whole-socket-disposal/join core. Dialed and accepted
Channels enter this **same** driver only after their explicitly selected session
hello/admission; the restricted accepted IPC constructor is unchanged. This is
not a public parallel Connection or arbitrary stream-adoption API.

Transport state and opaque Channel policy are separate typed responsibilities
under one coordinator. Channel admission inserts metadata and one encoded job
atomically. That job carries a clone of the **original** admitted reservation,
not a new permit, queue or result channel. Reserved/reply jobs precede ordinary
jobs; selected bytes retain that reservation through the full started suffix.
Channel still owns pending correlation/reply deadlines, ID allocation/history,
consuming grants, session envelopes, actor authorization and domain handoff.

Every initial socket attempt takes the visibility gate, samples trusted send
authority outside the coordinator, then holds the coordinator across Channel
eligibility, the actual socket poll and positive-only visibility recording.
This preserves cancellation/expiry/terminal coordination without claiming an
atomic transaction over independently owned domain authority. Local receipts
become operation results only through Channel policy; an RPC still awaits its
authorized correlated reply.

The engine consumes common controls before inline Channel policy delivery.
Ordered/held handoff is awaited before reading any later frame, including a
control; there is no read-ahead inbox. Policy expiry runs in the same owned
driver scope but stays above the engine. A domain-established Protocol cause
and an I/O failure use one first-cause transition; disposal escalation cannot
replace it. Join observers borrow the retained task handle, so their timeout
or cancellation cannot detach its cleanup ownership. Raw peer replication keeps
its independent owner, header admission and control/data queues over the
unchanged public raw primitives.

The former `GatewayProfile` API is removed, with no compatibility wrapper.
`native_demux::ChannelComposition` installs independent stateless envelope
validators for outbound raw/RPC/reserved one-way sends, ordinary replies, actor
ingress, optional normal-request grants and special duplex ingress/responses.
Protocol choices live in client/runtime adapters, not in the connection core:

| Client adapter | Outbound | Unsolicited ingress |
| --- | --- | --- |
| Ordinary | actor5 raw/RPC | none |
| Notifications | actor5 raw/RPC and consuming notification reply | exact actor5 notification envelope |
| Upload | actor26 raw and actor5 RPC | actor25/27 raw semantic ACK envelopes, **no reply capability** |
| Runtime IPC | Hosting/runtime/lease RPC, actor12 reports and separately reserved lifecycle responses | actor8 controls and actor12 lifecycle; independently authorized normal-request grants |

The operational `ActorDemux` is an immutable actor-only table, not the
declaration-only actor/action `routing::Router`/`Registry`. At most 32 unique
positive actors may be registered; duplicate, nonpositive and reserved
transport/security/role actors reject before connect. Table storage is shrunk
to the retained entries. No dynamic registry or executor is supplied.

Controls stay below external dispatch. After common-header validation, empty
effective RelatesTo and nonspecial ingress selects its registered actor,
**without testing ExpectsReply or Action**. Other messages enter reply handling:
special IsUncorrelatedReply goes through configured duplex authorization;
ordinary nonempty RelatesTo goes through configured ordinary authorization and
the request table. There is no actor fallback for a correlated message.
Envelope checks must not block, perform I/O or decode domain bodies; actual
domain handling occurs after bounded pull delivery, outside the driver.

Registration selects ingress only. It cannot grant sender permission or a
reply capability. ExpectsReply alone has no authority: a separate `RequestPolicy`
must authorize a normal request, or `DuplexPolicy` a special notification.
Only that independently authorized envelope mints the private consuming
original-socket/original-MessageId context with its captured response policy.
Public envelopes, flags, arbitrary actor IDs and equal target types cannot
forge or redirect it. Generic correlation and socket checks cannot be disabled
by composition. The API trusts composition code, not wire fields.

The client adapters require nonempty outbound/raw-ingress actions and exact
notification ingress/response actions. Ordinary reply domain Actor/Action
eligibility remains operation-local, including native replies without those
headers. Upload action/version/Activity/sequence/body policy
belongs to ImageStoreClient, not the channel.
Raw sends cannot bypass ordinary RPC or reply authority using ExpectsReply,
RelatesTo or special-notification flags. Ordinary replies must come through
this established socket. Their domain actor/action and body are opaque to the
channel; typed clients validate the particular operation. The adapters reject
upload actors on the ordinary reply path before request-table lookup.
Transport controls and special duplex ingress retain separate checks. Unsupported ingress, malformed
transport fields or unsupported security/reconnect control terminates the
foundation. A well-formed nonzero Fault returns `Error::Remote` with its exact
header code; absent/zero Fault returns the opaque message. Optional diagnostics
and domain bodies are not decoded by the foundation.

Native common-header scanning uses the **last** RelatesTo in wire order, not
the first or any value matching pending state. All records are preserved.
Repeated RelatesTo records have no special count or repetition-pattern limit;
the existing total header-count, header-byte and frame bounds apply. Every
record must decode, including earlier records; a valid empty earlier ID is not
the effective correlation. Only the final ID is used to look up a pending request.
An unknown final ID never falls back to an earlier neighbor. The channel does
not inspect the pending request's domain action or authorize query envelopes.
Typed clients validate their operation's normal actor/action/body profile;
invalid domain envelopes are call-local protocol errors, not channel failures.
Malformed earlier correlation records remain channel Protocol errors.

This follows pinned SF
[Add permits appending duplicates](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.h#L86-L91);
[the receive cache overwrites each RelatesTo](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L805-L823);
[RequestTable selects that final cached ID once](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L71).
The [outer proxy appends client correlation](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L479-L495)
without removing inner records. Opaque unowned extensions such as Idempotent
remain byte-preserved and confer no retry/replay authority.

Notification opt-in permits only a valid actor5
`ServiceNotificationRequest`: nonempty MessageId, ExpectsReply=true,
IsUncorrelatedReply=true, empty RelatesTo, no Fault or IPC identity, and validated
transport envelope fields. ExpectsReply still permits only a second identical
inbound record. Notification envelopes retain their empty-RelatesTo requirement.
Other owned transport fields remain unique.
**Caller inputs retain strict duplicate rejection without any inbound exception.**
The validated envelope mints a private consuming context on the
original target/ID. A response must be actor5/`ServiceNotificationReply`;
the context adds missing RelatesTo, accepts one matching value and rejects
duplicates/contradictions, ExpectsReply=true or special reply flags.
Domain processing-status encoding remains the caller's responsibility.
Composition without actor/duplex ingress refuses receiver creation. Ordinary and Upload adapters terminate
on notification ingress.

Upload ACKs with empty relation select registered actors. Nonempty effective
RelatesTo is rejected by the ordinary-reply adapter, and special flags by
separate duplex authorization, never by an actor-specific shortcut in the core.
ExpectsReply, Fault and IPC are also rejected,
even if a relation names a pending management RPC. MessageId and RelatesTo may
be absent. Accepted envelopes retain their original source and raw contents;
they never mint a notification reply context or consume notification ID history.
ImageStoreClient decodes semantic headers/body **at pull, before stale filtering**.
An unread domain-invalid ACK with a valid transport envelope remains bounded
raw ingress until pull; no idle domain-validation worker is added.

**Not supported by the client adapters:** arbitrary reverse RPC/one-way ingress.
The generic composition supports explicitly registered actors and the bounded
activated-host IPC composition below, not listeners, other transport roles, replication, actor36
application-service transport, TLS/claims/security negotiation, reconnect,
retry, replay, failover, pools, middleware, code generation, routing/registry
or typed codecs in the transport crate. The ordinary typed Client/ClientRequest
implementation lives in `fube-client`; there are no fake capability
implementations for those roles.

## Runtime IPC composition

[`native_ipc::connect_unsecured_loopback`](../../crates/libs/fube-transport/src/native_ipc.rs)
creates the IPC session above the shared channel, reuses its bounded
TCP/ListenInstance exchange and captures a nonempty bounded host identity plus
the actual OS PID. Numeric
loopback/nonzero-port restrictions remain; the runtime additionally requires
genuine node-injected activated single-code-package ExeHost context and rejects
SSL/container/activator/nonactivated profiles. No TLS/claims fallback is added.

Native [IpcHeader fields](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcHeader.h#L10-L32)
are host/client string and PID, distinct from MessageId GUID, runtime ID and
ListenInstance. Native [IpcClient sending](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcClient.cpp#L205-L245)
adds that identity and a missing MessageId to requests and semantic one-way
sends. There is no separate mandatory host-identity ACK. The Rust constructor
does not send a reconnect actor21 hello; reconnect remains unsupported.
Caller-supplied IPC headers and explicit empty IPC MessageIds reject. Correlated
context replies instead carry their original RelatesTo and **no automatic IPC
identity or new MessageId**, matching the separate native receiver-context path.

The generic channel has no IPC-role constructor or host/PID state. Its private,
bounded session-envelope seam validates inbound session envelopes and applies
outbound header/missing-ID policy, including through cloned owners; it is not a
public plugin or domain executor. Fixed common wire/control/correlation guards
and consuming reply authority remain outside that policy. Collision-aware ID
allocation/history, budgets and first-write
fencing remain shared-driver mechanisms. The internal
[`native_request_reply`](../../crates/libs/fube-transport/src/native_request_reply.rs)
component owns the consuming source-bound reply context for both normal and
special ingress. Runtime actor/action rules and control/health processing stay
in the runtime adapter; only opaque ordering acknowledgments live below it.

Hosting calls
[`native_ipc::accept_unsecured_loopback`](../../crates/libs/fube-transport/src/native_ipc.rs)
for the restricted client-only ListenInstance exchange and accepted IPC envelope
policy. A crate-private established-session adoption seam rechecks actual socket
endpoints, finite limits, allocator identity and deadline before publishing the
same channel driver. It does not expose arbitrary stream adoption or domain
admission. Accepted IPC policy permits identity on actor ingress for Hosting's
exact host/PID/session validation; normal gateway/client ingress and ordinary
correlated replies still reject it. Server semantic sends receive missing
MessageIds, not RelatesTo; original-context replies receive neither identity nor
a new automatic ID. Hosting's bounded no-eviction session ID history admits
legitimate priority-lane reordering, rejects replay and retires on exhaustion.
Hosting explicitly invokes generic whole-socket `dispose()` at connection
retirement, including authority withdrawal under backpressure; IPC role never
selects shutdown behavior. An uncertain partial reply on that disposed socket
is never reused. Ordinary abort/close started-suffix guarantees stay unchanged.
Hosting reserves one ordinary lifecycle send, one ingress and one reserved
response within its 4MiB connection scratch budget, leaving lease byte headroom.
See [production local composition and regressions](../cluster/StatelessRaRap.md).

Native [IpcDemuxer](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcDemuxer.cpp#L16-L32)
builds contexts for ordinary actor ingress without testing ExpectsReply; native
contexts are reusable and target-bound. The stronger Rust bounded profile mints
a consuming normal-request grant only after both actor and independent
`RequestPolicy.incoming` authorization, nonempty original MessageId,
ExpectsReply=true, empty final-effective relation and no Fault/special flag.
One-way actor ingress has no context. Special IsUncorrelatedReply remains a
different `DuplexPolicy` mechanism; enabling one does not enable the other.
Private grants capture the original channel/target/ID and response validator,
not subsequently edited public incoming fields. Authorized normal-request IDs
join the existing non-evicting lifetime history; duplicates/collisions fail,
history exhaustion terminates rather than permitting unsafe reuse.

[`fube-runtime/src/channel_adapter.rs`](../../crates/libs/fube-runtime/src/channel_adapter.rs)
owns Hosting/RAP action envelopes and timeouts, actor8/12 eligibility, finite
handoff and domain completion work. Raw registration never installs a domain
worker in transport. Actor8 readiness and invalidation are sampled during
runtime handoff **before any following wire completion is exposed**:
`ordered_ingress` pauses the reader until opaque handoff acknowledgment/drop,
with an independent 30-second bound. This opt-in intentionally orders later
controls/completions too; it is not an unlimited control-progress promise while
an application holds handoff. Client compositions leave it disabled.

Only RAPQuery uses a correlated actor12/RAPQueryReply context; lifecycle replies,
load/fault and endpoint reports use semantic one-way sends. Native
[RAP dispatch](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L180-L283)
and [correlated versus semantic sending](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L1721-L1740)
are the source boundary. Runtime generations/revisions, per-action native
metadata, binding proof, callback/disposal ownership and report semantics remain
above transport. Obsolete queued stateless success is suppressed without
retiring healthy siblings; real channel/authority failure remains terminal.
InvalidateLease's original bounded acknowledgment and orderly-unregister cleanup
remain permitted after serving retirement; neither renews serving authority.

Runtime composes 32 ordinary operations, 32 reserved semantic/reply operations,
32 backend ingress slots and a 3 GiB reservation ceiling (not eager allocation).
Its ordinary semaphore preserves absolute-deadline waiting for RPC versus
immediate bounded report admission; existing 16 global/one-per-partition report
budgets leave lease headroom. Reserved sends require distinct outbound permission
and never manufacture RelatesTo authority; all client adapters reject them.
Normal requests, reports and responses retain applicable per-attempt first-write
validity checks; started suffixes remain transport-owned.

Malformed health protocol success must retire the channel even when the caller
is not repolled, before a later correlated response succeeds. A generic
`request_held` completion returns opaque bytes and an acknowledgment/result
reservation; the reader waits under the original request deadline. Finite
runtime-owned completion work decodes, aborts before releasing malformed
completion, or acknowledges a valid one. Unobserved success/error results retain
runtime admission. Cardinality/identity/per-report domain errors remain distinct
from malformed wire decoding. No health codec or domain callback runs in the
transport. This ordered mode is opt-in; ordinary clients keep opaque replies.

The former parallel `Connection`, legacy reverse/IPC contexts and SendStatus
are removed, not wrapped. Still-used public `connection::{deadline_after,
read_message,write_message,MAX_FRAME_TIME}` remain; free I/O callers own their
watchdogs and partial-frame cancellation safety. This page states the source
boundary, not deployed qualification of an arbitrary later head. Historical
receipts below cannot qualify a changed runtime/channel build.

## Ownership and operations

```rust,no_run
use fube_client::{ClientOptions, FabricClient};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = FabricClient::endpoint_clear("127.0.0.1:19000".parse()?)?;
let channel = endpoint.connect().await?;
let client = FabricClient::new(channel, ClientOptions::default())?;
let _gateway = client.ping(None).await?; // typed domain result, not a raw write ACK
client.close().await?;
# Ok(())
# }
```

Domain protocols may require additional version/activity/priority/time headers.
The client adapter installs the appropriate composition for these protocols.
Native notification setup and processing-status schemas remain above the
foundation. Ordinary FabricClient uses the shared typed adapter above this
foundation. ServiceNotificationClient owns a separate notification-enabled
GatewayChannel/native session; ImageStoreClient owns a separate upload-composed
GatewayChannel with actor registrations and directly pulls bounded ACK ingress.

- Resolution is synchronous, performs no I/O and accepts only the configured
  numeric endpoint. It reuses one private target identity. Equal addresses or
  equal Rust types do not authorize a target from another foundation.
- Creating an operation future moves caller inputs but performs no admission,
  encoding or I/O until poll. Unpolled futures remain caller-owned. Immediate
  admission checks reject expired, overlarge, foreign or unauthorized inputs.
  There is no unbounded pre-admission waiter queue.
- Raw headers/body are preserved outside the explicit IPC identity composition.
  RPC adds missing MessageId and
  ExpectsReply=true, preserves valid supplied unused IDs and rejects duplicate
  or contradictory correlation. No TimeoutHeader, activity, version or client
  role is inserted outside that IPC opt-in. One unchanged deadline covers admission, send and RPC
  completion.
- Writer selection is **not** frame start. Until a positive first write, drop,
  expiry and close can retract the frame. The first positive write is fenced
  with the same visibility gate as cancellation. Optional bounded nonblocking
  validity predicates run outside the shared-state mutex on each pre-positive
  write attempt, followed by live-entry/deadline/terminal revalidation.
  Authority is sampled, not an atomic transaction over arbitrary domain state
  or a stopped clock during a socket syscall. Thereafter the complete
  suffix and reservation survive observer abandonment/expiry/close until a full
  write or the independent 30-second frame watchdog/socket failure.
- Raw/reply success means a **complete local frame write**, not native
  processing, receipt, acknowledgement, durability or exactly-once execution.
  Timeout/failure after a positive write has an uncertain remote outcome.
- Pending RPC ownership is installed before outbound visibility. A settled
  on-time reply/Fault/write result remains settled even if its observer is
  polled after the deadline. A genuinely late completion is Timeout.
  Explicit shutdown can instead retire undelivered results.
- Concrete `request_tracked` additionally returns a nonowning
  `RequestCancellation`. `cancel_unsent()` retracts under the same first-write
  lock and returns true only if no byte became visible; false means an
  unconfirmed remote outcome remains possible. This observation survives
  settlement/expiry/future drop. Ordinary RPC contracts do not acquire
  notification registration policy.
- Native controls and ordinary completions bypass application ingress.
  Receive transfers an envelope only in a ready poll before its deadline.
  Cancelled/expired receive does not consume queued ingress. Full ingress
  remains intact until transfer or explicit terminal retirement; overflow is
  an explicit ResourceLimit termination, never a silent accepted-message drop.
- Concrete `GatewayReceiver::drain_pending` consumes a finite snapshot of
  currently queued authorized ingress in wire order, without waiting for new
  traffic. The sole notification constructor uses this handoff before
  publication; contexts remain private, consuming and bound to their original
  socket. The snapshot retains each slot's reservation until individual
  transfer or Drop, avoiding bulk unaccounted input. Transferred values are
  caller-owned and require consumer accounting.
- Dropping an envelope or unused context emits no response. Calling consuming
  reply consumes its one attempt even if its returned owned future is never
  polled. Public IncomingMessage fields cannot mint private reply authority.

`GatewayChannel` clones are lifecycle owners. `GatewayTarget`, `GatewayReceiver`
and reply contexts are inert/nonowning capabilities; a typed façade should
borrow the owner instead of silently cloning or adopting it. Last owner drop
initiates terminal cleanup. `close(deadline)` initiates shutdown only on poll,
closes admission, discards backend-held queues/results/ingress and joins the
single owned driver after its I/O halves and expiry scope stop. A started frame
is still retired safely. Timeout/drop does not detach its retained join handle
or undo shutdown; repeated/concurrent closes can wait again. Later target/reply
attempts fail Closed. No unused capability keeps workers alive after close.
`abort()`/`abort_with_error()` initiate shutdown synchronously, preserving the
first cause; `closed(deadline)` only observes the retained driver join and never
initiates shutdown. Its timeout/drop also leaves the join handle owned.
`dispose()` explicitly initiates irreversible **whole-socket** retirement and
permits truncating a started frame; it is not operation/observer cancellation.
It can escalate ordinary shutdown without replacing the first cause and cannot
be downgraded or used to resume/reconnect the stream. New admission stops
synchronously; `closed` observes physical driver exit. This operation is
protocol-neutral and available independently of IPC policy.
`wait_terminal(deadline)` separately observes the first terminal cause without
waiting for a started frame's suffix or initiating shutdown; it is count/byte
bounded like other close observers.

## Finite retention and ordering

[`ChannelLimits`](../../crates/libs/fube-transport/src/native_channel.rs)
fixes count, frame/allocation, aggregate reservation and lifetime-ID limits before
connect. Defaults:

| Limit | Default |
| --- | --- |
| Ordinary raw/RPC operations | 8 |
| Consuming reply operations | 4 |
| Undelivered ingress | 8 |
| Concurrent receive waiters / close waiters | 8 each |
| One clear frame | 4 MiB |
| Aggregate retained-slot reservations | 512 MiB |
| Lifetime ID history, including local ListenInstance ID | 4096 |

An operation stays counted while queued, pending or completed-undelivered.
Deadline-aware once-only settlement and pending-correlation retirement are
shared by completion, queue expiry and the expiry worker. An on-time settled
result is not overwritten by later expiry; terminal shutdown/disposal still
discards undelivered results and releases their entry reservations.
The five private typed classes retain independent ordinary/reply/ingress/
receive/close counters; count or waiter-byte policy never depends on numeric
class ordering.
A selected/started writer retains the same reservation independently of an
abandoned observer. Each ordinary/reply/ingress slot conservatively reserves:
`5 * max_frame_bytes + 4096 * size_of::<Header>() + 6 * 65535` bytes.
On Linux x86-64 defaults that is **21,495,802 bytes** per slot. Waiters reserve
256 bytes each. These are conservative allocation allowances, not allocations
made up front, payload lengths or a process-RSS limit.

Input admission uses checked body capacity, header-vector capacity and each
value capacity, not just lengths; oversized empty allocations reject before
encoding. Frame lengths are checked before inbound tail allocation. Encoding,
maximum retained replies and header decoding have finite allowances. Additional
single-reader scratch/latest ListenInstance and finite table/queue/history
bookkeeping are bounded by configured counts, frame size and existing codec
limits. OS socket buffers, Tokio runtime internals, caller-owned unpolled
futures/delivered envelopes and arbitrary application allocations are not
claimed as channel payload accounting.

Replies use independent count capacity and priority over ordinary queued
writes, but cannot overtake a frame already started. Aggregate byte exhaustion
can reject any new data reservation. This cannot block an already reserved RPC
completion or a transport control's below-ingress processing. No fairness or
cross-operation completion ordering is promised.

All used outbound and authorized notification/normal-request IDs remain in finite lifetime history,
including raw-supplied IDs that could otherwise collide with a late reply.
Unknown/late/duplicate ordinary replies never complete another ID and are not
retained; more than 1024 consecutive unmatched replies terminates. History
exhaustion retires the channel instead of evicting IDs unsafely.
Duplicate/colliding notification IDs terminate without minting a second context.
This is transport-attempt collision protection, not semantic page deduplication.
Pinned [ServiceNotificationManagerProxy.cpp:453–483][notification-retry] replaces
the MessageId on **every proxy-to-client forwarding attempt**, including retries;
the sender's unchanged IPC request can retain an ID on a different transport leg.
Repeated logical page/version content under fresh client-leg IDs is admitted
normally and acknowledged through each attempt's original context.
ServiceNotificationClient owns bounded semantic progress and duplicate delivery
suppression; the channel neither decodes pages nor relaxes same-ID collisions.

The notification consumer uses 33 retained backend ingress slots, 32 client-staged
pages and one scoped staging/semantic worker, with the existing 8 MiB client
allocation budget. Its finite transport reservation ceiling is 2 GiB (not eager
allocation); default ordinary limits stay unchanged. Thirty-three maximum-frame
slots cannot fit the ordinary 512 MiB ceiling. Client page-count overflow is
notification ResourceLimit, memory exhaustion is Lagged; independently exhausted
backend admission retains its Common(ResourceLimit) cause.

The upload consumer uses **32 backend ingress slots** and a **1 GiB** finite
reservation ceiling (not eager allocation), with no second handoff queue or
worker. Ordinary operations retain their eight-slot default and consuming
reply capacity is never used by upload content/controls. Upload raw frames omit
ordinary MessageIds: their semantic ACKs correlate by client Activity/sequence,
not transport ID. Actor5 management RPC still allocates an ordinary ID and
retains the unchanged global lifetime-history rules.

## Native evidence and stronger Rust obligations

Source baseline is locally inspected SF
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][sf] and mssf
`35555883476ac5f78f5f53ef0215b6a32e43b574`. Source inspection is not executed
interoperability qualification.

- Native [generic RequestReply:57-122][rpc] adds ExpectsReply/missing ID and
  registers pending before send. [ClientConnectionManager:98-118][time] adds
  domain TimeoutHeader upstream; generic channels do not.
- [Ordinary native matching:281-286][sender] explicitly ignores sender.
  Fixed-socket backend-instance/role authorization is a stronger Rust policy.
  [Native disconnect:193-227][disconnect] exempts session-expiration faults;
  this clear Rust profile instead terminates unsupported session controls,
  without automatic recovery.
- [Demux:188-227][demux] separates actor ingress and ordinary replies, without
  testing ExpectsReply in the actor branch. [TCP controls:2181-2195][controls]
  bypass external ingress. Rust finite pull admission and cancellation-safe
  envelope transfer are stronger obligations, not native callback guarantees.
- [Duplex notifications:35-77][duplex] create context from the notification's
  own MessageId. [Native ReceiverContext:25-49][context] is reusable const Reply
  with captured target/ID and explicitly set/default expiration. Rust consuming
  authority, lazy full-write completion and inert-context shutdown are stronger.
- [Native initial synchronization:1016-1072][sync] always emits a notification,
  even if its domain entries are empty. That is a real native notification,
  not fabricated service content or arbitrary reverse RPC.
- [mssf client builder:267-280][mssf] wraps native COM APIs; it is not an
  independent implementation of these wire protocols.
- Native [gateway completion:465-494][gateway-correlation] appends RelatesTo
  while forwarding composition already carries correlation; generic
  RequestReply also appends ExpectsReply. Earlier task-local SF 11.8.121.4
  execution observed repeated headers. Current RelatesTo handling validates
  every record and selects only the final effective ID, without a bespoke
  repetition count/equality restriction or earlier-ID fallback. Only the
  second identical ExpectsReply exception remains; caller inputs stay strict.

## Verification and execution limits

Deterministic behavior:

```sh
cargo test --locked -p fube-transport native_ -- --test-threads=1
cargo test --locked -p fube-transport -p fube-client -p fube-test-host
```

The new suite covers lazy/expired operations, zero-progress and positive-progress
cancellation, whole deadlines and settled outcomes, raw preservation, Fault
provenance, target/role/correlation/history rejection, allocation/count/byte
overload, full ingress/control/completion separation, consuming replies and
retained inert-capability/timed/repeated/last-owner shutdown.

The maintained foundation validation is the focused deterministic Rust suite,
affected regressions and repository format/type/build/Clippy/Rustdoc checks.
No dedicated standalone Python/Rust onebox harness, native config contract or
framing bridge ships with this foundation.

Earlier task-local development runs exercised real SF 11.8.121.4 ordinary RPC,
raw full write, a genuine initial synchronization notification and its original
consuming reply. Their original source identities, failed/successful outcomes
and verified resource cleanup remain **historical LOCAL PAW evidence**. The
temporary qualification scaffolding was removed under explicit approved scope
reduction; those executions are not an ongoing maintained test for this PR.
Production fixes discovered during that work remain implemented and covered by
the expanded deterministic Rust channel tests.

Ongoing consumer end-to-end qualification belongs in the
[existing test infrastructure](../Development.md) and
[owner-guarded onebox lifecycle](../UbuntuOnebox.md). Ordinary FabricClient now
uses GatewayChannel and the maintained native group exercises naming, all
query/health/deployment/resolution families and one owned MovePrimary. Current
receipts come from rebuilt migrated consumers, not the historical foundation
smoke. The same maintained group now exercises independently owned notification
GatewayReceiver/session consumers and the existing loss relay: one successful
ACK is dropped, the same target logical page is retried under an observed fresh
full MessageId, and a successful ACK is fully forwarded for that retry's exact
ID without duplicate delivered versions. The receipt validator checks these
bounded witnesses. ImageStoreClient now installs its own upload composition;
the maintained `tests.onebox.native_upload` journey separately compares native
COM and Rust package copies, replacement, provisioning and deployed data.

No fresh live pass is inferred from scaffolding removal, source inspection or
deterministic checks. Exact-candidate live results require fresh maintained
receipts. Isolated primary-address-only filtering with distinct nonempty
addresses, secure clusters, arbitrary reverse RPC and unrelated runtime/
hosting qualification remain unclaimed. Upload source alignment and the
maintained journey do not establish secure/replay/general image-store parity.

[sf]: https://github.com/microsoft/service-fabric/commit/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[rpc]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L57-L122
[time]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L98-L118
[sender]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L281-L286
[disconnect]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L193-L227
[demux]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L188-L227
[controls]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2181-L2195
[duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L35-L77
[context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L25-L49
[sync]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManager.cpp#L1016-L1072
[mssf]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/client/mod.rs#L267-L280
[gateway-correlation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L465-L494
[notification-retry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManagerProxy.cpp#L453-L483
