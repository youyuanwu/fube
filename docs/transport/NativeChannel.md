# Operational native gateway channels

## Supported profile

`fube_transport::native_channel` implements the existing
[`MessageChannel`, `RpcChannel`, `MessageReceiver` and consuming `ReplyContext`
contracts](../../crates/libs/fube-transport/src/channel.rs) for one dedicated
**explicitly unsecured-development, numeric-loopback NamingGateway connection**.
This is native Fabric TCP messaging, not `IFabricTransport`, UDP, actor36,
a typed client, router or replacement for existing `Connection` consumers.

`GatewayChannel::connect_unsecured_loopback(endpoint, notifications, limits,
deadline)` establishes TCP and the clear ListenInstance exchange inside one
absolute deadline. It rejects non-loopback/zero-port endpoints and invalid
limits. Clear loopback assumes trusted local peers; it does not authenticate
hostile local processes and does not qualify secure production clusters.
Existing permissions, security controls and concrete connection roles are not
disabled or widened.

Only actor5 with a nonempty Action is authorized for outbound domain messages.
Raw sends cannot bypass ordinary RPC or reply authority using ExpectsReply,
RelatesTo or special-notification flags. Ordinary replies must come through
this established socket. Their domain actor/action and body are opaque to the
channel; typed clients validate the particular operation. Transport controls
and notification ingress retain their separate profile checks. Unsupported ingress, malformed
transport fields or unsupported security/reconnect control terminates the
foundation. A well-formed nonzero Fault returns `Error::Remote` with its exact
header code; absent/zero Fault returns the opaque message. Optional diagnostics
and domain bodies are not decoded by the foundation.

Native common-header scanning uses the **last** RelatesTo in wire order, not
the first or any value matching pending state. All records are preserved.
The ordinary native correlation profile supports `[inner, outer]` or
`[inner, outer, outer]`; an optional third record must repeat the second
byte-for-byte. Every forwarding record must decode to a nonempty MessageId.
An unknown final ID never falls back to an earlier neighbor. The channel does
not inspect the pending request's domain action or authorize query envelopes.
The client request's typed Reply family owns query eligibility and permitted
forwarding actor/action shapes; invalid domain envelopes are call-local protocol
errors, not channel failures. Fourth records, a nonrepeating third record and
malformed earlier records remain channel Protocol errors.

This two/three-record cap is a **Rust bound**, not a native maximum. Pinned SF
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
Disabled mode refuses receiver creation and terminates on notification ingress.

**Not supported:** arbitrary reverse RPC/one-way ingress, activated-host IPC,
listeners, other actors/transport roles, file upload, replication, actor36
application-service transport, TLS/claims/security negotiation, reconnect,
retry, replay, failover, pools, middleware, code generation, routing/registry
or typed codecs in the transport crate. The ordinary typed Client/ClientRequest
implementation lives in `fube-client`; there are no fake capability
implementations for those roles.

## Ownership and operations

```rust,no_run
use fube_transport::{
    channel::{MessageChannel, RpcChannel},
    message::{Header, Message},
    native_channel::{ChannelLimits, GatewayChannel},
};
use tokio::time::{Duration, Instant};

# async fn example() -> fube_transport::Result<()> {
let end = Instant::now() + Duration::from_secs(30);
let channel = GatewayChannel::connect_unsecured_loopback(
    "127.0.0.1:19000".parse().unwrap(), false, ChannelLimits::default(), end,
).await?;
let target = channel.resolve_target("127.0.0.1:19000")?;
let message = Message {
    headers: vec![Header::actor(5)?, Header::action("PingRequest")?],
    body: vec![],
};
let _opaque_reply = channel.request(target.as_ref(), message, end).await?;
channel.close(end).await?;
# Ok(())
# }
```

Domain protocols may require additional version/activity/priority/time headers.
The example demonstrates binding, not a complete typed FabricClient protocol.
Native notification setup and processing-status schemas remain above the
foundation. Ordinary FabricClient uses the shared typed adapter above this
foundation; notifications and image upload retain independent Connection owners.

- Resolution is synchronous, performs no I/O and accepts only the configured
  numeric endpoint. It reuses one private target identity. Equal addresses or
  equal Rust types do not authorize a target from another foundation.
- Creating an operation future moves caller inputs but performs no admission,
  encoding or I/O until poll. Unpolled futures remain caller-owned. Immediate
  admission checks reject expired, overlarge, foreign or unauthorized inputs.
  There is no unbounded pre-admission waiter queue.
- Raw headers/body are preserved. RPC adds only missing MessageId and
  ExpectsReply=true, preserves valid supplied unused IDs and rejects duplicate
  or contradictory correlation. No TimeoutHeader, activity, version or client
  role is inserted. One unchanged deadline covers admission, send and RPC
  completion.
- Writer selection is **not** frame start. Until a positive first write, drop,
  expiry and close can retract the frame. The first positive write is fenced
  with the same private-state lock as cancellation. Thereafter the complete
  suffix and reservation survive observer abandonment/expiry/close until a full
  write or the independent 30-second frame watchdog/socket failure.
- Raw/reply success means a **complete local frame write**, not native
  processing, receipt, acknowledgement, durability or exactly-once execution.
  Timeout/failure after a positive write has an uncertain remote outcome.
- Pending RPC ownership is installed before outbound visibility. A settled
  on-time reply/Fault/write result remains settled even if its observer is
  polled after the deadline. A genuinely late completion is Timeout.
  Explicit shutdown can instead retire undelivered results.
- Native controls and ordinary completions bypass application ingress.
  Receive transfers an envelope only in a ready poll before its deadline.
  Cancelled/expired receive does not consume queued ingress. Full ingress
  remains intact until transfer or explicit terminal retirement; overflow is
  an explicit ResourceLimit termination, never a silent accepted-message drop.
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

All used outbound and notification IDs remain in finite lifetime history,
including raw-supplied IDs that could otherwise collide with a late reply.
Unknown/late/duplicate ordinary replies never complete another ID and are not
retained; more than 1024 consecutive unmatched replies terminates. History
exhaustion retires the channel instead of evicting IDs unsafely.
Duplicate/colliding notification IDs terminate without minting a second context.
This intentionally does **not** implement native notification retry/deduplication.

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
  execution observed the repeated identical headers. The narrow inbound allowance
  validates equality without deleting records or relaxing caller contracts.

## Verification and execution limits

Deterministic behavior:

```sh
cargo test --locked -p fube-transport native_channel -- --test-threads=1
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
smoke. The independent notification/image-transfer Connection consumers remain
unchanged; their results are not fresh GatewayReceiver/upload-channel coverage.

No fresh live pass is inferred from scaffolding removal, source inspection or
deterministic checks. Live notification retry races, nonempty application
notification contents, secure clusters, arbitrary reverse RPC and unrelated
runtime/upload/hosting qualification remain unclaimed.

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
