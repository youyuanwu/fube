# furpc: Application ingress and receive capabilities

Ingress uses Tower services on both dialed and accepted scopes. Raw delivery
and optional Actor dispatch share native classification, byte admission,
complete-frame verification, and original-generation completion authority.
An ActorId is a numeric native routing code, not an actor identity, task,
mailbox, activation, or persistence framework.

## Classification and authority

Native receive processing distinguishes, in order:

1. Connection controls handled by the driver.
2. Special native notifications handled only by notification bindings.
3. Ordinary correlated replies handled by the pending table.
4. Ordinary application messages handled by ordinary bindings.

Acknowledgments are correlated replies, not notification deliveries.
An unmatched or rejected reply does not fall back to application ingress.
Notification and ordinary route misses never fall back to the other class.
Discard still verifies applicable frame integrity and is diagnosed.

Control/frame processing lives in
[driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs).
Pending-reply admission, application dispatch, and verified completion meet in
[session.rs](../../../../crates/libs/furpc/src/transport/session.rs).
Binding selection and authorization live in
[service.rs](../../../../crates/libs/furpc/src/service.rs).

Headers describe an envelope but confer no authority. Core records original
classification, binding, source, policies, correlation, budgets, and deadline
before exposing editable application metadata.

## Application dispatcher selection

Each logical scope installs at most one IngressService. Within each native
traffic class, bindings select either one raw destination or Actor dispatch.
Ordinary and notification classes can choose different dispatchers; neither
is a fallback for the other.

`ServiceBinding::raw` requires no ordinary Actor header. Actor bindings select
whole-Actor services or the optional Router's exact Actor/Action keys.
Raw and Actor bindings cannot coexist within one class. Duplicate, overlapping,
or reserved-control bindings reject explicitly without replacing configuration.

Endpoint can install reverse ingress before connecting. Channel can install
it later for subsequent admissions, including while disconnected, but cannot
recover previously discarded traffic. Server installs ingress before serving.
Bindings are fixed once installed; there is no live replacement/unregistration
controller.

`raw_only` selects caller-owned one-way IDs, not an ingress dispatcher. Raw
ingress can coexist with managed correlation, and Actor dispatch can coexist
with raw-only messaging.

## Ingress policies and send capabilities

The policy types and their checks are defined in
[service.rs](../../../../crates/libs/furpc/src/service.rs) and
[peer.rs](../../../../crates/libs/furpc/src/peer.rs):

| Policy | Permission |
| --- | --- |
| One-way IngressOptions | Deliver an authorized ordinary envelope without a reply ticket. |
| RequestPolicy | Independently authorize an incoming request and its original-path response. |
| NotificationPolicy | Independently authorize a native notification and its acknowledgment. |
| SendPolicy | Permit selected outgoing kinds/envelopes through a captured Peer. |

Envelope checks run synchronously before body retention. They must be bounded,
nonblocking header/source checks, not body decoders or async domain processing.
Native kind/control/managed-field checks remain mandatory.

Ingress registration alone grants no general send capability. Explicit
`with_send_policy` makes the received context expose a restricted Peer, and
its restrictions apply again at submission and commitment. Without that opt-in,
`RequestContext::peer` is absent even for a request with a private reply ticket.
Passive Source metadata cannot recover a Peer.

The server peer-ready hook grants its own independently restricted Peer; it
does not authorize ingress or widen a binding. All such Peers are nonowning,
generation-bound, and unable to dial replacements.

## Actor-less protocols and custom demultiplexing

An absent Actor remains absent in raw records. Native effective Empty semantics
do not require registering Actor zero or inserting a synthetic record.
Actor dispatch uses native effective properties; raw dispatch instead applies
its own authorized envelope check.

Classic replication uses raw ingress and domain ReplicationActor/
ReplicationEndpointId demultiplexing above the transport. Its schemas,
sender/session validation, operation queues, ACKs, and retry are not generic
Actor routing. Original-generation semantic sends use an explicitly granted
Peer, not the runner's private correlated-response ticket.

See the actual adapter in
[fube-data transport](../../../../crates/libs/fube-data/src/transport/connection.rs)
and the [resource boundary](Resources.md#replication-flow-control-boundary).

## Ingress across connection generations

Channel reconnect preserves service configuration, not old invocations.
Failed-generation readiness/handlers and reply/acknowledgment tickets are
cancelled, never replayed. New messages receive new source checks and original
generation bindings.

Application queues may retain bytes, request parts, and Peers after cancellation.
That retention cannot keep invocation state live, redirect completion onto a
replacement, or establish cross-generation ordering.

Service registration and waiting for inbound application work do not demand
reconnection. Later RPC readiness or Peer acquisition can demand it. Domain
session recovery remains separate from transport availability.

## Native notifications

Native notifications are explicit push/acknowledgment exchanges, not ordinary
one-way delivery. Their special flags select a separate traffic class even
when RelatesTo is present. Incoming authorization requires the original native
identity and notification policy, not editable application flags.

`Peer::notify` requires managed mode and an explicit NotificationPolicy. It
shares managed IDs, pending correlation, deadlines, native Fault processing,
and Attempt semantics with byte RPC. It does not reconnect or replay.
The origination implementation is in
[peer.rs](../../../../crates/libs/furpc/src/peer.rs), with managed envelope
assembly in [rpc.rs](../../../../crates/libs/furpc/src/rpc.rs).

Typed originators use RpcClient over an explicit consumer byte service
delegating to notify. Notification ingress uses the ordinary IngressService
boundary with notification bindings or Router notification routes.
`server::unary` adapts its typed body and acknowledgment data.

Only the runner can complete the original acknowledgment ticket, supplying
native flags and RelatesTo. NoReply, errors, dropped work, and cancellation
send no implicit acknowledgment or Fault. `server::one_way` rejects a
notification invocation.

Notification handlers share invocation-time/output policy with ordinary
handlers, not a separate transport queue or handler concurrency quota.
Admitted acknowledgments participate in server drain; general Peers do not
gain permission for new drain-time sends.

Concrete two-direction, policy, cancellation, and drain composition lives in
[notification tests](../../../../crates/libs/furpc/tests/transport/notifications.rs)
and application notification handling in
[fube-client](../../../../crates/libs/fube-client/src/notifications.rs).
