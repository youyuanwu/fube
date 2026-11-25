# furpc: Consumer composition

Furpc owns transport progress, native messaging/correlation, and guarded typed
adaptation. Consumers own SF schemas, envelope profiles, admission, sessions,
retry scheduling, and semantic completion. The
[core implementation map](Architecture.md#implementation-map) identifies
transport internals; this document identifies the application boundaries.

## Gateway and notifications

[FurpcClient](../../../../crates/libs/fube-client/src/furpc_client.rs) composes
RpcClient over byte-service middleware. Native method Actor/Action, query
profiles, application error validation, and session initialization remain
client policy. Request values reach their actual encoders without using raw
messages as a nominal typed payload.

[Notification handling](../../../../crates/libs/fube-client/src/notifications.rs)
owns its Channel/session and notification mutation state. Outbound registration
and synchronization RPCs use a byte service retaining the original Peer/Attempt;
incoming page acknowledgment serving uses server::unary on the original
invocation. Subscription recovery and domain notification semantics are not
generic reconnect policy.

## ImageStore

The [ImageStore transport](../../../../crates/libs/fube-client/src/image_store/transport.rs)
keeps Delete and upload command types until their actual encoder runs.
File-content commands deliberately emit raw content bytes; empty commands
emit an absent body. Header-dependent reply decoding reads actual envelope
metadata, and ACK serving uses the original one-way invocation.

The [upload owner](../../../../crates/libs/fube-client/src/image_store.rs)
owns transfer state, sequence/ACK policy, cancellation, and application
completion. A successful local write alone does not complete an upload.

## Runtime IPC

[Runtime clients](../../../../crates/libs/fube-runtime/src/ipc_client.rs)
compose typed Hosting RPCs over PeerRequestService and typed RAP semantic sends
over OneWayClient/PeerSendService. Runtime middleware owns method profiles,
original connection/serving authority, deadlines, host/PID metadata, and
ordinary versus reserved response admission.

[Runtime serving](../../../../crates/libs/fube-runtime/src/ipc_server.rs)
separates replying RPC handoff from deferred one-way work. Replying handoff
keeps the original runner waiting and invokes server::unary within its live
scope. One-way serving completes at bounded enqueue; later body adaptation
belongs to the owned domain executor, not an expired server invocation.

## Hosting and local RAP

[Hosting RPCs](../../../../crates/libs/fube-hosting/src/ipc/rpc.rs) use typed
server adaptation for registration and other replying IPC.
[Lifecycle sends](../../../../crates/libs/fube-hosting/src/ipc/client.rs) carry
the actual payload and concrete encoder through OneWayClient on the original
Peer. Hosting controls approved lifecycle action/envelope policy without
depending on RA body schemas.

The public lease boundary lives in
[ipc.rs](../../../../crates/libs/fube-hosting/src/ipc.rs).
`HostLease::send_lifecycle` transfers typed payload/encoder work into the owned
Hosting send path. Its middleware retains `Weak<Entry>` and checks the original
entry's liveness/readiness after encoding at service readiness/submission.
It neither creates an Entry/client ownership cycle nor silently retargets a
replacement host.

[Node RAP helpers](../../../../crates/libs/fube-node/src/local/rap_client.rs)
build actual request DTOs. [Local Node](../../../../crates/libs/fube-node/src/local.rs)
owns effect state, reply routing, and write outcomes. Encoding failure before
submission restores the effect/previous route and reports preparation failure;
it is not a failed-write receipt for work never submitted.

## FM, Node, peer control, and bootstrap

| Composition | Implementation | Domain invariant |
| --- | --- | --- |
| Node control requests | [node_client.rs](../../../../crates/libs/fube-node/src/control/wire/node_client.rs) | NodeRequest owns the selected DTO/generation snapshot. |
| FM control sends | [fm_client.rs](../../../../crates/libs/fube-node/src/control/wire/fm_client.rs) | Effects retain their actual DTO until encoder execution. |
| Peer control sends | [peer_client.rs](../../../../crates/libs/fube-node/src/control/wire/peer_client.rs) | Original Link role, source, and generation remain authoritative. |
| Shared one-way adaptation | [one_way.rs](../../../../crates/libs/fube-node/src/control/wire/one_way.rs) | Header preparation/sealing and typed body adaptation stay separate. |
| Bootstrap clients | [bootstrap.rs](../../../../crates/libs/fube-node/src/control/wire/bootstrap.rs) | NodeUp/LFUMUpload use DTOs; GenerationProposal uses explicit bodyless encoding; reply decoding reads actual headers. |
| Bootstrap replies | [bootstrap/server.rs](../../../../crates/libs/fube-node/src/control/wire/bootstrap/server.rs) | Reply DTO/unit encoding remains inside the original guarded response handoff. |
| Logical target routing and fresh bootstrap | [targets.rs](../../../../crates/libs/fube-node/src/control/wire/targets.rs) | Configured exact Node instances reuse accepted or outgoing Links; connection acquisition never replays submitted work or admits a Node. |
| Link authority and session ownership | [wire.rs](../../../../crates/libs/fube-node/src/control/wire.rs), [session.rs](../../../../crates/libs/fube-node/src/control/wire/session.rs) | Address/header edits cannot recreate original Link authority or move work between generations. |

Both launcher compositions use the same target/session policy for FM and peer
relationships. TCP initiation is separate from application direction: NodeUp is
always Node-to-FM, including when the FM opens the socket. Fresh bootstrap retries
a missing NodeUp ACK with a new request identity and requires the matching final
empty inventory ACK before returning to the local Node admission path.

The configured target set is fixed and caller-driven, with one live Link per
target. Duplicate live connections reject explicitly. Fresh connection demand
can replace a failed physical generation for the same exact Node instance;
existing Links and invocations never retarget. A higher Node incarnation requires
explicit owner-driven replacement after fencing. The target layer joins the old
transport but cannot prove process termination or reconcile replica inventory.
ListenInstance advertisements are not configuration or identity credentials.
The [target tests](../../../../crates/libs/fube-node/src/control/wire/targets/tests.rs)
exercise both TCP directions, bootstrap retry, reconnect without replay,
generation continuity, and stale-capability rejection.

Transport-only loss retains domain uncertainty. The local cluster's explicit
[NodeDown/restart owner](../../../../crates/tools/fube-cluster/src/restart.rs)
can provide physical fencing evidence, drive FM reconfiguration, and admit a
fresh incarnation. The standalone runner retains its original-session loss
gate. Shared target replacement itself does not provide failure detection,
automatic restart, process adoption, Federation membership, or durable RA recovery.
Cluster [parent controls](../../../../crates/tools/fube-cluster/src/process_control.rs)
are typed private process messages, separate from native FM/RA transport.

LFUM inventory is a domain-owned frozen DTO snapshot. Each call encodes under
its current operation deadline; serialization failure occurs before native
submission. Generation advancement, retry eligibility, and semantic replies
remain consumer state, not generic transport replay.

## Classic replication

[Replication wire code](../../../../crates/libs/fube-data/src/wire.rs) builds
native packet/envelope headers and operation buffers.
[The transport adapter](../../../../crates/libs/fube-data/src/transport/connection.rs)
uses raw ingress and original-generation Peer sends for that byte protocol.
It does not need a synthetic ordinary Actor or request/reply body wrapper.

The replication framework owns endpoint/session validation, operation queues,
ACK/RequestAck dispatch, scheduling, and retransmission. Local write,
secondary admission, application acknowledgment, and quorum completion remain
different boundaries. See
[Resources](Resources.md#replication-flow-control-boundary).

## Owned byte-facing boundaries

Byte-facing interfaces are intentional where ownership or the protocol is
itself byte-oriented:

| Boundary | Ownership and reason | Implementation |
| --- | --- | --- |
| Frozen Runtime fault/endpoint payloads | Encode once into shared Body; retries preserve bytes while each send gets a fresh native identity. Schema snapshots and retry lifetime are domain-owned. | [ipc_client.rs](../../../../crates/libs/fube-runtime/src/ipc_client.rs) |
| Fault reporting | Prepare payload before replacing retry state; domain ACK/lifetime rules govern retirement. | [reporting.rs](../../../../crates/libs/fube-runtime/src/reporting.rs) |
| Endpoint publication | Prepare bytes and admit the retry owner before superseding/cancelling the previous pending owner. Preparation/capacity failure preserves the original publication. | `notify_endpoint` in [stateful_reconfiguration.rs](../../../../crates/libs/fube-runtime/src/stateful_reconfiguration.rs) |
| Deferred Runtime one-way ingress | Decode after owned executor admission using retained body limits; bounded enqueue already ended furpc's one-way invocation. | `OneWayCall` in [ipc_server.rs](../../../../crates/libs/fube-runtime/src/ipc_server.rs) |
| Hosting-to-Node lifecycle intake | Opaque admitted bytes retain original Entry provenance. Node verifies ownership before bounded native DTO decoding. | [Hosting server](../../../../crates/libs/fube-hosting/src/ipc/server.rs), [Node intake](../../../../crates/libs/fube-node/src/local/rap_intake.rs) |
| Classic replication | Native envelopes and operation buffers compose a raw byte protocol. | [wire.rs](../../../../crates/libs/fube-data/src/wire.rs), [connection.rs](../../../../crates/libs/fube-data/src/transport/connection.rs) |
| ImageStore file content | Its actual typed encoder emits genuinely raw file bytes. | [transport.rs](../../../../crates/libs/fube-client/src/image_store/transport.rs) |
| Native fixtures and harnesses | Explicit raw construction supports independent schemas, exact bytes, and malformed envelopes. | [RA schemas](../../../../crates/libs/fube-ra/src/protocol.rs), [test-host](../../../../crates/libs/fube-test-host/src/lib.rs), [model peers](../../../../crates/libs/fube-rap-test-support/src/transport.rs) |

Standalone codec helpers transform bodies without invocation authority.
They do not justify recreating a server context, inventing a generic replay
cache, or treating raw qualification fixtures as typed application stubs.
Consumer preflight/canonicalization/diagnostic serialization is likewise
different from pre-encoding an ordinary typed transport payload.

## Completion and lifetime

The transport promises the boundary returned by its operation or service,
not every later domain effect. Consumers must distinguish local write, bounded
enqueue, correlated reply, semantic ACK, domain completion, and physical
retirement.

Domain workers requiring later semantic sends must finish that work before
releasing their connection owner or signaling shutdown. Retained bytes,
contexts, Peers, and retry wrappers cannot keep an ended transport scope live.
