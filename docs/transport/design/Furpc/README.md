# furpc design

**Status:** design contract for `crates/libs/furpc`, now in the workspace.
See [current implementation](Implementation.md#current-implementation) for
delivered functionality and remaining qualification. Signatures and examples
are not independent native interoperability evidence. Existing legacy transport
support claims remain unchanged; TLS/claims implementation is deferred.

## Summary

`furpc` rebuilds the shared Service Fabric native TCP transport as one reusable
connection/messaging core. Gateway clients, runtime/Hosting IPC, replication,
and future actor36 services are consumers above it, not special cases inside
a gateway-shaped channel.

Borrow Tokio, hyper, Tower, axum, and tonic's separation of I/O, connection
progress, services, and typed APIs. Keep Fabric's native wire protocol; do not
substitute HTTP/2 or gRPC or reproduce the entire ecosystem's abstractions.

| Area | Decision |
| --- | --- |
| Foundation | Immutable raw messages, segmented bodies, and one driver per physical generation. Standalone `Message::builder`/header construction is bounded; submission enforces scope policy. RPC is optional composition above raw messaging. |
| Ownership | Channel clones and submitted service futures share one private dialed scope. There is no separate public dialed owner. `Server` configuration is consumed by one `serve_with_shutdown` future owning accepted work. Native Peer capabilities do not own either scope. |
| Connections | URI-based Endpoint configures a stable logical Channel. Reusable `Service<http::Uri>` connectors establish/re-establish generations using the original URI on shared demand. Built-in TCP requires `tcp://host:port`; no HTTP wire semantics or implicit port/TLS defaults. Channel is the sole built-in client transport; Peer stays generation-bound. No replay/pooling/balancing. |
| Local listener | Native-style listen_address/instance builder settings on Endpoint and ServerBuilder; wire ListenInstance stays internal. Explicit listeners default to native instance zero; existing omitted behavior is preserved. Socket binding, publication, actual I/O diagnostics and authentication remain separate. |
| Requests | http-shaped, body-first `Request<B>` is shared by outbound calls, server middleware, and decoded handlers. Requests and parts allow bounded ordered-header edits; trusted context and original admission remain protected. Message/request/reply envelopes use generic header insertion; Actor/Action helpers are not special envelope fields. Body adaptation preserves current headers and context. |
| Responses | http-shaped `Response<T>` body access and parts reassembly preserve editable headers, read-only native provenance/settlement, and accounting. No HTTP status/version semantics. |
| Codecs | Native header/control serialization is fixed internally using `fubst`. `RpcClient<T>` wraps a byte-facing Tower transport (default Channel), uses ordinary mutable async calls, and consumes codecs per call under explicit client codec limits. Peer request/notify operations remain byte-facing; typed notifications reuse RpcClient over an explicit notify byte-service adapter. Server method services construct `server::Rpc<C>`; routers are codec-free. |
| Ingress | IngressService is canonical on Endpoint/Channel and Server: `Service<Request<Body>, Response = IngressResponse>` with shared bindings and scope-owned HandlerLimits. Ordinary and notification classes retain distinct authorization and class-local raw/Actor dispatch. Actor is optional; replication uses ReplicationActor without a synthetic Actor header or Actor-0 binding. |
| Capabilities | IngressOptions configures one-way/request/notification authorization, not implicit general send authority. Restricted context/peer-ready Peers remain independent; replies and notification acknowledgments stay runner-owned. No native application pull API or hidden inbox: bounded application work queues compose inside services. |
| Serving | The same raw service or optional Actor/Action Router installs on dialed and accepted owners. Separate RPC adapters provide typed handlers; the runner retains reply authority. Optional `on_peer_ready` work initiates accepted-side bootstrap after establishment. No full actor framework is introduced. |
| Resources | Native frame/send-queue limits and explicit execution/backpressure policy are separate from wire validity. Shared storage uses reference counting, not allocation leases or aggregate heap quotas. No extra standalone construction ceilings. |
| Framing | Internal incremental parsing and segmented output; applications receive complete messages, not streaming fragments or public frame objects. |
| Lifecycle | Tonic-shaped signal-and-await server shutdown returns `Result<()>`; Channels close on last-owner release. Peer handles, native operations, and received requests are nonowning. Admitted replies/notification acknowledgments participate in runner drain. No independent closure observers or public client drain/abort methods. Errors/cancellation do not guarantee joined background cleanup. |
| Security | Initial delivery is Linux/Tokio clear TCP. TLS/claims implementation is deferred, but native negotiation and protection boundaries are designed. No downgrade. |
| Domain protocols | Discovery, readiness, replication operation queues/ACK scheduling/retry, session recovery, and application errors stay in consumers. Replication follows native's framework-owned flow control above furpc. Physical reconnect restores no domain session and replays no calls. |
| Migration | Introduce a new crate alongside `fube-transport`; migrate adapters independently and qualify native support explicitly. |

## Design map

Each topic owns its detailed contract. Read the summary first, then follow
the public or internal boundary relevant to the change; there is no second
monolithic API document.

| Part | Owns |
| --- | --- |
| [Architecture](Architecture.md) | Source baselines, scope, layering, dependencies, type ownership, and shared driver. |
| [Connections](Connections.md) | URI Endpoint/validation, reconnecting logical Channel, native Peer, reusable URI connectors, Accept, serving/peer-ready hooks, and establishment/deadline budgets. |
| [Messages](Messages.md) | Native header fidelity, raw `Message`/`Body`, standalone bounded construction, and raw sends. |
| [Requests](Requests.md) | `Request<B>`/parts/context, transport-generic `RpcClient<T>`, client codec limits, `Response<T>`/parts, native IDs, and correlation/reply authority. |
| [Codecs](Codecs.md) | Native serialization, public body-codec traits, and bounded encode/decode behavior. |
| [Ingress](Actors.md) | Native classification, raw versus Actor service dispatch, ingress policies/send capabilities, generation binding, and Peer notifications with service-managed acknowledgments. |
| [Services](Services.md) | Canonical IngressService/IngressResponse on Endpoint, Channel, and Server; native bindings, typed method adapters, private reply completion, HandlerLimits, and lifecycle. |
| [Routing](Routing.md) | Byte-facing Actor/Action service routes, route options, exact matching, and routing limits. |
| [Resources](Resources.md) | Shared storage ownership, native send-queue accounting, admission, and scheduling. |
| [Operations](Operations.md) | Commitment, cancellation, deadlines, receipts, and error categories. |
| [Lifecycle](Lifecycle.md) | Channel lifetime, graceful server completion, drop/cancellation, and cooperative cleanup. |
| [Framing](Framing.md) | Private header codec, receive gates, integrity, discard, EOF, frame preparation, and write cursors. |
| [Security](Security.md) | Deferred TLS/claims state, native wire transitions, and security release requirements. |
| [Examples](Examples.md) | Raw and typed clients, routed/custom services, custom codecs/I/O, and shutdown usage. |
| [Implementation](Implementation.md) | Gap ownership, implementation order, migration, acceptance, and remaining evidence. |

For the shortest usage path, read Connections, Requests, Routing, and Examples.
For core work, read Architecture, Resources, Operations, Framing, and Lifecycle.
Cross-topic references point to the owning contract rather than duplicating
signatures or inventing a second control path.

The delivered consumer slices now include
[data replication](Implementation.md#data-replication-migration).
Its furpc API prerequisite,
[native-style listener configuration](Connections.md#native-style-listener-configuration),
is now implemented for clear transport;
the adapter uses the existing raw service/Peer contracts and
explicitly changes the legacy adapter's admission and cancellation guarantees.
The consumer migration is implemented; native replication qualification remains pending.

## Non-goals and evidence

The initial crate does not provide HTTP/gRPC, COM/IDL ABI compatibility,
automatic replay, remote cancellation, durable messaging, generated stubs,
actor activation/persistence, pooling/balancing, or transparent domain-session
recovery/discovery. TLS and
claims are designed future capabilities, not implemented support.

The [source and scope record](Architecture.md#evidence-compatibility-and-scope)
distinguishes native behavior, intentional Rust adaptations, finite resource
policy, and independently demonstrated interoperability. Internal round trips
and documentation consistency do not establish native compatibility.
