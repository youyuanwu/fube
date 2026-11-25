# furpc design

`furpc` is the shared Service Fabric native transport. It separates byte I/O,
connection progress, native messaging, Tower services, and typed body adaptation.
It preserves Fabric framing and serialization rather than substituting HTTP or
gRPC. The supported transport is clear TCP; native TLS and claims are not
implemented.

These documents describe responsibilities, ownership, and behavioral contracts.
The [crate exports and Rustdoc](../../../../crates/libs/furpc/src/lib.rs) are the
API reference. Signatures, defaults, executable examples, and implementation
details live beside the code, not in parallel Markdown declarations.

## Layers

| Layer | Responsibility |
| --- | --- |
| I/O adapters | Acquire owned Tokio byte streams through URI connector services or `Accept`. |
| Connection driver | Own one physical generation's I/O, native establishment, framing, ordered writes, timers, and retirement. |
| Native messaging | Preserve ordered headers and segmented bodies; classify controls, replies, notifications, and ordinary ingress. |
| Scope and services | Own dialed or accepted work, ingress bindings, service execution, correlation, and original-generation reply completion. |
| Typed adaptation | Encode and decode actual protocol values above byte services, with explicit direction states and body limits. |
| Consumers | Own SF schemas, protocol admission, sessions, retries, semantic acknowledgments, recovery, and application completion. |

`Endpoint` configures a logical `Channel`; Channel clones and submitted service
futures share its dialed lifetime. `Server::serve_with_shutdown` owns accepted
work. Both use the same private connection driver. A `Peer` is a nonowning
capability for one physical generation, never a reconnecting connection owner.

`RpcClient` and `OneWayClient` compose over byte-facing Tower services.
`server::unary` and `server::one_way` adapt typed handlers inside ingress
services. Routing remains byte-facing and optional. Actual encoder and decoder
values belong to each operation; the socket driver does not know application
body types.

## Design map

| Document | Contract |
| --- | --- |
| [Architecture](Architecture.md) | Layering, ownership, dependencies, and implementation map. |
| [Connections](Connections.md) | Endpoint, Channel, connectors, acceptance, listener advertisement, and reconnection. |
| [Messages](Messages.md) | Shared bytes, native headers, raw construction, and submission. |
| [Requests](Requests.md) | Request/response envelopes, typed clients, and correlation. |
| [Codecs](Codecs.md) | Directional body adaptation, header access, bounded buffers, and standalone helpers. |
| [Ingress](Actors.md) | Native classification, raw/Actor bindings, capabilities, and notifications. |
| [Services](Services.md) | Ingress execution, typed serving, private reply authority, and application queues. |
| [Routing](Routing.md) | Exact Actor/Action matching and original-route identity. |
| [Resources](Resources.md) | Wire limits, queue accounting, admission, and consumer backpressure. |
| [Operations](Operations.md) | Commitment, deadlines, cancellation, receipts, and errors. |
| [Lifecycle](Lifecycle.md) | Ownership, server drain, cancellation, and completion. |
| [Framing](Framing.md) | Private receive gates, integrity, segmented preparation, and partial writes. |
| [Security](Security.md) | Clear-transport boundary and native TLS/claims design constraints. |
| [Examples](Examples.md) | Navigation to executable examples and concrete application composition. |
| [Consumer composition](Implementation.md) | Application stubs, domain policy, and intentional byte-facing interfaces. |

For application integration, start with Connections, Requests, Services, and
Examples. For transport changes, start with Architecture, Framing, Resources,
Operations, and Lifecycle.

## Scope

The crate provides no discovery, pooling, load balancing, automatic replay,
remote cancellation, durable messaging, generated stubs, COM/IDL ABI, or actor
activation/persistence framework. Reconnection restores a physical transport
for later demand, not a domain session or a failed invocation.

Local write, correlated reply, bounded domain enqueue, semantic acknowledgment,
application completion, and physical cleanup are distinct boundaries. Shared
byte ownership is not a heap quota. Header metadata and advertised addresses
are not authentication or reply authority.
