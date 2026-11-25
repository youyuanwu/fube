# furpc: Architecture and ownership

`furpc` separates protocol progress from submission handles and application
services. Its boundaries follow Tokio's owned I/O, h2/hyper's connection driver,
Tower's readiness contract, and tonic's typed adaptation above byte transport,
without adopting their HTTP wire semantics.

## Architecture and placement

```text
consumer schemas, sessions, admission, retries, semantic completion
                       |
       typed clients / typed ingress method adapters
                       |
       byte-facing Tower services / optional Router
                       |
       native ingress, correlation, and Peer messaging
                       |
       Channel dialed scope / Server accepted scope
                       |
       one driver per physical connection generation
                       |
       private framing and fixed native serialization
                       |
                    Tokio I/O
```

RPC and Actor/Action routing are optional compositions, not mandatory stages.
Replication uses raw native messages and its own domain demultiplexing. Either
connection role can receive reverse traffic or originate authorized messages.
Initiating and accepted I/O share establishment, framing, messaging, and service
execution rather than maintaining separate protocol stacks.

The connection driver alone owns socket progress. It does not await handlers
or run application body codecs. Header-stage matching and source checks are
synchronous trusted checks; service readiness, body adaptation, and domain work
run outside that driver.

## Scope and ownership

| Object | Ownership and lifetime |
| --- | --- |
| `Endpoint` | Inert, cloneable URI and policy configuration, including an optional service prototype. Construction and cloning start no work. |
| `Channel` | Shared logical dialed owner. Clones and submitted byte-service futures keep the scope alive; each clone has independent readiness state. |
| Connector | One reusable URI service retained by a Channel for serialized establishment attempts. Separate explicit connects have separate connectors and scopes. |
| `Server` | Configuration consumed by one serving future, which owns acceptance and accepted sessions. |
| `Peer` | Restricted or unrestricted nonowning capability captured from one generation; it never dials or retargets. |
| `Source` | Original-generation provenance and diagnostic metadata, not a send capability or authenticated identity. |
| `IngressService` | Service/binding configuration. The owning Channel or serving future tracks invocation and completion work. |
| Received request/context | Editable application metadata plus read-only, nonowning original invocation context. Retention cannot extend its deadline or revive reply authority. |
| `Message`, `Body`, headers | Shared data storage, independent of socket lifetime and transport queue credit. |
| `Operation`, `Attempt` | Native operation result and commitment observation; neither owns the running scope. |
| Typed client | Owns its chosen byte service; lifetime follows that service rather than an implicit connection owner. |

There is no public physical-connection object, separate dialed owner, client
drain controller, or native application receive queue. The private supervisor
can replace a failed generation for later Channel demand, but no old request,
Peer, correlation entry, or reply grant follows that replacement.

Ingress configuration must not retain its own owning Channel and create a
cycle. Original-generation sends use independently authorized context Peers.
Consumers own any extra workers, queues, and longer-lived domain tasks.

## Implementation map

The [crate root](../../../../crates/libs/furpc/src/lib.rs) defines public exports
and contains entry-point examples. The [manifest](../../../../crates/libs/furpc/Cargo.toml)
is authoritative for dependencies. The transport depends on native
serialization and general-purpose Rust libraries, not on its application
consumers.

| Responsibility | Implementation |
| --- | --- |
| Segmented backing storage | [body.rs](../../../../crates/libs/furpc/src/body.rs) |
| Public bodies, raw messages, and builder | [message.rs](../../../../crates/libs/furpc/src/message.rs) |
| Ordered native header API | [header.rs](../../../../crates/libs/furpc/src/header.rs) |
| Request and protected received context | [request.rs](../../../../crates/libs/furpc/src/request.rs) |
| Response provenance and settlement | [response.rs](../../../../crates/libs/furpc/src/response.rs) |
| Typed reply data | [reply.rs](../../../../crates/libs/furpc/src/reply.rs) |
| Directional codecs and bounded buffers | [codec.rs](../../../../crates/libs/furpc/src/codec.rs) |
| Typed client composition | [client.rs](../../../../crates/libs/furpc/src/client.rs) |
| Typed ingress adaptation | [server.rs](../../../../crates/libs/furpc/src/server.rs) |
| Ingress bindings, invocation state, and reply completion | [service.rs](../../../../crates/libs/furpc/src/service.rs) |
| Actor/Action dispatch | [routing.rs](../../../../crates/libs/furpc/src/routing.rs) |
| Generation-bound capabilities and byte-service adapters | [peer.rs](../../../../crates/libs/furpc/src/peer.rs) |
| Managed IDs, correlation, and native Faults | [rpc.rs](../../../../crates/libs/furpc/src/rpc.rs) |
| Commitment observation and native operations | [operation.rs](../../../../crates/libs/furpc/src/operation.rs) |
| Public error categories and source mapping | [error.rs](../../../../crates/libs/furpc/src/error.rs) |
| Physical connection state and send admission | [connection/mod.rs](../../../../crates/libs/furpc/src/connection/mod.rs) |
| Sole I/O/protocol progress owner | [connection/driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs) |
| ListenInstance and native control schemas | [connection/native.rs](../../../../crates/libs/furpc/src/connection/native.rs) |
| Shared transport policy and listener configuration | [transport/mod.rs](../../../../crates/libs/furpc/src/transport/mod.rs) |
| Endpoint, Channel, and reconnect supervision | [transport/channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs) |
| Connectors, acceptors, and I/O metadata | [transport/io.rs](../../../../crates/libs/furpc/src/transport/io.rs) |
| Server execution and accepted-peer hook | [transport/server.rs](../../../../crates/libs/furpc/src/transport/server.rs) |
| Session classification and concurrent progress | [transport/session.rs](../../../../crates/libs/furpc/src/transport/session.rs) |
| Private wire namespace | [wire/mod.rs](../../../../crates/libs/furpc/src/wire/mod.rs) |
| Fixed headers, wire context, and checksums | [wire/frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs) |
| Native header schemas and decoding | [wire/headers.rs](../../../../crates/libs/furpc/src/wire/headers.rs) |
| Incremental receive and admission gates | [wire/reader.rs](../../../../crates/libs/furpc/src/wire/reader.rs) |
| Segmented frame preparation and cursors | [wire/writer.rs](../../../../crates/libs/furpc/src/wire/writer.rs) |

Concrete internal boundaries do not each require a public trait, queue, task,
or boxed I/O operation. Public extension points are URI connector services,
`Accept`, Tower application services, and body encoders/decoders.

## Wire, policy, and domain boundaries

Native framing and header/control serialization are fixed. `fubst` supplies
their schema machinery and an optional application-body format. Selecting an
application codec cannot change native headers, controls, correlation, or
security behavior.

Native representation bounds, configured transport limits, application-body
limits, and domain admission are different checks. A representable message
can fail a local frame-size or queue policy without being malformed.
Application allocations and shared backing storage are not governed by a
transport heap ledger.

The supported profile is clear transport. TLS/claims constraints belong to
[Security](Security.md); they are not an implemented security module.
Compatibility is specific to the exercised role, runtime, configuration, and
domain protocol. A model round trip does not establish native interoperability.

See [Consumer composition](Implementation.md) for SF-specific policy above the
transport, [Resources](Resources.md) for admission, and [Lifecycle](Lifecycle.md)
for scope completion.
