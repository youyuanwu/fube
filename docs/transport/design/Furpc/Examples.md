# furpc: Executable examples and source navigation

Examples live with the implementation and tests so their API use is checked
with the code. This page links to those locations rather than maintaining
duplicate Rust sketches.

## Public API examples

| Task | Source | What to follow |
| --- | --- | --- |
| Connect, send raw bytes, call typed RPC, or use byte-service readiness | [Crate Rustdoc](../../../../crates/libs/furpc/src/lib.rs) | Endpoint/Channel ownership, native Peer sends, direct encoder/decoder values, and an absolute deadline. |
| Build a native envelope | [Message Rustdoc](../../../../crates/libs/furpc/src/message.rs) | Ordered generic Actor/Action header insertion and fallible body construction. |
| Send an actual typed one-way payload | [OneWayClient Rustdoc](../../../../crates/libs/furpc/src/client.rs) | Encoder-only composition over PeerSendService and local-write receipt semantics. |
| Adapt a typed ingress handler | [Server Rustdoc](../../../../crates/libs/furpc/src/server.rs) | Byte-facing method service invoking unary inside its original received context. |
| Install exact Actor/Action dispatch | [Router Rustdoc](../../../../crates/libs/furpc/src/routing.rs) | Distinct route option types and IngressService installation. |
| Originate a native notification | [Peer Rustdoc](../../../../crates/libs/furpc/src/peer.rs) | Explicit NotificationPolicy and generation-bound acknowledgment exchange. |

## Composition examples

| Task | Executable source | Relevant contract |
| --- | --- | --- |
| Custom non-Serde codecs or a borrowed byte transport | [client.rs](../../../../crates/libs/furpc/tests/client.rs) | Per-call direction states, current headers, complete consumption, and original error sources. |
| Shared segmented body views | [messages.rs](../../../../crates/libs/furpc/tests/messages.rs) | Standalone construction without a runtime or connection owner. |
| Custom URI connector and finite accepted I/O | [transport.rs](../../../../crates/libs/furpc/tests/transport.rs) | Reusable same-instance connector readiness, native setup, and finite-source lifetime. |
| Demand-driven reconnect | [transport.rs](../../../../crates/libs/furpc/tests/transport.rs) | Coalesced replacement establishment without retargeting old Peers or calls. |
| Accepted-peer bootstrap and graceful shutdown | [transport.rs](../../../../crates/libs/furpc/tests/transport.rs) | Peer-ready work outside the driver and signal-and-await drain. |
| Native listener advertisement | [listener.rs](../../../../crates/libs/furpc/tests/transport/listener.rs) | Separate configured hello fields, actual socket metadata, and generation nonces. |
| Typed one-way send, byte limits, and cancellation | [one_way_client.rs](../../../../crates/libs/furpc/tests/transport/one_way_client.rs) | No response decoder/RPC slot and no false precommit-retraction claim. |
| Queued typed serving | [typed_server.rs](../../../../crates/libs/furpc/tests/transport/typed_server.rs) | Original context/deadline retained across bounded application work, with runner-owned replies. |
| Exact routing and metadata edits | [routing.rs](../../../../crates/libs/furpc/tests/transport/routing.rs) | Prepared-service affinity and rejection of route/ticket retargeting. |
| Notifications in either direction | [notifications.rs](../../../../crates/libs/furpc/tests/transport/notifications.rs) | Class-local bindings, service-defined ACKs, policies, and drain. |

## Application examples

Real consumer composition is indexed in [Consumer composition](Implementation.md).
Use the application implementation, rather than a transport echo handler, when
working on method-specific envelopes, authority, retries, or completion:

| Application | Entry point |
| --- | --- |
| Gateway/query RPC | [FurpcClient](../../../../crates/libs/fube-client/src/furpc_client.rs) |
| ImageStore commands and raw file content | [ImageStore transport](../../../../crates/libs/fube-client/src/image_store/transport.rs) |
| Runtime Hosting/RAP clients | [IPC client](../../../../crates/libs/fube-runtime/src/ipc_client.rs) |
| Typed Hosting lifecycle sends | [Hosting client](../../../../crates/libs/fube-hosting/src/ipc/client.rs) |
| Node/FM/peer control | [Control adapters](../../../../crates/libs/fube-node/src/control/wire/) |
| Bootstrap calls and replies | [Bootstrap client](../../../../crates/libs/fube-node/src/control/wire/bootstrap.rs), [server](../../../../crates/libs/fube-node/src/control/wire/bootstrap/server.rs) |
| Raw native replication | [Replication connection](../../../../crates/libs/fube-data/src/transport/connection.rs) |
