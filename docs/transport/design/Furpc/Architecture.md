# furpc: Architecture and ownership

**Status:** proposed; see the [design summary](README.md).

Defines scope, source baselines, layering, dependencies, and ownership.
Public signatures live with their topic rather than in a monolithic API file.

## Evidence, compatibility, and scope

The primary problem inventory is
[NativeTransportGaps.md](../../NativeTransportGaps.md). The existing
[operational channel](../../NativeChannel.md),
[native transport specification](../../Transport.md),
[message-format description](../../FabricClientMessageFormat.md), and
[FabricClient design](../FabricClient.md) supply additional evidence and context.

The Rust tree inspected for this proposal is
`0d071ef178eb3397e2a7c4654fe092360d215295`. The gap report compares a different
recorded baseline, `2b9e8436bc120b63a5dd01bcda6ba0bc3c45b2fb`; its claims must
not automatically be applied to every later tree.

The initial native source-conformance target is Service Fabric revision
[`51a7703a1f83749a9234d3ceb4296d8622e42cca`][sf-revision], used by the gap
report. Some existing documents use
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. Reconcile relevant differences
before treating an older description as normative for a feature. Native source
behavior, Rust adaptation, and deployed interoperability evidence must be
recorded separately.

Compatibility is qualified by native runtime release, transport role, security
profile, configuration, and exercised protocol operations. It does not mean
identical packetization, TLS ciphertext, C++ class structure, or process memory
usage. A native-valid message rejected by a finite local budget is a resource
policy rejection, not proof of malformed wire data or complete compatibility.

The historical unmerged V1 checkpoint described in the gap report is reference
material only. This design does not resume that workflow or assume its changes
are available on main.

Not included in the initial delivery:

- COM/IDL ABI compatibility or native library dependencies.
- HTTP/gRPC substitution, custom cryptography, or automatic security fallback.
- TLS and claims implementation, including certificate/trust configuration and
  secure-session negotiation.
- Other native authentication providers or protection formats.
- Application discovery, gateway selection, replication membership, or durable
  messaging inside the connection layer.
- Automatic request replay, remote cancellation, or exactly-once execution.
- A complete actor36 service API, code generator, or general extractor/actor
  framework beyond the minimal server service/router.
- A full actor framework with actor activation, execution scheduling,
  supervision, persistence, or transparent location.

The initial implementation is explicitly unsecured TCP. TLS and claims remain
designed future capabilities, not implementation or acceptance prerequisites
for this scope. Unsupported security configurations must fail explicitly,
never fall back to clear TCP. Do not add a TLS provider dependency or placeholder
security implementation merely to reserve the architecture.

## Architecture and placement

```text
fube-client       fube-runtime / Hosting       fube-data       future actor36
    |                       |                     |                  |
    +------- domain schemas, sessions, authorization, recovery ------+
                            |
      RpcClient<T> / per-call codecs    typed ingress method services
                |                                  |
    Channel / client byte services     method RPC adapters / codecs
                |                                  |
                |                         optional service Router
                +----------------------------------+
                            |
       requests + native notifications     raw one-way + semantic ACKs
                    \                       /
          raw application ingress / optional Actor dispatch
                            |
            native control / correlation classification
                            |
          Channel (dial) / Server (accept) ownership
           native Peer capabilities / receive contexts
                            |
               one driver per physical generation
         native controls / security / scheduling / lifecycle
                            |
              frame codec / ordered header records
                            |
                         Tokio I/O

fubst: native header/control codec and default typed-body implementation
```

The diagram shows responsibilities, not mandatory processing through RPC.
Replication can use the raw path without constructing a request table.
The public facades differ in establishment and lifecycle configuration, not
message capability. Endpoint configures and establishes an owning Channel.
Ordinary RPC uses its Tower service; native messaging explicitly uses Peer.
`Server::serve_with_shutdown` consumes configuration, an accept source, and a
shutdown future. Both dialed and accepted paths reuse the
same connection driver and raw/RPC/notification/actor components.
Clients can receive reverse traffic, and servers can originate authorized
messages. These are native-transport facades, not FabricClient or actor36 APIs.
Each scope explicitly selects raw application ingress or optional Actor
dispatch. Raw services need no ordinary Actor header; replication
performs its own ReplicationActor demultiplexing above the shared driver.
IngressService is the sole application ingress handler adapter on
Endpoint/Channel and Server. Both owners use the same service runner and
original-ticket completion, with no native application receive queue or second
reply pump. Native notifications have distinct classification/policies but use
that same service runner and runner-owned acknowledgment tickets.
Application queues compose inside services, not through a second public receiver.
The dispatcher choice is independent of managed/raw-only correlation mode.
Header-stage matching and authorization still precede bounded body retention;
there is no extra dispatcher inbox or implicit raw fallback.
An optional server `on_peer_ready` Tower service receives a restricted Peer
after establishment without waiting for application ingress. It runs outside
the driver and can perform domain bootstrap while native/application replies
continue to progress; it creates no new connection owner.

The physical connection and generic supervisor remain internal. Replication
composes Channels and a server under its domain supervisor, sharing protocol
configuration but keeping connection queue accounting independent. It does not
need another public dialed-scope owner, protocol stack, or implicit pool.
Channel's private supervisor retains one reusable connector and can replace a
failed physical generation for later demand. Logical destination/registry and
physical source/correlation identities remain distinct; no failed native
attempt or capability follows a replacement.

### Dependencies and modules

`furpc` may depend on `fubst`, Tokio, `tokio-util` for cancellation/task tracking,
`tower-service` for connectors and byte-facing services, `http` for URI types,
`bytes` for shared byte buffers/URI input, UUID support, and structured
diagnostics. Reusing `http::Uri` does not add HTTP framing or require hyper.
Full Tower middleware is not required for the initial connector
boundary. It must not depend on
`fube-client`, `fube-runtime`, `fube-data`, Hosting, or transaction engines.
Existing workspace dependencies should be reused; additional dependencies are
declared when implementation begins, not assumed to exist because an older
design mentioned them.

```text
crates/libs/furpc/src/
    lib.rs
    error.rs
    client.rs            transport-generic RpcClient and codec-limit policy
    server.rs            server configuration, execution, Rpc adapter re-export
    service.rs           method RPC adapters, bindings, responses, handler scope
    routing.rs           Actor/Action routing to byte-facing services
    wire/
        frame.rs          fixed header, CRC, incremental parsing
        headers.rs        records, public header namespace, typed native views
        message.rs        standalone builder, immutable headers/segmented body
    codec/
        mod.rs            public Codec, Encoder, Decoder, bounded buffers
        fubst.rs          default typed-body codec using fubst
    connection/
        driver.rs         sole I/O and protocol-progress owner
        state.rs          connection/control state transitions
        security.rs       deferred TLS/claims negotiation and protection
        send.rs           admission, scheduling, offsets, receipts
    transport/
        endpoint.rs       URI validation and inert native-profile configuration
        channel.rs        logical client service and shared reconnect state
        connector.rs      retained reusable Service connector and addresses
        accept.rs         Accept, accepted I/O, stream adapter
        tcp.rs            TCP connector/acceptor and socket configuration
        listener.rs       owned accept scope and bounded establishment
        peer.rs           nonowning native messaging capability and identity
        scope.rs          internal common connection/task supervision
    messaging/
        demux.rs          native classification, raw binding / optional Actor dispatch
        request_reply.rs  pending requests and transport Faults
        notification.rs   native notification operations and acknowledgment rules
```

These are proposed files, not a requirement to create empty modules. Implement
coherent vertical slices. Keep concrete internal types and narrow interfaces;
do not add a trait, boxed future, task, or queue for each conceptual boundary.

### Ecosystem patterns

| Precedent | Adopt | Do not assume |
| --- | --- | --- |
| [Tokio][tokio] | I/O, readiness, timers, cooperative task execution. | Another runtime abstraction is needed. |
| [h2][h2] | Separate submission handles from the driver owning protocol progress. | Fabric provides HTTP/2 streams or flow-control windows. |
| [hyper connection API][hyper] | A useful connection API independent of pooling and discovery. | Every consumer wants a batteries-included client. |
| [Tower][tower] | Readiness and optional service middleware. | Dropping a middleware future retracts committed bytes. |
| [axum][axum] / [tonic][tonic] | Handler/state ergonomics and replaceable typed-body codecs above transport. | Routing or generated stubs belong in the socket driver. |

## Types and ownership

| Type | Clone / concurrency | Ownership |
| --- | --- | --- |
| `Channel` | Cheap Clone, Send + Sync; sole built-in client Tower transport with mutable readiness/call. | Stable logical scope with a retained connector, current/retiring generations, and at most one reconnect attempt. Clones/service futures share ownership; no native request replay. |
| `Server` | Not `Clone`; configuration and registration before execution. | Consumed by its single async serve API. |
| Future returned by `serve_with_shutdown` | Owned `Send + 'static` future returning `Result<()>`, not a separate public wrapper type. | Drives acceptance and normal drain; drop requests cancellation without a post-drop join guarantee. |
| `Endpoint` | Clone, Send + Sync; URI/native policy, timeouts, HandlerLimits, and inert ingress service prototype including notification bindings. Prototype synchronization permits Send-only service configuration without requiring Sync. | Inert configuration; constructors validate without DNS/I/O. Explicit connects clone unprepared service configuration into independent scopes; Endpoint clones create no runners or queues. |
| `Endpoint::listen_address` / `instance`, corresponding ServerBuilder settings | Pre-start local transport configuration, copied with Endpoint. No public ListenInstance value. | Supplies internal native hello fields, not socket binding, actual I/O metadata, publication/membership authority or native's live SetInstance reset. |
| Connector service | Reusable `Service<http::Uri>`, Send, with no Clone/Sync requirement; transferred once into Channel. | Retained for serialized initial/reconnect attempts. Each attempt receives the original parsed URI and polls/calls the same instance for fresh I/O/DNS; private supervision owns native setup/progress. |
| `Accept` implementation | `Send`; polled through one mutable accept scope. | Transferred into a server when attached. |
| `Connected<I>`, `Accepted<I>` | Not `Clone`; own an I/O object and metadata. | Transfer one unpublished byte stream into native establishment. |
| Peer-ready service | Optional `Service<Peer, Response = ()>`; Clone + Send configuration and Send futures, no Sync requirement. | At most one readiness/execution per accepted generation, tracked by serving under finite connection capacity and its own startup deadline. No request-handler permit, public connection owner, or unbounded event queue. |
| `Peer` | Clone, Send + Sync; native send/request/reservation/notify methods, no constructor, dial, readiness, or Service API. | Nonowning generation-bound capability, retaining captured send restrictions. Notifications additionally require explicit NotificationPolicy. Cannot become an owning Channel or expose physical connection machinery. |
| `Source` | `Clone`, `Send + Sync`. | Nonowning original-connection identity/capability; cannot be constructed from an address. |
| `IngressOptions` | Raw/Actor envelope checks for one-way, request, or notification delivery, ingress limits, and optional independent SendPolicy. | Configures class-specific authorization/admission, not body codecs, source identity, or a lifecycle owner. |
| `NotificationPolicy` | Explicit header/source checks for notification and acknowledgment envelopes. | Captured by an outgoing Peer notification or incoming notification binding; cannot waive native flags, correlation, or a Peer send restriction. |
| `Message`, `Body` | Immutable, reference-counted shared storage. | Own byte storage, never socket/task lifetime or queue credit. |
| `Headers` | Ordered collection with fallible atomic edits under native widths; encoded records stay immutable and shared. | Owns header data, not provenance or admission; raw messages expose read-only access. |
| `MessageBuilder` / `HeaderRecord` constructors | Standalone construction under native representation limits only. | Caller-owned data, not transport admission; submission applies the actual scope policy. |
| `Router` / `IngressService<S>` | Cloneable byte-facing configuration/bindings; IngressService needs Clone + Send, not Sync; Router retains its immutable route-sharing bounds. Raw or Actor dispatch within ordinary and notification classes. | Canonically installed on Endpoint, Channel, or ServerBuilder; owner tracks all execution/replies/acknowledgments under shared HandlerLimits. No public owner or mandatory inbox. |
| `server::Rpc<C>` | Per-invocation codec adapter; methods take `&mut self`. No Clone/Sync requirement on its codec. | Decodes/calls/encodes within the supplied service future; no socket, admission pool, or reply grant. |
| `RequestRouteOptions` / `OneWayRouteOptions` / `NotificationRouteOptions` | Distinct immutable route-kind configuration with existing authorization and ingress/send limits. | Options cannot be interchanged to gain another kind's reply/acknowledgment authority. |
| `Request<B>` / `RequestParts` | http-shaped body-first envelope with editable headers and optional local deadline. No initial `Clone` implementation; context stays private and read-only. | Body adaptation preserves metadata, storage ownership, and received context. Metadata edits cannot change original admission, correlation, policies, or server deadline; no reply grant or lifecycle ownership. |
| `RequestContext` | Read-only received context with private constructors; accessed through the request or its parts. | Original source/deadline and optional restricted Peer; not a message-construction dependency, reply authority, or a separate handler argument. |
| `Reply<T>` / `IngressResponse` | Shared inbound-handler output data, not a send capability. | Channel/server ingress runner applies the original generation-bound response grant and completes transmission. |
| `RpcClient<T>` | Clone when T is Clone; ordinary async calls use `&mut self`. T need not be Clone/Sync/static. | Owns a byte-facing transport and finite codec-limit policy, not a codec instance or native correlation. Channel is the default transport; lifetime follows T. Each call consumes a codec. |
| `ClientCodecLimits` | Optional per-call payload restrictions; defaults add no ceiling below native frame capacity. | Restricts encoded output and response input, not decoded heap allocation or a private transport budget. |
| `Response<T>` / `ResponseParts` | http-shaped body access, decomposition, and reassembly with editable ordered headers; no initial Clone. Native provenance/settlement stays private and read-only. | Parts preserve metadata; decoded values have ordinary ownership and byte views share backing storage. Synthetic services construct source-free responses. |
| `Operation<T>` | Not `Clone`; owned `Send` future when its types are `Send`. | Observes one operation; driver owns admitted work independently. |
| `Attempt` | `Clone`, nonowning. | Observes/retracts an uncommitted attempt without retaining its payload/result. |
| `SendPermit` | Not `Clone`; consumed by one send. | Owns a bounded reservation, not the connection. |

Channel already provides shared dialed ownership, without another public owner
or Arc wrapper. A server is moved into its execution future; sharing application
state does not require sharing a live Server owner. A Peer, message or received request
does not own either running scope. A Channel clone shares its dialed
owner; RpcClient ownership follows its transport.
Stopping one scope does not implicitly stop another.
Physical disconnect does not close the logical Channel or its ingress registry.
Current, retiring, and reconnecting work share that scope's limits; only new
calls can use a successfully published replacement generation.

Synchronous construction and registration/configuration methods perform no
network I/O. Explicit async connection/binding methods document their progress.
Builders and unpolled operations do not start driver work or acquire new
admission. A native byte-service future may transfer an already-reserved
readiness slot, just as a send operation may receive an existing SendPermit.
Creating an unpolled server future likewise starts no
accept loop, though it owns its supplied acceptor and any already-bound socket.

The application must keep its Tokio runtime alive while awaiting graceful shutdown.
Dropping a runtime can abort tasks; `furpc` cannot guarantee progress after its
executor has stopped. Last-owner cleanup must signal already-owned work rather
than require spawning onto whatever runtime happens to be current in Drop.
Neither dropping an owner nor a serving error proves that background cleanup
has finished. No independent closure-observation API is provided.

## Wire and policy configuration

The wire codec understands the 12-byte fixed header, native little-endian
fields, CRC rules, the 32-bit frame-length field, and the 16-bit combined-header
area. It does not encode the current 4 MiB Channel policy as a format constant.

Use checked arithmetic before conversion, slicing, reservation, or allocation.
Representable length does not imply allocation permission. Resource checks must
occur early enough that a native-width length cannot trigger a proportional
allocation first.

Configuration has distinct responsibilities:

| Configuration | Responsibility |
| --- | --- |
| Wire/security support | Selected source baseline and supported protection mode. |
| Standalone construction | Native widths and schema checks only; no extra capacity, segment, nesting or work quotas. Submission still applies scope policy. |
| Native decode limits | Finite `TransportLimits` ceilings for native header/control strings, collections, objects, nesting, and extensions; no serializer selection. |
| Application-body policy | Per-call client codecs and server method RPC adapters above messaging; routers only dispatch byte-facing services. Body-specific per-call/handler budgets and codec-local format/work limits remain separate. |
| Connection limits | Frame admission, queued bytes, operation counts, handshake state, and timers. |
| Domain policy | Permitted protocols, session identity, actor/action, operation queues, semantic ACK scheduling/retry, and application semantics. Replication-specific behavior belongs to its framework above furpc. |

Native serialization is a transport implementation detail, fixed for the
supported protocol rather than selected by Endpoint or the server builder. Scope-owned
header validation/encoding, hello, controls, transport Faults, and native
manual/Serde decoding use one internal validated context derived from
`TransportLimits`. Native helpers must not silently reconstruct default
decoders and lose the applicable limits or schema behavior.
Standalone message/header constructors use the same fixed implementation with
native representation limits; they do not capture scope policy.
Submission validates against the actual scope before commitment. See the
[construction/submission boundary](Messages.md#construction-and-submission-limits).

Application-body codecs belong above byte transport/native messaging.
`RpcClient<T>` uses explicit per-call codec limits without reaching through T
to a private connection budget; Channel applies actual transport admission to the
encoded storage. Typed notifications reuse RpcClient over an explicit Peer::notify
byte-service adapter, with the same client codec limits before native handoff.
Peer methods do not select a body codec; server codecs use admitted invocation
budgets. Reusing `fubst` does not couple those configurations.
Shared byte storage follows reference-counted ownership independently of
native send-queue byte counters. Generic client codec work and decoded values
are application-owned. See the
[budget boundary](Resources.md#serialization-and-body-budgets).

Native semantics are the baseline, not a collection of unrelated `Native`
boolean opt-ins. Stricter consumer policies are named and documented above
decoding. Numerical defaults must be published with their resource meaning;
native defaults from different services are not interchangeable.

The [private framing design](Framing.md) specifies `FrameHeader`,
`FrameReader`, header/body admission gates, complete-frame integrity, and
`PreparedFrame`/`FrameCursor` ownership. These are internal concrete contracts,
not public frame APIs. The reader performs no I/O or domain routing; the
driver owns resource decisions, timers, security phases, and delivery.

## One driver, one progress owner

One owned driver controls a physical connection's socket, parsers, security
state, native controls, ordered output, timers, and first failure. Initiating
and accepted sockets enter the same implementation with an explicit role.
Transport-level ListenInstance is not a gateway ping or IPC registration.

Drive reads, writes, commands, and timers fairly with bounded work per turn.
Maintain explicit partial-I/O offsets. Do not await application processing,
hold a shared mutex across an await, or block all reads behind a large
`write_all`. Synchronous policy checks must be bounded and nonreentrant.

Keep protocol transitions separable from I/O for deterministic reasoning.
Effects such as emitting a control, releasing application traffic, or retiring
a connection have one owner; they do not introduce a second unaccounted queue.

Generic async I/O is useful for exercising the driver and later integrations.
Streams yielded by connectors or acceptors become connection-owned, not
borrowed while an unrelated owner may continue using them. External acceptance
cannot assert authenticated peer identity without configured establishment.
The endpoint's connector service and public `Accept` trait supply such I/O;
the generic driver does not downcast it to TCP or delegate native
establishment to the adapter.

[sf-revision]: https://github.com/microsoft/service-fabric/tree/51a7703a1f83749a9234d3ceb4296d8622e42cca
[tokio]: https://docs.rs/tokio/latest/tokio/
[h2]: https://docs.rs/h2/latest/h2/client/index.html
[hyper]: https://docs.rs/hyper/latest/hyper/client/conn/index.html
[tower]: https://docs.rs/tower-service/latest/tower_service/trait.Service.html
[axum]: https://docs.rs/axum/latest/axum/
[tonic]: https://docs.rs/tonic/latest/tonic/
