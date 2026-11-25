# furpc: Connections and serving

`Endpoint` configures dialed transport; `Server` configures accepted transport.
Both use the same native establishment and connection driver. Public API
definitions and defaults live in
[channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs),
[server.rs](../../../../crates/libs/furpc/src/transport/server.rs), and
[io.rs](../../../../crates/libs/furpc/src/transport/io.rs).

## Endpoint and I/O extension points

Endpoint construction validates addressing without DNS, I/O, readiness polling,
or task startup. Its configuration includes transport policy, establishment
timeout, local listener advertisement, and optional reverse ingress.
Each explicit connect creates an independent logical Channel. Endpoint clones
copy configuration; Channel clones share a running scope.

A connector is a reusable Tower `Service<http::Uri>` returning `Connected<I>`.
Channel retains that exact service for initial and demand-driven reconnect
attempts. Each attempt polls readiness and calls the same prepared instance
with the complete original URI, allowing fresh resolution. The connector needs
`Send`, not `Clone` or `Sync`, and must remain usable after cancellation of a
previous call future.

`Accept` is the single accepted-I/O extension point. `TcpAcceptor` binds or
adopts a Tokio listener; `StreamAcceptor` adapts a stream without adding
read-ahead or adopting producer tasks. External producers own their queue
bounds and cancellation. The server reserves connection/handshake capacity
before accepting another stream.

Connectors and acceptors transfer owned raw Tokio I/O, not established native
sessions. Native role, hello, framing, correlation, and retirement remain
transport-owned. `IoInfo` records actual adapter diagnostics, not authenticated
identity or proof of locality. Custom I/O cannot create a `Source` or bypass
native establishment.

## URI addressing and validation

Endpoint addresses are absolute hierarchical `http::Uri` values with a scheme
and nonempty credential-free authority. Raw fragment delimiters reject before
parsing. Typed URI construction can validate only the components its caller's
parser preserved. URI syntax supplies no HTTP semantics, native Actor/Action
routing, or authentication.

The built-in TCP connector requires `tcp://host:port`, with an explicit nonzero
decimal port, empty/root path, and no query. IPv6 literals use brackets.
Unsupported schemes do not fall back to TCP. Invalid destination addresses
and unsupported scoped syntax reject explicitly.

Custom connectors receive the full original URI, including path/query, and own
validation of their address conventions. They must not silently reinterpret
unsupported components. Resolution and I/O acquisition occur inside the
attempt's establishment budget.

The implementation authorities are `validate_uri`, `tcp_destination`, and
`TcpConnector` in [io.rs](../../../../crates/libs/furpc/src/transport/io.rs),
and Endpoint constructors in
[channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs).

## Native-style listener configuration

Endpoint and ServerBuilder expose pre-start `listen_address` and `instance`
settings. They configure the internal ListenInstance hello, not socket
binding, publication, reconnect destination, or actual I/O metadata.
The domain owns the advertised listener and its lifetime.

| Configuration | Initiating hello | Accepted hello |
| --- | --- | --- |
| No explicit listener | Empty address and instance zero. | Adapter local address and instance one when available; otherwise empty address and zero. |
| Explicit listener, no instance | Configured address and instance zero. | Configured address and instance zero. |
| Explicit listener and instance | Configured address and instance. | Configured address and instance. |

Explicit empty addresses reject. An instance requires an explicit listener,
and complete configuration/hello-size checks happen before acquiring I/O.
Endpoint clones and reconnects retain that configuration snapshot.
There is no live instance-reset API.

Initiating and accepted hellos use distinct nonce rules, independently of
listener instance and physical generation. Native hello construction and
validation live in
[connection/native.rs](../../../../crates/libs/furpc/src/connection/native.rs);
configuration and implicit values live in `ListenerConfig` in
[transport/mod.rs](../../../../crates/libs/furpc/src/transport/mod.rs).

ListenInstance addresses are not authenticated. A domain's published endpoint
and native listener advertisement are separate concepts; any required equality
or mapping belongs to consumer admission, not a generic transport identity rule.

## Channel and native peer capabilities

Channel is the built-in reconnecting byte-facing Tower transport. Its
`Service<Request<Body>>` implementation exchanges complete native responses.
Typed clients compose above it; raw messaging obtains a `Peer` through
`Channel::peer`.

Readiness reserves an operation/result slot for one physical generation, not
request-sized storage or guaranteed send-queue bytes. `call` consumes that
instance's ready state and requires an absolute request deadline. Calling
without readiness fails explicitly. Actual envelope, authority, frame-size,
and queue checks still apply at submission.

Clone before readiness for concurrent calls. A Channel clone shares logical
supervision, not the original instance's reservation. A reservation captured
from a retired generation fails rather than retargeting a replacement.
Submitted service futures retain the dialed owner while native Peer operations
do not.

A Peer has no constructor, dial, readiness, or Tower service API. It supports
native sends, byte requests, and explicit notifications on its original
generation. `PeerRequestService` and `PeerSendService` adapt an already-granted
Peer without reconnecting or widening its policy.
See [Requests](Requests.md#one-way-client-and-peer-service-adapters).

Managed correlation is the default. `ConnectionOptions::raw_only` leaves
one-way message IDs to the caller and rejects managed RPC/notification
origination. This selection is independent of raw versus Actor-dispatched
ingress.

## Reconnection without request replay

Initial connect eagerly attempts establishment and returns an error on failure.
After a successful connect, later RPC readiness or Peer acquisition can demand
a replacement generation. Channel clones share at most one attempt; an idle
disconnected Channel does not continuously redial.

Each attempt reuses the retained connector with the original URI and repeats
native establishment before publication. One failed attempt reports its error
to its waiters. A later fresh demand may try again; there is no internal
application retry loop, pooling, or replay.

Transient reconnect failures are retained in a Channel instance's ready state
and returned by its next call. Permanent service invalidity can fail readiness
directly. Peer acquisition returns attempt errors directly. Bounded waiter
admission and fail-fast behavior are implemented in `Logical` and Channel's
Service implementation in
[channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs).

A caller's shorter deadline ends only its wait, not the shared attempt needed
by other owners. The attempt has its own finite budget. Last-owner release
cancels establishment and prevents publication.

Failed-generation payloads, IDs, correlation entries, partial writes, Peers,
and invocation tickets never migrate. Service configuration persists for new
deliveries, but old invocations are not replayed.

## Server execution and accepted I/O

`Server::serve_with_shutdown` consumes configuration, an acceptor, connection
options, and a shutdown future. Polling its returned future drives serving;
construction alone starts no accept loop. Binding is separate, so callers can
inspect a listener's actual address before transferring it.

Each accepted stream gets the same native establishment machinery as a dialed
stream. Individual handshake or connection failures are diagnosed without
stopping unrelated acceptance. An accept-source error ends serving with its
cause.

Clean accept-source exhaustion stops acceptance but lets yielded connections
finish normally; an idle established peer can keep serving pending. A shutdown
signal instead stops acceptance and starts bounded drain. Dropping active
serving cancels work without a join guarantee. See [Lifecycle](Lifecycle.md).

Without application bindings, controls and correlation still progress while
unbound application traffic is diagnosed and discarded. There is no separate
receive loop or public accepted connection owner.

## Accepted-peer readiness hook

`ServerBuilder::on_peer_ready` installs optional server-owned Tower work after
native establishment, without requiring incoming application traffic first.
It receives a restricted, nonowning Peer under an explicit `SendPolicy`.
Ingress bindings remain independently configured.

The hook runs at most once per established accepted generation. Readiness and
execution use one prepared service instance under the hook's own absolute
startup deadline, outside the socket driver. It does not consume a generic
request-handler permit or gate unrelated ingress. Dialed generations do not
invoke it.

Success leaves the connection active. Readiness/call failure or expiry retires
that connection, not the listener. Connection retirement cancels the hook;
there is no retry or automatic domain acknowledgment. Server drain tracks
admitted hook work but grants no exception for new general sends.

`ReadyHook`, its factory, and timeout policy live in
[server.rs](../../../../crates/libs/furpc/src/transport/server.rs);
concurrent hook/driver progress lives in
[session.rs](../../../../crates/libs/furpc/src/transport/session.rs).

## Establishment timeout policy

Outbound connector readiness, resolution, I/O acquisition, and native hello
share one absolute deadline derived from `connect_timeout`. Accepted native
establishment uses `handshake_timeout` after I/O handoff, not while the listener
is idle. Stages do not restart either budget.

Operation, handler, peer-ready, partial-frame, write-watchdog, and server-drain
deadlines are separate policies. An outer workflow timeout can end the caller's
wait, but does not prove physical cleanup or reset other users' deadlines.
Defaults and validation live beside Endpoint, ServerBuilder, and
[TransportLimits](../../../../crates/libs/furpc/src/transport/mod.rs).

## Identity and domain recovery

Logical URI, resolved socket, physical generation, ListenInstance, and domain
session identity are distinct. Clear transport authenticates none of them.
Transport establishment does not restore IPC registration, notification
subscriptions, gateway initialization, or replication membership.

A domain handshake must gate submission on the same Peer it initialized.
Initializing a Peer and then separately calling Channel can race reconnection
onto another generation. A consumer byte service can acquire/initialize a Peer
and submit through that exact capability under the unchanged call deadline.
Failure ends that invocation; recovery applies to later work.
