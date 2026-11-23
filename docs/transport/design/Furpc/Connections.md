# furpc: Connections and serving

**Status:** proposed; see the [design summary](README.md).

Defines Endpoint configuration, the byte-facing Channel, minimal native Peer
capabilities, Tower connectors, Accept, optional accepted-peer readiness work, and the sole
`Server::serve_with_shutdown` entry point. See [lifecycle](Lifecycle.md) for
shutdown and [services](Services.md) for canonical scope-owned ingress handlers.

## Endpoint and I/O extension points

Follow [tonic's endpoint/custom-connector pattern][tonic-endpoint] and
[hyper-util's connector model][hyper-connectors]: `Endpoint` is a configuration
struct containing a destination `http::Uri`, native profile, finite transport limits,
establishment timeout and ingress configuration.
`Endpoint::connect` establishes an owning `Channel` using the built-in TCP
connector; `connect_with_connector` transfers a reusable custom connector into
that logical Channel for initial establishment and later reconnection. Both
use the same establishment engine. Endpoint configuration itself
owns no tasks, live channel, receiver queues, or connector. An optional
IngressService prototype is inert configuration, not a running handler scope.
There is no separate
public dialed-scope owner or public physical-connection API.

A connector is a standard `tower_service::Service` mapping a destination to
owned I/O. Do not define a parallel public `Endpoint` or `Connector` trait.
Hyper-util's `Connect` is a sealed convenience alias, not a trait for
downstream implementations to implement directly.

Use [`http::Uri`][http-uri] for Endpoint addressing and connector input, like
tonic, rather than a custom address enum. The small `http` crate supplies URI
syntax/types; it does not require hyper, HTTP framing, an HTTP request model,
or a gRPC protocol. Native messages and Actor/Action routing remain unchanged.
`Connected<I>` carries raw Tokio I/O and furpc metadata; it is not hyper-util's
similarly named connection-metadata type.

Use one canonical inbound trait, `Accept`. `StreamAcceptor` adapts a stream to
that trait, following [tonic's accepted-I/O stream support][tonic-streams] and
[axum's listener abstraction][axum-listener], rather than defining a second
connection-input trait. All application ingress, including native notifications,
uses Request<Body> with private received context and runner-owned completion.
Furpc connection-source types use `Acceptor` terminology; do not reuse an
ecosystem-specific name for a different abstraction.

```rust,ignore
pub trait TransportIo:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static
{}

impl<T> TransportIo for T
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{}

pub trait Accept: Send + 'static {
    type Io: TransportIo;
    type Error: Into<BoxError>;

    fn accept(&mut self) -> impl Future<
        Output = std::result::Result<Option<Accepted<Self::Io>>, Self::Error>,
    > + Send;

    fn info(&self) -> IoInfo { IoInfo::unknown() }
}

impl<I: TransportIo> Connected<I> {
    pub fn new(io: I, info: IoInfo) -> Self;
}

impl<I: TransportIo> Accepted<I> {
    pub fn new(io: I, info: IoInfo) -> Self;
}
```

Connectors and acceptors produce owned raw I/O, not established native sessions.
The transport still assigns initiating/accepted roles and owns hello, native
controls, security phases, framing, admission, and driver shutdown. Neither
extension point can publish a prevalidated `Source` or claim successful
authentication.

`IoInfo` holds optional local/peer diagnostic addresses or labels. A generic
I/O object need not have a `SocketAddr`; missing metadata remains unknown.
TCP adapters obtain addresses from their actual sockets. Metadata supplied
by custom adapters is trusted local configuration, not peer authentication.
Retained metadata counts toward bounded connection bookkeeping and is
validated before publication.
IPC/domain wrappers must require their approved adapter and genuine local
socket checks, not accept a label as proof of loopback or host identity.

Connector readiness and `Service::call` supply only I/O acquisition. The
transport enforces one configured establishment budget around readiness,
acquisition, and native establishment; custom connectors need not implement
timeout plumbing.
Channel retains the connector service, not an already-resolved socket or a
cloned readiness reservation. Each new attempt polls that same service's
readiness and invokes it once for fresh owned I/O. The service must support
repeated calls after success, failure, or cancellation of a previously returned
future. Attempt cancellation retires unpublished I/O; it cannot poison later
service use silently. A connector that cannot continue reports its error
explicitly. Initial connect failure/cancellation drops the unpublished Channel
and its connector; reconnect failure does not consume the live Channel's
connector. No Clone/Sync requirement or public connector-factory trait is added.
No adapter may detach application tasks or continue partially using a stream
after handing it to the driver.
Custom implementations are trusted cooperative code: future polling and I/O
destruction must not block. Trait bounds alone cannot guarantee that. Error
values must not retain live I/O or worker ownership after a failed handoff.

`Accept::accept` yields one stream, `None` for clean input exhaustion, or an
error. Errors propagate with their source and stop that accept scope by
default; they are not silently retried forever. An individual native handshake
failure retires that connection, not unrelated accepted connections. A TCP
adapter may handle documented transient accept errors with bounded backoff and
diagnostics, not a catch-all retry loop.

Clean input exhaustion stops new acceptance without cancelling previously
yielded I/O or its in-progress establishment. The server execution waits for
those establishments and their connections to finish normally; an established
idle peer can keep it pending. This is not a shutdown signal and lets a finite
single-stream input establish successfully.

Accept must be cancellation-safe: retain resumable progress in the acceptor or
dispose unpublished I/O on cancellation. Stop/shutdown cannot lose ownership
of a yielded stream. The transport reserves connection/handshake capacity
before polling for another stream. Adapters or upstream producers must bound
any I/O they queue before handoff; that external queue is not hidden furpc
capacity.

The extension points use Tokio I/O and URI destination syntax, not HTTP
request/response bodies or a runtime-neutral I/O facade. Implementations can use in-memory
streams or other approved byte transports without changing Fabric framing. This does
not qualify those transports as native TCP or supply the deferred Fabric TLS
implementation; a custom encrypted stream is not evidence of native security
negotiation.

## Endpoint, server, and built-in adapters

```rust,ignore
impl Server {
    pub fn builder() -> ServerBuilder;
    pub async fn serve_with_shutdown<A, F>(
        self, acceptor: A, options: ConnectionOptions, signal: F,
    ) -> Result<()>
    where
        A: Accept,
        F: std::future::Future<Output = ()> + Send + 'static;
}

impl ServerBuilder {
    pub fn limits(self, limits: TransportLimits) -> Self;
    pub fn listen_address(self, address: impl Into<String>) -> Result<Self>;
    pub fn instance(self, instance: u64) -> Self;
    pub fn handshake_timeout(self, timeout: Duration) -> Self;
    pub fn peer_ready_timeout(self, timeout: Duration) -> Self;
    pub fn on_peer_ready<S>(self, send_policy: SendPolicy, service: S) -> Result<Self>
    where
        S: tower_service::Service<Peer, Response = ()> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;
    pub fn drain_timeout(self, timeout: Duration) -> Self;
    pub fn build(self) -> Result<Server>;
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl Endpoint {
    pub fn from_shared(uri: impl Into<bytes::Bytes>) -> Result<Self>;
    pub fn uri(&self) -> &http::Uri;

    pub fn limits(self, limits: TransportLimits) -> Self;
    pub fn connect_timeout(self, timeout: Duration) -> Self;
    pub fn connection_options(self, options: ConnectionOptions) -> Self;
    pub fn listen_address(self, address: impl Into<String>) -> Result<Self>;
    pub fn instance(self, instance: u64) -> Self;

    pub async fn connect(&self) -> Result<Channel>;
    pub async fn connect_with_connector<C, I>(&self, connector: C) -> Result<Channel>
    where
        C: tower_service::Service<http::Uri, Response = Connected<I>>
            + Send + 'static,
        C::Future: Send + 'static,
        C::Error: Into<BoxError>,
        I: TransportIo;
}

impl TryFrom<http::Uri> for Endpoint {
    type Error = Error;
    fn try_from(uri: http::Uri) -> Result<Self>;
}

impl TryFrom<SocketAddr> for Endpoint {
    type Error = Error;
    fn try_from(address: SocketAddr) -> Result<Self>;
}

impl TcpAcceptor {
    pub async fn bind(address: SocketAddr) -> Result<Self>;
    pub fn from_listener(listener: tokio::net::TcpListener) -> Self;
    pub fn local_addr(&self) -> std::io::Result<SocketAddr>;
}

impl<S> StreamAcceptor<S> {
    pub fn new(stream: S) -> Self;
}

impl ConnectionOptions {
    pub fn managed() -> Self;
    pub fn raw_only() -> Self;
}
```

Endpoint constructors and fluent configuration are runtime-independent.
`ServerBuilder::build` validates finite server limits and timeouts;
Endpoint connect validates its complete configuration before admission or I/O,
then requires Tokio for establishment. No Channel is published before native
setup succeeds. Invalid limits, timeouts, or ingress configuration fail
explicitly rather than reverting to defaults.

Native header/control serialization is fixed by the supported transport
protocol and implemented internally using `fubst`; neither Endpoint nor the
server builder selects or accepts a native codec. Its resource ceilings belong to
[`TransportLimits`](Resources.md#serialization-and-body-budgets), not a
serializer configuration object. Application-body codecs are selected higher
up by [typed RPC calls](Requests.md#codec-and-call-behavior) and
[server RPC adapters](Services.md#typed-rpc-adapters), never by connect,
serving, or the router.

`Endpoint::service`/`handler_limits` and the corresponding ServerBuilder methods
configure the same canonical
[ingress handler execution](Services.md#service-boundary-and-native-bindings).
`Channel::register_service` supports explicit later installation under the
Endpoint's existing limits. Their signatures and bounds live with that contract.
Stored service configuration needs Clone + Send, not Sync; private prototype
synchronization preserves Endpoint's Clone + Send + Sync contract without
polling application futures under a lock.

Endpoint and Channel have no accept/serve API; Server has no dial/connect API.
Their registries, private connection drivers, codecs, and messaging algorithms
use the same implementation, not separate protocol stacks. Either direction
can carry native reverse ingress or authorized outbound messaging.

### Native-style listener configuration

Configure the local transport listen address and instance directly on Endpoint
and ServerBuilder. These settings are implemented for clear transport; no
public LocalListenInstance value is exposed. The wire ListenInstance and its
nonce remain private transport machinery. Offline API evidence is recorded in
[implementation](Implementation.md#current-implementation), not native
replication qualification.

At the pinned native baseline, [CreateTcp(address, id, owner)][sf-create-tcp]
constructs a listening transport, while CreateTcpClient has no listen address.
[IDatagramTransport::SetInstance][sf-set-instance-api] sets its local instance;
the native default is zero. TcpConnection internally constructs ListenInstance
from [ListenAddress(), Instance(), and ListenSideNonce()][sf-local-hello].
The corresponding Rust configuration is:

```rust,ignore
let endpoint = Endpoint::from_shared(destination)?
    .listen_address(local_listener)?
    .instance(listener_instance);

let server = Server::builder()
    .listen_address(local_listener)?
    .instance(listener_instance)
    .build()?;
```

These are pre-start builder settings, not runtime mutation APIs. Both feed the
same establishment engine, independently of ConnectionOptions' managed/raw-only
selection. No public hello Message, native codec, per-connection callback or
new shared DatagramTransport owner is introduced. Endpoint still owns only
configuration and dials the remote URI; Server still consumes externally owned
I/O through Accept. Unlike native CreateTcp, these setters do not bind sockets.

| Configuration | Initiating hello | Accepted hello |
| --- | --- | --- |
| Both settings omitted | Empty address, instance zero, as today. | Actual adapter local address and instance one when available; empty address and zero otherwise, as today. |
| Explicit listen_address, instance omitted | Exact configured address, native default instance zero. | Exact configured address, native default instance zero. |
| Explicit listen_address and instance | Exact configured address and instance. | Exact configured address and instance, independently of adapter diagnostics. |
| Either role | Transport-generated zero nonce when initiating. | Fresh transport-generated nonzero nonce when accepting. |

The existing implicit accepted instance one is a furpc default, not native
parity; this slice preserves it for existing consumers. Explicit named-listener
configuration uses native's default zero and can select any `u64` instance.
Changing implicit defaults globally would be a separate behavioral change.

`listen_address` rejects an empty explicit listener and validates native
string/control representability without DNS, URI interpretation, SocketAddr
requirements or arbitrary construction ceilings. Client-only use omits the
setting. An explicit `instance` requires an explicit nonempty listen_address,
matching native's requirement that SetInstance has a listening transport.
Setters can be ordered either way; Server build and Endpoint connect reject
incomplete configuration before accepting/acquiring I/O. They also validate
the configured hello against the selected native send-frame limit. Errors
preserve native serialization/resource causes without truncation or fallback.

Cloning Endpoint copies these settings. Explicit connects and demand-driven
reconnects use that snapshot; a Server uses its snapshot for every yielded
connection. Reconnect does not allocate a new listener instance or revive
an original Peer. Physical generations and directional nonces remain distinct
from the transport instance.

Native's [live SetInstance closes existing connections][sf-set-instance-reset]
because they may have advertised the old instance. Rust's consuming builder
method cannot mutate a live Channel/Server and does not claim that reset
contract. A domain changing its listener identity must retire the old owning
scopes, then construct replacements. An in-place reset API, if ever required,
needs a separate lifecycle design; it is not part of these settings.

The configured listen address does not rewrite IoInfo, the remote URI, DNS
results or actual socket endpoints. Furpc does not prove ownership of that
listener, resolve it, use it as a reconnect destination or authenticate it.
Custom adapters must not spoof socket metadata to obtain an advertisement.
The domain owns listener lifetime and validation of its configuration.

Native replication separately [constructs its published endpoint][sf-repl-publish]
and [serializes it in REFrom][sf-repl-from]. Therefore ListenAddress and REFrom
address equality is not a universal native contract. Keep publication/membership
policy above furpc; do not add a generic publish_address setter or use an
untrusted published address to recreate a source-session grant.

For this migration, fube-data retains its current equal-address supported
profile: bind first, validate the advertised IP/address family, and configure
the same transport listener address/instance on both roles. Its exact REFrom
check remains a documented domain-profile restriction, not native parity.
Supporting different native listen/publish addresses requires an explicit
domain mapping/admission policy and independent native qualification; this
design does not silently relax identity checks. TLS/claims remains deferred
and never turns either address into authenticated identity by itself.

`from_shared` is the fallible tonic-shaped URI constructor; callers can pass
owned String/bytes or static strings. `TryFrom<http::Uri>` accepts an already
parsed URI. `TryFrom<SocketAddr>` is only a convenience conversion into a
`tcp://` URI with a literal authority, not another stored destination type.
All constructors are pure configuration and initially select the clear native
security profile; URI syntax does not supply native authentication or TLS.
Detailed [URI validation](#uri-addressing-and-validation) is separate from
connection establishment and connector-specific destination interpretation.

`connect` supplies the built-in `TcpConnector`, itself a
`Service<http::Uri>`, to the common establishment path. It accepts only the
documented `tcp://host:port` form, not HTTP/HTTPS aliases or custom schemes.
`connect_with_connector` instead moves the supplied service into the Channel
and calls it with a clone of the original parsed URI. It does not first dial
TCP, synthesize another address model, or strip path/query components before
the custom service can validate them.
The same retained service is reused for reconnect attempts; Endpoint never
stores a connector or shares one implicitly between separate Channels.

First poll validates the Endpoint snapshot, admits one initial establishment
attempt, acquires I/O, installs configured bounded native ingress, and completes setup
before returning its Channel. Endpoint construction/cloning performs no
readiness polling, dialing, or connection publication. There is no separate
registration API for a physical connection and no publicly constructible
unestablished connection handle. Hostnames resolve in the TCP connector during
that attempt, never while constructing Endpoint.

Each explicit connect creates a separate logical Channel with an initial
physical generation; equal destinations or Endpoint clones do not merge them.
Reuse is explicit through `Channel::clone`; raw native sends use its Peer
capability.
Cloning an endpoint copies configuration, not a live connection or a readiness
reservation or connector state. Keep an endpoint clone if a later explicit
Channel is needed; explicitly supply another connector for a separate custom
connect. Reconnection within an existing Channel uses its retained connector.

For each attempt, await `poll_ready` on the retained connector service, then
invoke `call` on that same prepared instance. The transport does not clone it before
or after readiness; custom services need `Send`, not `Clone` or `Sync`.
Attempts within a Channel are serialized; calls from clones never race on the
connector. Private synchronization/supervision permits a Send-only connector
inside a Send + Sync Channel, without holding a lock across await. Preserve
both readiness and call errors with their sources. Cancellation drops the
attempt future and unused attempt state, not the retained service or a
different caller's shared attempt.

Reserve furpc establishment capacity before polling the connector. Connector
middleware may participate in readiness but must not add hidden unbounded
queues, detached workers, or automatic application replay. No Tower buffer or
retry layer is inserted by default. Simple connectors, like the default TCP
connector, may always report ready while their bounded connection futures
perform I/O.

Channel is a stable logical destination whose future calls can use replacement
generations. Peer capabilities, native operations, and reply grants never
switch generations or redirect already-admitted work. Reconnection is shared
within one Channel, not between independently connected Channels; there is no
pooling, load balancing, or request replay. `Peer::info` is an original-generation
metadata snapshot, not proof that the peer stays live, authenticated, or
domain-ready. Submission still rechecks
terminal state.

Endpoint defaults to `ConnectionOptions::managed`, enabling furpc-owned
RPC/correlation IDs. Its `connection_options` setter can select `raw_only` for
protocols that own their wire MessageIds; Channel's RPC service then fails
Unsupported, while raw native messaging remains available through Peer.
The same options apply to accepted I/O through serving. Modes do not change on
a live connection. Correlation mode is independent of
[application dispatcher selection](Actors.md#application-dispatcher-selection):
raw ingress can coexist with managed RPC, and raw-only messaging can use
optional Actor dispatch. Selecting `raw_only` does not install a raw receiver
or require an Actor header. Advertised native listen identity
must be validated and cannot override the actual peer or driver-generated
role/nonce. TCP-only settings such as interface binding, keepalive, and
TCP_NODELAY belong to `TcpConnector`/`TcpAcceptor`, not the generic driver.

## URI addressing and validation

These are local Endpoint addressing conventions, not a new native wire field,
HTTP target, Fabric naming URI, or Actor/Action path.
`Endpoint::from_shared("tcp://node.example:19000")?` and
`Endpoint::from_shared("tcp://[::1]:19000")?` use the built-in clear-TCP profile.
`memory://echo` illustrates a custom connector's logical destination, not a
TCP hostname with an invented port.

Common construction rules:

- Require an absolute hierarchical URI with a scheme and nonempty authority;
  relative paths, bare authority strings, empty targets, and request-target `*`
  are InvalidInput. Never assume a scheme or upgrade/downgrade one.
- Reject authority user information; credentials are not Endpoint addresses.
  `from_shared` rejects raw fragment delimiters before parsing because the
  underlying URI parser can discard fragments. A typed Uri constructor can
  validate only components still present; it cannot recover text already
  discarded by the caller's parser. Percent-encoded component bytes are not a
  fragment delimiter and are not decoded into routing or security policy.
- Preserve parsing failures as InvalidInput and retain the original URI
  semantics for each connection attempt. Do not impose extra URI-construction
  byte/work or backing-capacity quotas. No constructor performs DNS or I/O.

The selected connector applies additional rules before I/O:

| Connector | Accepted destination and explicit rejection |
| --- | --- |
| Built-in TCP | `tcp` scheme, nonempty hostname or supported IP literal, and explicit decimal port 1 through 65535. Require an empty/root path and no query, including empty queries. Apply existing TCP destination checks; bracket IPv6 authorities. Missing/zero/invalid ports and unsupported address syntax fail without defaults. |
| Custom | Receives the complete original parsed Uri on every attempt. It must document and validate its schemes, authority/port, path/query semantics, and supported address syntax. It rejects unsupported components rather than silently ignoring them. Native transport still owns hello, security phases, and managed messaging. |

Unknown/default-unsupported schemes return Unsupported from built-in connect,
not a TCP fallback. Furpc's common/TCP validation reports malformed components
as InvalidInput; custom services preserve their own error sources and connector
phase. No HTTP default
ports, path stripping, percent-decoding into a different destination, or
mapping URI paths to native Actor/Action is implicit. Root/no-path forms are
equivalent for TCP. Scheme comparison is case-insensitive, and DNS host handling
uses resolver rules; custom component semantics are not globally normalized.
None of these address rules changes native Action matching.

The SocketAddr convenience conversion uses correct IPv6 brackets and preserves
the full supported destination. Zero ports or scoped addresses that cannot be
represented/handled losslessly reject explicitly; never drop a scope ID.
SocketAddr remains the TCP listener bind/local-address type.

Channel retains the original parsed URI snapshot, not only the first resolved
IP or SocketAddr. Each initial/reconnect attempt passes its clone through the
same retained connector, allowing fresh DNS/custom resolution within that
attempt's one connect timeout. URI equality does not merge channels, prove
peer identity/locality, or carry reply authority. New generations still get
their own validated native identity and source.

No URI scheme enables the deferred native TLS/claims implementation.
`http`/`https` do not mean HTTP transport here, and builtin connect does not
accept them. A custom encrypted stream or secure-looking URI is not proof of
native TLS negotiation; unsupported native security profiles still reject
without downgrade. Future security configuration must preserve these native
phase boundaries rather than infer an ordinary HTTPS handshake.

## Server execution and accepted I/O

`Server::serve_with_shutdown` consumes the configuration, one `Accept` source,
and a shutdown future, following [tonic's shutdown-driven serving
model][tonic-shutdown]. Its ordinary async future represents the execution
lifetime, not a startup receipt. Normal completion waits for admitted work
and active connections; cancellation and errors are not task-join barriers.
Its unit-success result follows tonic/axum serving: `Ok(())` means normal
completion; serving failures use the existing structured error
type. See [completion results](Lifecycle.md#completion-results) for drain
expiry, cancellation, and the distinction from ordinary request/peer errors.
There is one serving API: no second serve overload, fluent shutdown wrapper,
or public execution-object type. Pass `std::future::pending::<()>()` when no
external shutdown signal is needed.

Calling the method creates an owned future without performing I/O. That future
drives the accept loop, rather than starting one hidden behind a handle.
First poll admits the server scope and starts accepting; do not await it merely
to obtain a running-server handle. The caller supplies `TcpAcceptor`,
`StreamAcceptor`, or a custom `Accept`, and drives serving concurrently with
any separately owned message-receiver processing work.
Binding stays separate: obtain TCP port zero's actual address from
`TcpAcceptor::local_addr`, or generic diagnostics from `Accept::info`, before
moving the acceptor. No listener-control handle is returned.
For explicitly background execution, spawn the returned future with
`tokio::spawn` and retain its task handle; the library does not make that
choice at construction.

Each yielded stream gets a finite handshake budget. Idle accepting has no
connection timeout. A completed graceful-shutdown signal immediately drops the
acceptor, stops new acceptance, and starts bounded drain. End of a finite accept
source instead lets admitted connections finish normally. An accept-source
error requests cancellation of remaining work and returns its error without
promising joined cleanup; it is never converted into a successful server exit.
Individual connection/handshake failures do not automatically stop acceptance
for unrelated peers.

An optional configured service/router makes this a complete handler-serving
path: execution owns its readiness, admitted handler futures, and response
completion. No separate application receive loop is exposed.
Without an ordinary service, unbound ordinary application messages are
diagnosed/dropped; native controls, correlation and an optional peer-ready hook
remain available. Notification-only ingress still installs an IngressService
with explicit notification bindings; it is not a separate receive facility.

Dropping a polled execution future stops acceptance and initiates irreversible
whole-scope cancellation, not graceful completion. Its acceptor is disposed;
connection work may still be exiting asynchronously. No independent completion
observer or post-drop join guarantee is exposed. Committed output cannot be
resumed on another stream. An unpolled execution future simply releases its
supplied input/configuration; no server task was started.

A cancelled Endpoint connect likewise cannot leave an unneeded unpublished
connection usable: dropping its future before handle handoff cancels that
attempt, including an established-but-undelivered result. Background work may
exit later; cancellation does not certify destruction of all associated tasks.

`StreamAcceptor` accepts a stream of
`Result<Accepted<I>, E>`, with `I: TransportIo` and `E: Into<BoxError>`.
It owns pinning if necessary, preserves error sources, and maps stream
exhaustion to `None`. It adds no read-ahead queue. Any existing external producer
tasks retain their explicitly separate owner; attaching a stream does not
silently adopt or join unrelated tasks.

Externally accepted I/O follows the same `serve_with_shutdown` path. Even one
already accepted stream can be supplied through a finite `StreamAcceptor`; no separate
public adoption method is needed. When `Accept` yields an `Accepted<I>`, the
owned accept scope passes it to an internal accepted-side establishment helper
using the same driver. Cancellation retires unpublished I/O; it never returns
a partially established stream for reuse.

Serving does not return an owner for every accepted peer. Normal service
context exposes source identity and an optional authorized Peer captured from
the actual driver; the runner retains reply authority. A handler can send semantic
ACKs through that handle without an owner-level connection lookup, address
parsing, or dialing. By default no raw send handle is granted;
[ingress policy](Actors.md#ingress-policies-and-send-capabilities) defines
the independent send restriction. Ordinary replies and native notification
acknowledgments remain runner-owned; context exposes no completion ticket.
The optional [peer-ready hook](#accepted-peer-readiness-hook) can instead receive
an independently authorized Peer without waiting for application ingress.

`Endpoint`, `Channel`, and `Server` remain nongeneric. Custom connect
methods are generic over their URI service and I/O; no type-erased connector factory needs to
be stored in the endpoint. Connection drivers can be scheduled through erased
task handles without boxing every socket read/write. The per-connection
establishment future may be boxed without putting dynamic dispatch in the per-byte path.
Use vectored writes where the supplied I/O supports them and preserve the same
partial-write contract with scalar fallback.

## Accepted-peer readiness hook

`ServerBuilder::on_peer_ready(send_policy, service)` installs one optional
Tower service for initiating application work on an accepted connection.
It works with service/router or outbound-only composition, including a finite
single-stream accept source. No public Connection, accepted Channel,
connection lookup, or second driver is introduced. Repeated hook installation
returns InvalidInput; without a hook there is no callback or extra send grant.

After native hello and all configured security/authorization release gates
complete, the server admits one hook execution for that accepted generation.
All configured ordinary/notification service bindings are already installed.
No application message needs to arrive first. The runner
clones the service before readiness,
polls readiness, and calls that same instance at most once with a restricted,
nonowning Peer. It never clones a readiness reservation. Failed establishment
does not invoke the hook; dialed Channel connects/reconnects do not invoke it.

The explicit SendPolicy is captured on the supplied Peer and checked for every
outbound operation, just as on a received Peer. It can allow bootstrap requests,
one-way announcements, or notifications without widening any ingress binding's
policy. Hook installation grants no inbound acceptance, acknowledgment ticket, source
authentication, managed-field bypass, or lifetime ownership. Raw-only mode
still rejects Peer::request and Peer::notify origination; policy
cannot enable an unavailable capability.
Escaped/cloned Peers remain restricted to this generation and never reconnect.

Hook readiness and application code run in server-owned work outside the
socket driver. The driver continues native controls, reads, writes, ordinary
RPC completion, and configured application dispatch while the hook waits.
The hook is not an ingress gate, nor is it guaranteed to run before already
arriving application messages. Waiting for the hook to finish before processing
its response would deadlock bootstrap. Domains requiring a session gate must
implement it above core without blocking the traffic needed to establish it.

There is at most one admitted hook execution per accepted connection, including
its readiness wait. Its service clone, future, timer, and completion metadata
are owned by that accepted connection. No separate allocation budget,
unbounded ready-event queue or detached worker is added.
Furpc imposes no request-service concurrency or readiness-waiter quota.
Consumers must not hold their own execution permits while waiting for a
service-handled bootstrap response if that would prevent bootstrap progress. Outbound operations and
incoming responses still require their normal independent admission.

`peer_ready_timeout` has a 30-second default, an explicit Rust policy rather
than a native constant. Server build rejects zero or unrepresentable values.
One absolute deadline starts when the established generation becomes eligible
for hook admission, before readiness, and covers the entire hook. It is not
restarted at call or by hook-issued RPCs, and is separate from native
`handshake_timeout` and each operation's explicit deadline. Custom services
remain cooperative; the timer cannot preempt blocking user code.

Success leaves the connection running and emits no automatic domain message
or acknowledgment. Readiness/call failure, resource-admission failure, or
timeout is diagnosed with a peer-ready phase and preserved source/category,
and retires that connection, not unrelated peers or the listener. Connection
retirement cancels its pending hook; neither failure nor cancellation retries
the hook or undoes committed application work. Peers retained elsewhere then
fail Closed with the available cause.

Serving owns and tracks admitted hook futures. Shutdown stops new hook
admission; existing hooks may finish within their original deadline capped by
the server drain deadline. Their Peers have no exception to closure of new
application sends, requests, or notifications. Connection failure, active
serving drop, fatal scope failure, or drain expiry cancels remaining hook work.
Normal serving completion waits for tracked hook work as well as connections;
there is no post-drop join guarantee or adoption of application-spawned tasks.

The capability follows native [`SetConnectionAcceptedHandler`][sf-accepted-api]:
[`OnConnectionAccepted`][sf-accepted-callback] supplies an anonymous send target
without requiring application ingress. Native starts the accepted connection
before that callback, but does not wait there for security negotiation to
complete. Furpc deliberately publishes this application hook later, at
peer readiness, hence `on_peer_ready` rather than a claim of identical native
accept-event timing. Bounded Tower execution, explicit policy, deadlines, and
nonowning generation binding are Rust adaptations.

The current [RA-node harness bootstrap](../../../../crates/tools/fube-cluster/src/node.rs#L125-L165)
can use the supplied Peer to send NodeUp, validate NodeUpAck, and continue
ReplicaUp/semantic-reply processing on the same connection. Domain bodies,
validation, and session publication remain outside furpc. This requirement
comes from that Rust consumer; it does not assert native RA uses the same
connection direction.

## Channel and native peer capabilities

Follow [tonic's Channel][tonic-channel] and
[`client::Grpc<T>`][tonic-client]: the default client transport is a cheap-clone
Tower service exchanging complete native byte envelopes. Typed
[`RpcClient<T>`](Requests.md#rpc-calls-and-responses) sits above it and supplies
a codec per call. Do not define another client transport trait, put codecs on
Channel, or make middleware operate on typed application schemas.

```rust,ignore
impl Channel {
    pub async fn peer(&self, deadline: Instant) -> Result<Peer>;
}

impl Peer {
    pub fn info(&self) -> PeerInfo;
}

impl tower_service::Service<Request<Body>> for Channel {
    type Response = Response<Body>;
    type Error = Error;
    type Future = std::pin::Pin<Box<
        dyn Future<Output = Result<Response<Body>>> + Send + 'static
    >>;

    fn poll_ready(
        &mut self, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>>;
    fn call(&mut self, request: Request<Body>) -> Self::Future;
}
```

All dialed policy/configuration lives on Endpoint, including
[preconfigured reverse service ingress](Services.md#service-boundary-and-native-bindings)
with explicit [native notification bindings](Actors.md#native-notifications).
Initial connect creates a private logical scope with its first driver, retained
connector and bounded ingress-service execution for all configured traffic classes.
Ingress services are installed before application traffic and need no
`take` step; dynamic `register_service` cannot recover earlier dropped traffic.
Channel clones and submitted outbound service futures
share that owner; there is no separate public owner to retain or move. Later
demand can establish replacement generations inside this same scope. A second
explicit Endpoint connect still creates a separate Channel and ingress
registries, without a transport-owned application receive queue.
Each connect also has an independent ingress runner and HandlerLimits, even
when application service clones intentionally share domain state.
Multiple Channels can share accounting without sharing lifetime, connector
state, or reconnect attempts. Channel has no second public connect API, load
balancer, or request-retry layer.

Channel is the only built-in client Tower transport. `peer(deadline)` explicitly
opts into native messaging: it awaits a currently established generation under
the caller's absolute deadline and returns a nonowning, generation-bound capability
for raw sends, reservations, native byte requests/attempt receipts, and
native notification origination. Peer has no constructor, dial, readiness, or
Service API; it is not an alternate generic RPC transport or public socket
wrapper. Accepted ingress exposes an optional restricted Peer through its
received context; `on_peer_ready` can supply one under its own configured
policy before application ingress. Neither path exposes an owning Channel.
Neither a Peer, source, received request, completion data, nor native operation
can keep that driver/scope alive. Retain the originating Channel or keep server execution running while
using those capabilities.

`Channel::peer` shares demand-driven establishment with RPC readiness and works
in raw-only mode too. It fails on deadline/establishment/capacity errors, never
returns a fabricated or last-retired Peer, and cannot guarantee that a returned
generation stays live. Acquiring it reserves no raw send/RPC permit; each
subsequent native operation still admits independently. A later acquisition
may return a replacement generation, but an existing Peer never follows it.
`Channel::peer` uses its dialed owner's outbound authority; an ingress- or
hook-supplied Peer has only its independently captured SendPolicy. Peers cannot be converted into
Channels, change generations, bypass managed IDs or captured policy, or keep
an accepted connection alive after server shutdown. Reply completion still
requires the original consuming grant. The physical connection type, driver,
and scope supervision remain private implementation details.

The Channel is Send + Sync and cheap to clone. Service methods use `&mut self`;
concurrent callers clone before readiness and use the exact ready instance for
`call`. A clone shares logical supervision, the current generation, and bounded
admission, not the source instance's readiness reservation. No extra Tower buffer,
worker, socket, or queue is required: the existing bounded native command
path provides shared submission. Boxed response futures are a signature sketch,
not a requirement to box socket I/O or add another progress task.

For a usable current generation, `poll_ready` reserves one operation/result
slot bound to that generation, not request-sized storage. While disconnected,
readiness can drive the [shared reconnect attempt](#reconnection-without-request-replay).
Waiters and ready/failure outcomes are bounded; zero waiters means fail-fast
and exhaustion is ResourceExhausted, delivered by the next `call` as described
below. Dropping the ready service releases its unused reservation/outcome.
A direct caller bounds readiness waiting itself, since
`poll_ready` has no request/deadline argument. RpcClient applies its original
call deadline while polling readiness. Any reservation still held after a
cancelled readiness wait belongs to that service instance until consumed or
dropped, following Tower semantics; never duplicate it by cloning.

`call` consumes that instance's ready slot and takes an owned `Request<Body>`
with a local absolute deadline. Calling without readiness returns InvalidInput
with a service-contract phase, not an unbounded queue or a hidden wait.
Missing/expired deadlines fail explicitly. Before driver retention/commit,
validate headers, managed fields, send authority, actual frame size, and
native send-queue capacity. Capacity known
only after seeing the request may still fail even after readiness succeeds.
If the ready generation retires before native admission, fail this call with
its cause instead of silently resubmitting to a replacement. A cached failed
readiness outcome returns its error without native admission or transmission.
The service uses the same native request/correlation machinery as
`Peer::request`; application-body decoding remains above it.

Channel service futures own their submission and scope guard, but not another
driver. Dropping them abandons the result and releases unused capacity;
committed native output remains driver-owned. Ordinary typed calls expose
async cancellation, not a generic retraction receipt. Low-level byte operations
retain [Attempt inspection](Operations.md#observation-and-cancellation).
Application middleware/custom services must preserve absolute deadlines,
native response metadata/storage, and cancellation, and must not silently retry
or replay calls. Service readiness/call errors preserve their sources and
phase; existing structured furpc errors retain their categories and disposition.
Inbound ingress-service futures are separately tracked by that same logical
scope, not additional public owners. Last-owner release cancels them; an idle
reverse service cannot keep a Channel alive or add another reader/progress pump.

## Reconnection without request replay

Follow [tonic's reconnecting transport service][tonic-reconnect]: initial
`Endpoint::connect` eagerly attempts native establishment once and returns an
error if it fails. After success, the Channel remains a stable logical handle
across physical disconnects. Its private coordinator moves through
`Connected(generation) -> Disconnected -> Connecting -> Connected(new generation)`.
This is not a public lifecycle controller or transparent domain recovery.

Later RPC readiness or Channel::peer(deadline) can demand reconnection.
Notification service registration and waiting on application queues do not
create reconnect demand. All clones and these demand paths share at most one
in-progress attempt. Idle disconnected Channels do not redial continuously.
An attempt performs fresh connector readiness/acquisition, native hello, and
any future configured security negotiation; it publishes no generation until
all applicable setup succeeds. The destination/native profile and scope policy
come from the original immutable Endpoint/URI snapshot. Each attempt receives
that URI rather than the previous resolved SocketAddr, so DNS/custom resolution
may run again. This is not gateway discovery, failover, or a security downgrade.

One failed attempt ends with an explicit source/phase-preserving error for its
waiting callers, not another automatic attempt for those same waits. A later
fresh demand may try again. There is no internal infinite retry/backoff loop
or application replay; callers/domain middleware may pace later demand.
Failure fan-out and diagnostics are bounded: retain outcomes only for bounded
waiters/ready instances plus current status, not an attempt history.

For transient connect/timeout/capacity failure, follow tonic's service pattern:
`poll_ready` returns Ready(Ok(())) with one private failed-attempt outcome,
and that instance's next `call` returns the actual error. This preserves Channel
usability through middleware that treats `poll_ready` errors as terminal.
It is not a successful RPC or a false capacity grant. Repeated polls of the
same ready instance retain the same outcome, never start another attempt;
cloning does not copy it. Permanent service invalidity, such as raw-only RPC
mode or a closed logical scope, may return `poll_ready` Err directly.
Peer acquisition returns reconnect errors directly. Peer-bound notification
operations never dial or retarget another generation.

Waiter exhaustion or zero-waiter fail-fast retains no request and reports
ResourceExhausted. Demand may still start the one bounded shared establishment
attempt without enqueueing that caller; later demand can observe a connected
generation. This does not retry the failed call. Each waiting caller retains
its original deadline; a shorter wait cannot reset or cancel the shared
attempt for other users. Once started, the attempt can complete while the
Channel is owned, even if its initiating waiter is dropped, but is bounded by
its own finite connect timeout. Last-owner release cancels it and prevents
later publication or redial.

Already-admitted requests belong to one physical generation, including
uncommitted/queued requests. A generation failure settles its still-pending
calls with errors, preserves commitment uncertainty, and never transfers their
payloads, IDs, correlation entries, or write cursors to another driver.
Already-settled results do not change. Readiness demand waiting before a native
attempt is created is not replay of a request; it must still meet its original
deadline when finally admitted.

Peer, Source, SendPermit, private completion tickets, and every native Attempt
remain pinned to their original physical generation. Native submission through
retired capabilities fails Closed with an available cause; passive Source
metadata and Attempt dispositions remain inspectable. Acquiring a fresh Peer
does not revive or redirect old capabilities. Accepted connections never dial
replacements through a received Peer.
Reconnection reuses the logical service registry, including notification
bindings but no failed-generation invocations, as specified by
[ingress across generations](Actors.md#ingress-across-connection-generations).
Old/current/retiring generations, results, and reconnect state all remain under
the same [scope accounting](Resources.md#bounded-admission); replacing a socket
does not create fresh budget.

## Establishment timeout policy

`Endpoint::connect_timeout` selects a positive finite duration for the
complete outbound establishment, not just the TCP socket connect.
`ServerBuilder::handshake_timeout` bounds native establishment after its
acceptor yields I/O; it does not time out idle accepting. Omitting either
selects its documented finite default, never infinity. Endpoint connect and
server build reject zero or unrepresentable configuration before starting work.

On starting a new outbound attempt, compute one absolute monotonic deadline
from the configured duration. Connector readiness, DNS/I/O acquisition, and
native hello share that deadline; a later stage never receives a fresh full
timeout. Both Endpoint connect paths use the same establishment routine and
compute that budget once; the TCP convenience path adds no nested
timeout or reset.
Each I/O object yielded by the accept scope starts its own establishment
deadline under the server's handshake setting, including externally accepted
streams. Future native security negotiation must fit within that budget when implemented.

The connector service takes an `http::Uri`, not a deadline; the transport
enforces its budget around readiness, the supplied future, and native
establishment. Each explicit initial connect owns an independent attempt and budget; each
shared reconnect gets one new finite attempt budget, never a new RPC deadline.
Cancellation before initial handle handoff retires unpublished I/O and its
scope. Cancelling one reconnect waiter does not cancel the shared attempt.

Applications needing an earlier whole-workflow deadline can compose with
`tokio::time::timeout_at` around `connect` or their workflow. This bounds their
wait and cancels an unpublished attempt; it is not proof of physical cleanup.
No independent cleanup observer is exposed. Do not add a second deadline-based
establishment API or a public context type until a concrete consumer requires
more than this composition.

Message send/RPC and admission waiting retain their separately documented
deadlines. `ServerBuilder::drain_timeout` bounds graceful drain from the instant
the signal is observed, using one nonrenewable absolute deadline. The
establishment timeout does not become an idle, request, partial-frame, or
graceful-drain timeout. Accepted application bootstrap has the separate
[`peer_ready_timeout`](#accepted-peer-readiness-hook); it does not extend native
establishment or any hook-issued operation's deadline.

## Identity and domain recovery

Peers that both dial and accept keep independent connection state, native
send-queue counters and shutdown scopes. See [peer queue policy](Resources.md#peers-and-queue-limits).

URI/logical destination, resolved socket address, physical generation, authenticated peer,
advertised ListenInstance, and domain session identity are distinct. Equal
socket addresses cannot make them interchangeable. Diagnostic I/O metadata
is not authentication or proof that a custom stream is genuinely local.

Connection fault detection, retirement, and demand-driven physical reconnection
belong in shared supervision. Transport readiness after reconnect is not
domain readiness. Gateway selection, IPC registration recovery, notification
resynchronization, and replication membership remain domain responsibilities. Unsupported
recovery must fail explicitly; reconnecting never implicitly replays
committed application messages.

A connection-bound domain handshake must gate submission on that same Peer.
Acquiring a Peer, initializing it, and then issuing a generic Channel call is
not sufficient: the Channel call could reconnect onto another generation.
Such clients can supply a domain transport service to RpcClient that acquires/
initializes a Peer under the call deadline and submits with `Peer::request`.
If it retires, that call fails; recovery is for later calls, not replay.

[tonic-endpoint]: https://docs.rs/tonic/latest/tonic/transport/struct.Endpoint.html#method.connect_with_connector
[tonic-streams]: https://docs.rs/tonic/latest/tonic/transport/server/struct.Router.html#method.serve_with_incoming
[axum-listener]: https://docs.rs/axum/latest/axum/serve/trait.Listener.html
[tonic-shutdown]: https://docs.rs/tonic/latest/tonic/transport/server/struct.Router.html#method.serve_with_shutdown
[hyper-connectors]: https://docs.rs/hyper-util/latest/hyper_util/client/legacy/connect/index.html#connectors
[tonic-channel]: https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
[tonic-client]: https://docs.rs/tonic/latest/tonic/client/struct.Grpc.html
[tonic-reconnect]: https://github.com/hyperium/tonic/blob/master/tonic/src/transport/channel/service/reconnect.rs
[http-uri]: https://docs.rs/http/latest/http/uri/struct.Uri.html
[sf-accepted-api]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/IDatagramTransport.h#L75-L106
[sf-accepted-callback]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpDatagramTransport.cpp#L1346-L1394
[sf-create-tcp]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/DatagramTransportFactory.h#L12-L19
[sf-set-instance-api]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/IDatagramTransport.h#L24
[sf-local-hello]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpConnection.cpp#L206-L209
[sf-set-instance-reset]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Transport/TcpDatagramTransport.cpp#L137-L170
[sf-repl-publish]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L137-L145
[sf-repl-from]: https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L920-L925
