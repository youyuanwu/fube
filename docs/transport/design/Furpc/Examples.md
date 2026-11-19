# furpc: End-to-end API examples

**Status:** proposed; see the [design summary](README.md).

Shows the agreed public API in use. These are design sketches, not
compilable programs or native-interoperability evidence. Use the
[topic map](README.md#design-map) for normative ownership and bounds.

## End-to-end API sketches

The examples use the crate's root exports and Tokio time types. Application
actor codes/actions and schemas below describe an owned echo protocol, not a
native gateway operation. `RequestPolicy` is supplied by trusted application
composition and must authorize the intended request/response envelopes.

### Raw client with a custom native header

```rust,ignore
use furpc::*;
use tokio::time::{Duration, Instant};

#[derive(serde::Serialize)]
struct TraceTag {
    sequence: u32,
}

async fn raw_client(destination: String) -> Result<WriteReceipt> {
    let channel = Endpoint::from_shared(destination)?
        .connect_timeout(Duration::from_secs(10))
        .connection_options(ConnectionOptions::raw_only())
        .connect().await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let peer = channel.peer(deadline).await?;

    let tag = HeaderRecord::native(
        HeaderId::new(0x9001)?, &TraceTag { sequence: 7 },
    )?;
    let message = Message::builder()
        .append_header(header::actor(ActorId::new(70000)?)?)
        .append_header(header::action("OneWay")?)
        .append_header(tag)
        .body(Body::from_bytes(vec![1, 2, 3])?)
        .build()?;
    let written = peer.send(message, deadline).await?;

    Ok(written)
}
```

The body is opaque bytes; no body codec is required. The header uses the
fixed native serializer under native representation limits.
Message/header construction needs no client or runtime. Submission separately
applies the Endpoint's configured native-header and resource limits.
Actor/Action use typed native header constructors plus generic ordered
insertion, not special fields on the message builder.
The write receipt is not a semantic ACK. The owning Channel drops when the function
returns; the awaited write is the required completion boundary here, not a
separate client-shutdown receipt.

### Preparing Actor-dispatched service ingress before connecting

```rust,ignore
async fn actor_ingress<S>(
    destination: String, actor: ActorId,
    check: EnvelopeCheck, ack_policy: SendPolicy, handler: S,
) -> Result<Channel>
where
    S: tower_service::Service<Request<Body>, Response = IngressResponse>
        + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    let ingress = IngressService::new(
        handler,
        vec![ServiceBinding::actor(
            actor,
            IngressOptions::one_way(check, IngressLimits::default())
                .with_send_policy(ack_policy),
        )?],
    )?;
    Endpoint::from_shared(destination)?
        .connection_options(ConnectionOptions::raw_only())
        .service(ingress)?
        .connect().await
}
```

Endpoint stores inert service configuration. Connect installs it before
application traffic and the Channel's scope drives reverse service calls;
there is no receiver to take or user receive loop to spawn. This one-way
handler returns IngressResponse::NoReply after its required work, and may use
the request context's independently authorized Peer for an earned ACK.
Retain the returned Channel while ingress is needed; handlers and context
Peers are not owners. Channel clones share one ingress registry/HandlerLimits.
Reconnection preserves configuration, not old in-flight invocations or domain
sessions, and an idle installed handler does not continuously redial.

### The same raw ingress service on Channel and Server

```rust,ignore
let ingress = IngressService::new(
    replication_service,
    vec![ServiceBinding::raw(
        IngressOptions::one_way(check, IngressLimits::default())
            .with_send_policy(ack_policy),
    )?],
)?;
let endpoint = Endpoint::from_shared(destination)?
    .connection_options(ConnectionOptions::raw_only())
    .handler_limits(HandlerLimits::default())
    .service(ingress.clone())?;
let server = Server::builder()
    .service(ingress)?
    .build()?;
```

This only configures the two owners. Drive Endpoint connect and server serving
concurrently as the topology requires; pass ConnectionOptions::raw_only() to
serve. Both owners invoke the same service contract directly,
but have independent runners and HandlerLimits under their shared native budget.
The application's service clones may intentionally share domain state.

For classic replication, `check` validates native ReplicationActor/
ReplicationFrom headers before body retention. `replication_service` receives
`Request<Body>`, dispatches verified messages by ReplicationEndpointId under the
domain's bounded policy, and handles ACK/RequestAck independently of the
replicated-operation queue. Data handlers try to admit into that domain queue
and promptly return IngressResponse::NoReply instead of waiting for application
acknowledgment or quorum completion. The domain owns later processing/retry
and sends earned ACKs using a retained, restricted original-generation Peer.
Queue-full drops must be explicit and cannot falsely advance acknowledged
progress. No ordinary Actor header or Actor-0 registration is added.

This follows the [native flow-control boundary](Resources.md#replication-flow-control-boundary):
generic furpc limits remain finite; the replication framework owns session
behavior, operation-queue admission, ACK scheduling, and retry.

The raw dispatcher is selected by ServiceBinding::raw, independently of
raw_only correlation mode. Actor bindings cannot be added to these scopes.
A Channel initially configured without a service can instead install one
through register_service under its existing limits, but preconnect service
installation avoids losing early reverse messages. Neither path hands out a receiver.

### Bounded application work queue

```rust,ignore
use tokio::sync::{mpsc, oneshot};

type Work = (
    Request<Body>,
    oneshot::Sender<std::result::Result<IngressResponse, BoxError>>,
);

let (queue, work) = mpsc::channel::<Work>(8);
let queued_service = tower::service_fn(move |request: Request<Body>| {
    let queue = queue.clone();
    async move {
        let (reply_tx, reply_rx) = oneshot::channel();
        queue.send((request, reply_tx)).await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "application work queue closed",
            )
        })?;
        reply_rx.await?
    }
});
let ingress = IngressService::new(
    queued_service,
    vec![ServiceBinding::raw(
        IngressOptions::requests(request_policy, IngressLimits::default()),
    )?],
)?;
```

Install `ingress` through Endpoint::service or ServerBuilder::service as above.
The application owns `work`, runs its bounded worker processing concurrently,
and returns each `Result<IngressResponse, BoxError>` through that item's sender.
Check the original context deadline and whether the sender is closed before
starting stale work. Queue/worker closure is an explicit service error, not
an empty response or fallback success.

The service future stays pending, so the runner retains reply authority,
HandlerLimits, and the original deadline until the worker responds or the
invocation is cancelled. Request context and shared byte storage move through
the application queue unchanged; no completion ticket is transferred to the worker.
Late/cancelled responses cannot revive a ticket. Queued storage stays alive
through ordinary references, not allocation-budget charges.

Furpc creates neither this queue nor its workers. Keep workers available while
admitted calls drain and coordinate their shutdown explicitly. For one-way
replication, domain queue admission can instead complete the handler promptly
with NoReply; do not wait here for an independent semantic ACK or quorum.

### Starting application work on an accepted peer

`bootstrap_peer` below is an application-supplied async function accepting
`Peer` and returning `Result<()>`. `ingress_service` supplies its domain's
already-configured byte-service bindings; `bootstrap_send_policy` explicitly
authorizes only the required outbound envelopes.

```rust,ignore
let server = Server::builder()
    .service(ingress_service)?
    .peer_ready_timeout(Duration::from_secs(20))
    .on_peer_ready(
        bootstrap_send_policy,
        tower::service_fn(bootstrap_peer),
    )?
    .build()?;
server.serve_with_shutdown(
    acceptor, ConnectionOptions::managed(), shutdown_signal,
).await?;
```

The function can immediately call `peer.request(request, deadline).await?`
without first receiving an application message. For the RA-node harness it
can send NodeUp, validate NodeUpAck, and continue ReplicaUp plus semantic-reply
processing on that same Peer. Domain encoding, validation, and session state
remain in the application, not this transport hook.

Native RPC completions and the installed ingress service continue running
while bootstrap waits. A server can omit ordinary `service` when it needs
only native RPC/notification acknowledgment correlation. Notification ingress
and ordinary semantic replies such as ReplicaUp need their respective service
bindings; neither has a separate receive registration.
The hook's Peer remains nonowning,
restricted, and generation-bound; no accepted Channel or reconnect is added.
Hook failure/timeout retires that connection, and success sends no automatic
domain acknowledgment. Its application-ready timing is deliberately later
than native's raw connection-accepted notification.

### Typed routed server

```rust,ignore
#[derive(serde::Serialize, serde::Deserialize)]
struct EchoRequest {
    text: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EchoReply {
    text: String,
}

async fn echo(request: Request<EchoRequest>) -> Result<Reply<EchoReply>> {
    let request = request.into_body();
    Reply::new(EchoReply { text: request.text })
        .append_header(header::actor(ActorId::new(70000)?)?)?
        .append_header(header::action("EchoReply")?)
}

async fn echo_text(request: Request<String>) -> Result<Reply<String>> {
    Reply::new(request.into_body())
        .append_header(header::actor(ActorId::new(70000)?)?)?
        .append_header(header::action("EchoTextReply")?)
}

async fn serve_echo<F>(
    address: std::net::SocketAddr, echo_policy: RequestPolicy,
    text_policy: RequestPolicy, shutdown_signal: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let actor = ActorId::new(70000)?;
    let echo_service = tower::service_fn(|request: Request<Body>| async move {
        let mut rpc = server::Rpc::new(
            FubstCodec::<EchoReply, EchoRequest>::default(),
        );
        rpc.unary(tower::service_fn(echo), request).await
    });
    let text_service = tower::service_fn(|request: Request<Body>| async move {
        let mut rpc = server::Rpc::new(Utf8Codec);
        rpc.unary(tower::service_fn(echo_text), request).await
    });
    let router = Router::new()
        .request(
            actor, "Echo",
            RequestRouteOptions::new(echo_policy, IngressLimits::default()),
            echo_service,
        )?
        .request(
            actor, "EchoText",
            RequestRouteOptions::new(text_policy, IngressLimits::default()),
            text_service,
        )?;
    let server = Server::builder()
        .handshake_timeout(Duration::from_secs(10))
        .drain_timeout(Duration::from_secs(5))
        .handler_limits(HandlerLimits {
            max_in_flight: 4,
            ..HandlerLimits::default()
        })
        .service(router.into_service()?)?
        .build()?;
    let acceptor = TcpAcceptor::bind(address).await?;

    server.serve_with_shutdown(
        acceptor, ConnectionOptions::managed(), shutdown_signal,
    )
        .await
}
```

The router only selects a byte-facing service. Each service creates its
tonic-style `server::Rpc` method adapter, which owns decoding, typed-service
invocation, and response encoding. The two method services select distinct
agreed body codecs; there is no sniffing or router-level default.
Serving owns admission, handler lifetime, and original reply completion.
`Utf8Codec` is defined below.

The outer services are cloneable shared configuration, but their per-call
codec and typed-service values need neither Clone nor Sync. Full Tower is
used here only for `service_fn`; the public boundaries use `tower-service`.

Applications needing queued processing can compose the bounded work-queue
service above. The router and runner still own invocation/reply completion;
there is no separate ordinary native receive loop.

### Native notification services

Notification bindings use the same byte service and typed unary adapter, not
a separate receive loop or public acknowledgment grant. Notification, Ack,
acknowledge, and the header policies below are application-supplied; the handler
returns Reply<Ack> only after its domain acknowledgment condition is satisfied.
The acknowledgment can carry application-defined data and headers, subject to
the acknowledgment policy. The runner supplies native correlation and flags.

```rust,ignore
let notification_service = tower::service_fn(|request: Request<Body>| async move {
    server::Rpc::new(FubstCodec::<Ack, Notification>::default())
        .unary(tower::service_fn(acknowledge), request).await
});
let policy = NotificationPolicy::new(check_notification, check_acknowledgment);
let ingress = Router::new()
    .notification(
        actor, "Changed",
        NotificationRouteOptions::new(policy.clone(), IngressLimits::default()),
        notification_service,
    )?
    .into_service()?;
```

Install ingress through Endpoint::service or ServerBuilder::service, exactly
like ordinary request/one-way services. A byte caller originates the native
notification on an already acquired, authorized Peer:

```rust,ignore
let acknowledgment = peer.notify(byte_request, policy.clone(), deadline).await?;
```

Alternatively, a typed caller reuses RpcClient over an explicit byte-service
adapter. The adapter selects the native operation; the generic client still
owns encoding/decoding and the original call deadline:

```rust,ignore
let target = peer.clone();
let transport = tower::service_fn(move |request: Request<Body>| {
    let peer = target.clone();
    let policy = policy.clone();
    async move {
        let deadline = request.deadline()
            .ok_or(Error::InvalidInput("notification call requires a deadline"))?;
        peer.notify(request, policy, deadline).await
    }
});
let mut client = RpcClient::new(transport);
let acknowledgment = client.call(
    typed_request, FubstCodec::<Notification, Ack>::default(), deadline,
).await?;
```

Retain the originating Channel or server execution: this adapter captures only
a nonowning Peer. It adds no socket, reader, retry or native correlation table.
Typed calls retain normal RpcClient future semantics without an Attempt; byte
notify callers retain the native Operation/Attempt contract. The adapter
preserves the native response and never silently chooses another generation.
The runner, not the handler's Reply data, owns acknowledgment correlation,
flags and original-generation submission. NoReply, failure and cancellation
send no automatic acknowledgment. Ordinary and notification routes may use
equal Actor/Action keys, but their native traffic classes and policies stay
separate; there is no fallback between them.

### Default fubst and an external non-Serde codec

```rust,ignore
struct Utf8Codec;
struct Utf8Encoder;
struct Utf8Decoder;

impl Codec for Utf8Codec {
    type Encode = String;
    type Decode = String;
    type Encoder = Utf8Encoder;
    type Decoder = Utf8Decoder;

    fn encoder(&mut self) -> Self::Encoder { Utf8Encoder }
    fn decoder(&mut self) -> Self::Decoder { Utf8Decoder }
}

impl Encoder for Utf8Encoder {
    type Item = String;

    fn encode(&mut self, value: String, dst: &mut EncodeBuf<'_>)
        -> std::result::Result<(), CodecError>
    {
        dst.write_all(value.as_bytes())
    }
}

impl Decoder for Utf8Decoder {
    type Item = String;

    fn decode(&mut self, src: &mut DecodeBuf<'_>)
        -> std::result::Result<String, CodecError>
    {
        String::from_utf8(src.copy_remaining()?)
            .map_err(CodecError::invalid_payload)
    }
}

async fn typed_client(destination: String) -> Result<()> {
    let channel = Endpoint::from_shared(destination)?.connect().await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let actor = ActorId::new(70000)?;
    let mut rpc = RpcClient::new(channel);

    let request = Request::new(EchoRequest { text: "hello".into() })
        .append_header(header::actor(actor)?)?
        .append_header(header::action("Echo")?)?;
    let response = rpc.call(
        request, FubstCodec::<EchoRequest, EchoReply>::default(), deadline,
    ).await?;
    println!("{}", response.body().text);

    let request = Request::new(String::from("hello"))
        .append_header(header::actor(actor)?)?
        .append_header(header::action("EchoText")?)?;
    let response = rpc.call(request, Utf8Codec, deadline).await?;
    println!("{}", response.body());

    Ok(())
}
```

One transport-generic `RpcClient<Channel>` sends both calls through the same
logical destination, consuming a different codec per call. Native pending
correlation remains local to each physical generation.
It uses `&mut self` for standard service readiness; clone the client before
readiness to issue concurrent calls. Channel clones share its dialed owner,
native admission, and driver without another public owner.
The matching server method services select fubst for Echo and UTF-8 for EchoText.
`FubstCodec` is explicitly supplied for the native-format call; custom-only
protocols do not instantiate it. `Utf8Codec` needs no Clone implementation on
either side: server method services construct a fresh instance per invocation.
The final owning client/channel drops after the required responses; the
application controls server shutdown independently. Real native operations still require their specified
schemas; the UTF-8 codec is not a
substitute for a native gateway body's Fabric serialization.

Configure transport limits, timeout, and native reverse ingress
directly on Endpoint before connecting:

```rust,ignore
let channel = Endpoint::from_shared("tcp://node.example:19000")?
    .limits(TransportLimits::default())
    .connect_timeout(Duration::from_secs(10))
    .connect().await?;
let mut rpc = RpcClient::new(channel).codec_limits(ClientCodecLimits::default())?;
```

Client codec limits govern bounded application-future work above the byte
service; Channel separately enforces actual native admission. Constructing a
generic client does not inspect its transport for private resource state.

Client destination strings include the explicit scheme/port, for example
`tcp://node.example:19000` or `tcp://[::1]:19000`. Endpoint parsing does no
DNS/I/O and does not interpret paths as native actions. TCP listener binding
still uses SocketAddr. `Endpoint::try_from(address)?` is available as a
SocketAddr-to-URI convenience, not a second destination model.

### Reconnection is not retry

The same RpcClient and all its Channel clones remain usable after a physical
disconnect. Readiness for a later independent call can share one reconnect
attempt with other callers and channel.peer(deadline).await.
Installed notification services or application queue receives do not redial.
The original call deadline still includes that readiness wait. A failed
connection attempt returns its cause for that wait instead of retrying
indefinitely; a later fresh demand can try again.

A call already bound to the failed generation returns an error, even if its
output had not committed; the example's `?` propagates that failure. It is not
automatically submitted again. Applications that continue using the logical
client handle/report the failure at their domain boundary, and decide whether
any retry is safe. Old Peer/SendPermit values, notification attempts and private
completion tickets do not
follow the replacement. For new native work, explicitly acquire a fresh Peer
with its own deadline and perform required domain/session recovery first.

### A custom byte-facing RPC transport

```rust,ignore
let transport = tower::service_fn(|request: Request<Body>| async move {
    Ok::<_, Error>(Response::new(request.into_body()))
});
let mut rpc = RpcClient::new(transport);
let deadline = Instant::now() + Duration::from_secs(10);
let response = rpc.call(
    Request::new(String::from("offline")), Utf8Codec, deadline,
).await?;
assert_eq!(response.body(), "offline");
assert!(response.source().is_none());

let (parts, text) = response.into_parts();
assert!(parts.source().is_none());
let mut response = Response::from_parts(parts, text.into_bytes());
response.body_mut().make_ascii_uppercase();
assert_eq!(response.body().as_slice(), b"OFFLINE");
```

This synthetic service exchanges complete byte bodies, with no socket, native
correlation, or fabricated source. Real custom services/middleware must honor
the request's absolute deadline and cancellation contract. The same client
adapter can wrap a Tower middleware stack over Channel. Physical connections
are private, and Peer deliberately has no Service implementation; native
operations are explicit capabilities rather than another generic RPC client.
Response parts preserve headers, optional provenance and settlement metadata
during body adaptation. Propagate fallible conversion errors before reassembly;
shared byte views retain their backing storage independently of parts.
Only application metadata and the body are mutable.

### Bounded shutdown with outstanding operations

The shutdown signal starts drain; the server's configured `drain_timeout`
bounds it even when a peer blocks output. For graceful completion, signal
shutdown and keep awaiting the execution rather than dropping it.

```rust,ignore
server.serve_with_shutdown(acceptor, options, shutdown_signal).await?;
```

The result is `Ok(())` after normal service and active-connection completion.
Fatal serving errors and drain expiry return structured errors but do not
promise every background task has exited. Ordinary request/peer failures remain
in their own paths and diagnostics. There is no second result conversion.
A peer supervisor finishes required client operations before dropping that
owner and signals/awaits its server independently.

An outer timeout is deliberately different: it polls the actual serving
future, but cancels that execution if its own deadline wins.

```rust,ignore
let serving = server.serve_with_shutdown(acceptor, options, shutdown_signal);
let outcome = tokio::time::timeout_at(deadline, serving).await;
```

Handle all layers of `outcome`: `Ok(Ok(()))` is normal server completion;
`Ok(Err(error))` is a serving error; `Err(elapsed)` is outer cancellation, not
successful shutdown. On outer expiry the serving future is dropped and
background work may still be exiting. There is no independent furpc observer
to await afterward. If the future was never polled it simply releases inputs.
For explicitly spawned serving, signal shutdown and await the Tokio task
handle; aborting that handle is cancellation, not an alternative graceful API.

### Custom connector and acceptor over in-memory I/O

These are application-defined implementations, not special cases in
either facade. The bounded channel is the adapter's explicitly owned queue; the
native handshake still runs in the same initiating/accepted driver.

```rust,ignore
use std::{future::Future, io, pin::Pin, task::{Context, Poll}};
use http::Uri;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tower_service::Service;

struct MemoryConnector {
    accepted: mpsc::Sender<Accepted<DuplexStream>>,
}

struct MemoryAcceptor {
    accepted: mpsc::Receiver<Accepted<DuplexStream>>,
}

impl Service<Uri> for MemoryConnector {
    type Response = Connected<DuplexStream>;
    type Error = io::Error;
    type Future = Pin<Box<
        dyn Future<Output = io::Result<Self::Response>> + Send
    >>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let accepted = self.accepted.clone();
        Box::pin(async move {
            let supported_scheme = matches!(uri.scheme(), Some(s) if s == "memory");
            if !supported_scheme
                || uri.authority().map(|a| a.as_str()) != Some("echo")
                || uri.path() != "/"
                || uri.query().is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput, "unsupported memory destination",
                ));
            }
            let (client, server) = tokio::io::duplex(64 * 1024);
            accepted.send(Accepted::new(server, IoInfo::unknown()))
                .await
                .map_err(|error| {
                    drop(error.0);
                    io::Error::new(io::ErrorKind::BrokenPipe, "memory acceptor closed")
                })?;
            Ok(Connected::new(client, IoInfo::unknown()))
        })
    }
}

impl Accept for MemoryAcceptor {
    type Io = DuplexStream;
    type Error = io::Error;

    async fn accept(&mut self) -> io::Result<Option<Accepted<Self::Io>>> {
        Ok(self.accepted.recv().await)
    }
}

fn memory_setup()
    -> Result<(Endpoint, MemoryConnector, Server, MemoryAcceptor)>
{
    let server = Server::builder().build()?;
    let (tx, rx) = mpsc::channel(4);
    let acceptor = MemoryAcceptor { accepted: rx };
    let endpoint = Endpoint::from_shared("memory://echo")?;
    let connector = MemoryConnector { accepted: tx };
    Ok((endpoint, connector, server, acceptor))
}
```

This only constructs configuration. The caller must poll
`server.serve_with_shutdown(acceptor, options, signal)` concurrently with
`endpoint.connect_with_connector(connector)` for the native
handshake to complete; there is no hidden startup. The ordinary `connect`
method uses TCP and rejects this custom scheme as Unsupported before I/O.
Apply the shutdown-signal
pattern above, await serving, and drop the Channel after required operations.
Configure any raw or Actor server ingress before creating the execution; message construction
does not depend on retaining or borrowing the server.
The custom connect returns the same reconnecting Channel as TCP, retaining
MemoryConnector for future attempts. Each attempt shares one finite budget
across connector readiness, acquisition, and native setup.

This is an offline driver-use example, not native interoperability or
IPC/security qualification. Failed channel handoff drops the unaccepted stream
rather than retaining it inside the returned error.
Connector readiness here means the service can accept a bounded connection
attempt; waiting for channel space occurs inside that future under furpc's
unchanged establishment timeout. Applications may use `tower::service_fn`
instead of a handwritten `Service` implementation when appropriate.
`MemoryConnector` needs neither Clone nor Sync: it is transferred once and then
polled/called repeatedly by the logical Channel, separate from clonable Endpoint
configuration. Each call creates a new stream pair and hands it to the same
bounded accept source; after that source closes, attempts fail explicitly.
This connector supports only `memory://echo` with an empty/root path and no
port/query. It validates the complete URI rather than ignoring unknown
destination components, and receives the same parsed URI on each reconnect.

## Custom Tower service

```rust,ignore
async fn raw_echo(request: Request<Body>) -> Result<IngressResponse> {
    let response = Message::builder()
        .append_header(header::action("EchoReply")?)
        .body(request.into_body())
        .build()?;
    Ok(IngressResponse::Reply(response))
}

let service = IngressService::new(
    tower::service_fn(raw_echo),
    vec![ServiceBinding::raw(
        IngressOptions::requests(echo_policy, IngressLimits::default()),
    )?],
)?;
let server = Server::builder().service(service)?.build()?;
```

This service explicitly selects raw application ingress. Its `echo_policy`
authorizes request/response envelopes without requiring an ordinary Actor;
the runner still supplies original-connection reply correlation. It cannot
share the scope with Actor bindings or a competing raw receiver. To select a
whole ordinary actor instead, use `ServiceBinding::actor(actor, options)` with
the same IngressOptions; the typed routed example above selects Actor dispatch.

Full Tower is optional for helper functions/layers; the core boundary only
requires `tower-service`. Middleware must preserve validated context and
bounded readiness rather than create competing ingress or completion pumps.
The handler builds only response data; the runner retains the original reply
grant and applies scope limits/correlation when submitting it.

Body-adapting middleware uses `map` for infallible transformations or moves
`RequestParts` through fallible decoding. It must not rebuild a received
request with `Request::new`, which intentionally discards received context.
One-way handlers use the same `Request<Q>` envelope but return `Result<(), E>`;
the envelope itself never promises a wire reply.

### Editing request metadata without losing context

```rust,ignore
fn tag_request<B>(mut request: Request<B>, sequence: u32) -> Result<Request<B>> {
    let tag_id = HeaderId::new(0x9001)?;
    let tag = HeaderRecord::native(tag_id, &TraceTag { sequence })?;
    request.headers_mut().remove_all(tag_id)?;
    request.headers_mut().append(tag)?;
    Ok(request)
}

fn strip_tag<B>(request: Request<B>) -> Result<Request<B>> {
    let (mut parts, body) = request.into_parts();
    parts.headers.remove_all(HeaderId::new(0x9001)?)?;
    Ok(Request::from_parts(parts, body))
}
```

Both helpers accept outbound or received requests. Header operations are
bounded and fallible; `tag_request` deliberately removes all older tag
occurrences before appending, rather than relying on duplicate lookup order.
The body and private context survive unchanged. Metadata edits do not change
the original deadline, admitted route, send restrictions, or reply authority.
Raw messages sharing the original header bytes remain unchanged.
