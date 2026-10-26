# FabricTransport: native API and TCP wire specification

## 1. Scope and source baseline

This document describes the implementation behind `IFabricTransport*` in
[`fabrictransport_.idl`][idl], including the shared native TCP transport and its
security layer. The file is named `fabrictransport_.idl`, with a trailing
underscore, in the source tree.

The source was inspected locally at
`/home/user1/code/reference/service-fabric`, at commit
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][revision], with no local
modifications. All source links below are pinned to that commit. Paths in the
implementation map are relative to that checkout.

**Contents:** [API contracts](#3-api-contracts),
[connection lifecycle](#4-addressing-and-connectionmessage-lifecycle),
[buffer ownership](#5-message-buffers-and-ownership),
[TCP wire format](#6-tcp-framing-and-serialization),
[TLS and security](#7-security-and-tls-on-the-wire),
[limits and errors](#8-limits-backpressure-timeouts-and-errors),
[interoperability checklist](#9-interoperability-and-debugging-checklist).

This is a **source-derived description of that revision**, not a Microsoft
interoperability guarantee or a specification of every released runtime. No
packet capture, live TLS negotiation, or native test execution was performed for
this document. In particular, a deployed runtime's TLS versions, cipher suites,
configuration, and build dependencies must be established separately.

The interfaces here are the buffer-based FabricTransport API, referred to as
"V2" in parts of the source. They are not the older
`IFabricServiceCommunication*` API, although both use
`Communication::TcpServiceCommunication`. Managed remoting method names,
application serialization, Service Fabric naming resolution, and reliable
replication protocols are outside this API's wire contract.

## 2. Essential model

FabricTransport provides binary request/reply messaging and server-to-client
one-way notifications over persistent TCP connections. It is not HTTP, gRPC,
Windows RPC/NDR, or a durable message queue. The IDL's interfaces are marked
`[object, local]`: they define an in-process COM-style ABI. COM pointers, buffer
descriptors, and async contexts do not cross the network. The DLL exports create
wrappers around the native communication classes. [Sources: [IDL][idl],
[exports][exports], [client factory][com-client-factory].]

```text
Application / Rust FFI / managed broker
    IFabricTransportMessage, IFabricTransportClient, IFabricTransportListener
        |
COM wrappers and proxies
    application buffers <-> Transport::Message
        |
TcpServiceCommunication
    service path, logical Connect, client ID, dispatch, service error replies
        |
Transport::RequestReply + Demuxer
    message IDs, correlation, reply table, actor routing
        |
TcpDatagramTransport -> TcpSendTarget -> TcpConnection
    ordered TCP byte stream, connection state, send/receive buffers
        |
SecurityContextSsl / SecurityContextWin, when configured
    negotiation, authentication/authorization, message protection
        |
OS TCP socket
```

Despite `TcpDatagramTransport`'s name, this path uses **TCP, not UDP**.
`TcpSendBuffer` gathers frame, header, and body buffers for socket transmission;
Linux uses `writev`/`readv`. Socket writes can contain multiple messages and can
complete partially. [Sources: [communication transport][service-transport],
[send buffer][tcp-send], [Linux socket I/O][socket-io].]

Three distinctions are fundamental:

- **Application headers are payload bytes.** They are not the native transport
  headers used for routing, correlation, timeout, and security.
- **TCP connection, secure session, and logical service connection are separate
  stages.** A successful TCP connect or TLS handshake does not mean the service
  has accepted the FabricTransport client.
- **TLS is not used from TCP byte zero.** TLS handshake tokens are initially
  carried inside clear Fabric TCP frames. Once secured, the data representation
  depends on the framing-protection mode.

## 3. API contracts

### 3.1 Objects and operations

| Surface | Operations and meaning |
|---|---|
| `CreateFabricTransportListener` | Creates a listener from settings, host/port/path, a request handler, connection handler, and outgoing-message disposer. Does not replace `BeginOpen`/`EndOpen`. |
| `IFabricTransportListener` | `BeginOpen`/`EndOpen` returns the service address; `BeginClose`/`EndClose` unregisters it; `Abort` initiates cleanup without the public async close protocol. |
| `CreateFabricTransportClient` | Creates and internally initializes a client for an already supplied connection address. It does not perform application naming resolution or complete the logical service connection. |
| `IFabricTransportClient` | `BeginOpen`/`EndOpen` establishes the logical connection; `BeginRequest`/`EndRequest` obtains a reply; `Send` is a no-reply-waiting send; close and abort terminate the client. |
| `IFabricTransportMessageHandler` | Service receives `clientId`, message, and timeout in `BeginProcessRequest`, and returns a reply in `EndProcessRequest`. Also declares `HandleOneWay`; see the implementation caveat in section 4.4. |
| `IFabricTransportConnectionHandler` | Service asynchronously accepts a client through `BeginProcessConnect`/`EndProcessConnect`, receiving an `IFabricTransportClientConnection`; disconnect is reported with a client ID. |
| `IFabricTransportClientConnection` | Service retains a handle for sending one-way notifications to that client on its existing connection; exposes `get_ClientId`. No reverse request/reply method is exposed in this IDL. |
| `IFabricTransportCallbackMessageHandler` | Client receives service notifications through `HandleOneWay`. |
| `IFabricTransportClientEventHandler` | Client observes `OnConnected(address)` and `OnDisconnected(address, error)`. |
| `IFabricTransportMessage` | Supplies one application-header descriptor and an array of body-buffer descriptors; also exposes `Dispose`. |
| `IFabricTransportMessageDisposer` | Receives a batch of outgoing messages whose buffers are no longer needed by transport. |

`Begin` and `End` use `IFabricAsyncOperationContext` and
`IFabricAsyncOperationCallback`. Completion can be synchronous; a binding must
follow the common Service Fabric async-context convention rather than assume
callbacks always happen later. Request timeout `INFINITE` (`0xffffffff`) becomes
`TimeSpan::MaxValue`. [Sources: [IDL][idl], [request wrapper][com-request],
[client wrapper][com-client].]

### 3.2 Native settings, not managed defaults

These are the semantics of the `FABRIC_TRANSPORT_SETTINGS` conversion in this
revision. A managed configuration layer may supply different nonzero defaults.

| Field | Native interpretation |
|---|---|
| `OperationTimeoutInSeconds` | Zero becomes infinite. Used for service connection/disconnection callbacks and as the dispatcher fallback when no `TimeoutHeader` is present; not a substitute for a request's explicit timeout. |
| `KeepAliveTimeoutInSeconds` | Zero disables transport's enabling of TCP keepalive. A positive value supplies the TCP idle interval before keepalive probes. It is not a Fabric heartbeat message. |
| `MaxMessageSize` | Must result in a positive native signed `int`; zero leaves the constructor's zero and fails validation. It feeds native frame-size settings, with overhead allowance and qualifications in section 8. |
| `MaxConcurrentCalls` | Zero selects the job queue's processor-count default. A positive value is passed as the dispatcher queue's worker-thread limit. It is not proven to cap outstanding async operations. |
| `MaxQueueSize` | Zero leaves the default `INT_MAX`; otherwise must remain positive after conversion. This is a service dispatch queue item limit, not TCP send-queue bytes. |
| `SecurityCredentials` | Null leaves security at `None`. Otherwise converted through `SecuritySettings::FromPublicApi`. |
| `Reserved` | Not consumed by the `FABRIC_TRANSPORT_SETTINGS` overload. Do not confuse this with the older `FABRIC_SERVICE_TRANSPORT_SETTINGS` extension that supports a connect timeout. |

The converter sets internal `connectTimeout_` to zero for this API; explicit
`IFabricTransportClient::BeginOpen(timeoutMilliseconds, ...)` supplies the logical
connect operation's timeout. Large public `ULONG` values are not necessarily
valid settings because several native storage fields are signed integers.
[Sources: [settings conversion][settings-cpp], [settings defaults][settings-h],
[dispatcher construction][dispatcher-h], [job queue defaults][job-queue].]

## 4. Addressing and connection/message lifecycle

### 4.1 Listener address and path routing

The listener accepts `IPAddressOrFQDN`, `Port`, and `Path`. The endpoint returned
by `EndOpen` has the form:

```text
host:port+servicePath
[IPv6-address]:port+servicePath
```

Port zero can request an OS-selected port; the native transport reads back its
actual listen address before constructing the endpoint. Clients split the
address at the **first `+`** and retain the remainder as the service path.
`+` is a Fabric address delimiter, not an HTTP URL separator. The path travels in
`ServiceLocationActorHeader`; it is not sent as a TCP connection preamble or
an HTTP request target. [Sources: [listener factory][com-listener-factory],
[transport open/address generation][service-transport], [client headers/address
parsing][client-headers], [constants][constants].]

Multiple listeners in the **same process** can share a native transport for the
same host/port and register distinct paths. Reuse requires the settings
comparison to succeed; incompatible settings return `AddressAlreadyInUse`, and
a duplicate registered path returns `AlreadyExists`. Notably, the comparison
checks message size, operation timeout, security, concurrent calls, and queue
size, but does **not** compare keepalive/connect timeout. The first shared
transport therefore determines those omitted transport settings. Closing one
listener does not stop the shared socket until its last listener closes.
[Sources: [shared listener factory][listener-factory], [settings
comparison][settings-cpp], [path registration][handler-collection],
[transport close][service-transport].]

### 4.2 Opening: three levels of connection

The normal buffer-based client sequence is:

```text
Client application               Native transport                 Service
      | Create client: local initialization only                      |
      | BeginOpen(timeout)                                             |
      |---------------------- lazy TCP connect ---------------------->|
      |<------------- security negotiation, if enabled -------------->|
      |<---------- protected transport control messages ------------->|
      |---------------- logical Connect request -------------------->|
      |                                         BeginProcessConnect   |
      |                                         EndProcessConnect     |
      |<-------------- Connect result / service error ----------------|
      | EndOpen success; OnConnected is posted                          |
      |---------------- application requests ------------------------>|
```

The exact control-message interleaving depends on authentication and connection
state. `ListenInstance` and connection authorization messages belong to the
transport, not to the service request handler.

The logical Connect is an otherwise ordinary correlated request with an empty
application body and these native headers:

| Header | Logical Connect value |
|---|---|
| `Actor` | `ServiceCommunicationActor`, numeric value `36` |
| `Action` | Wide string `Connect` |
| `ServiceLocationActor` | `servicePath,Connect` |
| `TcpClientIdHeader` | A GUID-form string generated for this client object |
| `Timeout` | Remaining connect-operation duration |
| `FabricActivity` | Activity identifier |
| `ExpectsReply`, `MessageId` | Added by the request/reply layer |

The service removes the `,Connect` compatibility suffix for lookup, records the
client ID and reply target, and invokes its connection handler. Acceptance sends
`ServiceCommunicationErrorHeader(Success)` and marks the client connected.
Messages received while acceptance is pending can wait in a per-client temporary
queue; accepted messages subsequently enter the service dispatcher.

The buffer-based factory requests **explicit connect**. `BeginRequest` while
the client is merely `Opened` fails with `ServiceCommunicationCannotConnect`;
it does not implicitly replace `BeginOpen`. The source also contains older
implicit-connect compatibility paths; those are not the normal contract of this
factory. [Sources: [COM client factory][com-client-factory], [native client
initialization][client-init], [logical connect operation][connect-op],
[request state check][request-op], [server routing/connect handling][service-receive],
[pre-connect queue][connection-state], [actor values][actor].]

### 4.3 Request/reply

For a normal request the client adds the service actor, action
`FabricServiceCommunication`, service path, client ID, timeout, and activity
headers. `RequestReply` adds `ExpectsReply(true)` and a `MessageId` if absent,
inserts a pending operation keyed by that ID, and sends the message.

The listener selects the service dispatcher by path, checks the logical client
connection, and schedules `BeginProcessRequest`/`EndProcessRequest`. The successful
application reply is converted back to a native message and receives
`ServiceCommunicationErrorHeader(Success)`. Handler failure produces an
error-only native reply instead.

`ReceiverContext::Reply` adds `RelatesTo(originalRequestId)` and sends through the
original reply target. The client demuxer recognizes a reply by nonempty
`RelatesTo` and routes it to `RequestTable`, not to the callback handler.
`EndRequest` converts service communication errors to its result.

Consequences:

- Multiple outstanding requests can share a connection and are matched by ID.
  Async handlers can complete out of request order.
- TCP preserves the order of bytes submitted on a connection, not the order of
  application-handler completion.
- `MessageId` is a process GUID plus a 32-bit index, not a textual UUID on wire.
  The logical client ID is a separate wide string.
- Local send completion or a successful `Send` is not a remote-processing
  acknowledgment. There is no application transaction, durable persistence,
  replay log, or exactly-once execution protocol in this layer.
- A request timeout stops waiting; it does not establish that the remote
  handler did not run, nor does the shown path send a remote cancellation.
  Automatic application-request replay is not implemented by this request path.

[Sources: [client header construction][client-headers], [request/reply][request-reply],
[dispatcher][dispatcher], [reply context][receiver-context],
[demultiplexing][demuxer], [reply table][request-table],
[request result conversion][request-op], [message ID][message-id].]

### 4.4 One-way sends and reverse notifications

**Service to client:** `IFabricTransportClientConnection::Send` adds the target
client ID, looks up that client's existing send target, and adds
`MessageId`, `IsAsyncOperationHeader(false)`, and the service actor. The client
dispatches it to `IFabricTransportCallbackMessageHandler::HandleOneWay`.
No second listening socket on the client is required. The underlying client
processing code can generate an internal reply after callback processing, but
this public `Send` does not expose or wait for it.

**Client to service:** `IFabricTransportClient::Send` converts the buffers and
calls `ServiceCommunicationClient::SendOneWay` directly. It does not take the
`TryConnectAsyncOperation` path or add `ExpectsReply`; callers should complete
explicit open first.

**Important implementation asymmetry:** although the IDL declares service
`IFabricTransportMessageHandler::HandleOneWay`, the inspected server dispatcher
unconditionally invokes its native `BeginProcessRequest` path, which the COM proxy
maps to `BeginProcessRequest` on that interface. It does not branch to service
`HandleOneWay` for a client `Send`. Thus "one-way" means that this sender does not
wait for a reply; it must not be interpreted as proof that this revision calls the
service's `HandleOneWay`. By contrast, the client's notification dispatcher
explicitly branches on `IsAsyncOperationHeader`.

The shared native connection abstraction also has reverse request/reply methods,
but the V2 callback COM proxy returns `NotImplemented` for them and the V2 IDL
does not expose them. Do not infer a symmetric RPC API from the shared C++
implementation. [Sources: [client wrapper][com-client], [client send/notification
processing][client-init], [server send][service-transport], [client-connection
wrapper][com-connection], [native client connection][native-connection],
[dispatcher][dispatcher], [service handler proxy][service-proxy],
[callback proxy][callback-proxy].]

### 4.5 Faults, close, and reuse

The client state machine is:

```text
Created -> Opening -> Opened -> Connected
                         |          |
                         v          v
                    ConnectFailed  Disconnected
                         \          /
                          -> Closed

Abort is also terminal.
```

`ConnectFailed`, `Disconnected`, `Aborted`, and `Closed` are terminal for logical
connect. After a logical disconnect/failure, create a new client rather than
assume `BeginOpen` reuses the old one. Client close calls `CloseClient`, stops its
transport, closes the request table and demuxer; it does not exchange a
service-level `"Disconnect"` request. Transport faults trigger server-side client
removal and disconnect notification. Listener close unregisters its path and
disconnects associated clients. This is not an application-work drain guarantee.
[Sources: [client states][client-states], [client release][client-init],
[client disconnect operation][disconnect-op], [listener lifecycle][listener],
[listener unregistration][unregister], [server disconnect/cleanup][service-receive],
[request/reply disconnects][request-reply].]

## 5. Message buffers and ownership

### 5.1 Public-to-native mapping

Given:

```text
application header = H bytes
body buffers       = B0, B1, ..., Bn
```

the wrapper constructs:

```text
native Transport::Message headers:
    FabricTransportMessageHeader(HeaderSize = len(H)), if len(H) > 0
    + routing/correlation/security headers added by other layers

native Transport::Message body:
    H || B0 || B1 || ... || Bn
```

`FabricTransportMessageHeader` has native header ID `0x804c`. Its field is a
Fabric-serialized `ULONG` application-header length, **not** the application
header bytes themselves. A headerless message omits it.

The array count and individual body-buffer lengths are not encoded as an
application scatter/gather directory. The receiver sees the same concatenated
bytes, but may receive a different number of body buffers with different
boundaries. When the application header crosses receive-buffer boundaries, the
wrapper copies it into one contiguous header buffer; otherwise it can reference
the receive storage directly. [Sources: [message conversion and split][com-message],
[application-header length header][app-header].]

### 5.2 Lifetime requirements

The outgoing conversion retains the COM message with `AddRef` and references
its supplied buffer memory. The application must keep that memory stable until
transport releases it through the disposal mechanism; returning from `Send` or
initiating `BeginRequest` does not authorize early buffer reuse.

Client sends and service replies normally queue disposal batches for
`IFabricTransportMessageDisposer::Dispose`. If enqueue fails, the code calls
the message's `Dispose` directly. The service-to-client connection wrapper uses
a direct per-message `Dispose` callback. The client disposer factory is a
process-wide singleton initialized with the first supplied disposer; do not
assume each subsequently created client gets a separate native disposer queue.

For received messages, `ComFabricTransportMessage` owns the native receive
message; its `Dispose()` is a no-op. Retaining the COM message keeps its buffer
storage alive; releasing the object releases that ownership. Copy bytes if they
must outlive the message. Do not retain raw pointers alone.

The outgoing converter dereferences the header descriptor even when its size is
zero. Supply a valid zero-length descriptor rather than assuming a null header
descriptor is accepted. In the incoming wrapper, no header item is allocated
when the application-header length is zero, so bindings must handle the
headerless result. [Sources: [message wrapper][com-message], [client disposer
factory][com-client-factory], [service disposal queue][service-proxy],
[batch disposer][disposer], [connection disposal][com-connection].]

## 6. TCP framing and serialization

### 6.1 Plaintext frame layout

Before security negotiation completes, and for connections with no security, the
TCP stream consists of consecutive frames:

```text
+-----------------------+----------------------------------+-----------------+
| TcpFrameHeader: 12 B   | native message headers: H bytes | body: B bytes   |
+-----------------------+----------------------------------+-----------------+
<------------------------ F = 12 + H + B ----------------------------------->
```

`TcpFrameHeader` is written as its raw C++ representation. On the supported
little-endian builds, its layout is:

| Offset | Size | Field | Meaning |
|---|---|---|---|
| `0` | 4 | `frameLength_` | Total frame bytes, **including** these 12 bytes. |
| `4` | 1 | `securityProviderMask_` | Provider bit mask, not a TLS-record type or the provider enum's ordinal. |
| `5` | 1 | `frameHeaderCRC_` | Optional CRC-8; zero means no CRC verification. |
| `6` | 2 | `headerLength_` | Total serialized native-message-header bytes. Not application-header size. |
| `8` | 4 | `frameBodyCRC_` | Optional CRC-32 over native headers plus body; zero means no CRC verification. |

The fixed-width integer fields are **little-endian**, not network-byte-order
integers. The source copies raw fields; it does not implement a portable
big-endian encoding. The class asserts its size is 12 bytes.

```text
F >= 12 + H
native_headers = frame[12 : 12 + H]
native_body    = frame[12 + H : F]
```

For an application message, with application-header length `A`:

```text
0 <= A <= len(native_body)
application_header = native_body[0 : A]
application_body   = native_body[A : ]
```

If the `FabricTransportMessageHeader` is absent, `A = 0`. These slicing formulas
describe valid frames; they are not a claim that the COM wrapper defensively
validates every malformed application-header length.

There is no separate frame magic, protocol-version field, body-length field,
application-buffer count, or method ID in this fixed header. Native header IDs
and serialized content provide the remaining interpretation. [Sources:
[frame declaration][frame-h], [frame construction/validation][frame-cpp],
[frame assembly][tcp-send], [frame extraction][tcp-receive].]

The provider mask is:

| Security provider | Enum ordinal | Mask byte |
|---|---|---|
| None | `0` | `0x00` |
| SSL/X509 | `1` | `0x01` |
| Kerberos | `2` | `0x02` |
| Negotiate | `3` | `0x04` |
| Claims over TLS | `4` | `0x08` |

For nonzero ordinals, the mask is `1 << (ordinal - 1)`. The receiver records the
first frame's mask and rejects subsequent inconsistent masks. The mask does not
by itself authenticate the peer or dynamically choose an arbitrary provider;
the security context comes from local settings. [Sources:
[provider values][provider-enum], [provider mapping][provider-map],
[receive state][tcp-receive], [security-context selection][security-create].]

### 6.2 Checksums

For an unencrypted frame, header-error checking defaults to enabled, while
message-error checking defaults to disabled.

Header CRC calculation copies the 12-byte frame header, zeros both CRC fields,
and computes CRC-8 across that copy. The implementation uses an initial value
of zero and the CRC-8 table corresponding to polynomial `0x07`. Body CRC uses
the standard reflected CRC-32 table (`0xedb88320`), initial value `0xffffffff`,
and final XOR `0xffffffff`, across:

```text
serialized native headers || native body
```

The body CRC excludes the fixed frame header. The receive path checks each CRC
only if its field is nonzero. These are corruption checks, **not** authentication,
MACs, or substitutes for TLS. Protected sends do not use this optional CRC
generation path. [Sources: [CRC setup and frame assembly][tcp-send],
[CRC algorithms][crc-h], [CRC tables][crc-tables], [header CRC validation][frame-cpp],
[body CRC validation][tcp-receive], [CRC configuration][crc-config].]

### 6.3 Native-message-header records

The `H`-byte native-header area is a concatenation of records:

```text
uint16 header_id                  raw little-endian
uint16 serialized_object_length   raw little-endian; excludes these four bytes
byte   serialized_object[serialized_object_length]
```

`MessageHeaders::Serialize` writes the ID and length as raw bytes, then uses
`FabricSerializableStream::WriteSerializable` for the object. Consequently, a
header containing a `ULONG` is **not** simply a two-byte ID plus a raw four-byte
value. Deleted headers can remain in the byte stream with ID `0x8000` and must be
skipped using their length; compaction does not necessarily occur on every send.
Combined native-header size is limited to `uint16::max`, or 65,535 bytes.
[Sources: [header serialization][header-serialization], [deleted-header
iteration][header-iteration], [header-size limit][header-limit].]

Relevant IDs and field schemas are:

| ID | Header | Serialized fields / role |
|---|---|---|
| `0x8001` | `Actor` | Actor enum; service traffic uses `36`, security negotiation uses `35`. |
| `0x8002` | `Action` | Wide string, such as `Connect` or `FabricServiceCommunication`. |
| `0x8003` | `MessageId` | GUID and 32-bit index. |
| `0x8004` | `RelatesTo` | Request's message ID, for reply correlation. |
| `0x8005` | `ExpectsReply` | Boolean set by `RequestReply`. |
| `0x8020` | `FabricActivity` | Activity ID used for tracing, separate from request correlation. |
| `0x8023` | `Timeout` | `TimeSpan`: signed 64-bit count of 100 ns ticks. |
| `0x8029` | `MessageSecurity` | Phase-dependent: `ClientAuthHeader(providerType_)` for qualifying client claims negotiation; `SecurityHeader` for legacy per-message protection (section 7.4). |
| `0x8041` | `ServerAuth` | Server authorization-status promise and optional claims metadata. |
| `0x8043` | `ServiceLocationActor` | Service path; logical Connect appends `,Connect`. |
| `0x8045` | `TcpClientIdHeader` | Logical client ID wide string. |
| `0x8046` | `ServiceCommunicationError` | Native `ErrorCodeValue` integer; converted to HRESULT at the COM boundary. |
| `0x8047` | `IsAsyncOperationHeader` | Distinguishes reverse async request vs one-way processing in the client path. |
| `0x8048` | `SecurityNegotiation` | Extra-framing flag, framing-protection flag, listen instance, maximum incoming frame size. |
| `0x804c` | `FabricTransportMessageHeader` | Application-header byte length. |

The table is a relevant subset, not a closed allowlist or a mandated header
order. In this source tree `0x804c` is also a historical compatibility alias
outside this protocol; interpret it in context. [Sources: [header IDs][header-ids],
[actor values][actor], [message ID][message-id], [timeout representation][timespan],
[security negotiation schema][nego-header], [application-header schema][app-header],
[service-error schema][error-header].]

`MessageSecurity` must be decoded in its protocol phase. The first qualifying
SSL/Claims client negotiation output uses `ClientAuthHeader`, whose sole
serialized field is `providerType_`; the legacy data-protection envelope uses
the different `SecurityHeader` schema under the same ID. The separate
`CustomClientAuth` enum member (`0x8035`) is not the binding used by this
claims-negotiation path. [Sources: [client-auth header binding][client-auth-header],
[negotiation sender][ssl-handshake], [negotiation receiver][claims-select],
[legacy security header][security-header].]

### 6.4 Fabric serialization inside each header

The object payload uses Service Fabric's own typed/scoped binary serialization,
not NDR, protobuf, JSON, or a packed C struct:

```text
Object marker
ObjectHeader { size, flags, native layout padding }
optional type-information data
one or more scopes containing typed fields in FABRIC_FIELDS order
ObjectEnd marker
```

The object writer reserves and later rewrites `sizeof(ObjectHeader)`. Its
`size` includes that object header and the subsequent object content, but not the
preceding Object marker. The declared fields are `ULONG size` and a byte-sized
flags enum; a reimplementation must preserve the serializer's actual layout,
not assume this is a packed five-byte structure.

Important coding rules:

- Metadata values include `Object = 0x00`, `ScopeBegin = 0x1f`,
  `ScopeEnd = 0x2f`, and `ObjectEnd = 0x3f`. Array and empty-value bits are
  `0x80` and `0x40`.
- Integers use type metadata followed by **most-significant-group-first
  7-bit compression**. Bit `0x80` means another byte follows; signed values use
  sign extension based on the first group's bit `0x40`. This is not protobuf
  LEB128 or ZigZag. Zero scalar integers can be represented by empty-value
  metadata alone.
- Nonempty array/string counts are compressed unsigned integers without their
  own type metadata. Empty arrays/strings normally use empty-array metadata and
  omit the count; a zero passed to the standalone no-metadata count encoder
  still needs a zero byte.
- `std::wstring` is serialized as a `WCHAR` array with an element count, not as
  a null-terminated UTF-8 string. The Linux build explicitly uses
  `-fshort-wchar`; with little-endian Windows-compatible characters these are
  two-byte UTF-16 code units.
- GUIDs use the typed GUID/blittable representation, not their human-readable
  string form. Enum fields normally pass through a signed 64-bit serializer.
- `FABRIC_FIELDS_*` creates scopes; `FABRIC_PRIMITIVE_FIELDS_*` emits the
  contained primitive fields without creating a nested object by itself.

An independent codec needs the serializer's unknown-scope, empty-value,
type-information, and nested-value rules as well as the outer record layout.
This section is a decoding guide, not an exhaustive replacement for that
serializer. [Sources: [object serialization][serializer-object],
[object-header declaration][serializer-header], [metadata values][serializer-types],
[primitive/string methods][serializer-primitives], [compression][compressor],
[array/scalar encoding][serializer-values], [field/enum helpers][field-helpers],
[primitive-object dispatch][field-dispatch], [string helper][string-helper],
[primitive field macros][primitive-fields], [Linux character ABI][short-wchar].]

## 7. Security and TLS on the wire

### 7.1 Provider selection and protection

`SecurityCredentials.Kind` is mapped to native settings:

| Credentials | Implementation | Network protection |
|---|---|---|
| Null / `NONE` | No security context | Clear Fabric frames. No peer authentication or cryptographic integrity. |
| `X509` | `SecurityContextSsl` | Certificate-authenticated TLS; native transport sets header/body protection to `EncryptAndSign`. |
| `X509_2` | SSL provider mapping, platform-specific parsing | Supported by the inspected Unix parsing path; explicitly rejected by the Windows `FromPublicApi` branch. Do not assume interchangeability with `X509`. |
| `WINDOWS` | `SecurityContextWin` on Windows | SSPI Kerberos or Negotiate, with configured signing/encryption. This is **not TLS**. The Unix secure-context factory does not implement this provider. |
| `CLAIMS` | TLS plus a claims token | Client-only credentials; server certificate authorization plus later client-token authorization where the receiving stack has been configured for it. |

TLS protection is not disabled merely by choosing a weaker public protection
level: `TransportSecurity::Set` assigns `EncryptAndSign` for SSL and Claims.
For Windows credentials the configured protection level is used by the SSPI
implementation instead. [Sources: [public provider mapping][provider-map],
[credential parsing][security-settings], [transport security setup][transport-security],
[context selection][security-create], [Windows protection dispatch][windows-security].]

### 7.2 Negotiation is Fabric-framed TLS, not immediate TLS

The client starts the security context when its TCP connection opens. Negotiation
produces TLS/SSPI token bytes and wraps them in a native `Message` with
`Actor::SecurityContext` (`35`) and a message ID. The first outgoing negotiation
message also carries `SecurityNegotiationHeader`; server negotiation output can
carry `ServerAuthHeader`. These messages are sent with `shouldEncrypt = false`.

For X509, a typical stream begins conceptually as:

```text
Client -> Server TCP bytes:
    clear Fabric frame {
        securityProviderMask = 0x01
        native headers = SecurityContext actor, MessageId, SecurityNegotiation, ...
        native body    = TLS ClientHello token bytes
    }

Server -> Client:
    clear Fabric frame { native body = TLS handshake output bytes }

... further Fabric-framed TLS handshake tokens as needed ...
```

The body can contain the TLS library's complete output for a negotiation step;
do not equate one Fabric negotiation message with exactly one TLS record.
Handshake TLS records remain TLS records, including any protection that TLS
itself applies during negotiation, but their outer Fabric frame and headers are
initially unprotected.

On Unix, the implementation feeds the body into an OpenSSL memory BIO, calls
`SSL_do_handshake`, and extracts output from another BIO. On Windows it supplies
the body to Schannel `InitializeSecurityContext` / `AcceptSecurityContext` and
wraps the resulting token.

Consequently, a stock TLS client such as `openssl s_client`, an HTTPS client, or
a TLS-terminating proxy cannot be assumed to connect to this port as ordinary
TLS. A transparent TCP proxy can relay the bytes; TLS termination requires
understanding Fabric's framing/state transitions. There is no HTTP CONNECT,
STARTTLS text command, HTTP/2 preface, or ALPN-based application protocol
implemented by the paths described here. [Sources: [connection negotiation][connection-security],
[negotiation-message construction][nego-message], [SSL negotiation][ssl-handshake],
[negotiation-header enqueue][send-queue].]

### 7.3 Secured connection with framing protection

#### SSL/Claims frame protection

For SSL/X509 or Claims with framing protection enabled, **the complete normal
Fabric frame is TLS plaintext**, including the 12-byte fixed header:

```text
TLS plaintext:
    TcpFrameHeader || native headers || native body

TCP wire after handshake:
    TLS record(s) containing encrypted Fabric frame bytes
```

There is no additional clear 12-byte Fabric header in front of each protected
data frame. The frame's `frameLength` describes its plaintext size, not the
ciphertext length on TCP.

Unix assembles those plaintext bytes, calls `SSL_write`, and sends the
resulting BIO output. Windows' framing-protected SSL encoder similarly includes
the frame header and fills Schannel TLS records with the message bytes.

Receiving reverses the order:

```text
TCP receive fragments
    -> TLS record processing / decryption
    -> accumulated plaintext stream
    -> parse 12-byte Fabric frame header
    -> wait for frameLength plaintext bytes
    -> split native headers and native body
    -> native header deserialization and service dispatch
```

TCP segment, socket-read, TLS-record, Fabric-frame, application-header, and
application-body-buffer boundaries are different concepts. A frame can span
TLS records and TCP segments; parsers must buffer incomplete input and consume
all complete frames without relying on socket read boundaries.

**Platform selection:** Unix `SecuritySettings::FramingProtectionEnabled()`
always returns true. On Windows the decision can come from an internal callback
or security configuration; the generic `FramingProtectionEnabled` setting is
false by default in this revision, while separate peer/client-mode settings also
exist. For the direct FabricTransport path, the settings getter falls back to
that generic setting unless an internal override applies; the presence of the
separately named client-mode setting alone does not make the fallback true.
Do not infer that every user of the shared transport takes the same setting.
The first received negotiation header can enable framing protection
when the peer advertises it. Thus Unix and Windows communication must account for
the negotiated mode, not just their local generic default.
[Sources: [mode selection][framing-setting], [configuration][security-config],
[Unix frame encryption][tcp-send], [Windows frame encryption][ssl-encode],
[decrypt-before-frame-parse][tcp-receive], [OpenSSL data path][ssl-unix],
[negotiation-header processing][security-verification].]

#### Windows Kerberos/Negotiate frame protection

Windows SSPI framing protection uses its own chunks, not TLS records. Each chunk
has this layout on the documented little-endian Windows ABI:

```text
SecBufferSizes: 8 raw bytes
    u32 dataSize | u16 paddingSize | u16 tokenSize

Following bytes:
    data[dataSize] | padding[paddingSize] | SSPI token[tokenSize]

Total chunk bytes = 8 + dataSize + paddingSize + tokenSize
```

The eight-byte size prefix is outer framing metadata, outside the protected
DATA buffer. The two supported protection modes have different confidentiality
properties:

| Mode | DATA | Padding and token |
|---|---|---|
| `EncryptAndSign` | Encrypted by SSPI `EncryptMessage`. | Optional padding, followed by the security token. |
| `Sign` | **Unencrypted**, authenticated by `MakeSignature`. | `paddingSize = 0`, followed by the signature token. |

The input DATA stream includes the fixed frame header, native headers, and
native body. After decrypting or verifying complete chunks, concatenate their
recovered DATA to parse the inner message:

```text
TcpFrameHeader || native headers || native body
```

There is no additional clear Fabric frame header before the SSPI chunks. One
Fabric frame can require multiple chunks. The receiver must wait for a complete
size prefix and the indicated chunk bytes before decrypting/verifying, then
accumulate enough recovered DATA for the inner frame parser. In Sign mode,
visible DATA is not a reason to skip signature verification or treat the
eight-byte prefix as a Fabric frame header.

This description applies only to Windows Kerberos/Negotiate with framing
protection, not to Windows SSL or Unix. OS cryptographic token internals and the
complete legacy per-message SSPI codec remain outside this document's scope.
[Sources: [mode dispatch][windows-security], [size prefix and encrypted
chunks][windows-sspi-encrypt], [signed chunks][windows-sspi-sign],
[decrypt/buffering][windows-sspi-decrypt], [verify/buffering][windows-sspi-verify],
[outer send-buffer selection][tcp-send], [decode before frame parsing][tcp-receive].]

### 7.4 Windows legacy message protection without framing protection

The non-framing-protected Windows SSL path uses a different arrangement:

```text
clear TcpFrameHeader
native headers containing MessageSecurity / SecurityHeader {
    protection levels
    encrypted original native headers as HeaderData
    token/padding fields used by security envelopes
}
native body containing encrypted original body records
```

The encoder encrypts the original native-header stream and body separately,
places the protected header bytes in `SecurityHeader.HeaderData`, creates the
protected body, adds the security header, and rebuilds the outer frame length.
The receiver first parses the clear Fabric frame, then reads `SecurityHeader`,
decrypts/restores the original header/body content, and continues dispatch.

This still uses Schannel TLS record encryption, but is **not** the same TCP byte
stream as whole-frame TLS protection. In particular, the clear fixed framing is
not made cryptographically protected by separately encrypting headers and body.
`MessageSecurity` has header ID `0x8029`.

Kerberos/Negotiate also have framing-protected and per-message SSPI paths, with
their own signing/encryption envelopes; those envelopes are not TLS records.
[Sources: [encryption mode branch][tcp-send], [legacy SSL encoder][ssl-encode],
[legacy SSL decoder][ssl-decode], [security-header schema][security-header],
[Windows SSPI selection][windows-security].]

### 7.5 Authentication is more than handshake success

After TLS negotiation, `SecurityContext` invokes remote authorization before
releasing normal traffic:

1. Load local certificate/private-key credentials and negotiate TLS.
2. Obtain the remote certificate and authorize it using the configured remote
   thumbprints or X509 names/issuers, certificate-chain policy, and revocation
   policy. X509 mode normally authenticates both peers.
3. Exchange protected transport connection-authorization status when promised
   by the server. The client can delay normal encrypted traffic until it
   receives that status.
4. Perform the separate logical service `Connect` and connection-handler
   acceptance described earlier.

#### Essential transport-control messages

These shared-transport messages are not application requests. Actors `Transport`
(`1`) and `TransportSendTarget` (`29`) are intercepted before application
dispatch; pending claims submission is consumed by the security layer itself.
The body lists below specify **Fabric-serialized field order**, not packed
public-API structs. The envelopes describe what the native producers emit,
not additional receiver validation requirements.

| Control | Emitted envelope | Ordered serialized body | Receive effect |
|---|---|---|---|
| Listen-instance | Actor `Transport` (`1`); no Action header, hence empty action; `MessageId` and `HighPriority`. | `ListenInstance`: `[wstring address_, uint64 instance_, Guid nonce_]`. [Schema][listen-instance-schema]. | Empty-action Transport traffic is decoded as a listen instance. Successful decoding calls `OnConnectionReady` and associates the advertised address/instance with the connection/send target. [Dispatch][transport-control-dispatch], [consumer][listen-instance-receive]. |
| Connection authorization | Actor `TransportSendTarget` (`29`); Action `"ConnectionAuth"`; `MessageId` and `FaultHeader(authStatus, true)`. | `ConnectionAuthMessage`: `[wstring errorMessage_, RoleMask::Enum roleGranted_]`. [Schema][connection-auth-schema]. | Combines the fault status with body information, aborts on failure, can remove the outgoing size limit for an Admin client, and releases held sends when waiting for status. [Consumer][connection-auth-receive], [status application][connection-auth-apply]. |
| Claims submission | Actor `Transport` (`1`); Action `"Claims"`; `MessageId`. | `ClaimsMessage`: `[wstring claims_]`. [Schema][claims-message-schema]. | While authorization is pending, requires a Transport actor and decodable claims body, consumes the message, and suspends receiving for the configured claims handler. Completion resumes receiving or denies/closes the connection. [Consumer][claims-processing], [completion][claims-completion]. |

Sources for the emitted envelopes are [listen-instance/claims
construction][connection-controls], [authorization-message
construction][connection-auth-producer], [actor values][actor], and
[control action literals][transport-constants]. The common Transport-actor
interception is in [connection dispatch][control-actor-dispatch].

The state and compatibility qualifications matter:

- Listen-instance emission can be suppressed. When emitted, it is queued for
  protection if a security context exists, and sent clear otherwise; it is not
  always a clear pre-handshake message.
- Waiting for `ConnectionAuth` requires client-only mode, the server's promise
  to send status, and a send-buffer mode supporting it. The consumer reads the
  status body when `HasFaultBody` is set; do not require that body from every
  historical peer merely because the current producer emits it.
- Claims authorization remains a separate pending gate. Initial secured-session
  success status can precede claims completion; it does not prove that a token
  has already been accepted. Successful claims completion sends another
  authorization status with the resulting role.
- `"Claims"` is the producer's action string. The pending-auth consumer checks
  the Transport actor and body, not that action as an extra validation gate.

These mappings do not impose one universal control-message ordering or add
claims-server support to the bare public listener API; the registration
restriction in section 7.6 still applies. [Sources: [listen-instance
suppression/protection][connection-controls], [status-wait
conditions][server-auth], [session/status sequencing][connection-security],
[authorization-body compatibility][connection-auth-receive],
[pending claims authorization][ssl-unix-auth], [claims handler
completion][claims-completion], [claims receive checks][claims-processing].]

Certificate identity is evaluated by Service Fabric's security settings, not
merely by checking whether the endpoint DNS name matches a browser-style
certificate. A successful low-level certificate callback alone is not the final
authorization result.

Unix starts with `SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT`. Its verify
callback can defer selected chain failures, including cases relevant to
thumbprint/public-key authorization, to `AuthorizeRemoteEnd`. Windows requests
manual Schannel certificate validation and subsequently applies the Service
Fabric certificate policy. Therefore do not simplify this to "any trusted CA is
allowed" or "self-signed certificates always fail"; the configured allowlists and
chain/revocation rules decide. [Sources: [credential acquisition][credentials],
[SSL initialization/verification callback][ssl-unix],
[Unix certificate authorization][ssl-unix-auth], [Windows certificate
authorization][ssl-win-auth], [authorization sequencing][security-negotiate],
[server authorization promise][server-auth], [connection status gate][connection-security].]

`SecurityNegotiationHeader` advertises framing capability, listen instance, and
maximum incoming frame size before the session is secured. The send buffer
repeats verification headers on the first protected outgoing message; the
receiver compares a repeated negotiation header with the previously observed
one. A mismatch fails processing. The implementation permits some compatibility
cases where the header is absent, so this should not be described as a mandatory
cryptographic transcript binding for every historical peer. The receive code
also applies a conservative pre-authorization size limit, including while
verification is pending. [Sources: [negotiation schema][nego-header],
[verification send][send-queue], [verification comparison][security-verification],
[receive limits][tcp-receive].]

### 7.6 Claims and platform/version limits

Claims mode uses TLS for server authentication and confidentiality, then sends
a protected `ClaimsMessage` containing the client token. A claims-enabled server
can relax certificate-based client mutual authentication and must complete
claims authorization before normal application traffic. `ProcessClaimsMessage`
requires a configured claims handler and rejects ordinary application messages
while authorization is pending.

This capability exists in the shared transport, but the two creation functions
in this IDL do **not** expose registration of a server claims-validation handler.
The public Claims credentials parser is explicitly client-side, and native
credential acquisition asserts that Claims credentials are client-only. Do not
assume a standalone listener created solely through these APIs is a complete
claims-authenticated server. [Sources: [claims negotiation selection][claims-select],
[claims processing][claims-processing], [claims enqueue][connection-controls],
[claims settings/acquisition][security-settings], [provider setup][transport-security].]

This revision uses `SSLv23_method()` on Unix as a version-flexible TLS method,
and Schannel credentials on Windows. These names do not establish that SSLv2 or
SSLv3 is negotiated, nor prove TLS 1.2/1.3 support or a particular minimum
version. The shown credential setup does not pin a cipher suite/minimum TLS
version through `FABRIC_TRANSPORT_SETTINGS`; the linked TLS library, OS policy,
and deployed runtime matter.

The shared security layer supports credential refresh, session expiration, and
session replacement. The configuration's default session duration is seven days
with additional scheduling variance; zero disables expiration. This is not a
promise that an application's logical client will transparently reconnect after
an ordinary terminal disconnect. No certificate hot-reload or TLS-version
selection method is exposed on `IFabricTransportClient` itself.
[Sources: [credential backend][credentials], [security configuration][security-config],
[session-expiration scheduling][security-negotiate], [client states][client-states].]

## 8. Limits, backpressure, timeouts, and errors

### 8.1 Message size is not simply payload length

The listener sets both incoming and outgoing size settings; the client constructor
sets its outgoing setting only. `TransportSecurity` expands a configured
`MaxMessageSize = M` into an internal frame allowance:

```text
Hmax = 65535

if M <= Hmax:
    internal_limit = M + 12 + Hmax + 8192
else:
    internal_limit = M + 12 + Hmax + floor(M * 0.05)
```

This is the source's overhead calculation, not an application-payload validator.
During security negotiation, a peer's nonzero advertised incoming limit can
**replace** the local outgoing connection limit; the code does not simply take
the minimum of the two.

The inspected `TcpConnection::SendOneWay` only returns its outgoing
`MessageTooLarge` result from this check for a secured, confirmed connection.
Similarly, the `TcpReceiveBuffer` frame-size checks shown here are guarded by
the presence of a security context. Thus it would be inaccurate to promise a
strict, symmetric `MaxMessageSize` enforcement boundary on every unsecured path.
Applications needing strict payload limits should enforce them at their own
message boundary as well.

Before authorization/verification completes, secured receive processing uses
`MaxMessageSizeBeforeSessionIsSecured`, default **128 KiB**, rather than the
normal authorized frame limit. The frame-length field itself has a 32-bit hard
limit, which is not an assurance that messages approaching 4 GiB are supported
by allocations, signed settings, header serialization, or a deployed runtime.
[Sources: [listener sizing][service-transport], [client sizing][client-init],
[internal size calculation][size-overhead], [peer limit replacement][peer-size],
[outgoing size check][send-size], [receive size checks][tcp-receive],
[pre-secure limit configuration][security-config].]

### 8.2 Queues and concurrency

There are distinct queues:

| Queue | What it holds / limit |
|---|---|
| Pre-connect client-state queue | Messages waiting for a service connection handler to accept a client. Separate from `MaxQueueSize`. |
| Service dispatch queue | Requests for a service path; configured with `MaxQueueSize` and worker count `MaxConcurrentCalls`. Full enqueue returns a `ServiceTooBusy` error reply. |
| Per-target TCP send queue | Buffered outgoing bytes, with a separate transport-configured byte limit. Enqueue failure can report `TransportSendQueueFull`. |
| Security-delayed send queue | Messages requiring protection but waiting for negotiation/authorization. Also subject to send-buffer limits. |
| Dispose queue | Outgoing-message lifetime cleanup, not message delivery. |

The service work item's `Process` starts an asynchronous dispatch operation and
does not wait for its completion before returning. Therefore its job-worker count
should not be advertised as a strict limit on all outstanding application async
requests. Also, concurrent handlers and worker-queue scheduling mean applications
should not rely on service execution order merely because transport is TCP.
[Sources: [pre-connect queue][connection-state], [dispatch queue setup][dispatcher-h],
[dispatch/queue overflow][dispatcher], [job queue][job-queue],
[timed work-item completion][timed-jobs],
[TCP/security send queues][send-queue].]

### 8.3 Timeouts and result reporting

Request timeout is represented both by the caller's pending request timer and by
a `TimeoutHeader` sent to the service. Queue residence reduces the dispatcher work
item's remaining time. An item that times out in the service queue logs the event;
its `OnTimeout` implementation does not itself send a timeout reply, so the
requester's own timer remains important.

`RequestReply` distinguishes a timeout before local send completion from a later
response timeout when `EnableDifferentTimeoutError` is enabled, as it is here:
the former becomes `SendFailed` before communication-layer error translation.

Outgoing queue expiration is a separate mechanism. The shared transport replaces
an infinite send-expiration argument with `DefaultOutgoingMessageExpiration`,
whose default is **180 seconds**; thus an infinite request/operation timeout
does not guarantee unlimited residence in the socket send queue. The default
per-target send-queue budget is **64 MiB**, with capacity adjustment to accommodate
the configured maximum message size. These are shared transport settings, not
fields in `FABRIC_TRANSPORT_SETTINGS`.

Service errors travel as native `ErrorCodeValue` in
`ServiceCommunicationErrorHeader`, not as a raw COM HRESULT body. Important
results include `InvalidAddress`, `EndpointNotFound`, `InvalidMessage`,
`ServiceTooBusy`, `MessageTooLarge`, `Timeout`, and
`ServiceCommunicationCannotConnect`. The communication helper preserves certain
authentication/service errors while mapping other transport failures to
`ServiceCommunicationCannotConnect`. Applications must inspect the returned
operation result rather than expect every failure as application reply bytes.

TCP keepalive is an OS socket option: Unix enables `SO_KEEPALIVE`, sets
`TCP_KEEPIDLE` to the configured duration and `TCP_KEEPINTVL` to one second;
Windows uses `SIO_KEEPALIVE_VALS` with a one-second retry interval. The shown
Unix function does not set `TCP_KEEPCNT`, so exact failure-detection time also
depends on OS policy. Both communication client and listener disable the shared
transport's idle-connection timeout by setting it to zero.
[Sources: [request timeout conversion][com-request], [dispatcher timeout][dispatcher],
[timed job behavior][timed-jobs], [send expiration selection][send-expiration],
[shared send-queue defaults][transport-defaults],
[send-vs-reply timeout distinction][request-reply], [error conversion][error-helper],
[service reply error][request-op], [keepalive options][keepalive],
[client initialization][client-init], [transport initialization][service-transport].]

## 9. Interoperability and debugging checklist

For an independent client/server or packet decoder:

1. Separate the local COM ABI from the network protocol. Implement the native
   frame and Fabric header serialization, not marshaling of IDL structs.
2. Read/reassemble the TCP stream. Parse little-endian 12-byte clear frames
   during initial negotiation; do not look for a TLS ClientHello at offset zero.
3. Handle transport control actors separately from actor `36` service traffic.
   Listen-instance, authorization, security, and claims messages are not
   application callbacks.
4. Configure the correct provider and complete certificate/claims/SSPI
   authorization. Do not treat provider mask or handshake completion as service
   acceptance.
5. Track the transition to whole-frame TLS versus legacy per-message protection.
   The final clear negotiation frame and the first protected bytes can be
   adjacent in the same TCP receive buffer.
6. Perform logical `Connect` using the exact returned service path, client ID,
   and correlated result before issuing normal requests.
7. Implement `MessageId`/`RelatesTo`, timeout/error headers, and unknown/deleted
   header handling. Do not confuse logical client ID with message ID.
8. Reconstruct the application header from the length recorded in header
   `0x804c`; treat the rest as the application body, without restoring original
   scatter/gather boundaries.
9. Apply explicit payload/resource limits and preserve buffer lifetimes. Do not
   infer robust bounds, ordering, remote acknowledgment, or outstanding-request
   limits from setting names alone.
10. Validate against the exact runtime and platform being targeted. Exercise
    unsecured, X509/framing-protected, and applicable Windows legacy paths
    separately, including malformed/partial input, concurrent replies,
    disconnects, and disposal.

Useful native tests to consult are
[`comfabrictransport.test.cpp`][com-tests] for COM request/reply, header
reassembly, notifications, explicit-connect failures and message size, and
[`SecureTransport.Test.cpp`][secure-tests] for the common security transport.
They are evidence of intended cases, not a claim that these tests were run here.

## 10. Implementation map

| Concern | Source under the Service Fabric checkout |
|---|---|
| ABI and exported creation entry points | `src/prod/src/idl/internal/fabrictransport_.idl`; `src/prod/src/retail/native/FabricTransport/dllmain.cpp` |
| COM messages and application-header split | `src/prod/src/api/wrappers/ComFabricTransportMessage.cpp`; `FabricTransportMessageHeader.h` in that directory |
| COM client/listener/callbacks/disposal | `src/prod/src/api/wrappers/ComFabricTransport*.cpp`; `ComProxyFabricTransport*.cpp` |
| Logical connection and client state | `src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient*` |
| Service addressing, shared listener and dispatch | `src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransport.cpp`, `ServiceCommunicationListenerFactory.cpp`, `ServiceMethodCallDispatcher.*` |
| Correlation and demultiplexing | `src/prod/src/Transport/requestreply.cpp`, `RequestTable.cpp`, `ReceiverContext.cpp`, `DemuxerT.h` |
| TCP frame construction and parsing | `src/prod/src/Transport/TcpFrameHeader.*`, `TcpSendBuffer.cpp`, `TcpReceiveBuffer.cpp` |
| Socket/connection/security state transitions | `src/prod/src/Transport/TcpConnection.cpp`, `SendBuffer.cpp` |
| Header records and identifiers | `src/prod/src/Transport/MessageHeaders.*`, `MessageHeaderId.h` |
| Fabric object/primitive codec | `src/prod/shared/serialization/src/FabricSerializableStream.cpp`, `ByteCompressor.h`; `src/prod/src/Common/FabricSerializationHelper.h` |
| Security settings and provider conversion | `src/prod/src/ServiceModel/transport/SecuritySettings.cpp`, `SecurityProvider*` |
| TLS/SSPI and certificate authorization | `src/prod/src/Transport/SecurityContext.cpp`, `SecurityContextSsl.cpp`, `SecurityContextWin.cpp`, `SecurityCredentials.cpp`, `TransportSecurity.cpp` |
| Transport/security configuration defaults | `src/prod/src/Transport/TransportConfig.h`; `src/prod/src/Common/SecurityConfig.h` |

<!-- Source references are pinned to the inspected commit, not the default branch. -->
[revision]: https://github.com/microsoft/service-fabric/commit/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/internal/fabrictransport_.idl#L20-L256
[exports]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/retail/native/FabricTransport/dllmain.cpp#L36-L68
[com-client-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportClientFactory.cpp#L17-L120
[com-listener-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportListenerFactory.cpp#L26-L101
[com-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportClient.cpp#L27-L160
[com-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportSendRequestAsyncOperation.cpp#L13-L85
[com-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportMessage.cpp#L15-L195
[app-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/FabricTransportMessageHeader.h#L10-L36
[com-connection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportClientConnection.cpp#L14-L42
[service-proxy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComProxyFabricTransportMessageHandler.cpp#L13-L163
[callback-proxy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComProxyFabricTransportCallbackMessageHandler.cpp#L28-L58
[disposer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComProxyFabricTransportDisposer.h#L10-L47
[settings-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/servicecommunicationtransportsettings.cpp#L86-L213
[settings-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransportSettings.h#L18-L29
[service-transport]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransport.cpp#L412-L683
[unregister]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransport.cpp#L280-L410
[service-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransport.cpp#L686-L1000
[client-init]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.cpp#L16-L397
[client-headers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.cpp#L399-L449
[client-states]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.h#L28-L36
[connect-op]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.TryConnectAsyncOperation.cpp#L47-L307
[request-op]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.SendRequestAsyncOperation.cpp#L41-L134
[disconnect-op]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.DisconnectAsyncOperation.cpp#L32-L36
[listener]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationListener.cpp#L45-L216
[listener-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationListenerFactory.cpp#L40-L150
[handler-collection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationMessageHandlerCollection.cpp#L16-L65
[constants]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/Constants.cpp#L12-L15
[dispatcher]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceMethodCallDispatcher.cpp#L55-L261
[dispatcher-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceMethodCallDispatcher.h#L19-L44
[job-queue]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/JobQueue.h#L16-L42
[timed-jobs]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/JobQueue.h#L828-L940
[connection-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ClientConnectionStateInfo.h#L20-L103
[native-connection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ClientConnection.h#L28-L60
[error-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationErrorHeader.h#L13-L41
[error-helper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationHelper.cpp#L13-L51
[request-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L40-L246
[receiver-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L26-L44
[demuxer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L196-L237
[request-table]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L77
[message-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageId.h#L29-L57
[actor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L17-L98
[frame-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.h#L10-L41
[frame-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.cpp#L14-L118
[tcp-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendBuffer.cpp#L88-L278
[tcp-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpReceiveBuffer.cpp#L18-L241
[socket-io]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2364-L2466
[header-serialization]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L163-L186
[header-iteration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L498-L565
[header-limit]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.h#L337-L340
[header-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaderId.h#L21-L166
[timespan]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/TimeSpan.h#L145-L162
[serializer-object]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L320-L425
[serializer-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializableStream.h#L228-L246
[serializer-types]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializationTypes.h#L12-L51
[serializer-primitives]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1006-L1140
[serializer-values]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L1979-L2134
[compressor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/ByteCompressor.h#L10-L161
[field-helpers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L44-L302
[field-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4825-L4846
[string-helper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4923-L4926
[primitive-fields]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4310-L4338
[short-wchar]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/CMakeLists.txt#L170-L175
[provider-enum]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecurityProviderEnum.h#L6-L15
[provider-map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecurityProvider.cpp#L15-L112
[security-settings]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L463-L735
[framing-setting]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L1550-L1572
[transport-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportSecurity.cpp#L55-L201
[size-overhead]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportSecurity.cpp#L233-L258
[peer-size]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L404-L425
[send-size]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L510-L564
[security-create]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L18-L42
[security-negotiate]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L253-L408
[server-auth]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L192-L252
[nego-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L438-L458
[security-verification]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L542-L679
[nego-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityNegotiationHeader.h#L10-L35
[connection-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1964-L2138
[connection-controls]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1391-L1499
[control-actor-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2181-L2195
[transport-constants]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Constants.cpp#L17-L21
[listen-instance-schema]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ListenInstance.h#L10-L47
[listen-instance-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpDatagramTransport.cpp#L472-L519
[transport-control-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpDatagramTransport.cpp#L1525-L1543
[connection-auth-producer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L524-L539
[connection-auth-schema]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.h#L156-L171
[connection-auth-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendTarget.cpp#L672-L697
[connection-auth-apply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L2205-L2237
[claims-message-schema]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ClaimsMessage.h#L10-L25
[claims-completion]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L108-L172
[send-queue]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SendBuffer.cpp#L134-L278
[send-expiration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpDatagramTransport.cpp#L632-L648
[transport-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportConfig.h#L49-L80
[ssl-handshake]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L176-L332
[ssl-unix]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L379-L613
[ssl-unix-auth]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L662-L747
[ssl-win-auth]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L1069-L1120
[ssl-encode]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L1982-L2154
[ssl-decode]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L799-L855
[security-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityHeader.h#L10-L99
[client-auth-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ClientAuthHeader.h#L10-L23
[windows-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextWin.cpp#L136-L176
[windows-sspi-encrypt]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextWin.cpp#L160-L277
[windows-sspi-sign]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextWin.cpp#L406-L489
[windows-sspi-decrypt]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextWin.cpp#L312-L405
[windows-sspi-verify]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextWin.cpp#L519-L601
[claims-select]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L33-L83
[claims-processing]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L334-L375
[credentials]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityCredentials.cpp#L71-L294
[security-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/SecurityConfig.h#L141-L214
[crc-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/crc.h#L11-L62
[crc-tables]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/crc.cpp#L7-L92
[crc-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportConfig.h#L153-L157
[keepalive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1177-L1238
[com-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/comfabrictransport.test.cpp
[secure-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecureTransport.Test.cpp
