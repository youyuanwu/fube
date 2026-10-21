# FabricClient: native API, gateway wire protocol, and FabricTransport relationship

## 1. Scope and source baseline

This document traces [`FabricClient.idl`][idl] through the native COM wrapper,
client connection manager, TCP transport, and gateway implementation. It
complements [Transport.md](Transport.md), which describes the buffer-based
`IFabricTransport*` API and the shared TCP framing/security machinery.

The inspected source is the local checkout at
`/home/user1/code/reference/service-fabric`, commit
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][revision], with no local
modifications. Source links are pinned to that revision.

**Contents:** [API surface](#2-what-fabricclient-provides),
[FabricTransport relationship](#3-relationship-to-fabrictransport),
[gateway connections](#4-creation-and-gateway-connection-lifecycle),
[wire format](#5-message-layout-on-the-tcp-wire),
[worked naming request](#6-worked-example-nameexists),
[queries, forwarding, and notifications](#7-other-message-patterns),
[TLS and authorization](#8-security-and-tls),
[limits and failures](#9-timeouts-limits-and-error-semantics).

This is a **source-derived specification of the inspected implementation**, not
a promise of a stable, independently supported cluster protocol. It describes
the native client-to-gateway TCP path. It is not a specification of the HTTP
management API, the entire cluster-internal routing/replication protocol, or every
operation-specific body schema. No live packet capture, TLS negotiation, or
native test execution was performed for this document.

The central result is:

> FabricClient and FabricTransport are different higher-level protocols built on
> the same native `Transport` messaging library. FabricClient does not send COM
> calls over RPC and does not call the public FabricTransport API to reach the
> cluster.

## 2. What FabricClient provides

### 2.1 A family of local interfaces, not one remote object

`FabricClient.idl` defines a collection of `[object, local]` interfaces. An
application obtains an interface using `FabricCreateClient*` or
`FabricCreateLocalClient*`, then can use supported COM interface versions on
the same implementation. Numbered extensions add local API methods; they are
not TCP protocol version numbers.

| Interface family | Functionality |
|---|---|
| `IFabricClientSettings*` | Security credentials, keepalive, client settings and settings results. |
| `IFabricPropertyManagementClient*` | Create/delete/check names, properties, typed property values, batches and enumeration. |
| `IFabricServiceManagementClient*` | Service creation/deletion/update, descriptions, partition resolution, resolution-change callbacks and notification filters. |
| `IFabricServiceGroupManagementClient*` | Service-group descriptions and lifecycle operations. |
| `IFabricApplicationManagementClient*` | Application/type lifecycle, provisioning, upgrades and deployment-related operations. |
| `IFabricClusterManagementClient*` | Cluster/code/config upgrades, node control and cluster maintenance operations. |
| `IFabricQueryClient*` | Queries for nodes, applications, services, partitions, replicas, deployment state and related cluster information. |
| `IFabricHealthClient*` | Health reports and health queries. |
| `IFabricInfrastructureServiceClient` | Infrastructure task operations. |
| `IFabricRepairManagementClient*` | Repair-task lifecycle and repair-related operations. |
| `IFabricFaultManagementClient`, `IFabricTestManagementClient*` | Fault/test operations, progress and orchestration interfaces. |
| `IFabricNetworkManagementClient` | Network creation/deletion and network information. |
| `IFabricSecretStoreClient` | Secret-store operations. |
| Result and event interfaces | Owned local results, service/partition notifications, gateway information and connection/claims callbacks. |

The DLL exports construct a native `ClientFactory` and `ComFabricClient`, then
`QueryInterface` the requested IID. The wrapper initializes interface pointers to
native management clients backed by `FabricClientImpl`. It does not serialize
the IID or COM vtable index into a remote call. [Sources: [IDL families and
exports][idl], [DLL entry points][exports], [client factory][factory],
[COM initialization][com-init].]

### 2.2 Calls, results, and ownership

Most remote operations use `BeginX`/`EndX`, with a timeout, async callback and
async context. The wrapper validates/converts public arguments into native
types, starts the native operation, and converts native results back to public
structures or result objects.

The wire carries neither COM pointers nor the public structures' pointer
addresses, `Reserved` pointers, result-object layouts, or async contexts.
`BeginX` and `EndX` are the two sides of a local asynchronous operation, not two
separate network commands.

Not every public API operation produces exactly one request: settings can be
local, resolution can be cached, notifications have a session protocol, and
subclients can batch or split work. Applications should retain result objects
while using pointers returned by those objects; the raw wire is not an
`IFabricTransportMessage` header/body buffer interface.
[Sources: [settings ABI][settings-idl], [example COM argument/result conversion][com-nameexists],
[client subcomponents][client-lifecycle], [cached resolution][resolve-cache].]

## 3. Relationship to FabricTransport

### 3.1 Shared foundation, separate protocol adapters

```text
FabricClient.idl                          fabrictransport_.idl
IFabric*ManagementClient / QueryClient     IFabricTransportClient / Listener
            |                                         |
ComFabricClient                           ComFabricTransport* wrappers
            |                                         |
FabricClientImpl                          TcpServiceCommunication
ClientConnectionManager                   ServiceCommunicationClient/Transport
ClientServerTcpTransport                              |
            |                                         |
            +-------------------+---------------------+
                                |
                Transport::Message / MessageHeaders
                RequestReply / Demuxer
                TcpDatagramTransport / TcpConnection
                TcpFrameHeader / TcpSendBuffer / TcpReceiveBuffer
                SecurityContextSsl / SecurityContextWin
                                |
                              TCP
```

FabricClient's TCP adapter directly creates
`DatagramTransportFactory::CreateTcpClient`, a `Demuxer`, and a
`DuplexRequestReply`. FabricTransport's native communication stack separately
creates its own TCP transport and request/reply objects. The common point is
the C++ transport library, **not** `IFabricTransportClient`.
[Sources: [FabricClient TCP adapter][tcp-adapter], [FabricTransport client][ft-client],
[FabricTransport listener transport][ft-transport].]

| Property | FabricClient | Buffer-based FabricTransport |
|---|---|---|
| Purpose | Cluster management, naming, discovery, queries and notifications. | Application/service messaging with supplied payload buffers. |
| Remote peer | Cluster client-connection gateway. | An application-created transport listener. |
| Address | Gateway `host:port`; no application service-path suffix. | `host:port+servicePath`. |
| Public connection API | Lazy internal open and gateway establishment; no matching `IFabricTransportClient::BeginOpen`. | Explicit service `BeginOpen`/`EndOpen`. |
| Initial higher-level request | `PingRequest` to the gateway. | Logical `Connect` to a registered service path. |
| External request actor | Normally `NamingGateway`, numeric `5`, including forwarded management requests. | `ServiceCommunicationActor`, numeric `36`. |
| Operation selection | Action strings such as `NameExistsRequest`, `QueryRequest`, or `ForwardMessage`, plus body/forwarding headers. | Service actor/path plus application-defined header/body content. |
| Body | Operation-specific native serialized object; some specialized protocols use other payload forms. | Concatenation of opaque application-header bytes and application-body buffers. |
| Application-header split | No generic `FabricTransportMessageHeader` split. | Header ID `0x804c` records application-header byte length. |
| Operation error envelope | `ClientOperationFailure` action with a serialized error body; operation-specific results can contain additional status. | `ServiceCommunicationErrorHeader`, ID `0x8046`. |
| Reconnection | Gateway list, connection manager, failover and notification resynchronization. | The inspected logical client becomes terminal after ordinary connect failure/disconnect. |
| Push mechanism | Naming notification protocol, uncorrelated-reply marker and correlated acknowledgment. | `IFabricTransportCallbackMessageHandler::HandleOneWay` notification path. |
| TCP frame and crypto implementation | Shared native transport. | Shared native transport. |

The differences above are protocol differences, not just renamed public
interfaces. A FabricTransport listener cannot serve FabricClient requests merely
because it understands the same 12-byte frame. It would also need the gateway
protocol, schemas, authorization, and cluster-side semantics. Conversely,
FabricClient does not expose arbitrary application request buffers for a
FabricTransport service. [Sources: [gateway validation][gateway-validation],
[naming actions][naming-actions], [request body construction][request-message],
[FabricTransport message conversion][ft-message], [connection manager][connection-open].]

### 3.2 How discovery and application traffic fit together

A common application uses both:

```text
1. FabricClient -> cluster gateway
      ResolveServicePartition("fabric:/app/service", partition key)
      <- resolved partition, endpoint location(s), role/version information

2. Application chooses the endpoint and its application protocol
      -> FabricTransport listener, HTTP listener, or another service endpoint
```

Step 1 is control/discovery traffic. Step 2 is a separate data-plane connection
and is not automatically a FabricClient operation. An endpoint returned by
resolution may happen to be a FabricTransport address, but FabricClient does not
make that address a FabricTransport message destination on the caller's behalf.

The native resolution path consults the client cache; on a miss it constructs
`ResolveServiceRequest` with `ServiceResolutionMessageBody`, and reads a
`ResolvedServicePartition`. The latter includes location/partition/version data,
not a connection handle to an application service.
[Sources: [public resolution entry][resolve-entry], [cache and network
resolution][resolve-cache], [resolution request schema][resolve-body],
[resolution result schema][resolve-result].]

## 4. Creation and gateway connection lifecycle

### 4.1 Creation and local-client variants

`FabricCreateClient`, `FabricCreateClient2`, and `FabricCreateClient3` accept a
list of gateway connection strings. The later variants add service-notification
and connection-event handlers. `FabricCreateLocalClient*` instead obtains the
node's configuration and local gateway address; the fourth local variant also
accepts a client role.

Creation does not prove that any gateway was contacted or authenticated.
`FabricClientImpl::EnsureOpened` opens the native client lazily when an operation
needs it. This initializes caches, health/file-transfer/notification clients,
and the connection manager. Security can be applied to the already-created
connection manager before that open.

**Local is not necessarily in-process.** The public local-client exports create
a config-backed `FabricClientImpl`, which uses the configured
`ClientConnectionAddress` and the TCP adapter. A different, internal factory
overload accepts an `INamingMessageProcessor` and uses
`ClientServerPassThroughTransport`; that path calls the processor directly and
does not have a client-to-gateway TCP wire at that boundary.
[Sources: [creation exports][exports], [factory overloads][factory],
[initialization/lazy open][client-lifecycle], [transport selection][connection-open],
[local gateway address][local-address], [pass-through adapter][passthrough].]

### 4.2 Establishing a usable gateway

The TCP connection manager validates endpoint strings, shuffles the configured
gateway list, and tries successive addresses. It creates a TCP send target,
then sends a gateway ping using the same request/reply mechanism used for
ordinary operations.

```text
Application          FabricClient connection manager        Cluster gateway
    | first operation / EnsureOpened                               |
    |                 select gateway and ResolveTarget             |
    |                 -------- TCP connect ----------------------->|
    |                 <------ security negotiation, if enabled ---->|
    |                 <------ transport authorization/control ----->|
    |                 -------- PingRequest ----------------------->|
    |                 <------- correlated gateway reply -----------|
    |                 record target + GatewayDescription           |
    |                 flush waiting operations; raise OnConnected  |
    |                 -------- requested operation --------------->|
```

There is no FabricTransport `servicePath,Connect` request or service connection
handler in this sequence.

The initial ping has:

```text
Actor                 NamingGateway (5)
Action                "PingRequest"
FabricActivity        ping activity identifier
ClientProtocolVersion current client version, 1.2 in this revision
Timeout               remaining connection-initialization timeout
ExpectsReply          true
MessageId             generated transport request ID
Body                  empty
```

The public gateway proxy answers the ping locally with a `PingReplyMessageBody`
containing a `GatewayDescription`: gateway address, node instance and node name.
The client parses this body when the reply's protocol version says the body is
supported.

**Wire-name detail:** although a `PingReply` action constant exists,
`NamingTcpMessage::GetGatewayPingReply` in this revision actually constructs the
reply with **`PingRequestAction`**. The reply is correlated through `RelatesTo`,
not identified merely by assuming its action must equal `PingReply`.
[Sources: [address validation/list setup][gateway-addresses],
[gateway selection][gateway-select], [connection-open workflow][connection-open],
[ping operation][ping-client], [ping gateway handler][gateway-dispatch],
[ping factory][ping-factory], [ping body][ping-body], [gateway description][gateway-description].]

### 4.3 Failover, priority, and callbacks

On a retryable fault, the manager resets the current target/gateway and schedules
another establishment attempt. A successfully established client reconnects
without requiring the application to recreate the entire client. Waiting
operations are notified after gateway establishment or fail with an appropriate
connection error/timeout.

Authentication failures such as `ConnectionDenied`,
`ServerAuthenticationFailed`, and `InvalidCredentials` are treated specially:
the manager records them as non-retryable, stops the normal connection retry
workflow, and can return the cached error. Successfully updating security
clears that error and restarts establishment. This is not unbounded failover
through gateways with incorrect credentials.

Gateway events are delivered through `IFabricClientConnectionEventHandler` with
an `IFabricGatewayInformationResult`; unlike FabricTransport's event handler,
the public FabricClient `OnDisconnected` signature does not take an HRESULT.
`IFabricClientConnectionEventHandler2` adds claims-token retrieval.

Normal and high-priority operations can use **different TCP connections** to
the same target: `TcpSendTarget` selects an existing connection whose priority
matches, or creates another. Notification session setup uses the high-priority
send path. Do not model one COM client as necessarily one TCP socket.
[Sources: [fault/security handling][connection-faults], [establishment
completion][gateway-select], [non-retryable errors][nonretryable],
[event ABI][event-idl], [per-priority connections][target-priority],
[notification session request][notification-connect].]

## 5. Message layout on the TCP wire

### 5.1 Shared frame format

For an unsecured connection, or for the clear outer frames carrying security
negotiation tokens, each native message is:

```text
TCP byte stream:
    Frame0 || Frame1 || Frame2 || ...

Frame:
    TcpFrameHeader[12]
    SerializedNativeHeaders[H]
    NativeBody[B]

F = 12 + H + B
```

The frame has the same layout as FabricTransport:

| Offset | Bytes | Field | Interpretation |
|---|---|---|---|
| `0` | `4` | `frameLength_` | Total frame length, including all 12 fixed-header bytes. |
| `4` | `1` | `securityProviderMask_` | `0x00` None, `0x01` SSL, `0x02` Kerberos, `0x04` Negotiate, `0x08` Claims. |
| `5` | `1` | `frameHeaderCRC_` | Optional CRC-8 over the fixed header with both CRC fields zeroed. |
| `6` | `2` | `headerLength_` | Total native-header area length, not a public API argument size. |
| `8` | `4` | `frameBodyCRC_` | Optional CRC-32 over native headers plus native body. |

Fixed-width integers use the raw little-endian representation of the supported
builds, not network byte order. A valid frame satisfies `F >= 12 + H`.
There is no COM IID, method ordinal, HTTP header, service-path field, or protocol
version number in this fixed header.

The receive path accumulates enough bytes to parse the frame, validates it, then
constructs a `Transport::Message` around the header/body ranges. Socket
read/write boundaries and TCP packet boundaries do not define message
boundaries. Multiple messages can be batched in a send; one frame can span
several receives. TLS-protected representations are described in section 8.
[Sources: [frame declaration][frame-h], [frame validation][frame-cpp],
[send assembly][frame-send], [receive extraction][frame-receive];
see also [Transport.md, TCP framing](Transport.md#6-tcp-framing-and-serialization).]

### 5.2 Native headers and protocol identity

The native header area is a sequence of:

```text
uint16 header_id                   raw little-endian
uint16 serialized_object_length    raw little-endian, excludes this 4-byte prefix
byte   serialized_object[serialized_object_length]
```

Each object's fields use Fabric serialization, not raw packing of the IDL's
public structure. Combined native-header bytes are bounded by the 16-bit header
length. Header ID `0x8000` can mark deleted records that remain until compaction.

These IDs are particularly relevant:

| ID | Native header | FabricClient role |
|---|---|---|
| `0x8001` | `Actor` | `NamingGateway = 5` on the ordinary external request path. |
| `0x8002` | `Action` | Operation name or forwarding envelope name. |
| `0x8003` | `MessageId` | Request ID: GUID plus 32-bit index. |
| `0x8004` | `RelatesTo` | The request ID associated with a reply. |
| `0x8005` | `ExpectsReply` | Set by request/reply. |
| `0x8007` | `Fault` | Shared transport-level fault handling. |
| `0x8020` | `FabricActivity` | Activity/tracing identifier. |
| `0x8023` | `Timeout` | Remaining operation duration, represented by `TimeSpan`. |
| `0x8025` | `ClientProtocolVersion` | Gateway protocol major/minor version. |
| `0x8028` | `ForwardMessage` | Original actor and action when wrapped for the gateway. |
| `0x802a` | `QueryAddress` | Optional query-routing address. |
| `0x802c` | `ServiceRoutingAgent` | Specialized system-service routing information. |
| `0x8038` | `ClientRole` | Optional role information used by authorized internal/local-client paths; not a substitute for security authorization. |
| `0x803e` | `UncorrelatedReply` | Marks a reverse notification that is not a response to an ordinary pending client request. |
| `0x8040` | `ClientIdentity` | Notification-client identity used by gateway registration/forwarding, distinct from FabricTransport's `TcpClientIdHeader`. |
| `0x8041` | `ServerAuth` | Security negotiation/status metadata. |
| `0x8048` | `SecurityNegotiation` | Framing capability, listen-instance and receive-limit negotiation. |

This is a subset, not a fixed header order or an exhaustive list for every
subprotocol. An independently implemented decoder must also handle unknown
headers and the serializer's compatibility rules.

The connection manager adds `ClientProtocolVersionHeader` to its requests;
its current constants are **major 1, minor 2** in this source. The gateway
validation requires the header, accepts major 1 with a nonnegative minor, and
also requires activity, timeout, and `Actor::NamingGateway`. Minor 1 enables the
ping reply body and minor 2 denotes chunked-file-upload support. Neither COM
interface suffixes such as `IFabricQueryClient10` nor the type library's version
are this wire header. [Sources: [header records][header-records],
[header IDs][header-ids], [request metadata][request-base],
[protocol version][protocol-version], [gateway validation][gateway-validation],
[actor values][actors], [optional client role][request-operation],
[notification identity][notification-registration].]

### 5.3 Body serialization, not application header/body splitting

For an ordinary operation, `ClientServerRequestMessage` constructs:

```text
native_headers =
    Action + Actor + FabricActivity
    + client/gateway/request-reply metadata

native_body =
    FabricSerializableStream.WriteSerializable(operation.GetTcpMessageBody())
```

It then appends its constructed headers to the `Transport::Message`. Depending on
the operation, there can be an empty body, a native request object, a forwarding
body, or a specialized transfer representation.

There is **no generic application-header prefix** to peel off using header
`0x804c`. The entire ordinary body is interpreted using the schema selected by
the operation/action and protocol version. FabricTransport's
`application_header || application_body` convention is not a shared framing rule.

Fabric serialization includes typed metadata, object headers, scopes,
empty-value rules, and compressed primitives. Wide strings are length-counted
two-byte `WCHAR` data in the Windows-compatible little-endian build, not
null-terminated UTF-8. Integer compression is most-significant-7-bit-group-first,
not protobuf LEB128/ZigZag. An enum field is normally serialized through a signed
64-bit value; a `TimeSpan` contains signed 64-bit 100 ns ticks.

Knowing the fixed frame and header IDs alone is insufficient to encode an
operation. Its body class and `FABRIC_FIELDS_*` ordering must also be reproduced.
[Sources: [request construction][request-message], [native body writer][message-body],
[Fabric serialization][serializer], [integer compression][compressor];
see [Transport.md, serialization](Transport.md#64-fabric-serialization-inside-each-header).]

## 6. Worked example: NameExists

This example follows one public method from argument conversion to the cluster
and back. The representation below is **symbolic**, not a captured hexadecimal
packet. In protected mode it describes the message before TLS protection.

### 6.1 Public call and native body

```text
IFabricPropertyManagementClient::BeginNameExists(
    name = "fabric:/app/service",
    timeoutMilliseconds,
    callback,
    out context)
```

`ComFabricClient::NameExistsAsyncOperation` parses `FABRIC_URI` into a
`Common::NamingUri`. `FabricClientImpl::BeginNameExists` ensures the client is
opened and calls:

```text
NamingTcpMessage::GetNameExists(NamingUriMessageBody(name))
```

`NamingUriMessageBody::GetTcpMessageBody()` returns the contained `NamingUri`
itself. Thus the serialized body is **not** an extra wrapper object around a
single UTF-16 URI string.

`NamingUri` inherits `Uri`, whose serialized field order is:

```text
Uri {
    type_,
    scheme_,
    authority_,
    hostType_,
    host_,
    port_,
    path_,
    query_,
    fragment_,
    pathSegments_
}
```

For example, the scheme/path information is represented in these parsed URI
fields; the caller's pointer to the textual `FABRIC_URI` is not transmitted.
[Sources: [IDL operation][property-idl], [COM conversion][com-nameexists],
[native naming operation][naming-client], [message factory][naming-factories],
[URI body adapter][uri-body], [URI inheritance][naming-uri], [URI schema][uri-schema].]

### 6.2 Resulting request message

```text
TcpFrameHeader {
    frameLength = 12 + nativeHeaderBytes + serializedUriBytes
    securityProviderMask = configured provider mask
    headerLength = nativeHeaderBytes
    optional CRCs
}

Native headers {
    Action                "NameExistsRequest"
    Actor                 NamingGateway (5)
    FabricActivity        activity ID
    ClientProtocolVersion major = 1, minor = 2
    Timeout               remaining duration
    ExpectsReply          true
    MessageId             request ID R
}

Native body {
    Fabric-serialized Common::NamingUri / Uri
}
```

The wrapper selects the action; the connection manager adds protocol version and
timeout; `RequestReply` adds reply expectation and a message ID if absent and
registers a pending request keyed by that ID. Header order in this diagram is
explanatory, not normative.
[Sources: [action constants][naming-actions], [request metadata][request-base],
[TCP adapter request][tcp-adapter], [request/reply correlation][request-reply].]

### 6.3 Gateway processing and cluster-internal hop

The public `EntreeServiceProxy`:

1. Receives the message under the `NamingGateway` actor and queues it.
2. Validates required headers/version and checks a positive timeout.
3. Performs operation-specific role/access checks.
4. Forwards the request to `EntreeService` through its proxy-to-service IPC
   channel, since this is neither a locally handled ping nor a notification
   registration request.

The naming operation parses the name, resolves the relevant Naming Store
location, and creates a separate peer naming request. The store-bound message
gets an activity header, remaining timeout, service-instance filter and request
instance, and is routed to the owning node.

The external gateway request is therefore **not simply the identical TCP frame
being forwarded unchanged all the way to a store replica**. Internal IPC,
request IDs, destination actors, routing/filter metadata, and security channels
belong to the respective hops. The cluster-internal protocol is a separate
boundary from the client/gateway framing specified here.
[Sources: [public gateway processing][gateway-processing],
[gateway request dispatch][gateway-dispatch], [IPC forwarding][proxy-forward],
[NameExists handler][nameexists-gateway], [store routing][naming-store-route].]

### 6.4 Success and failure replies

On success, `NameExistsReplyMessageBody` contains:

```text
FABRIC_FIELDS_02(nameExists_, userServiceState_)
```

The public proxy adds `RelatesTo(R)` to the reply before sending it back.
The client's reply table uses `RelatesTo`, not a COM operation number, to locate
the pending request. The native `EndNameExists` deserializes the body and
extracts `nameExists_`; the COM `EndNameExists` returns that as `BOOLEAN`.

The successful reply body is therefore not just a raw one-byte boolean. Its
Fabric object/scoping metadata and the user-service-state field are also part
of the wire schema. The naming reply factory uses the `NameOperationReply`
action family for this result.

On a gateway/operation failure, the proxy constructs:

```text
Native headers {
    Action     "ClientOperationFailure"
    RelatesTo  R
    other reply headers
}
Native body {
    ClientOperationFailureMessageBody {
        value_    native ErrorCodeValue enum
        message_  wide error-message string
    }
}
```

The connection manager recognizes the failure action, reconstructs the error,
and the COM boundary maps that error to HRESULT. This is different from
FabricTransport's `ServiceCommunicationErrorHeader`. Lower transport faults can
also complete a request through the shared `FaultHeader` path without this
operation-level body.
[Sources: [NameExists reply schema][nameexists-reply],
[naming reply factories][naming-replies], [gateway reply/error handling][gateway-replies],
[failure body][failure-body], [client failure decoding][request-base],
[native EndNameExists][naming-client], [shared fault handling][reply-table].]

## 7. Other message patterns

### 7.1 Queries

The public query API does not serialize every `IFabricQueryClient` method's
public structure as-is. For example, native query operations build query
arguments and reach `FabricClientImpl::BeginInternalQuery`, which creates:

```text
Actor   NamingGateway (5)
Action  "QueryRequest"
Body    QueryRequestMessageBody {
            queryArgs_,
            queryName_
        }
```

That field order is intentional: the external body declares
`FABRIC_FIELDS_02(queryArgs_, queryName_)`. `QueryArgumentMap` wraps a map whose
keys themselves are serialized key objects, so it is not interchangeable with
a JSON dictionary or an arbitrary raw `map<string,string>` encoding.

There is also a different `QueryRequestMessageBodyInternal` schema using a
query enum, argument map, and query type. Do not use that internal body merely
because an operation is a query.

`QueryResult` contains result kind, a polymorphic result wrapper, query-processing
error and paging status. Even when transport/request decoding succeeds,
`EndInternalQuery` checks `QueryProcessingError`. A successful frame receive is
not necessarily a successful query.
[Sources: [native query entry/result][query-client], [query action][query-action],
[query message factory][query-message], [external/internal query bodies][query-body],
[argument map][query-args], [query result][query-result].]

### 7.2 Forwarded management operations

Some management messages originate with an actor for a system service, but are
wrapped before crossing the client/gateway connection. For a representative
application create:

```text
Before wrapping:
    Actor  CM (4)
    Action "CreateApplicationAction"
    Body   CreateApplicationMessageBody

External client/gateway request after WrapForForwarding:
    Actor  NamingGateway (5)
    Action "ForwardMessage"
    ForwardMessageHeader {
        originalActor_  CM (4)
        originalAction_ "CreateApplicationAction"
    }
    Body   same serialized CreateApplicationMessageBody schema
```

The create body includes application name, application type, application version,
application parameters and application capacity in declared field order.
`FabricClientImpl::BeginInternalForwardToService` applies the wrapper, and the
gateway uses the original actor/action information for routing and access checks.

Other routing helpers use envelopes such as `ForwardToFileStoreMessage`,
`ForwardToTvsMessage`, or service-routing-agent headers. A decoder must distinguish
the **outer gateway operation** from the **inner operation**, rather than assuming
every management request's top-level actor names its final system service.
[Sources: [cluster-manager message][cm-message], [cluster-manager actions][cm-actions],
[create operation][create-app-call], [create body schema][create-app-body],
[forward operation][forward-operation], [native forward entry][forward-entry],
[forwarding rewrite][forward-wrapper], [forward-header schema][forward-header],
[gateway access dispatch][gateway-access].]

### 7.3 Service resolution and older change callbacks

On a resolution cache miss the client sends:

```text
Action  "ResolveServiceRequest"
Body    ServiceResolutionMessageBody {
            name_,
            request_,
            bypassCache_
        }
```

The request carries partition/version information through its nested resolution
request object. Previous-result metadata helps the client determine whether a
cached answer is useful. A successful response produces a
`ResolvedServicePartition` with locations, partition information and versions.

The older `RegisterServicePartitionResolutionChangeHandler` callback interface
is distinct from notification-filter registration. The client retains separate
service-address-tracking machinery, and its configuration includes a
`ServiceChangePollInterval` for the older polling model. Do not equate every
public change callback with one unsolicited application message.
[Sources: [resolution/cache flow][resolve-cache], [resolution body][resolve-body],
[result schema][resolve-result], [client component setup][client-lifecycle],
[resolution callback ABI][event-idl], [client polling configuration][client-config].]

### 7.4 Notification sessions and reverse traffic

The notification-filter APIs create a higher-level gateway session. Session
connection uses `NotificationClientConnectionRequest` with:

```text
NotificationClientConnectionRequestBody {
    clientId_,
    generation_,
    clientVersions_,
    filters_
}
```

On gateway change the notification client can reconnect, synchronize known
versions/deletions, and re-register filters before draining pending
notifications. This ID/version synchronization is not FabricTransport's
`Connect` protocol, even though both protocols have a notion of a client.

Gateway registration associates the notification client ID with the TCP send
target using `ClientIdentityHeader`. A pushed page is sent through
`RequestReply::BeginNotification`, which adds `UncorrelatedReplyHeader` and
starts a request/reply exchange. The client's `DuplexRequestReply` recognizes
that marker and dispatches to the registered notification handler instead of
trying to match an ordinary pending request.

```text
Gateway -> Client:
    ServiceNotificationRequest
    UncorrelatedReply = present
    ExpectsReply = true
    MessageId = N
    Body = ServiceNotificationRequestBody(notification_)

Client -> Gateway:
    ServiceNotificationReply
    RelatesTo = N
    Body = ServiceNotificationReplyBody(processing status)
```

The client updates its cache, accepts/posts notifications when synchronized,
and sends a reply, using high priority. Posting application callbacks and
acknowledging notification processing are not proof that arbitrary application
work triggered by a callback has finished.

These are reverse messages on client-established transport connections, not
inbound connections to an application-created FabricTransport listener. They
have naming notification bodies, not an `IFabricTransportMessage` application
payload. [Sources: [notification session connection][notification-connect],
[connection schema][notification-connect-body], [registration identity][notification-registration],
[gateway push exchange][notification-send], [uncorrelated notification send][notification-request-reply],
[duplex receive dispatch][duplex], [client notification processing][notification-process],
[notification body][notification-body], [synchronization protocol][notification-sync].]

### 7.5 Specialized subclients

`FabricClientImpl` also owns health-reporting and file-transfer clients.
Their batching, chunking, acknowledgments and retries are not described by a
single universal "one public method equals one serialized request object" rule.
For example, native `ReportHealth` returns the result of adding a report to its
health-reporting component; that return is not a remote Health Manager reply.
Protocol minor 2 also advertises chunked-file-upload support, and client
configuration contains separate health-report send intervals and file-chunk
retry controls.

The generic frame/header/security layer is reusable for these exchanges, but
their full transfer schemas and delivery guarantees require tracing the
specific subclient. This document's request examples must not be treated as an
exhaustive file-transfer or health-reporting protocol specification.
[Sources: [subclient initialization][client-lifecycle], [protocol capabilities][protocol-version],
[health-report entry][health-report], [health/file-transfer configuration][client-config].]

## 8. Security and TLS

### 8.1 Same crypto backend, different configuration path

`IFabricClientSettings::SetSecurityCredentials` converts
`FABRIC_SECURITY_CREDENTIALS` through the same `SecuritySettings::FromPublicApi`
machinery used by FabricTransport, then applies the settings to the client
connection manager and TCP transport. The gateway constructs its server
credentials from node/security configuration.

| Credentials/provider | Meaning |
|---|---|
| None | Clear native frames; no TLS protection at this layer. |
| X509/SSL | TLS plus Service Fabric certificate authorization of the peer. |
| Windows | Kerberos/Negotiate SSPI protection on supported Windows paths; not TLS. |
| Claims | TLS-authenticated server plus a client claims token, with gateway token validation and role assignment. |

A client to a secured cluster must use the corresponding configured
credentials; the first frame's provider mask is not a general-purpose
authentication fallback or trust negotiation.
[Sources: [COM settings conversion][com-settings], [FabricClient security overrides][client-security],
[gateway credential/claims configuration][gateway-security],
[TCP security delegation][tcp-adapter];
see [Transport.md, security](Transport.md#7-security-and-tls-on-the-wire).]

### 8.2 TLS negotiation and protected wire representation

The mechanics are shared with FabricTransport:

```text
Initial TCP bytes:
    clear Fabric frame {
        actor = SecurityContext (35)
        negotiation metadata in native headers
        body = TLS handshake output/token bytes
    }

After handshake/authorization, with framing protection:
    TLS record(s) whose plaintext is
        TcpFrameHeader || SerializedNativeHeaders || NativeBody
```

TLS does **not** start at TCP byte zero. A stock HTTPS/TLS client or a
TLS-terminating proxy expecting an immediate ClientHello cannot simply substitute
for FabricClient's transport. A transparent byte-stream proxy can relay the
protocol without interpreting it.

On Unix the shared implementation uses OpenSSL memory BIOs and
`SSL_do_handshake`/`SSL_write`/`SSL_read`; on Windows it uses Schannel for SSL
and SSPI for Windows credentials. Whole-frame protection encrypts even the
12-byte fixed Fabric header, so ciphertext record length is not the frame's
plaintext `frameLength`. TLS record, TCP packet, socket read, and Fabric message
boundaries must be handled independently.

**A meaningful difference from direct FabricTransport:** on Windows,
`FabricClientImpl::SetSecurity` explicitly installs the
`FramingProtectionEnabledInClientMode` callback, whose default is true. The
direct FabricTransport client path described in Transport.md does not install
that same override and can use the generic false-by-default setting. Unix's
settings getter always enables framing protection. Negotiation metadata and
compatibility paths still matter when communicating with other versions.

The legacy Windows non-framing-protected SSL path instead leaves the fixed
frame clear and separately protects native headers/body with a
`MessageSecurity` envelope. That is a shared fallback/mode, not a different
FabricClient IDL ABI.
[Sources: [FabricClient framing callback][client-security],
[framing settings getter][framing-settings], [security defaults][security-config],
[negotiation/state transitions][connection-security], [TLS handshake backend][ssl-handshake],
[frame protection][frame-send], [legacy/protected Windows SSL encoding][ssl-encode].]

### 8.3 Authentication, claims, and operation authorization

TLS handshake completion is not the full access decision. The shared security
layer applies certificate/identity authorization and connection-status handling.
Then the gateway validates the request and performs operation-level access
checks. For example, property batch reads and writes can require different roles,
and forwarded operations can be checked using their original action.

In contrast to the bare FabricTransport listener API, the FabricClient gateway
implementation explicitly installs a claims handler and can publish claims
retrieval metadata. A claims-enabled gateway validates tokens and calls
`CompleteClientAuth` with role claims and expiration. The client can acquire
tokens through its claims-retrieval callback path; a public event handler's
version 2 interface exposes this hook.

The public `clientRole` argument of a local-client constructor and any
`ClientRoleHeader` are not general proof of admin authorization. The native
request code attaches its requested role only on its authorized-role path, and
the gateway's credential/claims/access policies still govern permissions.
[Sources: [gateway channel/claims handler][gateway-channel],
[token validation completion][gateway-claims], [gateway access checks][gateway-access],
[client role header condition][request-operation], [claims event ABI][event-idl],
[gateway security policy][gateway-security].]

### 8.4 Session expiration and protocol-version limits

FabricClient and the gateway override shared security defaults:

- Session duration comes from `FabricGateway/SessionExpiration`, default
  **24 hours** in this revision, rather than the common transport's generic
  seven-day default.
- Both install a `ReadyNewSessionBeforeExpiration` callback returning false.
  The source notes an unresolved high-priority connection refresh concern.
- FabricClient's connection/notification machinery handles subsequent gateway
  establishment and resynchronization; this is not the bare FabricTransport
  terminal-client lifecycle.

These settings do not pin a TLS version or cipher suite in `FabricClient.idl`.
The actual negotiated TLS policy still depends on the linked backend, OS/runtime
policy, certificate configuration and deployed version. Likewise, a wire
`ClientProtocolVersion` of 1.2 is an application/gateway protocol capability
number, **not TLS 1.2**.
[Sources: [client session overrides][client-security],
[gateway session overrides][gateway-security], [session default][gateway-config],
[wire protocol version][protocol-version].]

## 9. Timeouts, limits, and error semantics

### 9.1 Connection retry is not exactly-once operation execution

The connection manager's request base adds a timeout computed from the remaining
operation duration. Its send-to-gateway operation waits for a connected target
and has retry paths for connection-establishment/disconnection conditions.
The source explicitly distinguishes those retries from replaying arbitrary
application operations: idempotency/retry decisions for an operation remain
the caller's responsibility.

A remote operation can have executed when the caller sees a timeout or loses
its reply. There is no exactly-once guarantee merely because request/reply has
a `MessageId`. The gateway can also retry cluster-internal routing separately,
with its own request-instance metadata. Connection recovery, internal routing
retry, and application-level operation retry are different mechanisms.
[Sources: [send/wait/retry behavior][gateway-request-retry],
[request/reply correlation][request-reply], [internal naming retries][naming-store-route].]

### 9.2 Defaults and queue behavior

These are source defaults, not assurances about a particular cluster's
configuration:

| Setting/path | Default or behavior |
|---|---|
| Client `ConnectionInitializationTimeout` | 2 seconds per connection-initialization workflow/attempt. |
| Client TCP keepalive idle interval | 20 seconds. This delegates to transport socket keepalive, not repeated `PingRequest` application packets. |
| Client partition location cache | 100,000 entries by default; zero means no configured cache limit. |
| Older service-change polling interval | 120 seconds. |
| Naming `MaxMessageSize` | 4 MiB input setting for the shared frame-size machinery, not an exact encrypted-wire byte count. |
| Naming message content ratio | 0.75 used by size/paging estimates. |
| Gateway request queue | 5,000 items. Full enqueue logs/drops the request in the inspected public-gateway code. |
| Maximum client connections per gateway | 1,000; the accepted-connection handler resets targets beyond the configured limit. |
| Gateway security session duration | 24 hours. |

The gateway queue-overflow behavior differs from FabricTransport's service
dispatcher, which constructs a `ServiceTooBusy` reply on failed enqueue. A
FabricClient caller can instead observe its request timeout when a gateway
request is dropped at this queue.

FabricClient's TCP adapter configures its outgoing frame size and deliberately
does not set a normal incoming size limit, with a source comment that the client
trusts the authenticated server. The gateway sets incoming and outgoing limits.
Shared transport overhead expansion, pre-authorization limits, role-dependent
behavior, and secured/unsecured-path qualifications still apply; do not interpret
the 4 MiB setting as a strict universal public-result or TLS-record limit.

The connection manager disables the shared idle-connection timeout and uses
gateway establishment/fault handling to maintain its connection. A configured
keepalive interval is not the entire failure-detection deadline.
[Sources: [client defaults][client-config], [settings getters][settings-values],
[gateway defaults][gateway-config], [naming size defaults][naming-config],
[accepted-connection limit][gateway-connections], [queue overflow][gateway-channel],
[client transport limits][tcp-adapter], [gateway transport limits][gateway-validation],
[FabricTransport queue comparison][ft-dispatch].]

### 9.3 Read errors at the right layer

| Layer | How failure is observed |
|---|---|
| Public argument conversion | Immediate or async-operation HRESULT, possibly before network activity. |
| TCP/security/request table | Connection/authentication error, timeout or shared transport fault. |
| Gateway validation/access/operation | `ClientOperationFailure` action and serialized native error body. |
| Operation result | Additional body-specific status, such as `QueryResult::QueryProcessingError`. |
| Public result conversion | HRESULT from native result handling; public result pointers are only valid according to their local object lifetime. |

Reply correlation is via `RelatesTo`, independent of operation success. A
properly framed reply is not evidence that authorization passed, a query
succeeded, an application endpoint was contacted, or a management change became
durably complete beyond that operation's own semantics.

For an independent decoder/client, validate lengths before allocation, tolerate
TCP fragmentation and header compatibility, implement the exact operation
schemas, and test against the targeted runtime/security mode. This document
does not turn the internal protocol into a stable public alternative to the
supported native client API.

## 10. Implementation map

Paths below are relative to the Service Fabric checkout:

| Concern | Source |
|---|---|
| Local ABI and exported constructors | `src/prod/src/idl/public/FabricClient.idl`; `src/prod/src/retail/native/FabricClient/dll/dllmain.cpp` |
| Public argument conversion/results | `src/prod/src/api/wrappers/ComFabricClient.cpp` and result wrappers |
| Native API operations and subclients | `src/prod/src/client/FabricClientImpl.cpp`; `FabricClientImpl.*AsyncOperation.cpp` |
| Gateway selection, ping, failover and request metadata | `src/prod/src/client/ClientConnectionManager.cpp` |
| TCP adapter and abstract messages | `src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp`, `ClientServerRequestMessage.h`, `ClientServerReplyMessage.h` |
| Operation actions and body schemas | `src/prod/src/client/ClientServerTransport/NamingTcpMessage.*`, `QueryTcpMessage.*`, `*MessageBody.h` |
| Public gateway TCP/security/access/dispatch | `src/prod/src/Naming/EntreeServiceProxy.cpp`, `EntreeServiceProxy.RequestAsyncOperationBase.cpp` |
| Gateway process security policy | `src/prod/src/FabricGateway/FabricGateway.cpp` |
| Proxy-to-node IPC and naming routing | `src/prod/src/Naming/ProxyToEntreeServiceIpcChannel.cpp`, `EntreeService.*AsyncOperation.cpp` |
| Forwarding envelopes | `src/prod/src/systemservices/ServiceRoutingAgentMessage.cpp`; `src/prod/src/Transport/ForwardMessageHeader.h` |
| Resolution cache and notification protocol | `src/prod/src/client/LruClientCacheManager.*`, `ServiceNotificationClient.cpp`; `src/prod/src/Naming/ServiceNotificationManagerProxy.cpp` |
| Shared wire framing, correlation and TLS | `src/prod/src/Transport/TcpFrameHeader.*`, `TcpSendBuffer.cpp`, `TcpReceiveBuffer.cpp`, `requestreply.cpp`, `DuplexRequestReply.cpp`, `SecurityContext*` |
| Companion specification | [Transport.md](Transport.md) |

<!-- Source references are pinned to the inspected checkout revision. -->
[revision]: https://github.com/microsoft/service-fabric/commit/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricClient.idl
[settings-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricClient.idl#L438-L456
[property-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricClient.idl#L459-L485
[event-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricClient.idl#L2425-L2459
[exports]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/retail/native/FabricClient/dll/dllmain.cpp#L41-L255
[factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientFactory.cpp#L34-L138
[com-init]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricClient.cpp#L14774-L14858
[com-settings]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricClient.cpp#L14863-L14898
[com-nameexists]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricClient.cpp#L4330-L4400
[client-lifecycle]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L40-L367
[local-address]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L11881-L11920
[connection-open]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L735-L884
[connection-faults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L1039-L1181
[gateway-addresses]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L960-L1032
[gateway-select]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L1183-L1407
[gateway-request-retry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L215-L429
[ping-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L435-L530
[request-base]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L34-L209
[nonretryable]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConnectionManager.cpp#L1748-L1791
[tcp-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerTcpTransport.cpp#L21-L249
[passthrough]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerPassThroughTransport.cpp#L19-L90
[request-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientServerRequestMessage.h#L55-L108
[request-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.RequestReplyAsyncOperation.cpp#L104-L154
[naming-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L6881-L7036
[naming-actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingTcpMessage.cpp#L13-L69
[naming-factories]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingTcpMessage.h#L210-L225
[ping-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingTcpMessage.h#L91-L99
[ping-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/PingReplyMessageBody.h
[gateway-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/GatewayDescription.h#L10-L77
[protocol-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientProtocolVersionHeader.h#L10-L82
[gateway-processing]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L394-L464
[gateway-replies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L466-L535
[gateway-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.RequestAsyncOperationBase.cpp#L37-L83
[gateway-channel]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L1023-L1145
[gateway-access]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L721-L902
[gateway-claims]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L546-L626
[gateway-connections]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L158-L192
[gateway-validation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/NamingUtility.cpp#L115-L231
[proxy-forward]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeServiceProxy.cpp#L674-L695
[uri-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NamingUriMessageBody.h
[naming-uri]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/NamingUri.h#L10-L14
[uri-schema]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Uri.h#L87-L120
[nameexists-gateway]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeService.NameExistsAsyncOperation.cpp#L25-L59
[naming-store-route]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeService.NamingRequestAsyncOperationBase.cpp#L42-L171
[nameexists-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NameExistsReplyMessageBody.h
[naming-replies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/NamingMessage.h#L108-L127
[failure-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClientOperationFailureMessageBody.h
[query-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L1450-L1494
[query-action]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/QueryTcpMessage.cpp#L15-L17
[query-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/QueryTcpMessage.h
[query-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/Query/QueryRequestMessageBody.h
[query-args]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryArgumentMap.h#L73-L105
[query-result]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/QueryResult.h#L140-L189
[cm-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClusterManagerTcpMessage.h#L43-L66
[cm-actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ClusterManagerTcpMessage.cpp#L13-L40
[create-app-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/CreateApplicationMessageBody.h
[create-app-call]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L3894-L3985
[forward-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.ForwardToServiceAsyncOperation.cpp#L35-L65
[forward-entry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L10694-L10719
[forward-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/systemservices/ServiceRoutingAgentMessage.cpp#L144-L189
[forward-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ForwardMessageHeader.h
[resolve-entry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L772-L869
[resolve-cache]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/LruClientCacheManager.ResolveServiceAsyncOperation.cpp#L51-L245
[resolve-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ServiceResolutionMessageBody.h
[resolve-result]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ResolvedServicePartition.h#L10-L114
[notification-connect]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L637-L664
[notification-connect-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NotificationClientConnectionRequestBody.h
[notification-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManagerProxy.cpp#L82-L120
[notification-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManagerProxy.cpp#L401-L520
[notification-request-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L261-L280
[duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L36-L77
[notification-process]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L1205-L1312
[notification-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ServiceNotificationRequestBody.h
[notification-sync]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L23-L62
[client-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L11123-L11148
[gateway-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/FabricGateway/FabricGateway.cpp#L530-L612
[client-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientConfig.h
[health-report]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L11216-L11223
[settings-values]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientInternalSettings.cpp#L101-L160
[naming-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceModelConfig.h#L69-L83
[gateway-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceModelConfig.h#L153-L158
[target-priority]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendTarget.cpp#L716-L791
[message-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Message.h#L290-L327
[request-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L58-L127
[reply-table]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/RequestTable.cpp#L46-L73
[frame-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.h#L10-L41
[frame-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpFrameHeader.cpp#L14-L118
[frame-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendBuffer.cpp#L88-L278
[frame-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpReceiveBuffer.cpp#L18-L241
[header-records]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaders.cpp#L163-L186
[header-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaderId.h#L21-L155
[actors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L17-L98
[serializer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/FabricSerializableStream.cpp#L320-L425
[compressor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/src/ByteCompressor.h#L10-L161
[framing-settings]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L1550-L1572
[security-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/SecurityConfig.h#L191-L201
[connection-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1964-L2138
[ssl-handshake]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L176-L332
[ssl-encode]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L1982-L2154
[ft-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationClient.cpp#L114-L246
[ft-transport]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceCommunicationTransport.cpp#L611-L648
[ft-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComFabricTransportMessage.cpp#L52-L122
[ft-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Communication/TcpServiceCommunication/ServiceMethodCallDispatcher.cpp#L209-L253
