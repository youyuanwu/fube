# FabricRuntime architecture

## 1. Scope and source baseline

FabricRuntime is the **application-side service-hosting runtime**, not the
Service Fabric cluster as a whole and not FabricTransport. Its public local
interfaces are the entry into a larger application-host, lifecycle and
replication implementation.

This is source research of the clean, explicitly authorized local checkout
`/home/user1/code/reference/service-fabric`, commit
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][native-revision],
inspected 2026-10-25. Native paths below are relative to `src/prod/src/`;
source links pin that revision. The accompanying
[pure Rust design](RustFabricRuntime.md) separates proposals from native facts.

**Evidence limit:** no replacement runtime was implemented, no native
FabricRuntime tests were executed, and no runtime IPC/TLS exchange was captured
or qualified for these documents. The repository's existing clear-TCP
FabricClient qualification is not FabricRuntime evidence. Source behavior here
must be checked against the actual deployed build; the 11.8.121.4 onebox is not
assumed to be built from this source revision.

Subsequent implementation work qualified a narrow **node-side** standalone
stateless test host against a real native app; see the separate
[MVP scope and execution evidence](RustTestHost.md#2-implemented-mvp-and-usage).
That does not implement the proposed pure-Rust application runtime.

## 2. Distinct boundaries

```text
Application process                              Existing native node

service factories / service instances
       |
local runtime API
       |
ApplicationHost + FabricRuntime + RAP  <--- IPC ---> Hosting + RuntimeManager + RA
       |
optional replication/storage engine  <----------> peer replica engines
       |
application-owned listener           <----------> application clients
```

These channels have different identities, authorization and recovery semantics:

| Boundary | Purpose | What it is not |
|---|---|---|
| Local runtime API | Factory registration, instance/replica callbacks and partition services | COM pointers marshaled to the node |
| Node/application-host IPC | Host/runtime/type registration and lifecycle/control/report traffic | FabricClient requests to NamingGateway on port 19000 |
| Replica peer transport | Copy and replication for the selected replication engine | All replicated data proxied through host IPC |
| Application listener | The service's own RPC protocol and returned endpoint address | An automatically provided FabricTransport listener |

The repository's `kvmap` illustrates the split: it creates a runtime and
registers a factory, obtains a native replicator through its partition, and
separately starts a tonic application server.
[Repository entry point][sample-main]; [replica open and listener][sample-replica].

## 3. Public API, exports and local ownership

### 3.1 IDL is an in-process ABI

[`FabricRuntime.idl`][public-idl] declares local `IUnknown`-based interfaces.
The DLL exports delegate to Hosting implementations; factories remain local
objects, and the native factory proxy calls `CreateInstance` directly.
Implementing callback vtables while still loading `FabricRuntime` is therefore
a Rust **binding**, not a runtime replacement. Conversely, a new Rust-native
service API can speak node protocols without reproducing COM.
[Export-to-container delegation][runtime-export]; [direct factory call][factory-proxy].

`FabricCreateRuntime(riid, output)` is a convenience wrapper over asynchronous
creation. It does not take a service URI or runtime ID. A process-wide
`ApplicationHostContainer` shares application-host initialization between API
calls, but creation produces a new runtime identity within that host.
The runtime, activation-context, node-context and activator getters share this
container while enforcing different host-mode eligibility rules.
[Create/runtime selection][runtime-create]; [host class selection][host-selection].

| Surface | Native role and boundary |
|---|---|
| `IFabricRuntime` | Register stateless/stateful factories and service-group factories; create a group builder |
| Stateless factory/instance | Factory receives type/name, opaque initialization bytes, partition ID and instance ID; open returns a service-address string; close and abort are separate |
| Stateful factory/replica | Open returns a replicator; role change returns a service-address string; those are different outputs |
| Partition interfaces | Local partition identity, load/fault/move-cost/health reporting; stateful access-status and replicator construction |
| Activation/packages | Deployment identity, directories, resolved endpoints, package/configuration access and package-change handlers |
| Node context | Node name/type/address/ID/instance and logical directory information |
| Code-package activator | Activate/deactivate/abort other code packages and receive events, only in an eligible designated activator host |
| Service groups | Member-factory composition and local `ResolveMember`; additional runtime scope beyond ordinary factories |
| Replication APIs | Failover-facing replicator controls, service-facing state replication, state providers, data/operation streams and acknowledgements |
| KVS/transaction APIs | Store factory/export family, transactions, enumeration, backup/restore and callbacks; not supplied merely by registering a factory |

Contract locations: [factories/lifecycle/partitions][service-idl],
[classic replication][replication-idl], [activation/packages][context-idl],
[KVS][kvs-idl], [node/activator][node-activator-idl].

Private [`FabricRuntime_.idl`][internal-idl] adds multi-code-package host
callbacks, internal partition/transactional-replicator interfaces, batching,
query/status and other extensions. Reliable Collections also has a separate C
API table export. Public runtime IDL is not the complete native dependency
surface.

### 3.2 Versioning, completion and ownership

Interfaces evolve through additional IIDs and inherited versions, for example
activation context through v6 and partition interfaces through v3. Public
settings use flags and `Reserved` extension chains. Neither typelib version
`1.0` nor the presence of an exported symbol proves all capabilities or platform
support. These ABI extensions are distinct from Fabric serialization's
versioned object scopes. [Interface versions][context-idl];
[replicator settings extensions][settings-extensions].

Begin/End may complete synchronously; a callback can occur before Begin returns.
The common async context retains its callback/root, and wrappers retain input
interfaces for outstanding work. Returned interfaces/string-result objects are
owned references, while raw context/package getter pointers are backed by the
owning object. Initialization-data length/pointer is not an ownership transfer.
[Async start/completion][async-context]; [factory ownership][runtime-wrapper];
[instance wrapper][instance-wrapper]; [context getters][context-wrapper].

Cancellation is advisory, not rollback or preemption. Stateless open/close
proxies explicitly retain the user operation until actual completion rather
than automatically completing on cancellation. Releasing the runtime wrapper
starts asynchronous unregistration; it is not a synchronous guarantee that the
node has removed all registrations. [COM cancellation][async-proxy];
[runtime destruction][runtime-destruction].

## 4. Application host, bootstrap and registration

### 4.1 Host variants and environment

The node selects an application-host proxy for its entry point. Exe/container
hosts normally use the activated single-code-package host; DLL hosting uses a
multi-code-package host, and eligible guest services can be hosted in-process.
A code-package activator is a host role, not a separate service factory kind.
[Node host selection][host-proxy-selection].

For an activated executable, bootstrap includes:

| Environment group | Meaning |
|---|---|
| `Fabric_ApplicationHostId`, `Fabric_ApplicationHostType` | Host identity and mode; both required together |
| `Fabric_IsContainerHost`, `Fabric_IsCodePackageActivatorHost` | Host roles; process ID is taken from the actual process, not environment |
| `Fabric_RuntimeConnectionAddress`, `Fabric_RuntimeSslConnectionAddress`, `Fabric_RuntimeConnectionUseSsl` | Ordinary/TLS destinations and selection; container mode selects TLS |
| `Fabric_RuntimeSslConnectionCert*` | Client credential material or Unix certificate-file path and expected server thumbprint; these are secrets/trust inputs, not values to log |
| `Fabric_CodePackageName`, `Fabric_ServicePackageName`, `Fabric_ApplicationId`, `Fabric_ApplicationName` | Nested package/application identity |
| `Fabric_CodePackageInstanceSeqNum`, `Fabric_ServicePackageInstanceSeqNum`, `Fabric_ServicePackageVersionInstance` | Package incarnation/version context |
| `Fabric_ServicePackageActivationGuid`, `Fabric_ServicePackageActivationId` | Activation scope and public activation identity; not interchangeable with host/runtime IDs |

Sources: [host context parsing][host-context], [address/certificate selection][host-environment],
[code-package context][code-context], [nested package/activation identity][package-identity].
Full nested identifiers have their own parsers; splitting a display string or
defaulting partially missing identity is not equivalent.

When both host ID and type are absent, native code chooses a **NonActivated**
host and reads the runtime address from node configuration. A partially supplied
host context is invalid. A Rust executable started outside an SF deployment
cannot assume it has activated-host context.
Runtime-access-removal policy suppresses environment injection; this is not a
policy that a replacement runtime should bypass.
[Missing-context handling][host-context-selection]; [environment injection policy][host-injection].

Multi-code-package hosting is a different protocol: register the local
`IFabricCodePackageHost`, then receive activation requests with per-package
context and runtimes. Ordinary `FabricCreateRuntime` rejects that mode.
Direct activation-context retrieval also rejects nonactivated, multicode-package
and ad-hoc contexts. [Host selection/eligibility][host-selection];
[multi-package activation][multi-activation].

### 4.2 Ordered bootstrap

```text
SF starts executable with host/package environment
  -> process-shared ApplicationHost initialization
  -> configure/open IPC and app-host handler; open health clients
  -> StartRegisterApplicationHostRequest
  <- node identity, process/lease data, deployment paths and settings
  -> initialize applicable process/lease monitoring and node context
  -> FinishRegisterApplicationHostRequest
  <- success
  -> open runtime table, replicator factory, RAP, KTL/log/transactional components
  -> single-code-package activation context reads deployed artifacts
  -> create a new local tentative runtime
  -> RegisterFabricRuntimeRequest
  <- success; validate local runtime and expose its wrapper
  -> add tentative local factory; RegisterServiceTypeRequest
  <- success/idempotent same-runtime result; validate factory
  -> node can dispatch service lifecycle work to this host
```

The native open path initializes replication/KTL/logging components even before
an ordinary application factory is used. A stateless Rust subset need not copy
unused engines, but must replace the hosting/lifecycle behavior they surround.
Host-registration completion alone does not prove those later components opened.
[Host open sequence][host-open]; [single-package context][single-context];
[runtime registration][runtime-registration]; [factory registration][factory-registration].

Nonactivated bootstrap additionally exchanges Fabric process SID/PID, arranges
process-monitor access and monitors the node process. Activated hosts skip that
particular process monitor. [Nonactivated setup][host-nonactivated].

### 4.3 Hosting messages and validation

Representative bodies below give **serialized field order**, not a complete
recursive wire specification:

| App to node actor/action | Body / reply |
|---|---|
| 7 / `StartRegisterApplicationHostRequest` | `(hostId, hostType, PID, timeout, isContainer, isActivator)`; reply has node/lease/context data |
| 7 / `FinishRegisterApplicationHostRequest` | `(hostId)`; reply `ErrorCode` |
| 7 / `UnregisterApplicationHostRequest` | `(ActivityDescription, hostId)`; reply `ErrorCode` |
| 9 / `RegisterFabricRuntimeRequest` | `(FabricRuntimeContext)`; reply `ErrorCode` |
| 9 / `UnregisterFabricRuntimeRequest` | `(runtimeId)`; reply `ErrorCode` |
| 9 / `RegisterServiceTypeRequest` | `(FabricRuntimeContext, ServiceTypeInstanceIdentifier)`; reply `ErrorCode`, explicit `RegisterServiceTypeReply` action |
| 15 / no action, empty body | Lease TTL poll; reply `LeaseReply(TimeSpan)` |

The start reply has 17 fields: node ID, node process ID, numeric lease handle,
lease duration, error, node instance, node name/type, client connection address,
deployment directory, node IP/FQDN, node work folder, initial lease TTL, logical
application/node directory maps and two shared-log-settings objects. Even a
stateless decoder must correctly consume or safely skip the nested settings.
The numeric lease handle is serialized data, not OS handle transfer.
[Start request][start-request]; [start reply][start-reply];
[runtime/type schemas][registration-schemas]; [unregister schema][unregister-host-schema];
[lease poll][lease-monitor].

`FabricRuntimeContext` contains runtime ID, application-host context and
code-package context. `ServiceTypeInstanceIdentifier` includes the service
package's activation-scoped identity plus type name. The factory pointer and
the stateful/stateless factory choice stay local: both factory kinds use the
same remote type-registration body.
[Runtime context][runtime-context]; [type identifier][type-identifier];
[factory registration][factory-registration].

Node registration is not a single "connected" bit:

- Host registration is in-progress until finish, with a finish timer using the
  start request's remaining overall timeout; nonactivated hosts add a monitored
  intermediate state.
- Runtime registration is tentative until its host is confirmed registered.
  App and node each maintain their own tentative/validated table state.
- Service-type registration checks valid runtime/host/activation context.
  Non-ad-hoc types must already be in the manifest-derived type state map.
  Same-runtime registration has distinct idempotent handling from registration
  to another runtime.

Sources: [host manager registration][host-register-manager],
[runtime manager][runtime-manager], [type-state admission][type-state].
Most hosting replies are body plus transport correlation; do not require an
invented action such as `StartRegisterApplicationHostReply` when the sender does
not add one.
The finish handler replies success without checking the state-transition
method's return; runtime registration independently requires a completed host.
Do not replace that later validation with trust in a finish reply alone.
[Finish handler][host-finish].

Runtime/host/package closure invalidates node-side runtime/type registrations.
Package-close cleanup matches package instance sequence rather than deleting
all future incarnations by name. There is no separate
unregister-service-type action in the runtime manager's dispatcher.
[Runtime manager dispatch/cleanup][runtime-manager]; [incarnation-scoped table][runtime-table].

Source defaults are 120 seconds for runtime creation, factory registration and
several context/host operations, with a 30-second hosting request timeout.
Individual requests usually cap that timeout by remaining overall budget.
The node's service-type-registration timeout is another package-level policy,
not the transport timeout. [Hosting configuration][hosting-config].

## 5. IPC and message routing

### 5.1 Physical transport and reverse route

Despite the name IPC, this path uses `IpcClient`/`IpcServer` over the native
**TCP datagram transport**, not Windows COM RPC, an HTTP endpoint or a Unix-domain
socket. The factory can substitute an in-memory test transport or an unreliable
wrapper through configuration; those are not the normal TCP deployment path.
The server requires a loopback address for its local unit and can have a
separate TLS unit. [Client creation][ipc-client]; [server creation and
open][ipc-server]; [transport factory][datagram-factory].

The application initiates the connection. The server records
`client ID -> (send target, process ID)` from incoming IPC traffic and can send
requests or one-way messages back on that route. A second independently
listening application port is not required for node lifecycle callbacks.
Reconnect traffic refreshes the route. [Client table and dispatch][ipc-server];
[server reverse send][ipc-server-send].

`IpcHeader`, header ID **`0x800b`**, serializes:

```text
native object {
    from:          native UTF-16 string
    fromProcessId: DWORD
}
```

The client adds it in `OnSending`, used by client requests and one-way sends.
The demultiplexer requires it when constructing a receive context for an
anonymous/client send target; messages from the server do not need that header.
Correlated replies sent directly through `ReceiverContext::Reply` instead gain
`RelatesTo`; they do not pass through `IpcClient::OnSending`.
This identity header must not be confused with authenticated OS peer credentials.
[Header schema][ipc-header]; [header IDs][header-ids]; [client send path][ipc-client-send];
[receive-context rule][ipc-demux]; [reply path][receiver-context].

### 5.2 Native envelope, not IDL marshaling

On a clear channel, messages use the shared native framing:

```text
12-byte Fabric TCP fixed header
    + ordered native message-header records
    + Fabric-serialized operation body
```

The fixed header contains frame/header lengths, a security-provider mask and
CRC fields. Each message-header record has a numeric ID, length and serialized
value. Nested body types follow `FABRIC_FIELDS_*`, not C++ memory layout, COM
vtable layout, JSON property names or public IDL parameter order.
See [the transport specification](../transport/Transport.md#6-tcp-framing-and-serialization)
for byte-level framing and primitive serialization.

Important actor IDs in this path are:

| Actor | ID | Role |
|---|---|---|
| ApplicationHostManager | 7 | Node-side application-host management |
| ApplicationHost | 8 | Application-host control |
| FabricRuntimeManager | 9 | Runtime/service-type management |
| RA | 12 | Both node Reconfiguration Agent and app-host RAP lifecycle protocol |
| RS | 13 | Separate reliability protocol; not the RAP lifecycle actor |
| Ipc | 21 | IPC reconnect/control |
| SecurityContext | 35 | Native security negotiation |

These values come from [the actor enumeration][actors]. A role in this table
does not mean all messages to that component use one transport request/reply
pattern; sender and receiver determine the semantics.

Demux routes a message with no `RelatesTo` and no uncorrelated-reply flag by actor.
Otherwise it invokes the reply handler. The ordinary request path adds
`ExpectsReply`, assigns a message ID when needed, and inserts a request-table
entry before sending. A correlated reply adds `RelatesTo` identifying that
request. A protocol's separate `...Reply` action sent one-way is a different
mechanism. [Demux implementation][demux]; [request table][requestreply];
[correlated reply][receiver-context].

### 5.3 Lifetime and limits

Native IPC disables connection idle timeout and secure-session expiration
because the server cannot originate a fresh connection back to the client.
It enables configured keepalive, retries disconnects through a timer, and may
exit the process after the configured disconnect threshold. Reconnect sends
an actor-Ipc message, not a replay of all application operations.
[Client lifetime/reconnect][ipc-client].

Inspected source defaults include a 16 MiB IPC message limit, a 3-second
reconnect delay, disconnect limit 100 and a 5-second keepalive idle interval.
They are configuration defaults, not measured production values or permission
to accept unbounded nested allocations. [Transport configuration][ipc-config].

## 6. Service lifecycle and the RAP

### 6.1 RAP is application-side control execution

The Reconfiguration Agent Proxy registers **actor RA (12)** on the application's
IPC client. Node RA sends to the registered **host ID**; the request body selects
the runtime and service unit. RAP queues work, finds/creates a failover-unit
proxy and selects an ordered action list. Independent service units can execute
concurrently; per-unit action/API compatibility controls overlap.
[RA-to-host send][ra-send]; [RAP dispatch][rap-dispatch];
[action-list admission][action-admission].

Ordinary lifecycle commands and their semantic replies are **one-way RA
messages**. For example, `ReplicaOpenReply` is a fresh RA/action message sent
through `IpcClient::SendOneWay`, not a reply correlated by `RelatesTo`.
`RAPQuery` is an explicit correlated-reply exception.
This distinction makes a gateway-only request/reply driver insufficient.
[Message construction][ra-message]; [RAP reply paths][rap-replies].

| Node to RAP | Completion / purpose |
|---|---|
| `ReplicaOpen`, `ReplicaClose` | Corresponding one-way replies; close flags include abort/drop |
| `StatefulServiceReopen` | Persisted-replica reopening |
| `UpdateConfiguration` | Role/configuration/catch-up/reconfiguration transitions |
| `ReplicatorBuildIdleReplica`, `ReplicatorRemoveIdleReplica` | Secondary build/removal |
| `ReplicatorGetStatus`, `ReplicatorUpdateEpochAndGetStatus` | Replicator progress and epoch work |
| `CancelCatchupReplicaSet` | Cancel/drain catch-up work |
| `ProxyUpdateServiceDescription` | Versioned service-description update |
| `RAPQuery` | Correlated query reply |
| `ProxyReplicaEndpointUpdatedReply`, `ReadWriteStatusRevokedNotificationReply` | Acknowledge RAP-originated retrying notifications |

Definitions and dispatch: [RA message actions][ra-message], [RAP dispatcher][rap-dispatch].
These are a protocol map, not an assertion that every action applies to a
stateless instance.

The common request body is
`(runtimeId, failoverUnitDescription, localReplica, remoteReplicas, serviceDescription, flags)`.
The common reply is
`(failoverUnitDescription, localReplica, remoteReplicas, proxyErrorCode, flags)`.
Nested descriptions carry partition/consistency-unit identity, epochs, replica
and **instance** IDs, roles, LSNs, package-version instance, service/replication
addresses and service settings. `ProxyErrorCode` includes error, text and API
description. Update/query replies have their own schemas.
[Request/reply schema][proxy-request]; [reply/error schema][proxy-reply];
[replica description][replica-description].

### 6.2 Stateless path

Native action lists are:

```text
open:   OpenPartition -> OpenInstance
close:  CloseInstance -> ClosePartition
abort:  AbortInstance -> ClosePartition
```

No replicator is opened for this path. Partition construction supplies singleton,
int64-range or named information. Open finds the runtime's local factory,
creates the instance with native IDs/initialization bytes, invokes user
Begin/EndOpen and records the returned endpoint string in `ReplicaOpenReply`.
The runtime does not start an application listener on the user's behalf.
[Action lists][action-lists]; [instance open][instance-open].

Close calls user Begin/EndClose; abort calls synchronous user Abort. Failed
graceful close can lead to abort/cleanup, after which the protocol reports the
unit closed: user-close failure and a successful terminal close acknowledgement
are not contradictory. Partition invalidation prevents subsequent calls from
using the disposed unit.
[Close/abort handling][rap-close]; [partition invalidation][stateless-partition].

### 6.3 Stateful order, identity and cancellation

Stateful service open returns the failover-facing replicator. RAP then opens
and drives that object separately from the service replica. Native orders
include replicator role before service role; graceful close closes replicator
before service and partition. Promotion/demotion additionally coordinate epoch,
catch-up/quorum, access revocation, data loss and reconfiguration completion.
Persisted drop can require opening old state and moving roles to `None` before
cleanup; it is not simply removing an entry from a map.
[Stateful action sequences][action-lists]; [returned replicator][replica-open].

Stale admission checks current epoch and replica instance, then revalidates
under the service-unit lock. Same-instance duplicate stateless open can return
the saved endpoint; old-instance work is dropped or rejected according to the
specific action/state. Incompatible work may deliberately receive no reply so
RA retries it. A universal "always return an error" handler would not reproduce
these semantics. [Context fencing][fup-context]; [open/close admission][rap-open-admission].

Open/close cancellation signals the user's async context but retains callbacks
until actual completion. Shutdown stops dispatch, cancels incompatible work and
drains action lists before disposing units. RAP's close entry point ignores its
timeout argument, and slow-API thresholds are not proof of preemptive completion.
This research does not establish a whole-process bounded shutdown guarantee.
[Cancellation][async-proxy]; [RAP close/drain][rap-drain].

## 7. Activation context and partition services

### 7.1 Deployment files, not just bootstrap environment

Activation context reads the deployed application package, current service
package, corresponding service manifest, **generated endpoint descriptions**
and configuration Settings plus overrides. Node EnvironmentManager writes
resolved/allocated endpoint data; reading only manifest port declarations loses
that information. Startup registration supplies deployment/node directories,
not an entire ready-made activation-context object.
[Context file loading][activation-files]; [endpoint file writer][endpoint-writer].

Package upgrades publish the current package file and send
`UpdateCodePackageContextRequest` to the application-host reverse route.
The host updates runtime contexts, rereads deployment state, computes code/config/
data package additions/removals/modifications, swaps internal state and calls
registered handlers with old/new package objects. Handler snapshots and object
ownership matter when unregister races delivery.
[Context update dispatch][context-update]; [package callbacks][package-update].

Do not infer an endpoint-change callback or universally refreshed public
endpoint list: the inspected update path swaps internal package/manifest/
settings state but does not rebuild the endpoint list initially exposed by
`EnsureData`. Host/package versions and exposed snapshots must be documented
per surface. [Initialization/update distinction][activation-files];
[package state swap][package-update].

### 7.2 Local capabilities and actual reporting routes

Partition objects are local, lifetime-bound capabilities with weak links to
their service unit. They do not marshal a partition COM object to the node.

| Operation | Actual path |
|---|---|
| Partition information | Local singleton/range/name information |
| Load | Validate/aggregate metrics, then periodic one-way RA `RAPReportLoad` |
| Move cost | Special metric on the load-report path, not an imperative relocation request |
| Fault | Retry-aware one-way RA `RAReportFault`, scoped to replica/instance and fault state |
| Instance/replica/partition health | Health client batches/reports through correlated IPC to RA, which forwards to Health Manager |
| Stateful read/write access | Local computed status plus host-lease check, not a node round trip per getter |

Sources: [partition implementation][stateless-partition], [load aggregation][load-reports],
[fault/health handoff][partition-reports], [IPC health client][ipc-health],
[stateful access getters][stateful-access].

Access is not simply `role == Primary`: promotion, catch-up, demotion, previous/
current configuration quorum and minimum replica count affect permissions.
Expired lease changes otherwise non-`NotPrimary` status to `TryAgain`.
An application using these APIs must honor their result; it is not automatic
interception of arbitrary application reads/writes.
[Access calculator][access-calculator]; [lease-aware getters][stateful-access].

## 8. Replication and storage are separate layers

### 8.1 Custom-replicator seam

Stateful open returns a local replicator; it need not come from
`partition.CreateReplicator`. RAP requires the failover-facing replicator and
primary-replicator contracts: lifecycle, role/epoch, progress/catch-up capability,
data loss, replica-set configuration, quorum wait, build and remove.
Optional query/catch-up marker interfaces are probed separately.
This permits custom application replication, but the node does not provide
its data plane or persistence for it.
[Returned interface checks][replica-open]; [replicator proxy operations][replicator-proxy].

### 8.2 Native classic engine

`partition.CreateReplicator(stateProvider, settings)` invokes the native
replicator factory and returns both failover-facing `IFabricReplicator` and
service-facing `IFabricStateReplicator`. The service's state provider still
supplies epoch/committed-LSN/data-loss/copy-context/copy-state behavior.
Secondaries consume copy and replication operation streams and acknowledge
according to the configured semantics.
[Partition construction][create-replicator]; [state-provider adapter][state-provider];
[operation/stream APIs][replication-idl].

The engine uses a **separate direct TCP replication transport**. Its endpoint
identity includes partition ID, replica ID and an incarnation GUID. Routing uses
`ReplicationFromHeader` and `ReplicationActorHeader`, not the RA actor.
Actions include replication/copy operations, acknowledgements, start-copy,
copy-context exchange and ack requests. Payloads can be segmented operation
buffers with metadata in headers or the body; they are not simply query-style
Fabric objects containing user values.
[Replication transport/addressing][replication-transport];
[replication endpoint identity][replication-endpoint]; [actual dispatch][replication-dispatch].

Epoch validation, copy boundaries, acknowledgements, LSN/quorum progress, retries
and queue policies belong to this engine. Replacing host IPC does not replace
it, and a custom all-Rust replication protocol is not automatically compatible
with native peers.

### 8.3 Transactional replicator, KVS and Reliable Collections

The inspected transactional replicator composes the classic engine with logging
replication and a StateManager. Its factory takes a KTL system and log manager;
it serializes logical log records over the underlying state replicator.
Transactions wait for apply/replication before unlock.
[Transactional composition][transactional-composition];
[factory dependencies][transactional-factory].

Second-generation `IStateProvider2` adds provider identity/hierarchy, lifecycle,
apply/unlock, copy ingestion, three-phase checkpointing, recovery, backup/restore
and state removal. Despite a COM factory boundary, its returned opaque pointer
is attached as a **native C++/KTL `IStateProvider2` object**. It is not a complete
language-neutral state-provider ABI.
[Provider obligations][state-provider2]; [opaque-pointer attachment][provider2-factory].

Reliable Collections C exports obtain the partition's internal KTL/transactional
interfaces and use native provider factories for Store/queue/dictionary
implementations. Calling those exports still loads/uses the native stack.
The public legacy KVS/ESE family is another compatibility surface: Linux symbol
export does not establish support; the inspected V1 ESE initializer explicitly
asserts unsupported on Unix.
[Collection integration][collections]; [legacy KVS qualification][legacy-kvs].

Thus stateless hosting, custom stateful services, native classic replication,
transactional replication and existing collection/disk-format compatibility
must be scoped and tested separately.

## 9. Security and liveness

### 9.1 Security is channel-specific

The local IPC server and optional TLS unit have separate security settings.
The latter requires the SSL provider. SSL client settings support a supplied
client certificate and expected server thumbprint, with encryption/signing.
Host IPC security is separate from application listener and replication
security. [IPC security][ipc-security]; [SSL client settings][security-settings].

There is an important **source/platform qualification**: the Windows-credential
configuration selects Negotiate/signing, but on Unix the transport setup returns
success early for Windows credentials and the secure-context factory excludes
`SecurityContextWin`. With no secure context, `TcpConnection` selects its
unsecured-message handler. Do not describe that source branch as Linux SSPI
authentication, and do not extrapolate it into a guarantee about every released
Linux runtime or a reason to downgrade a secure deployment.
[Negotiate settings][negotiate-settings]; [Unix transport setup][transport-security];
[secure-context factory][security-context-factory]; [connection selection][security-connection].

TLS is **Fabric-framed negotiation**, not TLS from byte zero. Security tokens
are native messages addressed to actor `SecurityContext`; Unix feeds their
bodies into an OpenSSL memory BIO. When framing protection is active, the
complete Fabric frame, including its fixed header, becomes TLS plaintext.
Unix enables framing protection in the inspected settings implementation.
Certificate authorization and connection-auth status processing are additional
steps; handshake success alone is not permission to dispatch service traffic.
[Negotiation message][security-tokens]; [Unix handshake][ssl-handshake];
[whole-frame encryption][ssl-frame]; [framing policy][framing-policy];
[secured connection][secured-connection].

Windows has separate SSPI and legacy/non-framing-protected paths. A stock
HTTPS/TLS stream wrapper is not a replacement for these transitions.
The [transport security specification](../transport/Transport.md#7-security-and-tls-on-the-wire)
documents the shared layer in more detail.

### 9.2 Default Linux app-host lease monitoring uses IPC

Start registration returns the node's lease handle/duration and initial TTL.
On Unix the application host selects **IPC lease polling** by default;
Windows noncontainer hosts instead use direct native lease queries.
The app-host poll is empty body, actor **Hosting (15)**, no action.
Node Hosting answers with TTL from its existing node lease agent. The host does
not register a new per-application lease agent on this traced Linux path.
That makes an application-side Rust implementation possible without replacing
the node's native lease system.
[Start lease data][host-register-manager]; [platform monitor selection][lease-selection];
[monitor implementation][lease-monitor]; [node TTL reply][hosting-ttl].

Successful polls use **monotonic request-send time + returned TTL**, not
reply-receipt time + TTL; failures retain the previous expiration. Delayed
responses must not extend authority merely because they arrived late.
There is also an explicit switch to disable IPC polling, so not every possible
Linux configuration can be described by the default path.
[Poll timing and direct-read branch][lease-monitor].

**Unresolved source discrepancy:** start reply produces a TTL duration, but the
application passes its `.Ticks` into initial monitor state that is treated as
an expiration timestamp, without the send-time addition used by later polls.
The code records a start request time but does not apply it in that conversion.
This investigation has not resolved the initial window against a running build;
do not silently redefine the field as an absolute monotonic timestamp.
[Initial TTL production][initial-ttl]; [initialization/conversion][lease-initialization];
[monitor initialization][lease-monitor].

Lease invalidation, TTL expiry, node-process exit and IPC disconnect are distinct:

| Event | Observed effect |
|---|---|
| TTL expiry | Lease monitor reports expired; stateful access getters apply the additional lease gate |
| `InvalidateLeaseRequest` | App closes lease monitor and replies; handler alone does not fire process-exit callback or abort the host |
| Nonactivated node-process wait completes | Marks node down, aborts host and dispatches retained runtime exit handlers |
| Activated process/node failure | Uses activation/process lifetime mechanisms rather than that nonactivated monitor; OS-level guarantees not traced here |
| IPC disconnect | Transport retry/threshold behavior; not itself proof the lease or registrations remain valid |

Hosting sends explicit invalidate-lease requests on shutdown to **nonactivated**
hosts. The source does not demonstrate that lease expiry forcibly terminates
every stateless listener or invokes every user's exit handler.
[Process monitoring/invalidation][host-liveness]; [node shutdown notification][host-invalidation];
[stateful access][stateful-access].

## 10. Investigation limits and test anchors

This is an architecture and feasibility map, not a complete recursively specified
hosting/RAP/replication wire protocol. Nested serializers, native release
differences, initial lease TTL behavior, Linux PAL process monitoring, full
health batching semantics and native log/checkpoint compatibility need further
qualification for an implementation.

Useful existing native test anchors include Hosting2's
`NonActivatedApplicationHost.Test.cpp`, RAP's
`Test.Unit.Proxy.OperationManager.cpp` and `Test.Unit.ReadWriteStatusState.cpp`,
replication's `ReplicateCopyOperations.Test.cpp` and
`ReplicationTransport.Test.cpp`, and transactional StateManager/logging tests.
Their assertions are research inputs, **not test results from this work**.
The small Linux runtime-host sample also demonstrates only a local factory and
opaque endpoint string, not a complete async/cancellation implementation.
[Host integration assertions][host-tests]; [Linux example][linux-example].

For the implementation boundaries, missing Rust capabilities, staged acceptance
criteria and native-versus-model evidence policy, see
[Pure Rust FabricRuntime design](RustFabricRuntime.md).

## Source index

[public-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl
[internal-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/internal/FabricRuntime_.idl#L153-L357
[runtime-export]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/retail/native/FabricRuntime/dll/dllmain.cpp#L62-L125
[factory-proxy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ComProxyStatelessServiceFactory.cpp#L13-L40
[runtime-create]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostContainer.cpp#L323-L475
[host-selection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostContainer.cpp#L1769-L1840
[service-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L356-L573
[replication-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L576-L852
[context-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L855-L1086
[kvs-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L1093-L1305
[node-activator-idl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L1456-L1527
[settings-extensions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricTypes.idl#L2910-L2960
[async-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ComAsyncOperationContext.h#L85-L100
[runtime-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ComFabricRuntime.cpp#L47-L107
[instance-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/api/wrappers/ComStatelessServiceInstance.cpp#L44-L69
[context-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ComCodePackageActivationContext.cpp#L32-L85
[async-proxy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ComProxyAsyncOperation.cpp#L66-L178
[runtime-destruction]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ComFabricRuntime.cpp#L383-L398
[host-proxy-selection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostProxy.cpp#L220-L280
[host-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostContext.cpp#L124-L184
[host-environment]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostContainer.cpp#L1626-L1746
[code-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/CodePackageContext.cpp#L124-L177
[package-identity]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/hosting2/ServicePackageInstanceIdentifier.cpp#L182-L225
[host-context-selection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostContainer.cpp#L1908-L1938
[host-injection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostProxy.cpp#L336-L416
[multi-activation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/MultiCodePackageApplicationHost.cpp#L168-L380
[host-open]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L593-L738
[single-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/SingleCodePackageApplicationHost.cpp#L189-L259
[runtime-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L1946-L2179
[factory-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceFactoryManager.cpp#L59-L214
[host-nonactivated]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L742-L854
[start-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/StartRegisterApplicationHostRequest.h#L42-L50
[start-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/StartRegisterApplicationHostReply.h#L85-L124
[registration-schemas]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/RegisterServiceTypeRequest.h#L10-L32
[unregister-host-schema]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/UnregisterApplicationHostRequest.h#L10-L30
[runtime-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeContext.h#L15-L60
[type-identifier]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/hosting2/ServiceTypeInstanceIdentifier.h#L70-L90
[host-register-manager]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1741-L1957
[host-finish]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L2011-L2063
[runtime-manager]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L474-L900
[type-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L438-L523
[runtime-table]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/RuntimeRegistrationTable.cpp#L269-L405
[hosting-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingConfig.h#L14-L27
[ra-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.SendMessageActions.h#L119-L170
[rap-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L70-L280
[action-admission]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.cpp#L133-L248
[ra-message]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Message.cpp#L14-L58
[rap-replies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L1685-L1741
[proxy-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ProxyRequestMessageBody.h#L130-L142
[proxy-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ProxyReplyMessageBody.h#L45-L69
[replica-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaDescription.h#L119-L131
[action-lists]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ProxyActionsList.cpp#L18-L215
[instance-open]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.OpenInstanceAsyncOperation.cpp#L14-L178
[rap-close]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L1386-L1458
[stateless-partition]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ComStatelessServicePartition.cpp#L35-L220
[replica-open]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.OpenReplicaAsyncOperation.cpp#L60-L180
[fup-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxyContext.h#L191-L255
[rap-open-admission]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L338-L569
[rap-drain]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.CloseAsyncOperation.cpp#L15-L109
[activation-files]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/CodePackageActivationContext.cpp#L254-L425
[endpoint-writer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/EnvironmentManager.cpp#L1965-L1973
[context-update]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L3243-L3324
[package-update]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/CodePackageActivationContext.cpp#L720-L783
[load-reports]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/LocalLoadReportingComponent.cpp#L122-L208
[partition-reports]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.cpp#L1852-L1927
[ipc-health]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/IpcHealthReportingClient.cpp#L94-L128
[stateful-access]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ComStatefulServicePartition.cpp#L75-L154
[access-calculator]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReadWriteStatusCalculator.cpp#L34-L157
[replicator-proxy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ComProxyReplicator.cpp#L13-L182
[create-replicator]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ComStatefulServicePartition.cpp#L164-L207
[state-provider]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ComProxyStateProvider.cpp#L65-L212
[replication-transport]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationTransport.cpp#L25-L165
[replication-endpoint]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/ReplicationEndpointId.h#L22-L81
[replication-dispatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Replication/Replicator.cpp#L618-L769
[transactional-composition]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp#L125-L301
[transactional-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/data/txnreplicator/ComTransactionalReplicatorFactory.cpp#L37-L59
[state-provider2]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/data/txnreplicator/common/IStateProvider2.h#L11-L157
[provider2-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/data/txnreplicator/ComProxyStateProvider2Factory.cpp#L70-L92
[collections]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/data/interop/ReliableCollectionRuntimeImpl/TransactionalReplicatorCExports.cpp#L873-L929
[legacy-kvs]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Store/KeyValueStoreReplica.cpp#L586-L619
[lease-selection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L3393-L3408
[lease-monitor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/LeaseMonitor.cpp#L28-L271
[hosting-ttl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingSubsystem.cpp#L2124-L2156
[initial-ttl]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1889-L1926
[lease-initialization]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L947-L1001
[host-liveness]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L3358-L3428
[host-invalidation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L150-L207
[host-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/NonActivatedApplicationHost.Test.cpp#L298-L356
[linux-example]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/test/LinuxRunAsTest/FabricRuntimeHost.cpp#L51-L73
[native-revision]: https://github.com/microsoft/service-fabric/tree/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[sample-main]: ../../crates/samples/kvmap/src/main.rs#L47-L79
[sample-replica]: ../../crates/samples/kvmap/src/kvmap.rs#L184-L259
[ipc-client]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcClient.cpp#L18-L162
[ipc-client-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcClient.cpp#L201-L245
[ipc-server]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcServer.cpp#L20-L302
[ipc-server-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcServer.cpp#L324-L399
[datagram-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DatagramTransportFactory.cpp#L12-L54
[ipc-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcHeader.h#L10-L31
[header-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaderId.h#L23-L51
[ipc-demux]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcDemuxer.cpp#L16-L32
[actors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L14-L84
[demux]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DemuxerT.h#L191-L232
[requestreply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L53-L128
[receiver-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L26-L39
[ipc-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportConfig.h#L90-L115
[ipc-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/IpcServer.cpp#L416-L440
[security-settings]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L133-L170
[negotiate-settings]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L20-L48
[transport-security]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportSecurity.cpp#L55-L94
[security-context-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L18-L42
[security-connection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1964-L1982
[security-tokens]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContext.cpp#L439-L461
[ssl-handshake]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/SecurityContextSsl.cpp#L176-L243
[ssl-frame]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpSendBuffer.cpp#L90-L131
[framing-policy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/transport/SecuritySettings.cpp#L1550-L1562
[secured-connection]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TcpConnection.cpp#L1985-L2048
