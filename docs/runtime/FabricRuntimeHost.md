# Node-side FabricRuntime control: RA and Hosting

## 1. Scope and essential distinction

The node's **Reconfiguration Agent (RA)** controls service instances/replicas
through the application's **Reconfiguration Agent Proxy (RAP)**. RA does not,
by itself, launch an executable or implement the runtime's entire server:
Hosting starts/manages the process, ApplicationHostManager registers it,
FabricRuntimeManager registers runtimes/types, and other components provide
leases, health handling and deployment artifacts.

This document describes those **node-side** responsibilities, complementing
[application-side FabricRuntime architecture](FabricRuntime.md). A proposed
[Rust standalone test host](RustTestHost.md) implements a deliberately bounded
subset of this node-facing contract, not another application-side runtime.

**Source baseline:** the local clean Service Fabric checkout at
[`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][revision], inspected 2026-10-25.
Native paths and links below refer to that revision. This is source research,
not a running test-host implementation, a full private-protocol specification,
or proof of compatibility with the repository's 11.8.121.4 runtime.

For the subsequent narrow standalone implementation and its actual-native
qualification, see [Rust test-host MVP](RustTestHost.md#2-implemented-mvp-and-usage).
The broader node responsibilities described here remain a source architecture,
not a production-RA-equivalence claim.

## 2. Which side owns what

```text
FM / other nodes / node lifecycle
               |
       node Reconfiguration Agent
       local failover-unit state, retries, epochs, configuration
          |                 |
          |                 +--> Hosting lookup / activation / termination
          |
    node IpcServer ----------------------> application IpcClient
          ^                                    |
          |                           Reconfiguration Agent Proxy
          +---- semantic RA replies ------------+
                                               |
                                   local runtime / factory / service
                                               |
                              optional native replicator and storage

Separate handlers on the node IPC server:
  ApplicationHostManager    host registration, host/context control, host health
  FabricRuntimeManager      runtime and service-type registration
  Hosting                   node lease TTL requests
  RA                        lifecycle replies, load/fault/health and notifications
```

| Component | Responsibility |
|---|---|
| Node Hosting / host proxies | Activation environment, process/container lifecycle, deployed context and updates |
| ApplicationHostManager, actor 7 | Host start/finish/unregister and host-targeted control |
| FabricRuntimeManager, actor 9 | Runtime/type registration, validation and invalidation |
| Node RA, actor 12 | Local replica intent, lifecycle/reconfiguration commands and completion processing |
| App RAP, also actor 12 | Execute the commanded local APIs and report results |
| Hosting, actor 15 | Answer app-host lease TTL queries using the node lease agent |
| Health subsystem / FM | Consume or forward health/load and update cluster-level state |
| Application | Implement factory/lifecycle callbacks and own its application RPC listener |

Actor values: [native enumeration][actors]. Registration and host selection:
[application-side bootstrap map](FabricRuntime.md#4-application-host-bootstrap-and-registration).
RA's adapter delegates its transport to `IpcServer`; there is no separate
"FabricRuntime control TCP port" required beyond the runtime IPC connection.
[RA transport adapter][ra-transport].

Two common naming traps: `ApplicationHost` is an app-side Hosting class, not
the node RA; **RS (13) is not the RA-to-RAP lifecycle actor**.

## 3. Getting from a service request to an available factory

### 3.1 Registration is a prerequisite, not an open callback

Hosting provides a `ServiceTypeRegistration` tying service/package activation
identity to **host ID, runtime ID, host PID and package version**. RA looks it up
using service type, package version and the service's activation context.
Lookup can cause Hosting to start activation; it may initially return "not
registered" while the process is starting.
[RA registration lookup][registration-lookup];
[lookup/retry wrapper][registration-wrapper].

The process must establish its IPC route, complete host registration, register
a runtime and register its service type. Only then does RA have the information
needed to target its local factory. The factory pointer stays in the process;
the node learns its identity, not a marshaled object.
[Hosting registration protocol](FabricRuntime.md#43-hosting-messages-and-validation).

A `ServiceTypeRegistered` event resumes pending open/close work. RA checks type,
package version and shared/exclusive activation compatibility instead of
attaching every waiting partition to the latest process that registered the
same short type name. Hosting usage counts and registration-change actions
track the dependency.
[Registration event handling][registration-event];
[Hosting adapter][hosting-adapter].

### 3.2 Normal stateless creation

```text
FM AddInstance
  -> node RA validates/creates local failover unit and service description
  -> marks local replica open pending
  -> finds/awaits activation-scoped service-type registration
  -> sends actor RA / ReplicaOpen to registration.HostId
       body contains registration.RuntimeId + service-unit descriptions
  -> app RAP invokes registered local factory and instance Open
  <- actor RA / ReplicaOpenReply, including service location
  -> node validates pending state, replica instance and reply
  -> updates local state/endpoints
  -> sends AddInstanceReply upstream to FM
```

This is why directly sending FM's `AddInstance` to an app is wrong:
`AddInstance` is node-side failover input; **`ReplicaOpen` is the app-side
control message**. [Add-instance processing][add-instance];
[open retry][open-retry]; [open reply/upstream completion][open-completion].

A successful process spawn, completed host registration or type registration
alone does not mean a service was opened. Conversely, closing one service unit
need not stop a shared code-package process.

## 4. The host-to-runtime protocol

### 4.1 Connection and envelope

The application connects outward to the node IPC listener. Client IPC traffic
provides host ID/PID in `IpcHeader`; the server remembers the reverse send target.
Node messages use that existing connection and do not need an `IpcHeader`
pretending to originate from another client.
Clear framing, correlation and optional security use the shared native
transport, not COM RPC, HTTP or JSON.
[IPC detail](FabricRuntime.md#5-ipc-and-message-routing).

`SendMessageToRAPAction` targets the registration's **HostId**. The runtime ID
is carried in the operation body where required. Node RA constructs a
`RAMessage`, adds actor/action, and queues its send as a state-machine action.
[Send-action implementation][send-action]; [RA message definitions][ra-messages].

| Traffic | Pattern |
|---|---|
| Host/runtime/type registration | App-initiated correlated request/reply |
| RA lifecycle commands | Node-initiated one-way messages |
| Lifecycle `...Reply` actions | App-initiated one-way semantic replies, not `RelatesTo` replies |
| `RAPQuery` | Correlated node request / `RAPQueryReply` |
| Endpoint/access-revocation notifications | App one-way notification and node one-way semantic acknowledgement |
| Health reports | Correlated app request/reply, routed through RA or host manager |
| Lease polling | Correlated app request to actor Hosting, no action and empty body |

The action name "Reply" does not determine which row applies. Node's
`RaToRapTransport` supports both request/reply and one-way sends; replacing it
with a single request-table API loses lifecycle semantics.
[Transport interface][ra-transport]; [RAP reply paths][rap-replies].

### 4.2 Commands and descriptions

| Command to app RAP | Host's purpose / expected completion |
|---|---|
| `ReplicaOpen` | Create/open stateless instance or stateful replica; `ReplicaOpenReply` |
| `ReplicaClose` | Graceful/abort/drop variant selected by flags; `ReplicaCloseReply` |
| `StatefulServiceReopen` | Reopen persisted state; `StatefulServiceReopenReply` |
| `UpdateConfiguration` | Apply roles, replica-set/catch-up/reconfiguration stage; `UpdateConfigurationReply` |
| `ReplicatorBuildIdleReplica`, `ReplicatorRemoveIdleReplica` | Build/remove an idle peer; corresponding replies |
| `ReplicatorGetStatus`, `ReplicatorUpdateEpochAndGetStatus` | Obtain progress, optionally update epoch; corresponding replies |
| `CancelCatchupReplicaSet` | Cancel catch-up work; corresponding reply |
| `ProxyUpdateServiceDescription` | Versioned service-description update; corresponding reply |
| `RAPQuery` | Native replica-detail query with its own response schema |

Definitions: [RA messages][ra-messages]. Not every command is applicable to every
service kind or lifecycle state.

The common serialized request is:

```text
ProxyRequestMessageBody {
    runtimeId,
    failoverUnitDescription,
    localReplica,
    remoteReplicas,
    serviceDescription,
    flags
}
```

Different constructors populate the fields needed by the command; not every
message requires a newly supplied service description/runtime ID. The common
reply carries failover-unit, local/remote replica descriptions, proxy error
and flags. Service-description and query responses have separate bodies.
[Request schema][proxy-request]; [reply schema][proxy-reply];
[sender overloads][sender-overloads].

Nested fields are significant: partition/consistency-unit identity, native node
instance, replica ID **and instance ID**, previous/current roles and epochs,
package version, service and replication endpoints, LSNs, stateful/persisted
settings, initialization data, metrics and update version. These are
Fabric-serialized objects, not public IDL struct layouts.
See [app RAP schema map](FabricRuntime.md#61-rap-is-application-side-control-execution).

## 5. RA is a state machine, not a command socket

### 5.1 State, action ordering and persistence

RA maintains local failover-unit state and pending open/close/reconfiguration
work. Entity jobs obtain the appropriate entity state, execute handlers, commit
when an update requires it, then execute queued side effects only when the job
was not dropped and commit succeeded. Actions are abandoned on failure.
The store can perform in-memory or persisted commits; not every command implies
a disk write.
[Entity execution/commit][entity-commit];
[post-commit action decision][entity-actions].

Native open retry also checks node readiness after the FM node-up
acknowledgement. That is a prerequisite inside the full node RA, not another
message an app RAP must receive. [Node readiness check][open-retry].

A minimal test host can model selected states in memory, but must not call
itself a complete production RA: node recovery, durable state, FM/other-node
messaging, upgrade/deactivation and retry policies remain outside that model.

### 5.2 Matching completion requires semantic identity

The transport's message ID is not the identifier of an outstanding
`ReplicaOpen`. RA checks that a local unit exists, is up/in the right phase,
has appropriate open/close pending state and matches the native replica
incarnation. Open also rejects stale state/abort combinations; close requires
the exact replica and instance IDs.
[Open completion guards][open-guards]; [close completion guards][close-guards].

Common IPC staleness checks reject obsolete epoch/instance information, but have
operation-specific exceptions: fault and access-revocation notifications can
remain relevant across an epoch change, and build completion compares primary
epoch rather than blindly requiring full epoch equality.
[Node-side staleness rules][staleness].

The app RAP independently fences its own operations. The two sides can
temporarily disagree because work or a reply is in flight. Retries must use the
same intended incarnation rather than create a second service every time.

### 5.3 Completion and endpoint handling

Successful stateless/primary open moves the local replica toward Ready,
clears open-pending state and stores the service/replication endpoints from
the reply. Stateful idle open instead moves toward InBuild; persisted reopen
returns to StandBy and follows a different upstream path.
[Replica-open state changes][unit-open-reply].

RAP's endpoint update notification is acknowledged so it stops retrying;
whether RA adopts the endpoint additionally depends on matching primary epoch
and reconfiguration phase. An acknowledgement is not proof that every supplied
endpoint was published into naming. Access-revocation notifications similarly
receive a semantic acknowledgement and can permit replica-down reporting before
all service close work is finished.
[Endpoint update][endpoint-update]; [access revocation][access-revocation].

## 6. Stateful control is a multi-stage protocol

Node RA constructs `UpdateConfiguration` from its current local/remote replica
view. Flags distinguish ordinary updates, **Catchup** and
**EndReconfiguration**. The reply handler interprets flags plus the current
phase; a generic "configuration OK" response is insufficient.
[Configuration send][configuration-send]; [configuration completion][configuration-completion].

RAP then sequences the service and replicator APIs. The host does not directly
invoke COM `ChangeRole` across the socket. It supplies the native epoch/
configuration/role description; RAP applies the ordered local calls and reports
their results. Promotion/demotion may require epoch/status queries, catch-up,
build/remove peer operations, revocation and endpoint acknowledgement.
[App-side ordering](FabricRuntime.md#63-stateful-order-identity-and-cancellation).

Replication bytes themselves flow between the replicas' replication transports,
not through these RA commands. A test host controlling applications that retain
the native default replicator need not implement that peer data protocol.
It still has to supply a consistent control sequence and valid peer endpoints.
This does not remove native replication/storage dependencies from the app.
[Replication boundary](FabricRuntime.md#8-replication-and-storage-are-separate-layers).

## 7. Reports and failure decisions flowing back to the node

| App-originated traffic | Native node behavior |
|---|---|
| `RAPReportLoad` | RA decodes aggregated reports and sends RS load reporting toward FM |
| `RAReportFault` | Validate current unit/incarnation; select a local close mode |
| `ReportHealth` through RA | Forward to Health Manager retaining correlated receiver context |
| `ReportHealth` through ApplicationHostManager | Separate host/package health route to the health subsystem |
| Endpoint/access notifications | Source-defined acknowledgements plus phase-sensitive state changes |
| Runtime/host unregistration or exit | Hosting invalidates registrations; RA processes affected units |

Sources: [RA report/fault handling][reports], [host health dispatch][host-health],
[host/runtime exit processing][host-exit].

For a permanent fault the shown handler selects abort. A transient fault selects
restart for persisted state or drop otherwise. These are state-machine inputs,
not merely log messages. Host crash while opening is processed as an
application-host-crash transition; whether to retry close/reopen depends on
persistence and node/application lifecycle.
[Fault decision][reports]; [host-exit transitions][host-exit].

Health replies are not bare `ErrorCode` objects: the native health client decodes
per-report results and sequence-stream results. Host health can use actor 7,
while RAP health uses actor 12. A host emulator must handle the routes its
profile advertises and must not return empty "success" as proof of health
acceptance.
[App health-client construction][health-clients]; [health completion decoder][health-decoder];
[health reply schema][health-reply].

## 8. Retries, close and authority

Node send expiration `RAPMessageRetryInterval` is **not** a timeout argument
passed to the user's `Open`. Pending local state drives semantic retries.
Registration-not-ready and user/API errors have distinct retry/health/drop
handling; repeated failures can progress to abort or host termination.
[Open retry logic][open-retry]; [unit retry/error handling][unit-open-reply];
[Hosting termination adapter][hosting-adapter].

`ReplicaClose` uses a close-mode-derived flag set and exact service-unit
identity. Close reply processing checks that the close is still pending.
Application graceful-close failure can lead to RAP abort/cleanup and then a
successful terminal close reply; a node must not equate the raw callback error
with "the process is still serving" or assume a successful close killed the
shared application process.
[Close sender][close-send]; [node close handling][close-guards];
[app close behavior](FabricRuntime.md#62-stateless-path).

Lease authority, process exit and connectivity are separate from these
operation deadlines. The node supplies lease data and answers TTL polls; the
native default Linux app host polls IPC rather than registering a new app-side
lease driver. Full production node leasing and its safety remain node
responsibilities. [Lease analysis](FabricRuntime.md#92-default-linux-app-host-lease-monitoring-uses-ipc).

## 9. Native test seams and evidence limits

Native RA tests inject federation, Hosting, health, storage, clock and
RA-to-RAP transport stubs. The RA transport stub records one-way messages by
target host; it does not connect a real application. That is useful
state-machine evidence, not proof that a standalone Rust IPC server can boot
an unmodified native runtime.
[RA unit context][test-context]; [transport stub][transport-stub].

Conversely, the Hosting runtime-registration tests create a `TestFabricNodeHost`
and open it. Their existence does not demonstrate clusterless external startup
with a minimal fake node.
[Hosting test setup][hosting-tests].

An independently launched application needs more than a registration reply:
native configuration, deployed context files, resolved endpoints/principals,
runtime library dependencies and native runtime subsystem startup all matter.
Those requirements and the deliberately bounded substitute authority are
spelled out in [Rust test host design](RustTestHost.md).

## Source index

[revision]: https://github.com/microsoft/service-fabric/tree/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[actors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h#L32-L60
[ra-transport]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/RAPTransport.h#L13-L97
[registration-lookup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L987-L1034
[registration-wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ServiceTypeRegistrationWrapper.cpp#L273-L334
[registration-event]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L1084-L1131
[hosting-adapter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingAdapter.cpp#L46-L158
[add-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L1639-L1735
[open-retry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L832-L881
[open-completion]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L883-L928
[send-action]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.SendMessageActions.h#L102-L171
[ra-messages]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Message.cpp#L14-L58
[rap-replies]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.cpp#L1685-L1741
[proxy-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ProxyRequestMessageBody.h#L130-L142
[proxy-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ProxyReplyMessageBody.h#L45-L69
[sender-overloads]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L3593-L3682
[entity-commit]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItemListExecutorAsyncOperation.h#L60-L126
[entity-actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h#L228-L278
[open-guards]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L3782-L3829
[close-guards]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L3903-L3946
[staleness]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/infrastructure.FailoverUnitEntityTraits.cpp#L292-L410
[unit-open-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L5250-L5423
[endpoint-update]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L4599-L4630
[access-revocation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L5547-L5591
[configuration-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L2509-L2535
[configuration-completion]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L3949-L4037
[reports]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L3693-L3774
[host-health]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1590-L1641
[host-exit]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L1133-L1198
[health-clients]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHost.cpp#L2655-L2672
[health-decoder]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/HealthReportingComponent.cpp#L314-L416
[health-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/management/HealthManager/ReportHealthReplyMessageBody.h#L12-L47
[close-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp#L4760-L4780
[test-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Utility.UnitTestContext.cpp#L81-L165
[transport-stub]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Utility.RAToRApTransportStub.h#L15-L65
[hosting-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/NonActivatedApplicationHost.Test.cpp#L128-L134
