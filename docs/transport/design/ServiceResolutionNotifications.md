# Uncached service resolution and notifications

**Status:** implemented as an experimental clear-TCP subset in `mssf-client`
and `mssf-transport`, with native qualification on SF 11.8.121.4. See
[usage, measured coverage and limitations](../RustFabricClient.md#owned-resolution-and-notification-qualification).
This design extends the current [Rust FabricClient](../RustFabricClient.md);
it does not claim TLS, automatic recovery, or native-client feature parity.

## 1. Scope and decisions

Add native service endpoint resolution and notification filters without adding
an endpoint-resolution cache.

| Area | Decision |
|---|---|
| Resolution | Every call sends a native gateway request, optionally carrying metadata from a caller-owned previous result. |
| Notifications | Standalone `ServiceNotificationClient` with its own connection/session, owning filter-registration guards, and one bounded async receiver. |
| Ownership | `FabricClient` and `ServiceNotificationClient` have independent construction, shutdown, and failure lifecycles within the same crate. |
| Cache | No stored endpoint results used to answer resolve calls, and no notification-driven endpoint cache. |
| Protocol state | Retain bounded session, filter, generation, and version bookkeeping needed for notification processing. |
| Delivery | Versioned endpoint updates, not a snapshot or an exactly-once application event log. |
| Recovery | Initially terminate with an explicit error on disconnect or loss of delivery continuity. No automatic reconnect or filter replay. |
| Security | Start with the existing explicitly selected clear-TCP development profile. A later TLS profile must authenticate the notification connection independently. |
| Excluded | Legacy resolution-change long polling, prefix resolution, service-group expansion, application endpoint connections, and background endpoint refresh. |

The [parent design](FabricClient.md) describes the broader client architecture.
This document replaces its earlier requirement to add a resolution cache and
defines the narrower initial notification API. Its source baseline is native
commit [`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`][native-revision].

## 2. Public API

Signatures below describe the implemented Rust API shapes; the snippets omit
imports and private fields rather than being standalone declarations.
Each client's `connect_clear(endpoint, timeout)` takes a
required duration that bounds establishment and sets its own operation default.
Subsequent remote operations follow the current timeout convention: `None` uses
that client's configured default, not infinity; `Some(duration)` supplies one
whole-operation override. There are no `_with_timeout` variants.

### 2.1 Resolution

```rust
pub enum PartitionKey {
    Singleton,
    Int64(i64),
    Named(String),
}

impl FabricClient {
    pub async fn resolve_service_partition(
        &self,
        name: &NamingUri,
        key: &PartitionKey,
        previous: Option<&ResolvedServicePartition>,
        timeout: Option<Duration>,
    ) -> Result<ResolvedServicePartition>;
}

pub struct ResolvedServicePartition {
    pub service_name: NamingUri,
    pub partition_information: PartitionInformation,
    pub kind: ServiceKind,
    pub endpoints: Vec<ServiceEndpoint>,
    pub version: ServiceEndpointVersion,
    // Private metadata retains the native store version and result identity.
}

pub struct ServiceEndpoint {
    pub address: String,
    pub role: ServiceEndpointRole,
}

pub enum ServiceEndpointRole {
    Stateless,
    StatefulPrimary,
    StatefulSecondary,
}

pub struct ServiceEndpointVersion {
    // Private service/partition identity, native generation, and lookup version.
}

impl ServiceEndpointVersion {
    pub fn compare(&self, other: &Self) -> Result<std::cmp::Ordering>;
}
```

Reuse the existing `NamingUri`, `PartitionInformation`, `ServiceKind`, `Uuid`,
`Error`, and `Result` types. Preserve the current checked URI profile rather
than silently broadening it to service-group fragments or root/prefix lookup.
Partition-key strings are not URI path components and need their own native
validation and serialization.

Returned values own their data. The caller may retain a result and pass it as
`previous`; the client does not retain that result after the operation finishes.
Keep refresh metadata private and validate its binding to the requested service
and partition selection before sending. Public field edits must not rebind an
opaque token to a different service or partition.

`ServiceEndpointVersion` is not a timestamp, continuation token, or generally
ordered integer. `compare` rejects different service/partition identities and
otherwise compares native generation first, then FM lookup version. Do not
derive `Ord` across unrelated partitions. The store version remains separate
refresh metadata, not an extra endpoint-ordering tie-breaker. Both resolution
results and notification updates use the same endpoint-version type.
[Sources: [result schema][resolve-result], [native version comparison][resolve-compare].]

### 2.2 Standalone notification client

```rust
pub enum NotificationError {
    Common(Error),
    Lagged,
    ResourceLimit(&'static str),
}

pub type NotificationResult<T> =
    std::result::Result<T, NotificationError>;

pub struct ServiceNotificationClient {
    // Private owned connection, session state, and notification receiver.
}

impl ServiceNotificationClient {
    pub async fn connect_clear(
        endpoint: SocketAddr,
        timeout: Duration,
    ) -> NotificationResult<Self>;

    pub async fn register_filter(
        &self,
        filter: &ServiceNotificationFilter,
        timeout: Option<Duration>,
    ) -> NotificationResult<ServiceNotificationRegistration>;

    pub async fn recv(&mut self) -> Option<NotificationResult<ServiceNotification>>;

    pub async fn close(self, timeout: Option<Duration>) -> NotificationResult<()>;
}

pub struct ServiceNotificationFilter {
    pub name: NamingUri,
    pub name_prefix: bool,
    pub primary_only: bool,
}

#[must_use = "Keep the registration alive while notifications are needed"]
pub struct ServiceNotificationRegistration {
    // Private native filter ID, session identity, and cleanup handle.
}

impl ServiceNotificationRegistration {
    pub async fn unregister(self, timeout: Option<Duration>) -> NotificationResult<()>;
}

pub struct ServiceNotification {
    pub service_name: NamingUri,
    pub partition_id: Uuid,
    pub kind: ServiceKind,
    pub endpoints: Vec<ServiceEndpoint>,
    pub version: ServiceEndpointVersion,
}
```

Notification-only failures belong to this API family, not the shared transport
`Error` used by ordinary queries/resolution. Every fallible notification method
uses `NotificationResult`; `Common` wraps the original shared error and preserves
its source. Shared codec/control resource limits stay common, while notification
delivery, filter, and bookkeeping limits use the scoped variants. Low-level
callback adapters must preserve the original notification cause before retiring
the connection; do not classify failures by matching diagnostic strings.

`ServiceNotificationClient` is not cloneable and owns one connection, one native
session, and one receiver. Multiple filters share that session and receiver;
they do not create separate TCP connections. Applications can construct multiple
independent notification clients when they need separate lifecycles, each with
its own bounded resources and native client ID. There is no per-`FabricClient`
session reservation or factory method.

Both public clients live in `mssf-client` and reuse transport, serialization,
endpoint/version models, and security configuration machinery. They do not own
or retain one another and do not share sockets or mutable session state.
Notification-only applications need not construct a `FabricClient`. Native
notification entries do not carry matching filter IDs, so do not invent
per-filter delivery, duplicate an event for every overlapping filter, or attach
an arbitrary matching ID.

`name_prefix = false` matches the exact service name. Prefix matching uses native
URI ancestry, not an arbitrary string prefix. `primary_only` requests the
native stateful-primary-change filter; it is not a request to strip secondary
endpoints from returned records. Stateless services retain native behavior.
Server-side filtering does not promise a distinct application callback for
every transition or perfectly deduplicated endpoint contents.

Registration returns a non-cloneable RAII guard, not a bare filter ID. Keep it
alive for the subscription's lifetime. It owns a session-scoped cleanup handle
rather than borrowing the receiver, so the caller can retain registrations
while calling `recv(&mut self)`. Native IDs stay private and cannot be reused
against a different or reopened session.

Dropping the guard initiates automatic unregister through the session worker.
For remote confirmation, call `registration.unregister(timeout).await`; this
consumes the guard. Neither form retracts notifications already admitted to the
local queue, and updates matching another active filter can continue. The
asynchronous cleanup and failure contract is defined in section 6.3.
[Sources: [filter schema][notification-filter], [primary-only matching][primary-matching],
[notification schema][notification].]

### 2.3 Usage

```rust
let endpoint: SocketAddr = "127.0.0.1:19000".parse()?;
let client = FabricClient::connect_clear(endpoint, DEFAULT_TIMEOUT).await?;
let mut notifications =
    ServiceNotificationClient::connect_clear(endpoint, DEFAULT_TIMEOUT).await?;
let name: NamingUri = "fabric:/MyApp/MyService".parse()?;
let registration = notifications
    .register_filter(
        &ServiceNotificationFilter {
            name: name.clone(),
            name_prefix: false,
            primary_only: false,
        },
        None,
    )
    .await?;

let initial = client
    .resolve_service_partition(&name, &PartitionKey::Singleton, None, None)
    .await?;

if let Some(update) = notifications.recv().await {
    let update = update?;
    // Compare versions for the same partition before replacing caller-owned state.
}

registration.unregister(None).await?;
notifications.close(None).await?;
client.close().await?;
```

The two clients are constructed and closed independently. Notification-only
applications omit the `FabricClient` and initial resolution; applications
combining resolution with updates choose endpoints for the same cluster.

Replace the explicit `unregister` call with `drop(registration)` or normal scope
exit when cleanup initiation without waiting for confirmation is sufficient.
Discarding the returned guard initiates cleanup immediately; `let _ = ...`
does not keep a filter active. Bind the guard to a named variable, including
`_registration` when it is retained only for its lifetime.

Register before fetching initial endpoints. A new filter is **not retroactively
applied to existing gateway cache entries**, and updates may arrive before
`register_filter` returns. The receiver must therefore exist before registration
is sent. Application code that combines the initial result with queued updates
uses identity/version comparison, not arrival order; a recreated partition has
a different identity and requires explicit handling.

The example shows API sequencing, not an atomic subscribe-and-snapshot operation
or a complete routing loop. An exact filter may cover multiple partitions, and
one resolve call discovers only the partition selected by its key.
[Source: [non-retroactive registration][notification-registration-semantics].]

## 3. Resolution behavior and wire mapping

Uncached means uncached **inside the Rust client**. The native gateway can still
serve its own cached service description or location. Do not equate this API
with an instruction to bypass every cluster cache.

```text
Client -> Gateway: actor NamingGateway (5), action ResolveServiceRequest
  ServiceResolutionMessageBody {
    name_,
    request_ { key_, version_, cuid_, includePsd_ },
    bypassCache_
  }

Gateway -> Client: ResolveServiceRequest (native reply action)
  ResolvedServicePartition {
    isServiceGroup_, locations_, partitionData_,
    storeVersion_, generation_, psd_
  }
```

The declarations show native serialization order, not Rust in-memory layout.
Despite the separately declared `ResolveServiceReply` action string, native
`NamingMessage::GetResolveServiceReply` constructs the message using
`ResolveServiceAction` (`ResolveServiceRequest`). Validate the actual action,
not the factory's name. [Source: [reply factory][resolve-reply-factory].]
Use the existing bounded native Serde facilities and normal request correlation,
remaining-time headers, fault handling, and operation-local native errors.

Reusing the public `PartitionInformation` type does not reuse its query wire
layout. Decode resolution's `PartitionInfo` as scheme, low key, high key, and
partition name, then combine it with the CUID from `locations_` to construct
that public value. Decode endpoint locations from the source-defined
`ServiceTableEntry`/`ServiceReplicaSet`, not replica-query records.
[Source: [resolution partition schema][partition-info].]

Without `previous`, send the native uninitialized version and invalid CUID.
With `previous`, encode its native generation, FM lookup version, store version,
and partition identity using the source-defined request fields. Set
`includePsd = false` and `bypassCache = false`; neither is a public cache option.
Do not serialize the entire previous result.

The gateway maps the key through its partitioned service description. It can use
locations newer than the supplied version or refresh through its resolver.
Supplying `previous` is refresh metadata, not a watch: a successful refresh may
return unchanged endpoints. Missing services and invalid keys remain errors;
unsupported service-group results fail explicitly rather than returning the
group's encoded endpoints as if they were ordinary service addresses.

Preserve primary-validity and stateful/stateless distinctions when constructing
endpoint roles. Addresses are opaque service-published strings, potentially
containing application-specific structured data; do not assume URLs, parse
listener names, select an endpoint, or connect to it.
[Sources: [request body][resolve-body], [nested request][resolve-request],
[default request metadata][resolve-request-defaults], [gateway resolution][resolve-gateway].]

## 4. Notifications without an endpoint cache

The notification wire body contains service table entries and version metadata,
not complete resolved partition descriptions. Native FabricClient obtains
optional partition information by processing those entries through its cache.
This client must not introduce hidden service-description queries or cache
population to reproduce that enrichment.

Expose service name, partition ID, service kind, endpoint addresses/roles, and
endpoint version directly from the notification. Do not promise the partition
name/range, store version, or a `ResolvedServicePartition` in a notification.
Callers needing those details resolve an appropriate key separately.

An empty endpoint set is an observable endpoint removal/invalidation update.
It must reach the caller, including for deletion-related updates. It does not
prove that the service was deleted rather than temporarily lacking endpoints.
Do not expose a synthetic `Deleted` event based solely on endpoint count.

Permitted retained state is bounded protocol bookkeeping:

- Client/session identity, actual gateway identity, and current generation.
- Desired filters and registration operations, including outcome uncertainty.
- Version ranges and page-processing metadata for retry/duplicate handling.
- Partition identities and versions needed by deletion synchronization.
- Pending decoded updates and reply context until admission/acknowledgment.

There is no map of last resolved endpoint lists, no local resolution fast path,
and no background resolve operation. Endpoint payloads live only in bounded
in-flight processing, queued updates, or caller-owned results. Duplicate handling
is based on protocol identities/versions, not stored endpoint-list equality.
[Sources: [wire notification][notification], [native cache enrichment][notification-enrichment].]

## 5. Session and transport architecture

```text
Application
  FabricClient
    ordinary connection -> resolve and existing request/reply APIs
  ServiceNotificationClient
    notification connection/session -> filters, reverse requests, replies
                                       bounded queue -> recv()
```

Constructing a `FabricClient` never opens notification transport.
`ServiceNotificationClient::connect_clear` establishes its own connection using
the explicitly configured endpoint and security profile, then connects the
native notification session. It needs no existing ordinary connection or second
public open step. This follows the native separation of normal/high-priority
connections while making ownership and failure isolation explicit. Do not dial
an advertised private gateway address merely because it appeared in a reply.

The notification connection performs its own transport establishment and native
session exchange. Its actual gateway can differ behind a load balancer; retain
the identity returned by the session connection reply instead of assuming the
ordinary connection's identity. Validate native priority-path behavior during
live qualification; opening another socket alone is not evidence of support.
Native notification gateway descriptions intentionally omit a public address;
retain their node identity/name without requiring or inventing an address.
[Source: [gateway description][notification-gateway-description].]

Extend `mssf-transport` with explicit duplex capability for this connection role.
Normal request-only connections continue to reject unsupported reverse traffic.
After controls, route the uncorrelated-notification marker before ordinary
`RelatesTo` lookup. Only the supported action, envelope, and body can enter the
session handler. Ordinary replies must never satisfy notification processing.

The writer needs a bounded reply/control path that does not require an
outstanding locally initiated request. Notification replies retain the original
request ID, connection/session identity, and send target. Never send an old
session's acknowledgment through whichever connection is currently available.
Reserve reply/control capacity; a full application queue must not block the
receiver or deadlock registration replies. Priority cannot preempt a TCP frame
whose write has already started.
[Sources: [native priority connection][notification-connect], [duplex routing][duplex],
[reply correlation][receiver-reply].]

### 5.1 Session exchange

| Direction | Action | Ordered body |
|---|---|---|
| Client to gateway | `NotificationClientConnectionRequest` | `clientId_, generation_, clientVersions_, filters_` |
| Gateway to client | `NotificationClientConnectionReply` | `generation_, lastDeletedEmptyPartitionVersion_, gateway_` |
| Client to gateway | `RegisterServiceNotificationFilterRequest` | `clientId_, filter_` |
| Client to gateway | `UnregisterServiceNotificationFilterRequest` | `clientId_, filterId_` |
| Gateway to client | `ServiceNotificationRequest` | `notification_`, containing `notificationPageId_, generation_, versions_, partitions_` |
| Client to gateway | `ServiceNotificationReply` | `error_` |
| Client to gateway, when synchronization requires it | `NotificationClientSynchronizationRequest` | `clientId_, generation_, undeletedPartitionsToCheck_` |
| Gateway to client | `NotificationClientSynchronizationReply` | `generation_, deletedVersions_` |

C++ vectors of shared pointers (initial filters and notification entries) encode
a UInt32 count followed by direct pointer values, **not** object-array slots.
Version-range value vectors remain native object arrays. Reuse the bounded
`CountedValues`/`Pointer` and object-array adapters accordingly.
[Source: [shared-pointer vectors][pointer-vectors].]

A fresh notification client starts a session with a new native client ID, no
filters, and native empty generation/version state.
`ServiceNotificationClient::connect_clear` returns after transport establishment,
the native session connection, and required synchronization complete, all under
its one establishment deadline. It does not wait for a user-visible endpoint
update. Empty protocol-only pages update bookkeeping and are acknowledged
without inventing an application event.

The gateway proxy associates the client ID with the send target through
`ClientIdentityHeader`; it is not an application-selected routing header.
The underlying transport establishment and notification session connection are
distinct protocols.

A pushed page carries the native uncorrelated-reply marker and starts a reverse
request/reply exchange. For its transport MessageId `N`, return
`ServiceNotificationReply` with `RelatesTo(N)` and the processing status. Do not
confuse that transport ID with the notification page's activity ID/page index.
[Sources: [session bodies][connection-body], [session reply][connection-reply],
[identity binding][notification-identity], [reverse exchange][reverse-request],
[page identity][notification-page].]

### 5.2 Admission, versions, and acknowledgment

Process a page as a bounded transaction:

1. Validate and decode the complete envelope/body under frame and collection
   limits, retaining the original reply context.
2. Compute accepted updates and prospective version changes without publishing
   them. Suppress already-covered retries and stale generations using native
   semantics, not TCP arrival order. Track per-partition versions so an older
   update cannot replace a newer update for the same identity. Scope bookkeeping
   to the native generation; a new generation resets the old version space.
3. Reserve delivery and bookkeeping capacity for the complete accepted page.
   Atomically commit the updates and corresponding version advancement. A
   duplicate page already committed can be acknowledged without redelivery.
4. Reply with successful processing status only after that commit. The caller's
   `recv` or application work is not part of acknowledgment.

Protocol-only version advancement is valid even when no user update survives
filtering. A page can contain multiple partitions, and pages are not an atomic
cluster-wide snapshot. If a reply is lost after admission, retry processing must
not enqueue the same admitted update again. Do not promise suppression of all
semantically identical endpoint payloads arriving under different versions.

If decoding, admission, version tracking, or acknowledgment fails, do not send
success while dropping the update. Send a native processing error if the
connection safely permits it, surface the failure locally, and retire the
notification session when continuity cannot be maintained. A malformed
notification cannot be treated as an empty success.
[Sources: [native processing and reply][notification-process],
[native delivery/version filtering][notification-accept],
[gateway retry behavior][notification-retry].]

## 6. Bounds, failures, and lifecycle

### 6.1 Resource bounds

Use named internal limits, not new public configuration in the first API:

| Resource | Initial implemented bound |
|---|---|
| Queued user updates | 256 entries and 8 MiB of accounted decoded memory, whichever is reached first. |
| Registered, pending, or cleaning-up filters | 32 per session; dropping a guard does not release its slot until cleanup completes or the session closes. |
| Tracked partition identities | 4,096 per session. |
| Native version-range intervals | 4,096 per session. |
| Page processing | One worker per session; at most 32 queued pages, charged to the same 8 MiB delivery budget. |
| Queued reply/control records | 32, with admission independent of ordinary outgoing requests. |
| Automatic cleanup commands | 32, with reserved admission independent of user-update delivery. |

Apply existing frame, object, string, collection, pending-request, and started-I/O
limits as well. Account for decoded allocations and processing copies, not just
wire bytes. A page that cannot fit as a complete admission transaction exceeds
this supported profile even if a native client would accept it.

Never evict version/partition state silently to stay within a limit. Queue
overflow yields a distinct notification-lag/resource error; bookkeeping
exhaustion yields an explicit resource-limit error. Both terminate this initial
session instead of claiming an uninterrupted subscription.

### 6.2 Receive and shutdown

`recv` is local and cancellation-safe: dropping an in-progress receive does not
dequeue an update or unregister filters. Callers can wrap it in
`tokio::select!` or `tokio::time::timeout`; there is no remote timeout to attach.

Terminal state has storage independent of the bounded update queue. On a
session failure, the next receive returns one explicit error and subsequent
receives return `None`. Buffered updates may be discarded on failure; the
terminal error invalidates any assumption that the stream remained complete.
Normal explicit shutdown ends with `None`, not a fabricated update.

`close(self, timeout)` stops new filter operations, takes responsibility for all
remaining registrations and queued cleanups, attempts deregistration under one
overall deadline, then stops the notification connection and waits for owned
tasks to exit. It returns an error if cleanup failed or timed out; socket
shutdown still proceeds. Dropping `ServiceNotificationClient` initiates local
connection shutdown but cannot await or guarantee remote deregistration.
Closing that dedicated connection releases the remote registration through the
gateway's disconnect path once it detects the disconnect, not an instantaneous
remote-cleanup guarantee.

Closing or dropping either public client does not close the other. A failure of
one client's connection does not retire the other's still-healthy connection.
Registration guards and background tasks must not keep a notification client
running after its owner is dropped, or create an ownership cycle.

Failed or cancelled notification-client construction closes the candidate
connection and stops any started tasks without publishing a partially
initialized client. After a terminal failure, recovery requires constructing
a new `ServiceNotificationClient`; it gets a fresh session with no inherited
filters or continuity guarantee. Old guards and outstanding reply contexts
remain bound to the retired client and cannot attach to its replacement.

### 6.3 Automatic filter cleanup

Rust's `Drop` cannot await a gateway operation. A registration guard's destructor
therefore transfers cleanup responsibility to a bounded, nonblocking session
command path. A session-owned worker sends the native unregister request using
the configured operation timeout. Do not block in `Drop` or spawn an unbounded
task for each dropped guard.

Automatic cleanup guarantees initiation while the session is live, not
immediate remote confirmation. If the command cannot be admitted, the worker
has stopped unexpectedly, or deregistration fails or cannot be confirmed,
record a terminal session error, initiate notification-connection shutdown, and
surface the error through `recv`. Ordinary client operations remain unaffected.
The failure signal must not depend on successfully enqueuing another cleanup
command or finding room in the user-update queue.

For explicit `unregister(self, timeout)`, apply one deadline from submission
through the remote reply. Transfer responsibility to the worker atomically;
disarm the guard's drop action only once another owner guarantees completion
or session shutdown. Successful completion must not schedule a second unregister
when the consumed guard is destroyed. Errors are returned to the caller and,
when cleanup is not confirmed, also terminate the notification session rather
than abandoning a live filter without an owner.

Coordinate explicit unregister, queued drop cleanup, and session close through
one registration state transition. The session maintains cleanup ownership
even if an awaiting future is cancelled. Apply the uncertainty rule in section
6.4 when an already-sent operation cannot be confirmed. Native filter slots stay
accounted for until removal is confirmed or the owning session shuts down.

Closing or dropping `ServiceNotificationClient` cleans up all filters independently
of guard lifetimes. Surviving guards cannot keep a closed connection alive,
reopen it, or unregister a filter in a replacement session. Dropping a guard
whose session has already shut down needs no further request; an explicit
unregister against that closed session returns a closed-session error.

### 6.4 Filter-operation uncertainty and recovery

Before sending, invalid inputs, exhausted local limits, and expired deadlines
fail without creating remote filters. Definitive native registration errors
remain errors rather than successful empty handles.

After sending, timeout or future cancellation can leave a registration or
unregistration outcome unknown. Since the caller might never obtain a handle
for a remotely created filter, conservatively retire this notification session
on such uncertainty. Do not silently retry a mutation or leak an unreachable
subscription. This does not cancel work remotely or affect the ordinary client
connection.

The initial API has no automatic reconnect, durable cursor, filter restoration,
or synthesized catch-up snapshot. On failure, callers construct a new
notification client, register filters again, and fetch any required initial
state again through a separate `FabricClient`.

Native transparent recovery is a separate extension: retain desired filters,
connect with committed generation/version ranges, compare the deleted-entry
watermark, page checks for still-known partition IDs, synthesize required empty
updates, and buffer incoming pages until synchronization completes. That needs
only bounded identity/version state, not an endpoint cache, but it must be
implemented and qualified before promising continuity across reconnects.
[Source: [native two-phase synchronization][notification-sync].]

## 7. Validation and delivery

Implement in independently testable steps: uncached resolution/models; duplex
connection and reply admission; standalone notification client/filters/receiver; live
qualification and public coverage documentation. Notification support does not
depend on adding a resolution cache or implementing the legacy long-poll API.

Offline coverage must exercise:

- Singleton, named, and signed-int64 keys; previous-result metadata; unchanged
  refreshes; native errors; partition identity changes; and version comparison.
- Stateful/stateless roles, missing primary, opaque addresses, empty endpoints,
  unsupported groups, and malformed present fields.
- Notification-only construction, parallel independent notification clients,
  registration-before-reply updates, multiple/overlapping filters,
  session-bound registration guards, and unregister races.
- Reverse request/reply correlation alongside ordinary pending requests,
  fragmentation, duplicate pages, generation changes, and errors in page bodies.
- Atomic page admission, acknowledgment loss/retry, every configured bound, and
  terminal-error delivery when the update queue is full.
- Cancellation before/after sending, receive cancellation, close/drop, failure
  isolation, and absence of stranded tasks, registrations, or permits.
- Guard scope exit, explicit unregister without duplicate cleanup, cancellation
  during cleanup handoff, a full cleanup queue, worker failure, and cleanup
  timeouts, including terminal-error delivery when the update queue is full.
- Session close while guards survive, filter-slot accounting during cleanup,
  and stale cleanup commands that must not affect a replacement client/session.
- Independent public-client close/drop and connection faults in both directions;
  failed/cancelled notification construction must not affect a `FabricClient`.

Live tests use only disposable owned services and subscriptions. Compare
resolved endpoints with independently established deployment state; then
exercise endpoint/role changes, empty updates, exact/prefix/primary-only
filters, and native acknowledgment/retry behavior. Record runtime, gateway
identity, and security profile. An unsecured onebox qualifies only clear TCP;
do not claim TLS or transparent failover from those tests.

Check URI/result compatibility against the pinned schemas and the actual live
runtime separately. Public documentation must continue to distinguish proposed
APIs from demonstrated coverage.

## 8. Source map

| Concern | Native authority |
|---|---|
| Public resolve/filter methods and distinct older callback API | [FabricClient.idl][public-api] |
| Resolution envelope and previous-version metadata | [Body][resolve-body], [request data][resolve-request], [defaults][resolve-request-defaults] |
| Gateway partition selection/cache refresh | [ServiceResolutionAsyncOperationBase][resolve-gateway] |
| Result fields and comparison | [ResolvedServicePartition][resolve-result], [comparison implementation][resolve-compare] |
| Filter flags and notification/page fields | [Filter][notification-filter], [matching][primary-matching], [notification][notification], [page ID][notification-page] |
| Session connection and identity | [Client connection][notification-connect], [request][connection-body], [reply][connection-reply], [gateway proxy][notification-identity] |
| Filter mutation and processing-result bodies | [Register][register-body], [unregister][unregister-body], [processing reply][processing-reply] |
| Cache enrichment intentionally not reproduced | [UpdateServiceTableEntryAsyncOperation][notification-enrichment] |
| Processing, acknowledgment, duplicate handling, and retry | [Client processing][notification-process], [version acceptance][notification-accept], [sender][notification-retry] |
| Native reconnect/deletion synchronization | [Two-phase protocol][notification-sync], [request][synchronization-body], [reply][synchronization-reply] |
| Reverse transport dispatch/correlation | [RequestReply][reverse-request], [DuplexRequestReply][duplex], [ReceiverContext][receiver-reply] |

[native-revision]: https://github.com/microsoft/service-fabric/tree/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d
[public-api]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricClient.idl#L654-L742
[resolve-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ServiceResolutionMessageBody.h#L10-L55
[resolve-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ServiceResolutionRequestData.h#L10-L65
[resolve-request-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ServiceResolutionRequestData.cpp#L13-L68
[resolve-gateway]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeService.ServiceResolutionAsyncOperationBase.cpp#L45-L194
[resolve-result]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ResolvedServicePartition.h#L10-L114
[partition-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/PartitionInfo.h#L10-L118
[resolve-compare]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ResolvedServicePartition.cpp#L122-L211
[notification-filter]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceNotificationFilter.h#L10-L48
[primary-matching]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/MatchedServiceTableEntry.cpp#L27-L60
[notification]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ServiceNotification.h#L10-L65
[notification-page]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/naming/ServiceNotificationPageId.h#L10-L52
[notification-registration-semantics]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L220-L229
[notification-connect]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L637-L705
[connection-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NotificationClientConnectionRequestBody.h#L10-L48
[connection-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NotificationClientConnectionReplyBody.h#L10-L41
[register-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/RegisterServiceNotificationFilterRequestBody.h#L10-L36
[unregister-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/UnregisterServiceNotificationFilterRequestBody.h#L10-L36
[processing-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/ServiceNotificationReplyBody.h#L10-L35
[synchronization-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NotificationClientSynchronizationRequestBody.h#L10-L51
[synchronization-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ClientServerTransport/NotificationClientSynchronizationReplyBody.h#L10-L37
[notification-identity]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManagerProxy.cpp#L81-L125
[notification-enrichment]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/LruClientCacheManager.UpdateServiceTableEntryAsyncOperation.cpp#L35-L95
[notification-process]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L1205-L1306
[notification-accept]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L1577-L1680
[notification-retry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationSender.cpp#L328-L501
[notification-sync]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/ServiceNotificationClient.cpp#L515-L526
[reverse-request]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/requestreply.cpp#L263-L280
[duplex]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/DuplexRequestReply.cpp#L36-L77
[receiver-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ReceiverContext.cpp#L25-L39
[resolve-reply-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/NamingMessage.h#L110-L116
[notification-gateway-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManager.cpp#L1141-L1147
[pointer-vectors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h#L4732-L4753
