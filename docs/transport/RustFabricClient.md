# Rust FabricClient POC

**Status:** experimental native clear-TCP subset. Gateway ping, NameExists,
typed GetNodeList, single-node GetNodeHealth, native CreateName/DeleteName,
and application/application-scoped service lists
have been demonstrated against the repository's Ubuntu onebox running
Service Fabric **11.8.121.4**. This is not a complete FabricClient replacement
and does not support TLS or authentication.

## 1. What is implemented

`mssf-transport` implements bounded Fabric serialization, clear frames and CRCs,
ordered native headers, client-only ListenInstance establishment, and
continuously driven, bounded concurrent Tokio request/reply I/O. `mssf-client`
adds native gateway ping, NameExists, typed node/application/service pages, node health and
single-name creation/deletion, with owned results and explicit errors.

Neither package calls the native Service Fabric client or COM libraries.
Neither uses HTTP to execute client operations. "Clear TCP" still carries the
**native binary Fabric protocol**, not JSON or line-based messages.

| Operation | Native behavior and evidence |
|---|---|
| Gateway ping | Sends PingRequest with an empty body and decodes gateway address, node ID, node instance, and node name. Full identity compared with independent onebox observations. |
| NameExists | Sends a parsed ten-field native URI object; independently established existing/absent names returned true/false on the same connection. |
| CreateName / DeleteName | Send the same direct URI object, with explicit-delete header `0x8033` for deletion. Require actor 5 / `NameOperationReply` and zero body bytes. Unique owned lifecycle independently observed absent → present → absent; native duplicate/missing errors followed by healthy same-connection reuse qualified live. |
| GetNodeList | Sends the external `GetNodeList` string query to NamingGateway using `QueryRequest`; decodes all 21 source-defined node fields, nested deactivation data and explicit paging. Five-node membership/overlapping fields independently checked through management; binary ports checked against the onebox manifest. |
| GetNodeHealth | Sends external `GetNodeHealth` via NamingGateway; validates the HM single-item envelope, aggregate, full events and Event evaluations. Independently injected Warning, false/true/inherited policy, event masks, unknown-node error and healthy same-connection reuse qualified live. |
| GetApplicationList | Native external query with owned application identity/type/version/status/health/parameters/definition; two unique nonempty applications, exact/type/definition filters, parameter exclusion and explicit native continuation qualified live. |
| GetServiceList | Public Rust method sends external **GetApplicationServiceList** for an ordinary application. Owned kind/type/manifest/status/health/persistence/group results; stateless and volatile stateful guest services, filters and real native continuation qualified live. |
| Native faults/errors | Retains native signed error codes and available diagnostics. Malformed GetNodeList continuation produced live `E_INVALIDARG` (-2147024809) or `FMFailoverUnitNotFound` (-2147017462); the same connection remained usable. Other fault forms have offline model-peer coverage. |
| Concurrent operations | Mixed ping, NameExists, node-list and node-health queries on clones sharing one connection; real onebox coverage. Deterministic cancellation/late-reply safety remains model-covered. |
| Idle disconnect | Real native ping through a test-owned proxy, followed by proxy cutoff; closure detected without another operation. |
| Controls and stream lifecycle | Idle ListenInstance, deadlines, fragmentation, backpressure, correlation, cancellation, and close/drop; offline model-peer coverage. |

The client advertises protocol **1.1**. The demonstrated gateway replies with
protocol **1.2** for ping. The observed GetNodeList aggregate success has no
actor, action or client-version header; correlation and typed body validation
remain mandatory. Coverage is qualified only for the observed runtime,
clear-TCP profile, and these operations, not all Service Fabric releases.
GetNodeHealth instead returns actor **HM (16)** and action
**OperationSuccessAction**; it is not the bare list-aggregation reply.

## 2. Run against a disposable onebox

Use the repository's Rust toolchain. A native SDK or C++ build is unnecessary.
The [Ubuntu onebox guide](../UbuntuOnebox.md) supplies Docker prerequisites.
Run from the repository root:

```sh
just onebox-build
just onebox create poc

cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 ping

cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 node-list

cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 node-health N0020 \
  --events-filter 8 --warning-as-error true
```

The example prints the gateway's advertised address and full node identity.
`node-list` prints one page and its continuation token, not an implicit complete
cluster inventory.
The advertised address can be the container's private address rather than the
loopback endpoint used to dial it.

Prepare a synthetic name through **management only**, on your own disposable
cluster:

```sh
curl --noproxy '*' --fail --silent --show-error \
  --max-time 30 --request POST --header 'Content-Type: application/json' \
  --data '{"Name":"fabric:/rust-poc-existing"}' \
  'http://127.0.0.1:19080/Names/$/Create?api-version=6.0'

cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists fabric:/rust-poc-existing

cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists fabric:/rust-poc-absent
```

The naming results should be true and false respectively, provided the second
name has not been created. A native failure never becomes false.
Port **19000** is native TCP; **19080** is HTTP management, not a fallback.
The `--clear` flag is mandatory. Endpoints are IP socket addresses, including
bracketed IPv6, not DNS names or URLs.

To exercise native writes instead of management preparation, use a fresh name
that belongs only to you on your disposable allocation:

```sh
owned_name="fabric:/rust-poc-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists "$owned_name"
# Proceed only after confirming absence; never adopt an existing name.
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 create-name "$owned_name" --timeout-seconds 5
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists "$owned_name"
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 delete-name "$owned_name" --timeout-seconds 5
```

These separate CLI calls are not an ownership journal or automatic cleanup
tool. **After send, timeout, cancellation or disconnect can leave a write's
outcome unknown**, even if the name currently appears absent. Do not blindly
retry or claim nonexecution. Keep ownership until the write is conclusively
settled or your exact disposable allocation is destroyed. The automated
qualification below journals ownership before writes and uses one shared client.

The default connection/per-operation timeout is 30 seconds. Override it with
`--timeout-seconds 5`; only finite positive durations of at least 100 ns that
fit native TimeSpan and the monotonic clock are accepted. Errors use a nonzero
exit code.

Remove only your disposable allocation when finished:

```sh
just onebox destroy poc --confirm poc
```

## 3. Rust API and lifecycle

```rust,no_run
use mssf_client::{FabricClient, DEFAULT_TIMEOUT};

# async fn example() -> mssf_client::Result<()> {
let endpoint = "127.0.0.1:19000".parse().expect("IP socket endpoint");
let client = FabricClient::connect_clear(endpoint, DEFAULT_TIMEOUT).await?;
let clone = client.clone();
let (gateway, exists) = tokio::try_join!(
    client.ping(),
    clone.name_exists("fabric:/app/service"),
)?;
client.close().await?;
# Ok(())
# }
```

The handle is **cloneable**; operations take `&self` and share one connection.
Admission allows 32 operations, including completed results not yet delivered
to their awaiting futures. Excess callers wait within their original deadlines.
There is no automatic reconnect, retry, or replay.

`create_name(&str)` and `delete_name(&str)` return owned `Result<()>`.
`create_name_with_timeout(&str, Duration)` and
`delete_name_with_timeout(&str, Duration)` supply one deadline calculated
**before URI preparation**, through admission and reply decoding. The default
variants use the connection's configured timeout. All four reuse the existing
bounded URI encoder and shared driver; the supported URI grammar is unchanged.
For example, with a unique caller-owned name on a disposable cluster:

```rust,no_run
# async fn write(client: &mssf_client::FabricClient, owned_name: &str) -> mssf_client::Result<()> {
client.create_name_with_timeout(owned_name, std::time::Duration::from_secs(5)).await?;
client.delete_name(owned_name).await?;
# Ok(())
# }
```

Creation can create implicit ancestors or promote an implicit name. Creating
an already **explicitly created** name returns native `FABRIC_E_NAME_ALREADY_EXISTS`
(-2147017792), not an idempotent success. Deleting an absent name returns
`FABRIC_E_NAME_DOES_NOT_EXIST` (-2147017791). Deletion is not recursive and can
reject a nonempty name; the service may remove empty implicit ancestors.
Native codes and available diagnostics remain `Error::Remote`, operation-local
when the stream is healthy. Write success has an empty body, unlike NameExists
despite their shared reply action.

Writes are never automatically retried or replayed. A timeout, cancellation or
disconnect after sending **does not establish that the write failed to execute**:
replication may commit after local waiting stops. Dropping the future or closing
the connection does not roll back a sent write.

Implementation: [write facade](../../crates/libs/mssf-client/src/lib.rs#L85-L136)
and [empty-success decoder](../../crates/libs/mssf-client/src/protocol.rs#L165-L171).
Native contract: [public request construction](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L6881-L6976)
and [gateway create reply](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeService.CreateNameAsyncOperation.cpp#L24-L49).

Each operation has one local monotonic deadline covering encoding, sending,
control dispatch, receiving, and result decoding. Replies match both message
GUID and index. Unknown, duplicate, and late replies cannot complete another
operation; their bodies are discarded and a saturating diagnostic count is
retained. More than 1,024 consecutive unmatched replies retires the connection.

One background driver per connection supervises independent persistent receive,
write, and deadline processing. It continues receiving while idle and while
output is blocked. Supported ListenInstance controls update a peer snapshot.
Transport controls are handled before reverse traffic and ordinary correlation;
unsupported reverse requests fail explicitly and receive no acknowledgment.

Cancellation or a reply-wait timeout removes that caller's pending entry without
closing an otherwise healthy connection. Cancelled/expired queued frames are
discarded before writing. A started frame is finished independently of the
caller, or the entire connection closes: its suffix is never removed from a
reusable stream. Started writes have an independent 30-second local progress
limit: a cancelled/expired caller's deadline cannot truncate that frame or
prematurely fail its neighbors. Partial incoming frames must complete
within 30 seconds of their first byte; an idle connection has no inactivity
timeout. A stalled partial read/write, I/O failure, or malformed frame closes
the connection and resolves pending callers.

Successful operation bodies are decoded on the driver before result delivery.
Malformed success closes the shared connection, even if its waiter has not been
polled again. An ordinary complete native operation failure preserves its error
code without itself poisoning the stream. Cancellation/timeout does not imply
that remote execution was cancelled. Invalid naming input sends nothing.

`is_closed()` observes terminal state, including idle failures;
`terminal_error()` returns the first terminal cause. Later operations surface
that cause rather than reconnecting. `close()` is shared and idempotent, resolves
pending callers and waits for driver exit. Cancelling its wait does not cancel
shutdown. Dropping one clone leaves others usable; dropping the final handle
signals shutdown without an asynchronous join. Use `close()` when release must
be observed before continuing.

`ScheduleClose` conservatively retires immediately, preserving a readable native
fault code; native delayed security-session draining is not implemented.
`Reconnect`, authorization/security controls, and unknown control actions are
unsupported in this clear-only profile and terminate explicitly. This is not
qualification of native secure-session recovery or service notifications.

### Native node pages

```rust,no_run
use std::num::NonZeroU64;
use mssf_client::{FabricClient, NodeQueryDescription, NodeStatusFilter};

# async fn pages(client: &FabricClient) -> mssf_client::Result<()> {
let mut query = NodeQueryDescription {
    status_filter: NodeStatusFilter::UP | NodeStatusFilter::DOWN,
    max_results: NonZeroU64::new(2),
    ..Default::default()
};
loop {
    let page = client.get_node_list(&query).await?;
    for node in page.nodes {
        println!("{} {} {:?}", node.name, node.id, node.status);
    }
    query.continuation_token = page.continuation_token;
    if query.continuation_token.is_none() {
        break;
    }
}
# Ok(())
# }
```

The operation returns an owned `NodeListPage`; callers choose traversal and
overall budgets. `get_node_list_with_timeout` supplies a per-request timeout,
not a whole-traversal deadline. Nodes can change between pages; native
aggregation is best-effort when individual subqueries fail. **No continuation
means paging ended, not proof that every aggregation source answered.**

`node_name` is an exact, case-preserving filter, not a prefix or naming URI.
Empty name/token values are omitted like the native facade. Default status
filter `0` excludes Unknown/Removed, not all non-Up states; `ALL = 0xffff`
bypasses status matching. Other masks combine distinct status **bits**, not
`NodeStatus` discriminants. `from_bits_retain` preserves any UInt32 native mask.
`max_results` is optional `NonZeroU64`, rejected locally above `i64::MAX`.
No limit still allows server byte-size paging: always inspect continuation.
A final page with exactly the requested count can have no token.

The result includes identity, address, node type, versions, status/health,
seed/domains, instance, native ports, uptime/downtime, up/down timestamps,
stopped flag and deactivation tasks/safety checks. `NodeId` is high/low UInt64,
not a UUID; its display matches native hexadecimal numeric formatting.
`NodeTimestamp` preserves signed native 100-nanosecond ticks since 1601 UTC;
zero means native default. Durations remain signed native seconds.
Scalar enums distinguish named `Unknown` from `Unrecognized(i64)`; unrecognized
numbers are retained, not silently coerced. Unsupported polymorphic wrapper/
safety-check tags or malformed field structure fail explicitly and retire the
stream. Nonempty deactivation/safety variants have source/model coverage only.

### Native node health

```rust,no_run
use mssf_client::{
    FabricClient, HealthEventsFilter, NodeHealthPolicy, NodeHealthQueryDescription,
};

# async fn health(client: &FabricClient) -> mssf_client::Result<()> {
let mut query = NodeHealthQueryDescription::new("N0020");
query.events_filter = Some(HealthEventsFilter::ERROR);
query.health_policy = Some(NodeHealthPolicy { consider_warning_as_error: true });
let health = client.get_node_health(&query).await?;
println!("{:?}", health.aggregated_health_state);
for evaluation in health.unhealthy_evaluations {
    println!("{evaluation:?}");
}
# Ok(())
# }
```

`get_node_health_with_timeout` uses one deadline from preparation through driver
decode, like node-list. `NodeHealth` owns its node name, event vector, aggregate
`HealthState` and unhealthy evaluations. This is **actual detail health**, not
`Node.health_state` from GetNodeList. Unhealthy aggregate is successful data;
unknown node returns native `FABRIC_E_HEALTH_ENTITY_NOT_FOUND`
(-2147017705 / 0x80071c17), not an empty healthy result.

The node name is required, case-preserving, 1..=4096 UTF-16 units without NUL.
It is not a naming URI/prefix; HM performs native identity resolution. Empty
names are rejected locally, unlike node-list's optional empty-name omission.

| Request option | Supported semantics |
|---|---|
| `health_policy: None` | Omit ClusterHealthPolicy; inherit native effective cluster policy. |
| `Some(NodeHealthPolicy::default())` | Explicit warning-as-error **false**, overriding cluster policy. |
| `consider_warning_as_error: true` | Treat reported Warning as evaluated Error. Only this node-relevant override is exposed, not cluster aggregate thresholds/application maps. |
| `events_filter: None` | Omit EventsFilter; all otherwise eligible events. |
| `Some(DEFAULT)` / `Some(ALL)` | Explicit masks 0 / 65535; select all. |
| `Some(NONE)` | Exact mask 1; no events, **without** suppressing aggregate/evaluations. |
| `OK`, `WARNING`, `ERROR` | Bits 2, 4, 8, combinable with `\|`. Selection uses **evaluated** state; ERROR under policy true can return reported Warning. |
| `from_bits_retain(u32)` | Sent unchanged. Other bits do not match; exact special-value equality matters: NONE \| ERROR = 9 selects Error, not none. |

No raw JSON/arguments, node-ID selector, paging, health-statistics, child filters
or full cluster-health policy API is exposed. Policy/filter arguments are JSON
**strings within the native binary query map**, not an HTTP request.
See the public types in
[`health.rs`](../../crates/libs/mssf-client/src/health.rs).

Each `HealthEvent` includes source/property, signed TTL ticks, reported state,
description, signed sequence, source/modified timestamps, expiry/removal flags,
and all three transition timestamps. TTL is signed 100ns duration ticks
(`i64::MAX` means infinite); dates reuse `NodeTimestamp`'s signed 1601 epoch
ticks, with zero preserved as native default. No timestamp is synthesized.
Scalar health values retain unknown numbers as `Unrecognized(i64)`.

Supported `HealthEvaluation::Event` includes description, the complete unhealthy
event, applied warning-as-error flag and evaluated aggregate. The pinned
single-node detail path generates Event evaluations, including synthetic HM
reports; it may return a representative explanation, not every unhealthy event.
Other general health evaluation kinds/recursive child trees are **not**
implemented: unknown structural tags, tag/kind disagreement, mandatory nulls
and malformed success retire the shared stream explicitly.

The exact source pin remains
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`, separate from deployed runtime
**11.8.121.4**. Native serialization definitions:
[NodeHealth](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/NodeHealth.h#L38),
[HealthEvent](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/HealthEvent.h#L66),
[EventHealthEvaluation](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/EventHealthEvaluation.h#L48).
The result is Item=1, wrapper tag NodeHealth=16, empty list then mandatory item,
no paging; events and Event evaluations are flattened field scopes, not a
public C API pointer layout or safety-check-style derived scope.

### Native application and service pages

```rust,no_run
use std::{num::NonZeroU64, time::Duration};
use mssf_client::{
    ApplicationListFilter, ApplicationQueryDescription, FabricClient,
    ServiceListFilter, ServiceQueryDescription,
};

# async fn workloads(client: &FabricClient) -> mssf_client::Result<()> {
let mut applications = ApplicationQueryDescription {
    filter: ApplicationListFilter::TypeName("MyApplicationType".into()),
    max_results: NonZeroU64::new(1),
    ..Default::default()
};
let page = client.get_application_list(&applications).await?;
for application in page.applications {
    println!("{} {} {:?}", application.name, application.type_version, application.health_state);
}
// Caller decides whether/when to issue another operation with this opaque token.
applications.continuation_token = page.continuation_token;

let mut services = ServiceQueryDescription::new("fabric:/MyApplication");
services.filter = ServiceListFilter::TypeName("MyServiceType".into());
let page = client.get_service_list_with_timeout(&services, Duration::from_secs(5)).await?;
for service in page.services {
    println!("{} {:?} {:?}", service.name, service.kind, service.status);
}
println!("continuation={:?}", page.continuation_token);
# Ok(())
# }
```

`get_application_list` / `get_service_list` use the connection's configured
default operation timeout. Both have `_with_timeout` variants with one deadline
from preparation through admission, write waiting and driver decode. Neither
automatically pages, retries, reconnects or replays. Native errors stay
`Error::Remote { code, message }` and permit same-connection reuse; malformed
successful results retire the shared stream before pending callers can reuse it.

| Description | Selector and independent controls |
|---|---|
| `ApplicationQueryDescription` | `ApplicationListFilter::All` (default), `Name`, `TypeName`, or `DefinitionKind`; exactly one. Independent `exclude_application_parameters` (default false), continuation and count. |
| `ApplicationDefinitionKindFilter` | DEFAULT=0, ALL=65535; bits SERVICE_FABRIC_APPLICATION_DESCRIPTION=1, COMPOSE=2, MESH_APPLICATION_DESCRIPTION=4, combinable with `\|`. `try_from_bits` rejects unnamed bits except exact ALL. These are **not** result discriminants. |
| `ServiceQueryDescription::new(application)` | Required ordinary application scope; `ServiceListFilter::All` (default), exact `Name` or exact `TypeName`. Independent continuation and count. |

Type selectors are nonempty and case-sensitive; names are exact, not prefixes.
Conflicting selectors are unrepresentable. Selected empty type/name is an error,
not a silent default. Application/service status, health, version and service
kind are not available filters. Ad-hoc/root and exact `fabric:/System` service
scopes use other native routes and are **explicitly rejected** by this API.
Names in requests **and returned URI objects** use the narrow 4096-byte naming
subset below; URI components and segment vectors must agree. This does not
implement the full native URI grammar.

Empty optional continuation is omitted; nonempty tokens are opaque bounded
strings, not node IDs. `max_results` is optional positive `NonZeroU64` at most
`i64::MAX`, the native query-map range, not the COM facade's 32-bit count.
Each call returns one native page. Both message-size and count limits can page;
exactly count rows need not imply a continuation. The count is applied to native
subqueries; a divergent CM/FM **service union can exceed it**. Pages and
entity/health observations are not snapshots. No continuation means paging
ended, not that every aggregation source answered or health was available.

`ApplicationListPage.applications` contains owned `Application` records:
name, type_name/type_version, `ApplicationStatus`, `HealthState`, a sorted owned
parameter map and `ApplicationDefinitionKind`. Parameter exclusion yields an
empty map, indistinguishable from an actually empty map without request context.
Application statuses are Invalid/Ready/Upgrading/Creating/Deleting/Failed.
Definition results are ServiceFabricApplicationDescription/Compose/
MeshApplicationDescription/Invalid (0/1/2/65535); missing optional definition
defaults to Invalid. Deprecated upgrade fields are structurally consumed but
not exposed; nonempty values are an explicitly unsupported result profile.

`ServiceListPage.services` contains owned `Service` records: name, type_name,
manifest_version, `ServiceKind`, `HealthState`, `ServiceStatus`, is_service_group.
Manifest version is not application type version and may be empty.
`ServiceKind::Stateful { has_persisted_state }` distinguishes persistence from
kind; `Stateless` cannot report persisted state. Service statuses are
Unknown/Active/Upgrading/Deleting/Creating/Failed; absent optional status/group
defaults to Unknown/false. Named health Unknown is valid and does not mean Ok.
Unlike the existing node/health scalar policy, unnamed scalar values in these
new lists fail explicitly; existing APIs retain their behavior. Missing optional
suffixes require a legitimate scope end; truncation/wrong metadata is an error.

Application exact-name misses and service misses **within a valid application**
can be empty successful pages. A nonexistent application for service listing
returns a native error (live observed ApplicationNotFound -2147017763), never an
invented empty page.

CLI commands print exactly one owned page, including continuation:

```sh
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 application-list --type MyApplicationType \
  --exclude-parameters true --max-results 1 --timeout-seconds 5
cargo run -p mssf-client --example onebox -- \
  --clear 127.0.0.1:19000 service-list fabric:/MyApplication \
  --type MyServiceType --max-results 1
```

Both accept `--name` instead of `--type` and `--continuation-token TOKEN`.
Application listing additionally accepts `--definition-kind U32` instead of
name/type. No management writes are added to the production client/example.

Native layouts are independently source-derived, not copied from node results:
[ApplicationQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationQueryResult.h#L54-L63),
[ServiceQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceQueryResult.h#L76).
Both use a List result, dynamic wrapper tags 4/2, a null single-item pointer,
native scalar query error and optional untyped paging object. Record names are
ten-field URI objects, not strings; service records are not the C API kind/value
union. Native aggregate reply constructors allow absent actor/action headers;
present headers are validated.

## 4. Supported inputs and bounds

Naming input is literal lowercase `fabric:/` followed by nonempty ASCII
`[A-Za-z0-9_-]+` segments separated by single slashes. Path case and spelling
are preserved. The entire input is limited to 4,096 bytes.

Root names, authorities, Unicode, escapes, query/fragment, whitespace, dot
segments, other punctuation, and trailing/repeated slashes are unsupported
and return an explicit input error. This is a deliberate subset, not a full
native NamingUri parser.

| Resource | Limit |
|---|---:|
| Entire frame | 4 MiB |
| Native header area | 65,535 bytes |
| Header records, including deleted records | 4,096 |
| Object extent, excluding its leading marker | 4 MiB, additionally constrained by the containing frame |
| Object nesting | 32 |
| String | 32,768 UTF-16 code units |
| Byte-array/object-array/type-information count | 4,096 |
| Application/service rows, parameter pairs, URI segments | 4,096 each (the row bound is independent of requested count) |
| Admitted operations, including unconsumed results | 32 |
| Queued output frames, including cancelled commands awaiting disposal | 32 |
| Started write / partial incoming frame | 30 seconds; request expiry ends local waiting, not started frame completion |

The queue is a count bound, not an aggregate memory limit of one frame: 32
maximum-sized queued frames can occupy 128 MiB. Memory also includes active
encoding and frame copies, one active writer, one incoming frame and decoded
results. Admission and queue capacity are acquired before transport frame
encoding, and admission follows each result through delivery. Facade-prepared
URI/query bodies and activity/version headers already exist before admission; waiting
calls can own these bounded buffers. Cancelled commands retain no admission but
remain within the queue bound until discarded. Returned values and caller-created
input buffers remain the caller's responsibility.

The decoder skips unknown/deleted header records by their bounded record
lengths. Effective common headers use the last visible occurrence; first
typed version lookup remains first-match. Supported appended values/scopes
are typed-skipped, not found by searching for delimiter bytes. The bounded codec
supports nullable object pointers, typed objects, object arrays, UInt16 and
base/derived scopes needed by node results.
Unsupported trailing metadata, no-copy extensions, or object flags fail
explicitly. General native forward compatibility is not claimed.

### 4.1 Native Serde payloads

`mssf_transport::serialization::serde` provides `to_vec` / `from_slice` over
the existing bounded native codec, not a JSON, bincode or postcard encoding.
Compatible ordered wire DTOs can use ordinary Serde derives:

```rust
use mssf_transport::serialization::serde::{from_slice, to_vec, Guid};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ListenWire {
    address: String,
    instance: u64,
    nonce: Guid,
}

let value = ListenWire {
    address: "node".into(), instance: 7, nonce: Guid([0; 16]),
};
let bytes = to_vec(&value).unwrap();
assert_eq!(from_slice::<ListenWire>(&bytes).unwrap(), value);
```

| Mapping | Native behavior |
|---|---|
| bool, u16, i32/u32, i64/u64, str/String | Existing typed metadata, compressed integers and strict little-endian UTF-16. |
| Named struct / multi-field tuple struct | One ordered native object. Fields follow declaration order; field names/renames do not appear on the wire. |
| Newtype struct | Transparent; `struct Wrapper(T)` does **not** add an object. Use a named one-field struct when an object wrapper is required. |
| `Guid([u8; 16])` | Native GUID metadata and Windows field-order bytes, e.g. `Uuid::to_bytes_le()`, not UUID network order or byte-array metadata. |
| `ByteArray<T>` | Native byte-array metadata; encode Vec/slice/array bytes, decode `ByteArray<Vec<u8>>`. |
| `ObjectArray<T>` | Native object-array metadata even when empty; encode a Vec/slice of struct-shaped elements, decode `ObjectArray<Vec<Row>>`. |
| `to_fields` / `from_fields` | Fixed ordered struct fields inside an already-open codec object, without introducing or closing an object/scope. |
| `bitflags` with Serde derives | Underlying supported integer, without an object wrapper or flag-name string; unknown bits survive decoding. Match the native field's width and signedness. |

Scalar roots are native scalar fragments; struct roots include object framing.
`from_slice` requires complete root consumption. Decoding uses owned strings
and bytes, not borrowed UTF-8 views of native UTF-16. Existing limits above
apply; 4 MiB is a **per-object** extent limit, not an aggregate limit on a
standalone root object array. Transport frames retain their separate limit.
`collect_str` formats through a bounded sink that checks UTF-16 units before
buffering each chunk, then uses the same native string encoder.

`NodeStatusFilter`, `HealthEventsFilter` and `ApplicationDefinitionKindFilter`
are Serde-enabled `bitflags` types backed by u32. They support standard bitwise
operators and methods such as `contains`, `insert` and `remove`. Node/health raw
constructors now use `from_bits_retain`, replacing the previous infallible
`from_bits`. Their unnamed all-bits mask preserves arbitrary UInt32 values
through bitwise operations. Use the native `ALL = 65535` constant when requesting
its special behavior: bitflags' `all()` is `u32::MAX` for those two types.
`HealthEventsFilter::NONE = 1` is not an empty bitset.

For application definition filters, use `try_from_bits` (the replacement for the
previous Result-returning `from_bits`) to check combinations of bits 1/2/4 or
exact ALL. The generated bitflags `from_bits` returns Option and is not that
domain validator: ALL makes every low bit known to bitflags. Query encoding
rechecks the strict rule even for values created through Serde, raw constructors
or operators. Invalid masks fail locally before sending a request.

Enable the `bitflags` crate's `serde` feature and derive Serialize/Deserialize
on additional flag types. The backend's non-human-readable mode makes them use
integer masks directly. Human-readable Serde formats such as JSON instead use flag-name
strings; query-map filters still explicitly encode numeric decimal values,
including numeric `HealthStateFilter` inside the EventsFilter JSON string.

Generic Vec/array/tuple/map, Option, enums, unit, floats, chars and other integer
widths are unsupported unless used through an explicit native adapter.
Ordinary enum indices are not native signed-64-bit codes: wire DTOs use i64
with operation-specific conversion. Empty container kind is never inferred
from the sequence length or first element.

Pointers, type information, recognized polymorphic kinds, base/derived scopes
and compatibility suffixes remain explicit `Encoder`/`Decoder` operations.
Bridge their compatible ordered bodies with `to_fields` / `from_fields`;
validate tags/kinds and use `is_scope_end()` for genuinely absent suffixes.
An Option/default is not a substitute for pointer metadata or an absent
trailing field. `from_fields` leaves following fields available and does not
finish the enclosing object. Encoder errors roll back the failed bridge's
bytes; decoder errors do not rewind input.

Only fixed positional schemas are wire-preserving. Do not apply flatten,
skip/skip_serializing_if, default or enum-tagging attributes to native DTOs.
The backend rejects observable unsupported calls, but cannot detect every
attribute or custom serializer that hides a positional shape change.
Serialization custom errors are bounded static `InvalidInput` diagnostics;
deserialization custom errors are `Protocol`, retaining malformed-success
connection retirement. Codec errors retain their existing categories.
Supported unknown trailing values/scopes are skipped, not retained for
lossless re-emission.

Production adoption includes listen-instance controls and common header
writers, shared query-request DTOs, ping/NameExists replies, health/event
fields and application/service scalar blocks/parameter pairs. Header record
framing/duplicate rules are unchanged. Query argument
order remains the supplied slice order; QueryAddress is not added to the body.
Naming URI's inline string-vector, query-result pointer/tag policy, node-list
structural polymorphism, strict list conversions/deprecated fields/suffix
defaults, and failure-detail fallbacks remain explicit manual boundaries.

See the [backend contract and compiling example][native-serde],
[transport payloads][serde-messages], [query DTOs][serde-query],
[health conversion][serde-health] and [list conversion][serde-lists].

[native-serde]: ../../crates/libs/mssf-transport/src/serialization/serde.rs#L1-L198
[serde-messages]: ../../crates/libs/mssf-transport/src/message.rs#L12-L123
[serde-query]: ../../crates/libs/mssf-client/src/query_protocol.rs#L13-L39
[serde-health]: ../../crates/libs/mssf-client/src/health_protocol.rs#L71-L151
[serde-lists]: ../../crates/libs/mssf-client/src/list_protocol.rs#L195-L312

## 5. Offline and native qualification

Use nextest 0.9.146, pinned in CI. The repository requires at least 0.9.133 for
test-group filters. Ordinary checks need neither onebox nor native client
libraries:

```sh
cargo nextest run --locked -p mssf-transport -p mssf-client \
  -E 'not group(onebox)' --no-tests fail
cargo clippy --locked -p mssf-transport -p mssf-client --all-targets -- -D warnings
cargo doc --locked -p mssf-transport -p mssf-client --no-deps
```

**Live onebox exchanges are the wire-compatibility authority.** Offline tests
exercise parser bounds, malformed input, internal round trips, and connection
lifecycle behavior; they do not establish native protocol correctness.
There are no committed golden packet hex files or normalized capture snapshots.
Native compatibility claims require the self-contained live scenarios below
to pass against the real onebox.

The native Serde migration was qualified with independent old/manual-versus-new
encoded-byte comparisons for every migrated encoder family, malformed and
missing-versus-truncated suffix regressions, exact/over-bound adapter tests,
and all four live scenarios below on SF **11.8.121.4**. The qualification used
a uniquely owned unpublished onebox and a nonroot Ubuntu24.04 archive consumer
sharing only that runtime's network namespace, with read-only checkout/root
and private writable evidence. All four passed; owned fixture cleanup and
independent allocation/container/volume/network/private-tag removal were
verified. This does not extend compatibility to untested native schemas or
make test-only REST preparation part of the production client.

Tests that need onebox are named `onebox_*` and assigned to the shared
**`onebox` nextest group** in `.config/nextest.toml`. They remain `#[ignore]`
for ordinary Cargo runs. The group permits only one test at a time; any
scenario may run first. Each test prepares its own fresh independent
observations and completes its cleanup before returning. Setup and restoration
are helper functions, not tests with ordering dependencies. Select the group
once; no individual test-name selectors or manual preparation are needed.

On a fresh disposable local onebox, provide absolute private output paths and
a unique fixture owner. The baseline helper uses test-only REST
calls to prepare synthetic names and independently observe healthy five-node
membership. Health restores its own report; names journals its own unique entry:

```sh
mkdir -p build/poc-native
export MSSF_TEST_CONFIG="$PWD/build/poc-native/config.json"
export MSSF_TEST_REPORT="$PWD/build/poc-native/report.json"
export MSSF_HEALTH_CONFIG="$PWD/build/poc-native/health-config.json"
export MSSF_HEALTH_REPORT="$PWD/build/poc-native/health-report.json"
export MSSF_HEALTH_OWNER="poc-unique-task-id"
export MSSF_NAME_CONFIG="$PWD/build/poc-native/name-config.json"
export MSSF_NAME_REPORT="$PWD/build/poc-native/name-report.json"
export MSSF_NAME_OWNER="poc-unique-task-id"
export MSSF_LIST_CONFIG="$PWD/build/poc-native/list-config.json"
export MSSF_LIST_REPORT="$PWD/build/poc-native/list-report.json"
export MSSF_LIST_OWNER="poc-unique-task-id"
cargo nextest archive --locked -p mssf-transport -p mssf-client \
  --archive-file build/poc-native/native-client-tests.tar.zst
export MSSF_TEST_ARCHIVE_SHA256="$(sha256sum build/poc-native/native-client-tests.tar.zst | cut -d ' ' -f 1)"
cargo nextest run --archive-file build/poc-native/native-client-tests.tar.zst \
  --run-ignored all -E 'group(onebox)' --no-tests fail
```

Preparation uses the published local HTTP gateway at `127.0.0.1:19080` and records
the first native gateway at `127.0.0.1:19000` / N0010. Every execution leg
automatically refreshes its observations before native assertions. The baseline's
read-test prerequisite accepts HTTP conflict followed by an independent existence
check; unexpected HTTP results fail. Node observation timestamps
must be no more than 120 seconds old and not future-dated at native test start.
Proxy use, redirects, and automatic request retries are disabled. No additional
CI job is needed. `openapi-service-fabric` 0.1.1 and `reqwest` 0.13 are
dev-dependencies with default/TLS features disabled, not production client
dependencies. The generated client handles node observations, successful
application reads, image-store/application-type observations, and health report
submission. Existing strict required-field checks and evidence schemas remain
in place; generated optional response fields are not defaulted into success.

Some HTTP calls deliberately remain handwritten until the SDK/specification gaps
are fixed: binary uploads (no body parameter), legacy API 6.0 provisioning,
service and health reads (incorrect polymorphic tags/missing subtype fields),
and application absence (documented empty 204). Exact-status name operations and
owned mutations also retain raw response checks: generated unit-returning methods
discard successful status codes and accept redirects. These are explicit
test-only paths, never automatic fallbacks from failed SDK calls. Health reports
use documented ISO-8601 TTL strings, and bodyless cleanup calls omit JSON bodies.

The baseline helper writes a private JSON file with the following fields;
the native assertions consume this independent observation, not their own
query results:

| Field | Meaning |
|---|---|
| `profile` | Exactly `"clear"`. |
| `gateway` | Native IP socket endpoint, such as `"127.0.0.1:19000"`. |
| `runtime_version` | Actual code version independently observed from the target. |
| `existing_name`, `absent_name` | Distinct supported names independently confirmed existing/absent. |
| `expected_node_name` | Gateway node name, e.g. `"N0010"` for the published first gateway. |
| `expected_node_id` | Canonical lowercase hexadecimal native node ID. |
| `expected_node_instance` | Numeric native node instance. |
| `observed_at_unix_ms` | Independent observation start time; maximum age 120 seconds. |
| `expected_nodes` | Five unique healthy node observations in management JSON shape: identity, address/type/versions, status/health/seed/domains, durations/timestamps, deactivation and stopped state. |
| `report_path` | Absolute path for a sanitized JSON result, in an existing private directory. |

Use HTTP `GET /Nodes?api-version=6.3`, following continuation, for independent
node/version/field observations. `InstanceId` and durations are decimal strings.
Verify fixture state
independently using `GET /Names/NAME?api-version=6.0`; absent is HTTP 404.
HTTP is fixture preparation/observation only. Native results are obtained
exclusively through the Rust native connection.

Use **absolute paths** for all configuration/report outputs; integration tests
run from the package directory. Profiles are generated inside each scenario,
not reused as an externally ordered prerequisite.

Tests fail on missing/wrong configuration, inaccessible native gateways or unmet
prerequisites. The baseline scenario writes a result
only after node field/filter/paging/error assertions, sequential ping/true/false,
32 initially Pending overlapping mixed operations on clones (14 ping, 6 existing
names, 6 absent names, 6 node queries),
clone-drop survival, explicit close and a test-owned proxy idle-disconnect case
pass. The proxy first proves a real native ping, then cuts the TCP connection;
no new application operation is used to detect the failure. It does not restart
onebox or qualify native failover/control emission. Runtime metadata comes from
independent preparation; ping does not advertise the server's code version.

On the qualified healthy five-logical-node, single-host **11.8.121.4** profile,
page limits 1 and 2 yielded lengths **[1,1,1,1,1]** and **[2,2,1]**, with four
and two server-issued continuations. Their strictly NodeId-ordered unique unions
matched management membership. Exact-name hits, case/prefix/nonexistent misses,
default/All/Up/Down masks, empty input omission, full terminal pages and valid
continuation after the final independent ID were checked. Stable fields match
exactly; duration observations allow at most 120 seconds of sequential skew,
timestamps match management's millisecond precision (<10,000 ticks), and
deactivation/health must match the stable healthy prerequisite.
Reports retain independent management observations **and** native durations,
raw timestamp ticks and native observation start/end times, not just pass flags.

Malformed-token errors are runtime-dependent even on this healthy profile:
independent HTTP probes also returned both E_INVALIDARG (400) and
FMFailoverUnitNotFound (500). The native parallel-query handler returns the
last error when every subquery fails
([pinned source](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/query/QueryMessageHandler.cpp#L212-L225)).
The qualification test accepts these two specifically observed native codes,
records the actual code/diagnostic, and requires healthy queries before/after.
The client neither normalizes this variability nor retries it.

This does **not** qualify non-Up transitions, populated safety/deactivation
states, partial aggregation failures, byte-size-limited paging, membership
changes during traversal, unknown numeric values on a real runtime, other
versions or multi-host behavior. Those bounds are not erased by terminal
paging. Synthetic model peers test nested/unknown/malformed safety behavior,
not native compatibility.

### Independent unhealthy node-health qualification

The health test uses independent management preparation helpers before its
native assertions:
POST `/Nodes/N0020/$/ReportHealth?api-version=6.3&Immediate=true` and
GET/POST `/Nodes/N0020/$/GetHealth?api-version=6.3`.
HTTP event selection uses `EventsHealthStateFilter`, not the native map name.
No production query uses HTTP.

Preparation owns a unique non-System source/property, positive sequence,
30-minute TTL and remove-on-expiration. It persists ownership **before**
submission and polls independent false/true policy observations up to 60 times
with one-second spacing and ten-second per-HTTP-call timeout. Submission is not
ingestion. A genuine matching Warning Event explanation and false=Warning /
true=Error aggregates must converge before any native health assertion.
Omitted policy is observed separately, not assumed to equal false.

Private `MSSF_HEALTH_CONFIG` records clear endpoint/runtime, owner/source/
property/sequence/TTL, report path, archive SHA256, independent unknown-node
HTTP code, and 17 snapshots with node/policy/mask and start/end times. Snapshots
cover healthy, inherited/false/true, omitted/default/all/NONE/OK/WARNING/ERROR,
combined, NONE|ERROR and raw high-bit masks. The assertion phase uses only
native TCP to compare full event/evaluation data and independently recorded
membership. Report identity/state/sequence/TTL/flags/descriptions are exact;
event order is not significant. Snapshot age is at most 120 seconds, never
future-dated. Source/transition times match HTTP's millisecond rendering
(<10,000 ticks); only LastModified may advance from its independent value up
to the native query end +1ms. This is an actual observation window, not a blanket
120-second timestamp tolerance.

The sanitized success report retains independent snapshots and native raw
signed ticks/sequence as decimal strings, full Event explanations, native
observation windows and archive hash. It also records unknown-node
HealthEntityNotFound followed by healthy same-connection reuse, 32 mixed
ping/list/health operations, and reuse after a first-polled health future is
dropped. That live cancellation does not prove wire emission on fast loopback;
model tests deterministically cover cancelled/timed-out late malformed replies.

The health test always awaits owned restoration after preparation/native
errors or assertion panics. It supersedes only its report with higher-sequence
Ok/one-second TTL, then independently polls until the report is gone and both
detailed health and node-list health are Ok. This permits the baseline test to
run before or after it. Primary and restoration failures remain visible, and
success evidence is written only after restoration succeeds. Ownership mismatch
fails instead of adopting someone else's profile. A rerun removes stale success
evidence, restores its prior owned report, then replaces the profile.

Keep all profiles/reports ignored. Always destroy only your disposable
allocation by exact name, including after a killed test process: in-process
restoration cannot run after a hard kill. CI retains its always-run allocation
teardown. Never use these write fixtures on a shared cluster.

Live coverage is one five-logical-node, single-physical-container runtime.
Nonempty Warning and warning-as-error Error are qualified; separately reported
Error, expired nonremoved reports, synthetic HM conditions, future evaluation
kinds and other runtimes are not live-qualified. Their source/model coverage
must not be described as native interoperability.

### Owned native name-management qualification

The name scenario is self-contained and does not use HTTP to create the name
being qualified. It persists a UUID-based, owner-specific, single-segment name
before observation; it confirms initial absence independently with HTTP and
records that proof **before any native mutation**. An existing candidate is
never adopted or deleted. Existing journals are never overwritten, including
same-owner unresolved writes.

On one shared native client it confirms initial absence, creates the name,
checks HTTP/native presence, requires duplicate-create error -2147017792 and
healthy reuse, deletes the name, checks HTTP/native absence, then requires
missing-delete error -2147017791 and healthy reuse/ping. Management HTTP is only
an independent observation and owned-cleanup oracle: GET `/Names/NAME` uses
200/empty 404 statuses; cleanup uses DELETE `/Names/NAME`, API version 6.0.
It is never a production fallback.

The private `MSSF_NAME_CONFIG` journal contains the exact owner, canonical UUID,
derived name, archive SHA256 and stage. Pending is persisted before each write.
Only a conclusive success or the specifically expected duplicate/missing
rejection settles it. Timeout, cancellation, disconnect or unexpected failure
retains ownership. Cleanup can best-effort delete only that pre-owned name but
**cannot release an uncertain write merely because it observes absence**:
a delayed create can still commit. Such cleanup reports uncertainty and retains
the journal until proven completion or verified exact-allocation destruction.
Preparation/native errors and assertion panics pass through the shared
cleanup-before-report wrapper. A hard kill/external cancellation can bypass
in-process cleanup; exact-allocation always-run teardown remains essential.

`MSSF_NAME_REPORT` is written only after settled owned cleanup succeeds. It
records independent lifecycle observations, native results/error codes,
same-client reuse, runtime and archive identity. Keep journals private/ignored;
after exact destruction, remove private profiles and extracted consumers.
Never use these fixtures on a shared cluster.

The native-name archive qualified on **11.8.121.4**, five logical nodes in one
Ubuntu container, with **119 offline tests and all three live group scenarios**.
Offline tests cover exact requests, malformed-success retirement, concurrent
reordering, peer-observed cancellation/timeouts without replay, partial setup,
collision/foreign-owner refusal, errors/panics, cleanup failure and
absence-then-late-create ownership retention. They do not establish runtime
compatibility on their own. This live lifecycle does not qualify recursive or
property operations, nonempty-name deletion, implicit-ancestor promotion,
arbitrary runtime versions, TLS, retry/reconnect or remote completion after a
real transport failure.

### Owned nonempty application/service qualification

The list scenario provisions one unique type/version and **two** unique
applications, each with a stateless and volatile stateful implicit-host guest
service. Its test-only HTTP upload includes inert package contents and native
`_.dir` commit markers bottom-up. It provisions synchronously, creates the
applications, and waits for independently observed Ready/Active/Ok before native
assertions. This qualifies stateful **kind**, not durability or replication of
application data; persisted-state=true remains source/model-covered only.

The four-slot private `MSSF_LIST_CONFIG` journal binds owner, canonical UUID,
archive SHA, exact upload root, type/version 1.0 and both application identities.
All initial absences must be observed before any mutation; exact application
lookup uses 204 NoContent for no match. Collisions are not adopted. Pending is
durably recorded before upload, provision, create and cleanup mutations.
Unexpected/ambiguous outcomes remain Pending: even observed absence cannot
release them. Destroy only the exact-owned disposable allocation to resolve
such uncertainty. No prefix scan, shared-resource cleanup or automatic mutation
retry occurs.

Native checks require both applications and four services, compare identity,
type/version, status/health, parameters, kind/persistence and group information
with independent management observations, exercise exact/type/definition
selection and parameter exclusion, require **real continuations** at count 1
for applications and services, and follow them in distinct calls. Mixed
application/service/node/ping operations and reuse following missing-application
native error also run on one shared connection. These are not empty-list-only
compatibility claims.

Cleanup deletes only the journal's exact applications, waits for absence,
unprovisions its exact type/version, then deletes its upload root. A success
report is written only after cleanup. Preparation failures, assertion panics
and cleanup errors use the same cleanup-before-report harness; Pending entries
block success. File sync and rename run on every platform; only directory fsync
is gated to Unix. A hard kill can skip in-process cleanup, so always-run exact
allocation teardown remains essential.

The **11.8.121.4** archived group now has four live scenarios and passes all four.
The application/service delivery also passes 148 offline tests plus four CLI
parser tests. Offline evidence covers strict suffixes, hostile bounds, enums,
malformed successes, native errors, out-of-order mixed queries, deadlines,
ownership collisions, ambiguous mutation retention and cleanup dependency order.
Live coverage does not establish Compose/Mesh applications, service groups,
persisted stateful data, upgrade/deletion transitions, partial aggregation,
byte-size paging, divergent unions, other runtime versions or full URI grammar.

### Build once, run the archive on onebox

The Linux Rust job compiles the selected packages' test binaries, including
ignored onebox cases, into a Zstandard-compressed tar archive:

```sh
cargo nextest archive --locked -p mssf-transport -p mssf-client \
  --archive-file build/native-client-tests.tar.zst
cargo nextest run --archive-file build/native-client-tests.tar.zst \
  -E 'not group(onebox)' --no-tests fail
```

CI uploads that archive after the Rust checks pass. The Linux lifecycle matrix
depends on the producer; only its onebox entry downloads and consumes the
archive. No additional job is created. Both runners use Ubuntu 24.04 and the
same pinned nextest version.

On the target, check out the same source revision and provide the absolute
private output paths as above. Rust and Cargo are not required to
execute the archive; use the standalone runner:

```sh
archive=/absolute/path/native-client-tests.tar.zst
set -e
export MSSF_TEST_ARCHIVE_SHA256="$(sha256sum "$archive" | cut -d ' ' -f 1)"
export MSSF_HEALTH_CONFIG="$PWD/build/poc-native/health-config.json"
export MSSF_HEALTH_REPORT="$PWD/build/poc-native/health-report.json"
export MSSF_HEALTH_OWNER="poc-unique-task-id"
export MSSF_NAME_CONFIG="$PWD/build/poc-native/name-config.json"
export MSSF_NAME_REPORT="$PWD/build/poc-native/name-report.json"
export MSSF_NAME_OWNER="poc-unique-task-id"
export MSSF_LIST_CONFIG="$PWD/build/poc-native/list-config.json"
export MSSF_LIST_REPORT="$PWD/build/poc-native/list-report.json"
export MSSF_LIST_OWNER="poc-unique-task-id"
# MSSF_TEST_CONFIG / MSSF_TEST_REPORT must also be set, as above.
cargo-nextest nextest run --archive-file "$archive" --workspace-remap "$PWD" \
  --run-ignored all -E 'group(onebox)' --no-tests fail
```

These commands extract and execute prebuilt tests without rebuilding them.
`--workspace-remap` handles a different checkout path. Empty selections fail
rather than silently reporting success. Only the four sanitized native success
reports and compiled test archive are uploaded, never the private configurations,
raw runtime logs, or credentials.
Every host or compiler-free consumer leg uses this single group command;
each test automatically repeats its independent preparation and restoration.
The native-name archive was exercised offline and on the real onebox through
the standalone host consumer, without compilation during either consumer leg.
Earlier read/health archives also ran in an Ubuntu 24.04 consumer container with
standalone nextest and no cargo/rustc in PATH, using host networking for loopback
gateways, read-only source and a private writable evidence directory. That
historical container-consumer result is not a new native-name qualification.
No compiler/SDK installation or separate CI infrastructure is required.

## 6. Deliberate exclusions

No TLS/rustls, authentication, HTTP operation fallback, Windows-specific
integration, native client wrapper, full URI/API coverage, service resolution,
notifications, automatic recovery/replay, or production failover is provided.
The onebox is privileged and unsecured: keep it local with loopback-only
published endpoints. Do not use this profile on a production or shared cluster.

The [production design](design/FabricClient.md) and
[broader implementation plan](design/ImplementationPlan.md) remain separate.
This POC does not complete their production lifecycle or TLS requirements.
