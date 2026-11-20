# Rust FabricClient POC

**Status:** experimental native clear-TCP subset. Gateway ping, NameExists,
typed GetNodeList, single-node GetNodeHealth, native CreateName/DeleteName,
application/application-scoped service lists, service partitions,
partition replicas/stateless instances, node-local deployed applications,
service packages, code packages and deployed replicas, uncached partition
resolution, and independently owned service notification sessions
have been demonstrated against the repository's Ubuntu onebox running
Service Fabric **11.8.121.4**. This is not a complete FabricClient replacement
and does not support TLS or authentication.

**Client migration:** Ping, NameExists, CreateName/DeleteName, cluster/topology/deployment
lists, node health, MovePrimary and resolution are implemented only by `FurpcClient` over furpc. Their old `FabricClient`
methods, `PingRequest`, `NameRequest`/`NameOperation`/`NameResponse` and legacy query `ClientRequest`
implementations have been removed. There is no compatibility wrapper or fallback.
`FabricClient`, borrowed `GatewayClient`, `Client`/`ClientRequest` contracts,
`ClientOptions`/`ClientLimits` and typed retained-allocation accounting are retired.
ServiceNotificationClient owns its **own furpc Channel/native session**; it
does not adopt an ordinary client's channel or lifecycle. Authorized furpc
ingress and `server::Rpc::unary` decode notifications and return typed ACK data;
the runner owns native correlation and original-generation ACK writes.
Session/filter RPCs reuse the gateway adapter on the original furpc Peer.
The legacy notification adapters, receiver/reply-context plumbing and allocation
ledger are removed. Only ImageStoreClient retains an independent legacy
GatewayChannel with registered raw ACK actors.
Rust error/admission/lifecycle changes below
are deliberate API breaks, not compatibility wrappers. Historical source/wire
evidence in later sections is not a substitute for current migration receipts.

Native ImageStore v1.2 package copy has also qualified on **11.8.121.4**:
native SDK predecessor/Rust replacement comparison, completion markers,
provisioning/creation, independent live application contact and deployed Data
SHA256 comparison passed. Repeated native SDK copy reliability is not claimed.

**Testing:** `fube-client` no longer contains synthetic native-wire fixtures or
model-peer tests. Non-wire JSON, validation and state tests, native onebox
scenarios and independent report validators remain. Historical offline-wire
coverage descriptions below refer to retired tests, not current qualification.
The fubst and furpc crates retain their own codec/transport tests.

## 1. What is implemented

### Separate furpc-backed client

`FurpcClient` is the canonical implementation of thirteen read-only native operations
and three mutations: CreateName, DeleteName and MovePrimary.
There is no remaining ordinary legacy façade:

| Method | Typed request body | Typed response body |
|---|---|---|
| `ping` | `()` (zero native bytes) | `GatewayDescription` |
| `name_exists` | `NamingUri` | `bool` |
| `create_name` | `NamingUri` | `()` (zero native bytes) |
| `delete_name` | `NamingUri` | `()` (zero native bytes) |
| `move_primary` | `MovePrimaryQueryDescription` | `()` (empty native QueryResult) |
| `resolve_service_partition` | `ResolutionRequest` | `ResolvedServicePartition` |
| `get_node_list` | `NodeQueryDescription` | `NodeListPage` |
| `get_application_list` | `ApplicationQueryDescription` | `ApplicationListPage` |
| `get_service_list` | `ServiceQueryDescription` | `ServiceListPage` |
| `get_partition_list` | `PartitionQueryDescription` | `PartitionListPage` |
| `get_replica_list` | `ReplicaQueryDescription` | `ReplicaListPage` |
| `get_deployed_application_list` | `DeployedApplicationQueryDescription` | `DeployedApplicationListPage` |
| `get_deployed_service_package_list` | `DeployedServicePackageQueryDescription` | `Vec<DeployedServicePackage>` |
| `get_deployed_code_package_list` | `DeployedCodePackageQueryDescription` | `Vec<DeployedCodePackage>` |
| `get_deployed_replica_list` | `DeployedReplicaQueryDescription` | `Vec<DeployedReplica>` |
| `get_node_health` | `NodeHealthQueryDescription` | `NodeHealth` |

Each method accepts `furpc::Request<T>` and an absolute deadline, and returns
`furpc::Response<U>`.
It follows tonic's generated-client style: `FurpcClient<T = furpc::Channel>`
wraps a byte-facing Tower service, methods borrow `&mut self`, and callers
explicitly clone cloneable clients for concurrency. `new(transport)` is local
and inert; Endpoint still owns connection setup. Custom transports need not be
Clone, Sync or static. Native body schemas, validation and public models are
reused without legacy typed-request implementations. No legacy Message, channel,
typed budget or lifecycle is adopted.

```rust,no_run
use fube_client::{FurpcClient, NamingUri, NodeQueryDescription};
use furpc::Request;
use std::time::Duration;
use tokio::time::Instant;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let channel = FurpcClient::endpoint_clear("127.0.0.1:19000".parse()?)?
    .connect().await?;
let mut client = FurpcClient::new(channel);
let deadline = Instant::now() + Duration::from_secs(30);
let gateway = client.ping(Request::new(()), deadline).await?.into_body();
let name: NamingUri = "fabric:/Example".parse()?;
let exists = client.name_exists(Request::new(name), deadline).await?.into_body();
let page = client.get_node_list(
    Request::new(NodeQueryDescription::default()), deadline,
).await?.into_body();
# let _ = (gateway, exists, page);
# Ok(())
# }
```

The stub owns `RpcClient<GatewayService<T>>`. The gateway byte-service adapter
forwards readiness to that exact transport instance, adds the remaining Timeout
header after readiness, and captures reply validation in each call's future.
The method selects its validator through `RpcClient::get_mut` before invoking
the RPC; validators are neither inferred from an edited reply nor shared between
clones. The adapter validates the reply envelope or decodes
ClientOperationFailure before the success body codec. Ping uses the standard
`FubstCodec<(), PingReplyBody>`: fubst's root unit maps to zero body bytes, while
the reply uses its native object schema. Nested unit values remain unsupported
and empty structs retain native object framing.
Gateway Actor, activity and protocol 1.1 remain method-owned native headers.
NameExists sends the structured native NamingUri and requires NamingGateway's
NameOperationReply. CreateName/DeleteName reuse the same checked URI and reply
envelope, but `FubstCodec<NamingUri, ()>` requires zero success-body bytes.
DeleteName adds native explicit-delete header `0x8033` with `true`; creation
does not add that header. These writes use the same deadline, source/settlement
metadata and error path as the reads, without retry or replay.
GetNodeList uses the shared native query envelope and typed
node schema; aggregate Actor/Action headers are optional but checked when present.
Query error codes remain domain failures, and malformed typed payloads or
disagreeing repeated node IDs cannot be hidden behind a nonzero query error.

All ten read queries and MovePrimary delegate to the private generic `query`
helper and `QueryCodec<Q>`.
Query declarations supply the native name, Serde arguments, parameter checks,
typed reply profile and record validation. No query uses a legacy Message or
typed-request adapter. Profiles retain their own payload tags, list/item/empty shapes,
required or optional Actor/Action headers, and strict or absent paging rules.
The generic path supports paged and unpaged request descriptions without adding
public methods for unmigrated queries.

MovePrimary takes the unpaged `MovePrimaryQueryDescription::new(
MovePrimaryQueryParameters::new(partition_id, target_node))`. Parameters use
native JSON names `PartitionId` and `NodeName`; a nil partition, empty/NUL node,
or target longer than 4096 UTF-16 units is rejected before submission. Target
case and whitespace are preserved. No force/ignore option or paging is exposed.
Success requires the existing RS13/QueryReply empty-QueryResult profile, not
the zero-byte naming-write body. Completion does not establish a placement
transition: observe replicas/discovery separately. Timeout, cancellation and
disconnection after submission can leave the mutation outcome unknown; the
client never retries or replays it.

Application and ordinary application-scoped service lists use strict paging
metadata and optional checked NamingGateway/QueryReply headers. Node health is
an unpaged item requiring Actor 16 and OperationSuccessAction. It preserves the
distinction between omitted and explicit policy overrides, and sends event
filters as numeric-mask JSON. An unhealthy aggregate is successful data.
Partition and replica lists use the same strict list profile with tags 5 and 6.
Their native names are GetServicePartitionList and GetServicePartitionReplicaList;
both accept continuations but reject MaxResults before transport submission.

Node filters and paging retain their native values and empty-string omission
semantics. The new query codec does not adopt legacy standalone string/count
ceilings. Signed native MaxResults, codec byte limits and actual transport
admission still apply. Each call returns one page; only the caller follows
continuation tokens. No typed-retention ledger or second RPC engine is introduced.

The client preserves native Fault, domain failure, malformed-body and protocol
errors separately, including original error sources. Response mapping preserves
headers, native source and settlement metadata. The earlier explicit/request-local
deadline covers encoding, readiness/reconnect, submission and receipt under
furpc's on-time settlement rules; a failed invocation is never replayed.
With the default transport, clones share Channel ownership and pending capacity,
and the final owner drop closes it. This replaces the original immutable
`ping(timeout) -> GatewayDescription` API. The duplicate legacy read APIs have
also been retired; callers must use this typed request/deadline interface.

The existing `onebox_ping_name_exists_and_node_list` test now also sends three
overlapping pings on the new client and compares every full gateway identity with
fresh HTTP management observations.
Its `furpc_ping` and `furpc_management` evidence is required by the existing onebox
report validator; no separate fixture, test partition, or HTTP execution fallback
was added. The generic typed Ping was first requalified against a fresh five-node
Service Fabric **11.8.121.4** onebox. The extended baseline then passed NameExists
true/false, native node identities and stable fields compared with HTTP and the
source-derived binary-port checks, exact-name/status selection, real 2/2/1 server paging, native
invalid-token error and healthy reuse on the original furpc generation, and
concurrent Ping/NameExists/GetNodeList on clones. Reports retain actual node identities, page lengths,
native error provenance and native observation times; offline negative models
reject missing, boolean-substituted or mismatched evidence.

The existing application/service-list and node-health scenarios also qualify
the new client using owned fixtures and independent HTTP observations.
The initial six-method archived run passed nonempty two-application/four-service
comparisons, real 1/1 server paging for applications and each application's
services, selectors and parameter exclusion, and all 17 node-health policy/filter
cases including unhealthy success results. Missing application and node-health
queries retained native errors **-2147017763** and **-2147017705**. Each scenario
checked mixed concurrent clones and healthy reuse on its original furpc generation.
Reports now retain one canonical furpc result plus the independent HTTP
observations, not a second legacy implementation. CI compares complete list
fields, health events/evaluations, TTLs and timestamps directly with those
observations. Only native health-event LastModified timestamps may advance
within the actual observation window. Legacy typed-slot/explicit-close cases
are not imposed on furpc; its own transport suite covers managed lifecycle.
The initial three archived scenarios passed together on Service Fabric
**11.8.121.4**, also requalifying Ping, NameExists and GetNodeList after the
generic-codec refactor.

The partition/replica migration reuses those declarations and retires both
legacy methods and typed-request adapters. Its updated owned list scenario
records canonical furpc partition/replica fields alongside independent HTTP,
checks six partitions and 22 replicas/instances, and verifies native
missing-partition error **-2147017753**, original-generation reuse and concurrent
partition/replica/Ping calls. Movement still uses the legacy writer but observes
replicas through furpc.

The updated archived scenario passed against a fresh five-node Service Fabric
**11.8.121.4** onebox after the deployment migration and synthetic-test removal.
It checked six partitions, 22 replicas/instances, real deployed-application
paging, ten service packages, ten code packages and 22 deployed replicas against
independent HTTP. All topology/deployment responses retained the original furpc
generation. The four deployment methods preserved missing-node error
**-2147017795**, and the existing owned MovePrimary transition and cleanup passed.
CI independently validated the actual list, topology, deployment and movement
receipts, including entry-point statistics and native/REST timestamp-label mapping.

The migrated native name-lifecycle archive also passed on a fresh five-node
**11.8.121.4** onebox. Native create/delete and NameExists matched independent
HTTP observations; duplicate **-2147017792** and missing **-2147017791** remained
domain errors. All successful writes, reads and three concurrent cloned
Ping/NameExists operations retained furpc generation **1**. The actual receipt
passed `validate_furpc_names` after owned cleanup removed the name journal.

The updated archived list scenario also passed on the retained five-node
**11.8.121.4** onebox with MovePrimary entirely on furpc. One constraint-enforced
request moved the captured owned partition from **N0020** to its existing ready
secondary on **N0040**. Independent native/HTTP observations retained the same
replica identities and confirmed the role swap; the mutation response and
subsequent cloned replica/partition/Ping calls all retained generation **1**.
The actual list, topology, deployment and movement receipts passed their
independent validators after all owned fixture resources were cleaned.
This does not qualify force/ignore options, native movement rejections, replay
or remote completion after a real transport failure.

The updated archived resolution/notification scenario passed on the retained
**11.8.121.4** onebox. Eight resolution/previous-result refresh cases covered
singleton, negative/positive signed-int64 and both named keys. Native partition
details and endpoint/role multisets matched independent HTTP, with endpoint
counts **5/3/3/5/3/3/1/1**. Missing service **-2147017779** remained a domain
failure; subsequent concurrent resolution/Ping/node calls retained generation **1**.
The existing endpoint-count change, primary replacement, version progression,
real notification ACK loss/retry and independent notification-owner teardown
also passed. Actual resolution and retry receipts passed their validators after
owned cleanup. That earlier run used the independent legacy notification transport.

The notification migration then reran the same targeted archive on the retained
default-Docker **11.8.121.4** onebox. Ordinary calls retained furpc generation
**1**, with three independent notification sessions on generations **2/3/4**.
Native session establishment, four explicit filter unregister confirmations,
endpoint-count/primary changes and empty invalidations passed. A real successful
ACK was dropped, the native gateway retried under a fresh request ID, and the
furpc runner sent its correlated ACK without duplicate application delivery.
Actual notification/resolution/retry receipts passed independent validators and
owned cleanup completed; the onebox was left running.

Together with the earlier native runs, this qualifies thirteen reads, two naming
writes, MovePrimary and notification receiving/ACKs over initiating clear TCP on
the observed runtime, not native notification origination, accepted-side serving,
TLS, other gateway operations, or ImageStore over furpc.

### Operation inventory and independent clients

`fube-transport` implements bounded Fabric serialization, clear frames and CRCs,
ordered native headers, client-only ListenInstance establishment, and
continuously driven, bounded concurrent Tokio request/reply I/O. `fube-client`
has migrated every implemented ordinary operation and notification sessions to furpc.
All paged and unpaged list queries use furpc; their legacy methods,
typed-request implementations and input-accounting adapters are retired.
Uncached resolution and independent notification sessions add endpoint discovery
without an endpoint-result cache.
An independently owned `ImageStoreClient` adds native application-package copy
without sharing the ordinary management or notification client's connection.

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
| GetPartitionList | Furpc sends **GetServicePartitionList**, wrapper tag 5. Six owned singleton/stateless and int64-range/stateful partitions, field projections and GUID selectors independently compared live. Named keys and message-size continuations remain unqualified. |
| GetReplicaList | Furpc sends **GetServicePartitionReplicaList**, wrapper tag 6. Twenty-two stateful replicas/stateless instances, ID selectors, Ready filtering, placement and native error reuse independently compared live on the original furpc generation. Message-size continuations remain unqualified. |
| GetDeployedApplicationList | Sends **GetApplicationPagedListDeployedOnNode**, wrapper tag 10. Node/application selectors, directory fields, requested/default health and real count-limited continuation independently compared live; missing-node error followed by healthy reuse. |
| GetDeployedServicePackageList | Sends **GetServiceManifestListDeployedOnNode**, wrapper tag 11. Ten service-package observations independently compared live, including manifest/version/status/activation and exact manifest selectors. No paging. |
| GetDeployedCodePackageList | Furpc sends **GetCodePackageListDeployedOnNode**, wrapper tag 14. Ten ExeHost code-package observations independently compared live, including entry points/statistics, host/activation fields and exact code-package selectors. Setup execution and container hosts remain unqualified. No paging. |
| GetDeployedReplicaList | Sends **GetServiceReplicaListDeployedOnNode**, wrapper tag 12. Twenty-two stateful/stateless observations independently compared live, including partition selectors, host process IDs, activation and stateful reconfiguration details. Reply actor is RS (13), not RA (12) or NamingGateway (5). No paging. |
| ResolveServicePartition | Uncached native gateway request with singleton, named or signed-int64 selection and optional previous-result metadata. Owned partition details, endpoints/roles and identity-scoped generation/lookup versions. All three key kinds, refreshes, missing-service errors and independent endpoint/role comparisons qualified on 11.8.121.4. |
| Service notifications | Standalone noncloneable `ServiceNotificationClient`, independent connection/session/receiver, many owning RAII filter guards. Exact/prefix/primary-only registration, native updates, endpoint-count changes, primary replacement, empty invalidations, unregister and independent close qualified on 11.8.121.4. A lost successful ACK caused a real native retry without duplicate delivery. Distinct-address primary-only delivery remains separately unqualified. |
| Native ImageStore package copy | Dedicated `ImageStoreClient`, v1.2 session/chunk/commit protocol and native AtomicCopy root replacement. Rust's complete inventory matched one native SDK predecessor minus a deliberate obsolete file, and independently matched local files/zero-byte postorder `_.dir` markers. Nested Unicode/space paths, empty files/directories, 7 MiB data, a 132 MiB binary, stale removal, provisioning/creation, independent live application contact and deployed Data SHA256 comparison passed on 11.8.121.4. Repeated native SDK copy reliability is not claimed. |
| MovePrimary | Furpc constraint-enforced native FM query with RS13/QueryReply and empty QueryResult. No force/ignore option, paging or replay. The owned fixture observes the actual native/HTTP primary transition, source generation, mixed cloned reuse and cleanup. |
| Native faults/errors | Furpc operations preserve Native/Decode/Protocol/Domain categories, including native Fault bodies and original sources. Independent notification/ImageStore clients retain their own error contracts. |
| Concurrent operations | Mixed read queries on clones sharing one connection; real onebox coverage. Deterministic cancellation/late-reply behavior belongs to furpc's transport suite. |
| Idle disconnect | Historical legacy proxy qualification is retired with its read façade; managed furpc lifecycle has its own transport coverage. |
| Controls and stream lifecycle | Underlying furpc/fube-transport suites cover managed transport behavior. Synthetic fube-client wire/model-peer tests are retired. |

Ordinary management requests advertise protocol **1.1**; native ImageStore
uploads use canonical **1.2**, without legacy fallback. The demonstrated gateway replies with
protocol **1.2** for ping. The observed GetNodeList aggregate success has no
actor, action or client-version header; correlation and typed body validation
remain mandatory. Coverage is qualified only for the observed runtime,
clear-TCP profile, and these operations, not all Service Fabric releases.
GetNodeHealth instead returns actor **HM (16)** and action
**OperationSuccessAction**; it is not the bare list-aggregation reply.

## 2. Run against a disposable onebox

### Endpoint, Channel and local stub

```rust,no_run
use std::time::Duration;
use fube_client::FurpcClient;
use furpc::Request;
use tokio::time::Instant;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = FurpcClient::endpoint_clear("127.0.0.1:19000".parse()?)?
    .connect_timeout(Duration::from_secs(10));
let channel = endpoint.connect().await?;
let mut client = FurpcClient::new(channel);
let mut other = client.clone();
let deadline = Instant::now() + Duration::from_secs(30);
let (first, second) = tokio::try_join!(
    client.ping(Request::new(()), deadline),
    other.ping(Request::new(()), deadline),
)?;
drop(client);
drop(other); // last ordinary owner retires the logical channel
# Ok(())
# }
```

Ordinary calls use `furpc::Channel`, not the legacy GatewayChannel re-exported
from `fube-client` for independent notification/ImageStore clients. Endpoint
configuration is inert; connect owns establishment and `FurpcClient::new` binds
locally without another handshake. Every ordinary call takes an absolute deadline;
the earlier request-local deadline wins. Connection timeout is independent.

Clones share furpc lifecycle/capacity, not a second typed budget. Dropping one
owner leaves surviving owners usable; dropping the last retires the logical
channel. Later independent calls may reconnect, but submitted calls are never
replayed. Peer/source metadata does not own that lifecycle or retarget the
gateway to a service-published endpoint. There are no ordinary `close`,
`is_closed`, `terminal_error`, optional-duration or typed-slot compatibility APIs.

ImageStore's actual connect path uses its own Endpoint factory and common target,
but keeps a dedicated mutable serial session, one private receiver, actor26 raw
sends/actor25–27 semantic ACKs, delete RPC and cancellation retirement. Runtime/
Hosting likewise use the same backend after their outgoing/accepted native IPC
setup, without creating new inbox pumps. Raw receiver Clone/concurrent receive
is supported, not safe arbitrary session sharing; explicit domain retirement
can close retained raw observers. See [common composition](NativeChannel.md).

### Disposable onebox commands

Use the repository's Rust toolchain. A native SDK or C++ build is unnecessary.
The [Ubuntu onebox guide](../UbuntuOnebox.md) supplies Docker prerequisites.
Run from the repository root:

```sh
just onebox-build
just onebox create poc

cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 ping

cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 node-list

cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 node-health N0020 \
  --events-filter 8 --warning-as-error true
```

The example's `ping` demonstrates two concurrent calls through explicit stub
clones on one connection, printing both advertised addresses/full node identities.
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

cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists fabric:/rust-poc-existing

cargo run -p fube-client --example onebox -- \
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
cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists "$owned_name"
# Proceed only after confirming absence; never adopt an existing name.
cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 create-name "$owned_name" --timeout-seconds 5
cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 name-exists "$owned_name"
cargo run -p fube-client --example onebox -- \
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
use fube_client::{FurpcClient, PartitionKey, ResolutionRequest};
use furpc::Request;
use std::time::Duration;
use tokio::time::Instant;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = "127.0.0.1:19000".parse().expect("IP socket endpoint");
let channel = FurpcClient::endpoint_clear(endpoint)?.connect().await?;
let mut client = FurpcClient::new(channel);
let name = "fabric:/Example/Service".parse()?;
let key = PartitionKey::Singleton;
let first = client.resolve_service_partition(
    Request::new(ResolutionRequest::new(&name, &key, None)?),
    Instant::now() + Duration::from_secs(30),
).await?.into_body();
let refreshed = client.resolve_service_partition(
    Request::new(ResolutionRequest::new(&name, &key, Some(&first))?),
    Instant::now() + Duration::from_secs(30),
).await?.into_body();
let order = first.version.compare(&refreshed.version)?;
# let _ = order;
# Ok(())
# }
```

`ResolutionRequest::new` validates a checked service name, singleton/named/signed
int64 key and optional previous result, returning `fubst::Result`. It copies only
the selection and private native refresh metadata, never previous endpoints.
Public edits to service/partition/version identity cannot rebind that metadata.
Named keys reject empty/NUL values; native body byte/string widths, rather than
the retired standalone 32768-unit construction ceiling, bound serialization.

Resolution uses a dedicated native `FubstCodec<ResolutionRequest, ResolveReply>`,
not the query envelope. NamingGateway actor 5 and `ResolveServiceRequest` reply
action are checked before body decode. Identity/version, partition details and
the requested service/key are checked before returning `Response<ResolvedServicePartition>`.
Addresses remain opaque, endpoint roles retain native semantics, and version
ordering stays service/partition scoped, including the generation owner.
No endpoint cache, description lookup, HTTP fallback, retry or replay is added.

All ordinary RPCs use `FurpcClientError`: `Native`, `Decode`, `Protocol` or
`Domain`, preserving original sources and native Fault bodies. Response source,
headers and settlement survive model conversion. On-time native settlement stays
on time when observed/decoded later; there is no legacy final-decode deadline check.
Independent notifications and ImageStore retain their existing error/lifecycle contracts.

`FurpcClient::create_name(Request<NamingUri>, deadline)` and `delete_name` return
`Response<()>` through `FurpcClientError`, using the earlier explicit/request-local
absolute deadline for encoding, readiness/reconnect, submission and receipt.
Parse the checked URI before calling. Native on-time settlement remains on time
when observed or decoded later; the old legacy post-decode deadline check is not
reintroduced. Both use the shared native URI schema; the supported grammar is unchanged.
For example, with a unique caller-owned name on a disposable cluster:

```rust,no_run
# async fn write(client: &mut fube_client::FurpcClient, owned_name: fube_client::NamingUri) -> Result<(), fube_client::FurpcClientError> {
let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
client.create_name(furpc::Request::new(owned_name.clone()), deadline).await?;
let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
client.delete_name(furpc::Request::new(owned_name), deadline).await?;
# Ok(())
# }
```

Creation can create implicit ancestors or promote an implicit name. Creating
an already **explicitly created** name returns native `FABRIC_E_NAME_ALREADY_EXISTS`
(-2147017792), not an idempotent success. Deleting an absent name returns
`FABRIC_E_NAME_DOES_NOT_EXIST` (-2147017791). Deletion is not recursive and can
reject a nonempty name; the service may remove empty implicit ancestors.
Native body codes/diagnostics remain `FurpcClientError::Domain`, operation-local
when the stream is healthy. Write success has an empty body, unlike NameExists
despite their shared reply action.

Writes are never automatically retried or replayed. A timeout, cancellation or
disconnect after sending **does not establish that the write failed to execute**:
replication may commit after local waiting stops. Dropping the future or closing
the connection does not roll back a sent write.

Implementation: [typed stub](../../crates/libs/fube-client/src/furpc_client.rs)
and [empty-root codec](../../crates/libs/furpc/src/codec.rs).
Native contract: [public request construction](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L6881-L6976)
and [gateway create reply](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/EntreeService.CreateNameAsyncOperation.cpp#L24-L49).

Furpc owns managed correlation, send commitment, deadlines and physical
generation retirement. Operation codecs own their success/failure envelopes and
body validation. Malformed application data fails the call without inventing a
successful result; transport integrity failures independently retire the
physical generation. See the [furpc implementation](design/Furpc/Implementation.md)
for current lifecycle/settlement behavior and [NativeChannel](NativeChannel.md)
for the separately retained notification/ImageStore transport.

### Native ImageStore application-package copy

`ImageStoreClient` is a noncloneable owner of a dedicated native upload
`GatewayChannel` with client-installed authorization and actor25/27 registration.
Registration alone grants neither sender nor consuming reply permission.
It does not borrow a `FabricClient` or
notification session.
Production upload uses no native SDK, COM library, HTTP gateway or subprocess.
Only native `fabric:ImageStore`, canonical protocol **1.2**, and explicitly
unsecured **numeric, nonzero-port** development gateways are supported;
loopback and non-loopback addresses are accepted, while unspecified, multicast
and zero-port destinations reject. There is no TLS, authentication,
backend selection or v1.1 fallback.

Non-loopback endpoint acceptance is a transport capability; it does not extend
the historical deployed/native qualification to remote clusters.

Build the thin CLI and put `target/debug` on your PATH, or invoke that binary
directly:

```sh
cargo build --locked -p fube-cli
fube --endpoint 127.0.0.1:19000 --unsecured application upload --path ./MyApp --timeout 300
```

`--endpoint` is a native IP socket address, not a DNS name or HTTP URL.
`--unsecured` is required. The destination is the resolved local directory's
final name (`MyApp` here); the CLI has no destination, provisioning, application
creation, compression or checksum options. Timeout is positive integer seconds,
defaults to **300**, and covers the **whole CLI operation**, including local path
resolution, connection establishment, copy and close. Failures exit nonzero;
the success message is printed only after package completion.

```rust,no_run
use std::{path::Path, time::Duration};
use fube_client::{ImageStoreClient, ImageStoreClientOptions};

# async fn upload() -> fube_client::Result<()> {
let address = "127.0.0.1:19000".parse().expect("native IP socket endpoint");
let endpoint = ImageStoreClient::endpoint_clear(address)?
    .connect_timeout(Duration::from_secs(10));
let channel = endpoint.connect().await?;
let mut client = ImageStoreClient::new(channel, ImageStoreClientOptions {
    default_timeout: Duration::from_secs(300),
})?;
client.copy_application_package(Path::new("./MyApp"), "MyApp", None).await?;
client.close().await?;
# Ok(())
# }
```

`new` consumes the actual established `Channel` synchronously. It only validates
operation defaults and acquires the original connection target and private ACK
receiver: no dial, native session request, hidden startup or permission change.
Options default to **30 seconds**, independently of endpoint establishment.
Only `new(Channel, ImageStoreClientOptions)` constructs an uploader; Endpoint
owns connection establishment, separately from the later copy default.

Use a dedicated upload connection with at most one uploader/protocol consumer.
Raw channel clones and repeatable/cloned raw receivers remain available, but
competing consumers can steal semantic ACKs; they are unsupported for this session.
Binding does not reserve exclusive inbox ownership or upgrade an ordinary
channel's permissions. Invalid options or unavailable inbox only drop the supplied
handle, without explicitly aborting retained raw clones. Normal uploader drop
likewise releases its owner handle; existing last-owner cleanup still applies.
Explicit upload cancellation or uncertain failure retires the shared connection
even when raw clones survive. No new uploader drop policy is introduced.

`copy_application_package(&mut self, source: &Path, destination: &str,
timeout: Option<Duration>) -> Result<()>` serializes copies through its mutable
receiver and uses the existing `fube_transport::Error`/`Result`.
`None` uses `ImageStoreClientOptions::default_timeout` (or the legacy connection
duration); `Some(duration)` overrides the
copy budget, never meaning infinity. Unlike the CLI's enclosing deadline,
library connection establishment and a later copy each have their own deadline.
The copy deadline spans local preparation, root Delete, content, file commits
and all directory markers.

Copy matches native **AtomicCopy/Overwrite**, not an in-place merge or one
server-side atomic transaction. The source root must be a nonsymlink directory;
final destination and complete root-marker FileCreate frame validation happen
before remote deletion. Invalid destinations send nothing and leave a healthy
client reusable. One routed
native Delete removes the entire top-level destination, including stale extra
files. Missing destination is accepted only as native FileNotFound
**-2147017728**; other native errors propagate. Child recursion does not delete
again. Files and child directories complete before each directory's empty
**`_.dir`** marker; the root marker is last. Exact source `_.dir` entries are
skipped and regenerated, not copied.

Destination names are ImageStore-relative paths, **not** the restricted ASCII
`NamingUri` grammar. Spaces and Unicode are supported. Validation rejects
initially rooted input, converts backslashes to slashes, and repeats empty/`.`
component collapse and newly exposed outer-whitespace trimming until the path
is stable. The final native-effective path is checked for empty/rooted/drive
forms, NUL, exact `..` components and case-insensitive top-level
**Store/WindowsFabricStore**, then used unchanged for Delete and every FileCreate.
Thus whitespace/dot aliases such as `./ Store/.` are rejected, while ordinary
internal spaces and Unicode remain supported.
Local non-UTF-8 filenames, backslash-containing names, symlinks and special
entries are explicitly unsupported. Sources are not snapshotted: a changing
source can fail or produce mixed-time content.

| Upload resource | Limit and behavior |
|---|---|
| Outstanding content | **One** negotiated chunk; nonfinal chunks retain the full negotiated size so native sequence-derived offsets remain correct. |
| Content frame | **4 MiB total**, including the **12-byte** fixed header and every native header. An oversized negotiated threshold fails before content is read/sent; it is never silently shrunk. A zero advertised threshold uses **3 MiB**. |
| Directory traversal | **128** simultaneously open directory levels, including the source root; **1,000,000** visited entries. No package-sized file or GUID snapshot is retained. |
| Native path encoding | Each native string is at most **32,768 UTF-16 units**; the complete native header area must fit **65,535 bytes**. Before Delete, the generated `/_.dir` FileCreate is encoded as a complete frame, including Actor/Action/MessageId, Activity/version, upload fields, Timeout and record overhead, with maximum relevant MessageId index/timeout widths. These are encoded-size limits, not a promise that every 32,768-unit path fits. |
| Reply handoff | **32** raw backend ACK envelopes, no second queue/worker; overflow is terminal. Transport authority is validated on ingress; domain headers/body are decoded at client pull before stale filtering. Unread domain-invalid ACKs remain bounded raw ingress until pull. |
| Stale/duplicate replies | At most **64 consecutive** well-formed obsolete replies per awaited stage; the 65th fails. Wrong future sequences/actions and malformed bodies fail, not count as harmless stale traffic. |
| Transport capacity | **8** ordinary admitted sends/requests, including started writes and completed-undelivered results; upload content/controls never use consuming reply slots. **1 GiB** conservative aggregate reservation ceiling, not eager allocation. |

Every file uses FileCreate, acknowledged raw content chunks, then actor-27
commit success followed by a flushed FileUploadCommitAck. Even an empty file
sends **one empty chunk** before commit. Content/chunk ACKs alone cannot report
file success. Native semantic ACKs can omit MessageId/RelatesTo: strict actor
25/27, action, version, Activity GUID and sequence/body checks are required.
Late prior-file cleanup results and duplicate commits cannot complete a later
file. Upload actor26 frames omit ordinary MessageIds; this avoids consuming
transport lifetime-ID history per chunk without relaxing history/collision rules.
Root Delete is still ordinary actor5 RPC on this upload socket.
OperationsPending can resend the **same GUID's commit** within the original
deadline; there is no reconnect or whole-package replay.

**Failure is not rollback.** Root replacement has already removed the previous
package; partial new files and completed child markers can remain. A normal
transfer error may flush best-effort native DeleteSession only while the
original deadline and socket permit it, without replacing the primary error.
It never deletes the remote directory as failure recovery. File success does
not wait for the later native session-cleanup result.

Once remote work is guarded, dropping the upload future synchronously aborts
the dedicated connection. **No asynchronous remote cleanup is spawned after
future drop**, and no rollback or remote-session removal is promised; native
expiry may eventually reclaim staged data. Uncertain errors/deadlines also
retire the client. `is_closed()`/`terminal_error()` expose terminal state and
reuse fails rather than reconnecting. Validation failures before guarded remote
work can leave the client usable; successful copies retain a reusable socket.
`close(&self) -> Result<()>` awaits the retained socket-driver join with a
separate **31-second teardown budget** (30-second full-frame watchdog plus
one second), without extending the upload deadline. A started frame may finish
its suffix after observer cancellation; completion means local full-frame
write, never remote ACK, commit or rollback. Close/disposal success is separate
from the retained terminal cause.

Implementation: [public API and copy lifecycle](../../crates/libs/fube-client/src/image_store.rs#L59),
[native codecs](../../crates/libs/fube-client/src/image_store_protocol.rs#L144),
[complete root-marker preflight](../../crates/libs/fube-client/src/image_store_protocol.rs#L310),
[strict client semantic ACK decoding](../../crates/libs/fube-client/src/image_store_protocol.rs#L48)
and [client-owned upload envelope authority](../../crates/libs/fube-client/src/channel_adapter.rs#L72).
Native copy semantics follow the pinned
[NativeImageStore directory path](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Management/ImageStore/NativeImageStore.cpp#L1139-L1369).
Native gateway ACKs use raw actor25/27
[SendOneWay replies](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/FileTransferGateway.cpp#L1789-L1843);
they are not ordinary RPC completions. mssf's package-copy reference invokes
native COM, not a second wire implementation. This directory-root v1.2 subset
does not claim all native/.NET image-store stores, direct-file copy, retries,
security profiles or source-lock/cancellation semantics.

### Query parameters, paging, and serialization

List requests compose `PagedQuery<P> { parameters, paging }`; node, application,
and service description names are aliases for this wrapper with their respective
parameter types. `Paging` shares `continuation_token` and `max_results` across these lists.
Unpaged health uses `QueryDescription<NodeHealthQueryParameters>` instead, so it
has no meaningless paging controls. Every list returns `QueryPage<T> { items,
continuation_token }`, with operation-specific page names retained as aliases.
Tokens remain opaque strings; request controls and response metadata are distinct.

Descriptions, parameter structs, selectors, health policy, and `Paging` support
Serde serialization/deserialization for application configuration, including
`serde_json::to_string` / `from_str`. Naming selectors and service application
scope use checked `NamingUri` values, including during JSON deserialization.
Other operation validation (filter validity, count range and native limits)
still happens before sending. A JSON round trip does not itself qualify a value
for the native wire.

Internally, parameter structs implement `Query: Serialize`, binding the native
query name, optional `validate_parameters` hook, paging placement, output and
declarative reply profile. Furpc request wrappers use the generic query codec,
not the retired sealed ClientRequest family. One absolute deadline covers
encoding, readiness and receipt; validation/decoding stays on the polling
observer, never on the transport driver. The shared encoder validates and
serializes parameters directly; operations no longer implement parameter encoding.
A dedicated Serde argument serializer consumes the parameter types and `Paging`.
Ordinary `rename`, `flatten`, `skip_serializing_if` and reusable `with` field
adapters declare native names, selector fields, numeric masks, omissions and
field order on those structs. No parallel projection structs, serialization
trait, per-request serialization wrapper or additional derive-macro crate is
needed. The encoder
accepts flat string-keyed structs/maps, formats scalar values as native string
arguments, and rejects duplicates, unsupported nested shapes and bounds
violations. Values use their ordinary human-readable representation, so checked
`NamingUri` values serialize as strings without a query-specific handler.

This is separate from the positional Fabric Serde backend, which serializes the
binary request envelope. Health fields select the reusable `json` field adapter:
the normal nested object becomes JSON text only when encoding a query argument.
Ordinary nested objects remain unsupported; JSON output is bounded after
escaping. `NodeHealthPolicy` and `HealthEventsQueryFilter` declare the native
property names, and the reusable `flags` adapter selects numeric masks. Missing
policy/filter remains distinct from explicit false/zero.
Replies select shared `ListReply<Record, TAG, STRICT_PAGING>`,
`UnpagedListReply<Record, TAG, ACTOR>` or
`ItemReply<Record, TAG, ACTOR>` profiles rather than implementing a decoder per
operation. A shared `decode_reply<Q>` function selects the profile and record
validator; `Query` has no decoding method. Records use ordinary `Deserialize`
derives and native field adapters; an optional `validate_record` hook enforces
domain restrictions. Native payload type IDs are centralized as named `i32`
constants in `query_protocol::result_tags`, matching `QueryResultHelpersEnum.h`.
For example, service queries declare `ListReply<Service, { result_tags::SERVICE }>`
and node health declares `ItemReply<NodeHealth, { result_tags::NODE_HEALTH }, 16>`
(the final argument is the actor ID, not a payload tag).
Lists allow absent actor/action headers but check
them when present; items require their declared actor and `OperationSuccessAction`.
The shared decoder validates both native list/item slots, required pointers,
result kind and paging. Node lists retain permissive paging tags, application/
service lists require empty tags, and successful items cannot be paged.
Unpaged lists default their optional actor check to NamingGateway (5);
deployed replicas select RS (13). Hosting service/code-package responses can
omit actor/action headers. Unpaged lists return `Vec<Record>` and reject any paging-status pointer,
including an empty token, rather than silently discarding it. Native
pointers, polymorphism and versioned record suffixes remain explicit.

**API migration:** move operation fields into `.parameters`, request token/count
into `.paging`, and use `.items` instead of `.nodes`, `.applications` or
`.services` on returned pages. Wrap service/health parameters with the corresponding
description's `new(parameters)` constructor. Named client methods are unchanged.

**Query JSON migration:** parameter and paging JSON now uses the native argument
names and numeric masks rather than snake_case fields and flag-name strings.
Selector enums flatten into keys such as `ApplicationName`, `ApplicationTypeName`
or `ServiceTypeName`; a missing selector means All. For example, application
parameters can be `{"ApplicationTypeName":"MyType","ExcludeApplicationParameters":true}`.
Paging uses `ContinuationToken`/`MaxResults`; the outer description still uses
`parameters`/`paging`. Optional/default values are omitted where declared, so
empty names/tokens canonicalize to absence. Conflicting, duplicate, unknown or
malformed selectors and unknown parameter fields fail instead of silently
becoming an unfiltered query. Explicit application definition DEFAULT (0) can be
read as 0 or null, serializes as null, and omits the native argument.

Health parameter JSON keeps nested objects, not JSON strings:

```json
{
  "ClusterHealthPolicy": { "ConsiderWarningAsError": false },
  "EventsFilter": { "HealthStateFilter": 0 },
  "NodeName": "N0020"
}
```

`events_filter` now holds `HealthEventsQueryFilter { health_state_filter }`.
Rust callers can use `Some(HealthEventsFilter::ERROR.into())`; flag operations
remain on `HealthEventsFilter`. Existing JSON configurations must be migrated;
the emitted native request bytes, validation and CLI arguments are unchanged.

### Native node pages

```rust,no_run
use std::num::NonZeroU64;
use fube_client::{
    FurpcClient, NodeQueryDescription, NodeQueryParameters, NodeStatusFilter, Paging,
};

# async fn pages(client: &mut FurpcClient, deadline: tokio::time::Instant) -> Result<(), Box<dyn std::error::Error>> {
let mut query = NodeQueryDescription {
    parameters: NodeQueryParameters {
        status_filter: NodeStatusFilter::UP | NodeStatusFilter::DOWN,
        ..Default::default()
    },
    paging: Paging {
        max_results: NonZeroU64::new(2),
        ..Default::default()
    },
};
loop {
    let page = client.get_node_list(furpc::Request::new(query.clone()), deadline).await?.into_body();
    for node in page.items {
        println!("{} {} {:?}", node.name, node.id, node.status);
    }
    query.paging.continuation_token = page.continuation_token;
    if query.paging.continuation_token.is_none() {
        break;
    }
}
# Ok(())
# }
```

The operation returns an owned `NodeListPage`; callers choose traversal and
overall budgets. Reusing the same absolute deadline bounds the whole traversal;
choosing a new deadline gives each request a separate budget. Nodes can change between pages; native
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
safety-check tags or malformed field structure fail explicitly for that typed
call, without aborting the owner. Nonempty deactivation/safety variants have source/model coverage only.

### Native node health

```rust,no_run
use fube_client::{
    FurpcClient, HealthEventsFilter, NodeHealthPolicy, NodeHealthQueryDescription,
    NodeHealthQueryParameters,
};

# async fn health(client: &mut FurpcClient, deadline: tokio::time::Instant) -> Result<(), Box<dyn std::error::Error>> {
let mut query = NodeHealthQueryDescription::new(NodeHealthQueryParameters::new("N0020"));
query.parameters.events_filter = Some(HealthEventsFilter::ERROR.into());
query.parameters.health_policy = Some(NodeHealthPolicy { consider_warning_as_error: true });
let health = client.get_node_health(furpc::Request::new(query), deadline).await?.into_body();
println!("{:?}", health.aggregated_health_state);
for evaluation in health.unhealthy_evaluations {
    println!("{evaluation:?}");
}
# Ok(())
# }
```

`get_node_health` uses furpc's absolute deadline and on-time settlement rules,
like node-list. `NodeHealth` owns its node name, event vector, aggregate
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
| `Some(DEFAULT.into())` / `Some(ALL.into())` | Explicit masks 0 / 65535; select all. |
| `Some(NONE.into())` | Exact mask 1; no events, **without** suppressing aggregate/evaluations. |
| `OK`, `WARNING`, `ERROR` | Bits 2, 4, 8, combinable with `\|`. Selection uses **evaluated** state; ERROR under policy true can return reported Warning. |
| `from_bits_retain(u32)` | Sent unchanged. Other bits do not match; exact special-value equality matters: NONE \| ERROR = 9 selects Error, not none. |

No raw JSON/arguments, node-ID selector, paging, health-statistics, child filters
or full cluster-health policy API is exposed. Policy/filter arguments are JSON
**strings within the native binary query map**, not an HTTP request.
See the public types in
[`health.rs`](../../crates/libs/fube-client/src/health.rs).

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
and malformed domain success fail the typed call explicitly.

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
use fube_client::{
    ApplicationListFilter, ApplicationQueryDescription, ApplicationQueryParameters, FurpcClient,
    Paging, ServiceListFilter, ServiceQueryDescription, ServiceQueryParameters,
};

# async fn workloads(client: &mut FurpcClient) -> Result<(), Box<dyn std::error::Error>> {
let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
let mut applications = ApplicationQueryDescription {
    parameters: ApplicationQueryParameters {
        filter: ApplicationListFilter::TypeName("MyApplicationType".into()),
        ..Default::default()
    },
    paging: Paging {
        max_results: NonZeroU64::new(1),
        ..Default::default()
    },
};
let page = client.get_application_list(furpc::Request::new(applications.clone()), deadline).await?.into_body();
for application in page.items {
    println!("{} {} {:?}", application.name, application.type_version, application.health_state);
}
// Caller decides whether/when to issue another operation with this opaque token.
applications.paging.continuation_token = page.continuation_token;

let mut services = ServiceQueryDescription::new(ServiceQueryParameters::new("fabric:/MyApplication".parse()?));
services.parameters.filter = ServiceListFilter::TypeName("MyServiceType".into());
let page = client.get_service_list(furpc::Request::new(services), deadline).await?.into_body();
for service in page.items {
    println!("{} {:?} {:?}", service.name, service.kind, service.status);
}
println!("continuation={:?}", page.continuation_token);
# Ok(())
# }
```

`get_application_list` / `get_service_list` take typed requests and absolute
deadlines under furpc settlement rules. Neither automatically pages or replays
a submitted operation; later independent calls may reconnect. Domain errors stay
`FurpcClientError::Domain`, transport Faults remain native, and malformed bodies
or protocol violations remain operation-local without failing independent neighbors.

| Description | Selector and independent controls |
|---|---|
| `ApplicationQueryParameters` | `ApplicationListFilter::All` (default), `Name`, `TypeName`, or `DefinitionKind`; exactly one. Independent `exclude_application_parameters` (default false). |
| `ApplicationDefinitionKindFilter` | DEFAULT=0, ALL=65535; bits SERVICE_FABRIC_APPLICATION_DESCRIPTION=1, COMPOSE=2, MESH_APPLICATION_DESCRIPTION=4, combinable with `\|`. `try_from_bits` rejects unnamed bits except exact ALL. These are **not** result discriminants. |
| `ServiceQueryParameters::new(application)` | Required ordinary application scope; `ServiceListFilter::All` (default), exact `Name` or exact `TypeName`. |
| `PagedQuery<P>::paging` | Shared `Paging` controls: opaque continuation token and positive count. |

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

`ApplicationListPage.items` contains owned `Application` records:
name, type_name/type_version, `ApplicationStatus`, `HealthState`, a sorted owned
parameter map and `ApplicationDefinitionKind`. Parameter exclusion yields an
empty map, indistinguishable from an actually empty map without request context.
Application statuses are Invalid/Ready/Upgrading/Creating/Deleting/Failed.
Definition results are ServiceFabricApplicationDescription/Compose/
MeshApplicationDescription/Invalid (0/1/2/65535); missing optional definition
defaults to Invalid. The native deprecated fields `upgrade_type_version` and
`upgrade_parameters` are exposed in native field order, defaulting to an empty
string/map when their suffixes are absent. Nonempty values remain an explicitly
unsupported application-list result profile.

`ServiceListPage.items` contains owned `Service` records in native field order:
`ServiceKind`, name, type_name, manifest_version, has_persisted_state,
`HealthState`, `ServiceStatus`, is_service_group.
Manifest version is not application type version and may be empty.
`ServiceKind` is a scalar enum: Invalid/Stateless/Stateful (0/1/2); persistence
is the separate `service.has_persisted_state` field. Deserialization retains
the native fields independently; service-list validation rejects Invalid kind
and stateless records claiming persisted state. Service statuses are
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
cargo run -p fube-client --example onebox -- \
  --clear 127.0.0.1:19000 application-list --type MyApplicationType \
  --exclude-parameters true --max-results 1 --timeout-seconds 5
cargo run -p fube-client --example onebox -- \
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

### Partition, replica and deployed application pages

Three additional list APIs extend discovery beyond applications and services:

| API | Scope |
|---|---|
| `get_partition_list` | A service's partitions, including partition identity/key information and observed status/health. This is not endpoint resolution. |
| `get_replica_list` | A partition's stateful replicas or stateless instances, with node placement, status and service address. |
| `get_deployed_application_list` | Applications deployed on one selected node, not the cluster-wide application inventory. |

Each accepts a `PagedQuery`-based description and returns one owned `QueryPage`.
All three take `furpc::Request<Description>` and an absolute
deadline, returning the page inside `furpc::Response`. They follow furpc
settlement/deadline rules.
Continuations are opaque and must
be followed explicitly; observations across pages and families are not a
snapshot. Native domain errors and malformed success bodies remain operation-local;
furpc transport/protocol failures may retire a generation, without replaying a
submitted call. The existing restricted naming URI profile still applies
to service names and application selectors.

```rust,no_run
use std::time::Duration;
use fube_client::{
    DeployedApplicationQueryDescription, DeployedApplicationQueryParameters,
    FurpcClient, PartitionQueryDescription, PartitionQueryParameters,
    ReplicaQueryDescription, ReplicaQueryParameters,
};
use furpc::Request;
use tokio::time::Instant;

# async fn topology(reads: &mut FurpcClient, partition_id: uuid::Uuid) -> Result<(), Box<dyn std::error::Error>> {
let deadline = Instant::now() + Duration::from_secs(30);
let mut partitions = PartitionQueryDescription::new(PartitionQueryParameters::new(
    "fabric:/MyApplication/MyService".parse()?,
));
let page = reads.get_partition_list(Request::new(partitions.clone()), deadline).await?.into_body();
for partition in page.items {
    println!("{} {:?}", partition.partition_information.id, partition.kind);
    let replicas = ReplicaQueryDescription::new(ReplicaQueryParameters::new(
        partition.partition_information.id,
    ));
    println!("{:?}", reads.get_replica_list(Request::new(replicas), deadline).await?.into_body().items);
}
partitions.paging.continuation_token = page.continuation_token;

let replicas = ReplicaQueryDescription::new(ReplicaQueryParameters::new(partition_id));
let page = reads
    .get_replica_list(Request::new(replicas), Instant::now() + Duration::from_secs(5))
    .await?.into_body();
println!("{:?}", page.items);

let mut deployments = DeployedApplicationQueryDescription::new(
    DeployedApplicationQueryParameters::new("N0010"),
);
deployments.parameters.include_health_state = true;
let page = reads.get_deployed_application_list(Request::new(deployments), deadline).await?.into_body();
println!("{:?}", page.items);
# Ok(())
# }
```

Request JSON retains separate `parameters` and `paging` objects. These APIs use
the same shared declarations and list reply profile as the existing queries.
Furpc owns all ordinary query/resolution paths and independently owned
notification transport. No query
adds automatic paging, retry, an endpoint cache or HTTP fallback.

Native partition and replica queries accept continuation tokens but not
`MaxResults`. Their shared paging field must leave `max_results` as `None`;
unsupported counts fail locally rather than being ignored or sent to the
gateway. Message-size paging can still occur. Deployed-application pages support
both continuation and the ordinary positive count limit.

`PartitionQueryParameters` requires a service `NamingUri` and optionally selects
one `partition_id`; a nil selector is omitted, like the native facade.
`PartitionInformation` retains the native key kind, UUID, name and signed
inclusive range bounds. Singleton, Int64Range and Named kinds are distinct from
the partition's Stateless/Stateful service kind. Results retain the signed
instance count (`-1` means every node), replica-set sizes, health, partition
status, quorum-loss duration and raw configuration epoch. Epoch field order is
configuration version then data-loss version; it is **not** the masked REST
`PrimaryEpoch` projection.

`ReplicaQueryParameters` requires a partition UUID. Its optional
`replica_or_instance_id` uses the native `ReplicaId` selector for either kind;
zero is omitted and other signed i64 values are retained. `status_filter` is a
numeric `ReplicaStatusFilter`: DEFAULT excludes Dropped, ALL is exactly 65535,
and IN_BUILD/STANDBY/READY/DOWN/DROPPED bits are combinable. Unknown UInt32 bits
are retained and forwarded, not treated as ALL. Records retain both positional
IDs; use `replica_id` and `role` for stateful replicas, `instance_id` for
stateless instances. Addresses are opaque strings, not parsed URLs.

`DeployedApplicationQueryParameters` requires a nonempty node name of at most
4096 UTF-16 units without NUL, with optional exact `application_name`.
`include_health_state` defaults to false and is explicitly sent; result health
is then Unknown, not an inferred Ok. When true, health aggregation is
best-effort and can still yield Unknown. Unfiltered deployment lists normally
exclude System; an explicit `fabric:/System` application selector is supported.
Unlike application-list records, deployed names are native strings, not
structured URI objects. Results include type name, `DeploymentStatus`,
work/log/temp directories and health, but no application-type version.

These records preserve complete native positional layouts, including inactive
service-kind fields. Strict scalar status/kind mappings and list-level health
validation reject unsupported values. Optional native suffixes default only at
genuine scope boundaries: partition duration/epoch and replica build duration
default to zero; deployed directories default empty and health defaults Unknown.
Malformed present fields are not treated as absent.

Native layouts:
[ServicePartitionQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePartitionQueryResult.h#L88),
[ServicePartitionInformation](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePartitionInformation.h#L58),
[ServiceReplicaQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceReplicaQueryResult.h#L92-L101),
[DeployedApplicationQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/DeployedApplicationQueryResult.h#L66-L73).
Native query specifications reject unknown arguments, including count limits
on the partition/replica routes; the shared encoder's query capability metadata
rejects those counts before any request is sent.

The existing owned application/service onebox scenario covers these
APIs. Its stateless service runs on all five nodes and its stateful service has
two int64-range partitions. Independent REST observations must converge to
Ready/Ok partition and replica state before native field comparisons. The live
fixture now also qualifies all six partitions, 22 replicas/instances,
node-local deployment pages and native error reuse through furpc, with current
archived receipts and independent HTTP field comparisons.
It does not exercise Named keys live, manufacture
partition/replica continuation evidence, or qualify persisted data replication.

### Node-local service packages, code packages and deployed replicas

These APIs inspect one application's deployment on a selected node. They are
distinct from the cluster-wide service list and partition-scoped replica list.

| Method | Native query | Payload tag |
|---|---|---:|
| `get_deployed_service_package_list` | `GetServiceManifestListDeployedOnNode` | 11 |
| `get_deployed_code_package_list` | `GetCodePackageListDeployedOnNode` | 14 |
| `get_deployed_replica_list` | `GetServiceReplicaListDeployedOnNode` | 12 |

Each takes `furpc::Request<QueryDescription<Parameters>>` and returns
`furpc::Response<Vec<Record>>`, not a
`QueryPage`. The native facade/specifications expose **no paging controls** for
these queries: there is no `paging`, `MaxResults` or continuation token to set.
Each takes an absolute deadline under furpc settlement rules, with native-error
reuse and call-local domain validation. Later calls may reconnect; submitted
calls are never replayed.
Unpaged description JSON rejects unknown outer fields, including `paging`,
rather than ignoring unsupported controls. This also tightens the shared
`NodeHealthQueryDescription` wrapper; valid existing request JSON is unchanged.

```rust,no_run
use std::time::Duration;
use fube_client::{
    FurpcClient, DeployedServicePackageQueryDescription,
    DeployedServicePackageQueryParameters, DeployedCodePackageQueryDescription,
    DeployedCodePackageQueryParameters, DeployedReplicaQueryDescription,
    DeployedReplicaQueryParameters,
};
use furpc::Request;
use tokio::time::Instant;

# async fn inspect(client: &mut FurpcClient) -> Result<(), Box<dyn std::error::Error>> {
let deadline = Instant::now() + Duration::from_secs(30);
let application = "fabric:/MyApplication".parse()?;
let packages = DeployedServicePackageQueryDescription::new(
    DeployedServicePackageQueryParameters::new("N0010", application),
);
for package in client.get_deployed_service_package_list(Request::new(packages), deadline).await?.into_body() {
    println!("{} {}", package.service_manifest_name, package.service_manifest_version);
}
let mut code = DeployedCodePackageQueryDescription::new(
    DeployedCodePackageQueryParameters::new("N0010", "fabric:/MyApplication".parse()?),
);
code.parameters.service_manifest_name = Some("MyServiceManifest".into());
code.parameters.code_package_name = Some("Code".into());
println!("{:?}", client.get_deployed_code_package_list(Request::new(code), deadline).await?.into_body());

let mut replicas = DeployedReplicaQueryDescription::new(
    DeployedReplicaQueryParameters::new("N0010", "fabric:/MyApplication".parse()?),
);
replicas.parameters.partition_id = Some(uuid::Uuid::parse_str(
    "00112233-4455-6677-8899-aabbccddeeff",
).expect("partition UUID"));
let deployed = client
    .get_deployed_replica_list(Request::new(replicas), Instant::now() + Duration::from_secs(5))
    .await?.into_body();
println!("{deployed:?}");
# Ok(())
# }
```

All three require `node_name` (1..=4096 UTF-16 units, without NUL) and a
checked `application_name`. They accept an optional exact
`service_manifest_name`; code packages additionally accept `code_package_name`,
and deployed replicas accept `partition_id`. Empty string filters and nil UUID
filters are omitted like the native facade. There is no activation-ID filter
on these facade methods; returned activation IDs distinguish deployment
instances. Request JSON keeps the `parameters` object and native argument names:

```json
{"parameters":{"NodeName":"N0010","ApplicationName":"fabric:/MyApplication","ServiceManifestName":"MyServiceManifest"}}
```

`DeployedServicePackage` retains manifest name/version, `DeploymentStatus` and
activation ID. `DeployedCodePackage` retains the complete native record,
including setup and main entry points, entry-point statistics, activation ID,
host type/isolation, node name and the native internal service-name field.
`DeployedReplica` retains service/type/manifest/code-package identity,
partition UUID, both signed replica/instance IDs, role/status, opaque address,
nullable activation ID, signed host process ID and reconfiguration information.
These are node-local observations, not atomic snapshots or service endpoint
resolution. Native paths, addresses and returned names are not normalized.
Compatible suffix defaults apply only at real native scope ends, never to
malformed present metadata.

The models match the pinned native source layouts below. Supported newer
appended fields are bounded-skipped, not exposed: the qualified runtime's REST
projection also includes fields such as container IDs and code-package usage
statistics that are outside this source baseline. Entry-point timestamps keep
their binary meanings: the pinned native JSON serializer swaps
`LastActivationTime` and `LastExitTime` labels; the test oracle accounts for
that discrepancy rather than swapping the Rust model fields.
`ReconfigurationPhase` includes the native `AbortPhaseZero = 7` state.

Native facade:
[FabricClientImpl](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L2207-L2252),
[code-package and deployed-replica queries](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/client/FabricClientImpl.cpp#L2621-L2729).
Layouts:
[DeployedServiceManifestQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/DeployedServiceManifestQueryResult.h#L51-L55),
[DeployedCodePackageQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/DeployedCodePackageQueryResult.h#L87-L100),
[DeployedServiceReplicaQueryResult](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/DeployedServiceReplicaQueryResult.h#L119-L134).

The owned application/service live scenario also qualifies these three queries
on SF **11.8.121.4**: ten service packages, ten code packages and 22 deployed
replicas/instances across five nodes and two owned applications, with independent
REST comparisons, exact selectors, empty selections, mixed queries and ping,
and missing-node native errors followed by healthy same-connection reuse.
The fixture uses shared activation and ExeHost without a setup entry point.
Exclusive/nonempty activation, setup execution, container/HyperV hosting,
nondefault reconfiguration phases and fields without REST projections are not
independently qualified; reconfiguration transitions were not exercised live.
No TLS or cross-release compatibility is claimed.

### Uncached resolution and independent notifications

Both clients live in `fube-client`, but neither owns the other or shares its
socket/session. Notification-only applications need no `FurpcClient`; multiple
notification clients can coexist. Each is constructed from its own Channel;
notification options set remote-operation defaults independently of dialing.
Their `None` uses the session default, not infinity; ordinary calls take absolute deadlines.

```rust
use fube_client::{
    FurpcClient, NamingUri, NotificationClientOptions, PartitionKey, ResolutionRequest,
    ServiceNotificationClient, ServiceNotificationFilter,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = "127.0.0.1:19000".parse().unwrap();
let channel = FurpcClient::endpoint_clear(endpoint)?.connect().await?;
let mut ordinary = FurpcClient::new(channel);
let channel = ServiceNotificationClient::endpoint_clear(endpoint)?.connect().await?;
let mut changes =
    ServiceNotificationClient::start(channel, NotificationClientOptions::default()).await?;
let name: NamingUri = "fabric:/MyApp/MyService".parse()?;
let registration = changes.register_filter(&ServiceNotificationFilter {
    name: name.clone(), name_prefix: false, primary_only: false,
}, None).await?;
let initial = ordinary.resolve_service_partition(
    furpc::Request::new(ResolutionRequest::new(&name, &PartitionKey::Singleton, None)?),
    tokio::time::Instant::now() + std::time::Duration::from_secs(30),
).await?.into_body();
if let Some(update) = changes.recv().await {
    let update = update?;
    if update.partition_id == initial.partition_information.id {
        let _ordering = update.version.compare(&initial.version)?;
    }
}
registration.unregister(None).await?;
changes.close(None).await?;
drop(ordinary);
# Ok(())
# }
```

Notification transport also composes through the same shared Endpoint/Channel:

```rust
use fube_client::{NotificationClientOptions, ServiceNotificationClient};
use std::time::Duration;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = ServiceNotificationClient::endpoint_clear(
    "127.0.0.1:19000".parse()?,
)?; // Local furpc configuration only; start installs authorized notification ingress.
let channel = endpoint.connect_timeout(Duration::from_secs(10)).connect().await?;
let changes = ServiceNotificationClient::start(channel, NotificationClientOptions {
    startup_timeout: Duration::from_secs(15),
    default_timeout: Duration::from_secs(30),
}).await?; // Real native NotificationClientConnectionRequest.
changes.close(None).await?;
# Ok(())
# }
```

These are three distinct operations: Endpoint configuration performs no I/O;
`Endpoint::connect` establishes transport; notification `start` asynchronously
creates a fresh native session. Unlike ordinary `FurpcClient::new`, `start` is
not local stub binding. It captures the original established furpc Peer,
not the gateway address advertised in the native reply. The factory
returns `furpc::Result<furpc::Endpoint>`; startup and subsequent protocol
operations return `NotificationResult`.

`NotificationClientOptions` defaults both budgets to 30 seconds. The Endpoint's
connection timeout, startup budget and later-operation default are independent.
Startup uses one absolute deadline from polling through owner publication,
including early-ingress admission. To retain one budget across transport setup
and native startup, compose both beneath a caller-owned deadline:

```rust,no_run
use fube_client::{NotificationClientOptions, ServiceNotificationClient};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let deadline = Instant::now() + Duration::from_secs(30);
let changes = timeout_at(deadline, async {
    let channel = ServiceNotificationClient::endpoint_clear("127.0.0.1:19000".parse()?)?
        .connect().await?;
    ServiceNotificationClient::start(channel, NotificationClientOptions {
        startup_timeout: deadline.saturating_duration_since(Instant::now()),
        default_timeout: Duration::from_secs(30),
    }).await.map_err(Box::<dyn std::error::Error>::from)
}).await??;
changes.close(None).await?;
# Ok(())
# }
```

The outer deadline also bounds startup publication; it never renews the dial
budget. Failed/cancelled startup closes local session admission and releases its
Channel owner; it does not add a furpc physical-retirement receipt.

Use a **dedicated furpc Channel with no existing ingress service**. `start`
installs the Actor 5 notification binding and exact envelope/ACK policies before
the native connection request. Do not retain outside Channel clones if owner
drop must release its last owner. There is no second reader or public native
notification receiver. Endpoint HandlerLimits govern incoming invocation
capacity and ACK deadlines independently of the notification operation default.

Keep the returned registration bound to a variable. Dropping it initiates
bounded asynchronous cleanup; `let _ = register_filter(...).await?` immediately
drops it. Consuming `unregister(timeout)` waits for confirmation. Guard drop
never blocks or spawns a task per guard; pending cleanup still occupies its
filter slot. Guards do not borrow the receiver or keep a dropped owner running.

Register **before** initial lookups: filters are not retroactive, updates can
precede registration confirmation, and one lookup selects only one partition.
Reconcile same-identity versions, not arrival order. Generation precedes lookup
version; store version is private refresh metadata, not an ordering tie-breaker.
Different partitions/services cannot be compared. Previous results cannot be
rebound through edits to public identity fields.

Notifications contain service name, partition ID, kind, opaque endpoint
addresses/roles and version, not description-enriched partition ranges/names or
store versions. Empty endpoint sets are invalidations, not proof of deletion.
Prefix means native URI ancestry. Primary-only is a native change filter, not
secondary-endpoint stripping; overlapping filters do not add per-filter IDs.

`recv(&mut self)` is local and cancellation-safe. On failure, its next result is
one terminal error, then `None`, independently of a full update queue. Buffered
updates may be discarded on terminal failure. Full-page delivery and progress
commit before successful native ACK; application processing is not part of ACK.
Unconfirmed filter mutations/cleanup retire only this notification session.
Explicit close attempts deregistration under one deadline, awaits its cleanup
worker and releases its Channel. Owner drop signals that same worker and closes
local admission. Neither promises furpc driver/socket teardown completion or
revokes outside Channel clones. Remote disconnect cleanup remains eventual.
An idle physical disconnect need not wake local `recv`, because furpc has no
independent Channel closure observer; use a caller-owned receive timeout when needed.

**Notification endpoint policy:** `endpoint_clear(SocketAddr)` accepts
explicit numeric unicast/nonzero-port NamingGateway endpoints, including
non-loopback addresses. Unspecified, multicast and zero-port destinations fail
InvalidInput. No hostname/advertised-address
fallback, compatibility wrapper or TLS/claims/security expansion is supplied.
Session/filter RPCs are typed calls through the shared gateway adapter over a
small Peer-backed byte service, so they cannot reconnect or replay. Early typed
notification pages wait for Connect's native generation under furpc handler
limits/deadlines; no pull/staging handoff or retained-allocation ledger remains.
Furpc Attempt retraction fences cancellation against the first positive write.
An unconfirmed committed registration terminates this domain session rather
than leaking a filter handle or retrying the mutation.

Native retries preserve logical page identity but the proxy assigns a fresh
client-leg MessageId on every forwarding attempt
([pinned source:453–483](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Naming/ServiceNotificationManagerProxy.cpp#L453-L483)).
The client suppresses accepted versions, merges progress and ACKs each admitted
attempt. Furpc owns transport correlation without a domain transport-ID history.
ACK success means whole-page local acceptance, not application
consumption or a per-page fixed-body fingerprint.

Notification construction, registration, receive, unregister, and close return
`NotificationResult<T>`, whose error is `NotificationError`. Delivery-queue
exhaustion is `NotificationError::Lagged`; notification filter/bookkeeping/queue
limits are `NotificationError::ResourceLimit`. Shared transport, protocol,
timeout, input, native faults and domain failures are retained as
`NotificationError::Common(FurpcClientError)`, including original decoder/control
resource limits. There is no legacy Error conversion. The ordinary
`FurpcClient` uses `FurpcClientError` and has no notification-only error cases;
notification-only failures stay above the shared channel and its opaque RPC boundary.
This changes notification error/lifecycle contracts, not native payloads.
Match `NotificationError::Common(FurpcClientError::Native(furpc::Error::Timeout(_)))`
for a native notification-operation timeout.

There is no endpoint cache, hidden description query, reconnect, filter replay,
or snapshot. The gateway can still use its own cache: independent management
and native observations converge but are not an atomic cluster snapshot.
See the [focused design/API reference](design/ServiceResolutionNotifications.md).

## 4. Supported inputs and bounds

Naming input is literal lowercase `fabric:/` followed by nonempty ASCII
`[A-Za-z0-9_-]+` segments separated by single slashes. Path case and spelling
are preserved. The entire input is limited to 4,096 bytes.

Root names, authorities, Unicode, escapes, query/fragment, whitespace, dot
segments, other punctuation, and trailing/repeated slashes are unsupported
and return an explicit input error. This is a deliberate subset, not a full
native NamingUri parser.

`NamingUri` is a distinct checked value, constructed with
`"fabric:/App".parse::<NamingUri>()?` or `NamingUri::parse("fabric:/App")?`.
`as_str`, `AsRef<str>`, `Display` and `into_string` provide string access.
JSON represents it as a string, while native Serde represents the complete
structured URI object. Application/service result `name` fields,
`ApplicationListFilter::Name`, `ServiceListFilter::Name` and
`ServiceQueryParameters::application_name` now use this type rather than
`String`. Construct service parameters with
`ServiceQueryParameters::new("fabric:/App".parse()?)`.
These field/constructor changes require callers to parse their names explicitly;
Names remain strings in query JSON, with invalid names rejected earlier; the
query-field JSON migration is described above.
All three naming methods require a checked `NamingUri` in `furpc::Request`,
with an absolute deadline. There are no legacy string-convenience write methods.

The legacy limits below belong only to ImageStore, not the
migrated furpc operations, which use their native frame/codec limits.

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
| Legacy RPC/lifetime IDs, including handshake | 4096; terminal exhaustion |
| Legacy upload channel aggregate retained-byte reservation | 1 GiB, not eager allocation |
| Started write / partial incoming frame | 30 seconds; request expiry ends local waiting, not started frame completion |

Native ImageStore's separate chunk, traversal, path and stale-reply limits are
listed in [its API section](#native-imagestore-application-package-copy).

ImageStore retains its independent upload-channel and protocol limits.
Ordinary furpc calls and notifications do not use sealed typed budgets or
retained-allocation leases.

The decoder skips unknown/deleted header records by their bounded record
lengths. Effective common headers use the last visible occurrence; first
typed version lookup remains first-match. Supported appended values/scopes
are typed-skipped, not found by searching for delimiter bytes. The bounded codec
supports nullable object pointers, typed objects, object arrays, UInt16 and
base/derived scopes needed by node results.
Native upload semantic ACKs additionally reject duplicate visible header IDs;
they do not inherit the common last-occurrence acceptance policy.

Notifications retain 256 queued application events, 32 filters including pending
registration/cleanup, 4,096 partition identities and version intervals each,
and 32 automatic cleanup commands. Furpc owns frame/codec, pending-operation,
handler and ACK capacity; Endpoint limits are configured before dialing.
There is no notification heap ledger, extra construction ceiling or lifetime
transport-ID history. A page must fit as one transaction. Delivery lag and protocol-bookkeeping/
cleanup resource failures are terminal; status does not need a queue slot.
Local filter-admission exhaustion is instead rejected before sending without
retiring the healthy session (`NotificationError::ResourceLimit("notification filters")`).
Unsupported trailing metadata, no-copy extensions, or object flags fail
explicitly. General native forward compatibility is not claimed.

### 4.1 Native Serde payloads

The transport-independent `fubst` crate owns the bounded native
`Encoder` / `Decoder` and Serde adapters. `fubst::serde` provides
`to_vec` / `from_slice`, not a JSON, bincode or postcard encoding.
Ordered native-shaped models use ordinary Serde derives and explicit field
adapters without parallel wire records. Codec calls return `fubst::Error`;
transport-facing APIs convert these into the corresponding
`fube_transport::Error` category. TCP framing and transport message/header
schemas remain in `fube-transport`:

```rust
use fube_transport::message::ListenInstance;
use fubst::serde::{from_slice, to_vec};

let value = ListenInstance {
    address: "node".into(), instance: 7, nonce: [0; 16],
};
let bytes = to_vec(&value).unwrap();
assert_eq!(from_slice::<ListenInstance>(&bytes).unwrap(), value);
```

| Mapping | Native behavior |
|---|---|
| bool, u16, i32/u32, i64/u64, str/String | Existing typed metadata, compressed integers and strict little-endian UTF-16. |
| Named struct / multi-field tuple struct | One ordered native object. Fields follow declaration order; field names/renames do not appear on the wire. |
| Newtype struct | Transparent; `struct Wrapper(T)` does **not** add an object. Use a named one-field struct when an object wrapper is required. |
| `Guid([u8; 16])` | Native GUID metadata and Windows field-order bytes, e.g. `Uuid::to_bytes_le()`, not UUID network order or byte-array metadata. |
| `#[serde(with = "fubst::serde::guid")]` | The same explicit GUID mapping on an existing `[u8; 16]` field, without a parallel record or changing its public type. |
| `ByteArray<T>` | Native byte-array metadata; encode Vec/slice/array bytes, decode `ByteArray<Vec<u8>>`. |
| `ObjectArray<T>` | Native object-array metadata even when empty; encode a Vec/slice of struct-shaped elements, decode `ObjectArray<Vec<Row>>`. |
| `EmptyObjectArray` | A required object-array slot that must have zero elements; rejects nonempty arrays and wrong metadata. |
| `CountedStrings<T>` | Native UInt32 count followed by inline strings, including an explicit zero count. Encode Vec/slice strings; decode `CountedStrings<Vec<String>>`. Not string-array or object-array metadata. |
| `Pointer<T>` | Explicit nullable native object pointer. `None` writes the empty-pointer marker; a plain struct target requires empty type information. |
| `FixedTag<T, TAG>` | Automatically validate a fixed i32 type tag, then derive-deserialize the struct fields. The tag is not an extra body field. |
| `Untagged<T>` | Automatically require empty type information for a struct-shaped object. |
| `RawTagged<T>` | Retain bounded raw type information separately from ordinary derived fields; the caller must validate the tag according to its operation profile. |
| `NativeEnum<T>` | Derived polymorphic enum: explicit i32 variant codes select the object tag, the matching i64 body kind and inline variant fields. |
| `NumericEnum<T>` | Strict derived unit enum encoded as a single signed i64 code, without object framing. |
| `Typed<T>` / `NativeType` | Escape hatch for exceptional tag-dependent schemas, not required for fixed schemas, raw-tag retention or derived polymorphic enums. Supports roots, nested objects, object-array elements and pointer targets. |
| `Inline<T>` | Struct fields in the current object scope without a nested object. |
| `Derived<T>` | Advance to the next native scope, then read/write the struct's fields inline. |
| `Trailing<T>` | A positional suffix: `None` is genuine absence at the current scope end, not a null pointer. Present malformed data remains an error. |
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
integer masks directly. Standalone flags in human-readable formats use flag-name
strings. Query parameter fields instead select a numeric flag adapter in both
configuration JSON and native query maps, including `HealthStateFilter` inside
the EventsFilter JSON string.

Generic Vec/array/tuple/map, Option, enums, unit, floats, chars and other integer
widths are unsupported unless used through an explicit native adapter.
Ordinary enum indices and Rust discriminants are not native codes. Explicit enum
adapters use canonical signed decimal variant renames, never declaration order.
Empty container kind is never inferred from the sequence length or first element.

Native structure is explicit in the wire DTO rather than inferred from ordinary
Option/Vec/enum types. For example, a native object array of evaluation wrappers
uses `ObjectArray<Vec<Slot>>` with a named `struct Slot { value:
Pointer<NativeEnum<Evaluation>> }`; a transparent one-field tuple struct would omit
the required outer object.

For fixed schemas, use `FixedTag<Fields, 16>` or `Untagged<Fields>` with ordinary
Serde struct derives. No `deserialize_fields` callback is needed. For polymorphic
objects, derive an enum with explicit native codes:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
enum Evaluation {
    #[serde(rename = "1")]
    Event { description: String },
    #[serde(rename = "0")]
    Invalid,
}
```

Wrap it in `NativeEnum<Evaluation>`. The backend implements Serde
`EnumAccess`/`VariantAccess`: it reads the raw four-byte i32 tag, verifies the
matching i64 body kind, selects the derived variant and deserializes its fields
inline. Unit, struct and tuple variants are supported; a newtype variant must
contain a struct-shaped field schema. Do not repeat the kind in the payload
DTO. A partition safety-check payload uses a `Derived<PartitionFields>` field
to enter its derived scope. Encoding uses the same mapping in reverse.

`NumericEnum<T>` instead maps derived unit variants to signed i64 scalar codes.
It remains available for derived unit enums; unknown codes are errors.
Application status, definition kind, service kind and service status implement
their explicit signed-i64 mappings directly and reject unknown codes without
parallel wire enums. Strict application, service, partition, replica, deployment,
host, entry-point and reconfiguration scalar enums declare their variants and
native codes once through `strict_enum!`, which generates the enum and its Serde mappings.
Documentation, derives, default variants and Rust variant order are preserved;
native codes are not Rust discriminants. Node scalar enums (`NodeStatus`,
`HealthState`, deactivation/task enums and `SafetyCheckKind`) implement Serde
through the shared numeric-enum macro: both native and JSON formats use their
signed i64 codes, including `Unrecognized(i64)`. This preserves the deliberate
strict-list versus unknown-retaining-node distinction.

Both adapters require explicit canonical numeric variant names. Unknown codes
are rejected even if an enum declares `#[serde(other)]`; fallback variants cannot
silently accept unknown structural kinds. Use one unique canonical code per
variant; Serde aliases are outside the native schema contract. Nonnumeric names
are rejected.

Only exceptional raw tag policies need `NativeType::deserialize_fields`.
The client retains it for paging status, where existing node-query policy
accepts arbitrary type information but application/service queries require an
empty tag. The supplied deserializer still handles the ordinary struct fields.

`Inline` and `Derived` require an open object. `Trailing` is only for positional
suffixes: once one suffix is omitted, all following fields in that scope must
also be omitted. `Trailing<Pointer<T>>` distinguishes an absent suffix from a
present null pointer. Defaults are applied by model field adapters or conversion
code only after decoding genuine suffix absence, never by catching malformed
input. `Derived` skips supported unread base fields
using the codec before entering the next scope.

`to_fields` / `from_fields` remain escape hatches for partial schemas and policies
that need incremental decoding. They leave following fields available and do
not finish the enclosing object. Encoder errors roll back the failed bridge's
bytes; decoder errors do not rewind input.

Only fixed positional schemas are wire-preserving. Do not apply flatten,
skip/skip_serializing_if, default or Serde enum-representation attributes
(`tag`, `content`, `untagged`) to native DTOs. Explicit numeric variant renames
are the supported mapping for the two enum adapters.
The backend rejects observable unsupported calls, but cannot detect every
attribute or custom serializer that hides a positional shape change.
Serialization custom errors are bounded static `InvalidInput` diagnostics;
deserialization custom errors are `Protocol`, retaining malformed-success
connection retirement. Codec errors retain their existing categories.
Supported unknown trailing values/scopes are skipped, not retained for
lossless re-emission.

Production adoption includes listen-instance controls/common header writers,
query requests/results, ping/NameExists replies, complete node/deactivation/task
and node-health records, application/service records, naming URI bodies and
complete optional fault details. Safety checks and health evaluations use
derived enums through shared native dispatch. Query wrappers use FixedTag;
naming URI encode/decode share the type's private Untagged component schema
using CountedStrings.
Header record framing/duplicate rules and query argument order are unchanged;
QueryAddress is not added to the body.

Aligned public models deserialize directly: `Node`, `NodeHealth`, `HealthEvent`,
`EventHealthEvaluation`, `HealthEvaluation`, `NodeDeactivation`,
`NodeDeactivationTask`, `NodeDeactivationTaskId`, `SafetyCheck`, `Application`
and `Service` no longer need parallel wire records. Shared field adapters select
object arrays and required tagged-pointer slots without exposing wrappers in
model fields.
`NodeTimestamp` transparently maps signed i64 ticks; `NodeId` maps an exactly
16-byte numeric byte array (high then low little-endian words), not a GUID.
Health models also serialize using the same native mapping.
`Node` retains both native `id` and `id_copy` fields; node-list validation checks
their agreement after deserialization.
`SafetyCheck` exposes all eight native variants with explicit codes, rather than
collapsing partition variants into `Partition { kind, partition_id }`.
The six partition variants share `PartitionSafetyCheck { partition_id }`; a
field adapter enters the native derived scope and decodes GUID bytes into a UUID.
`SafetyCheck::kind()` returns the discriminant implied by the variant.
`PartitionInformation` uses an ordinary Deserialize derive; its containing
partition field selects a shared untagged-object adapter, and its UUID field
selects the shared native GUID adapter. JSON retains the ordinary object and
UUID-string shapes. Deployed application health uses the generic trailing-default
adapter; `HealthState` owns its Unknown default through `Default`. The codec has
no health-specific adapter or default policy. Neither record needs a handwritten
deserializer or remote derive; malformed tags, GUID metadata and present
suffixes still fail through the real query path.
`NodeDeactivationTask` preserves the nested native identity as
`id: NodeDeactivationTaskId { id, task_type }`, followed by `intent`.
Callers access the task string through `task.id.id` and its subsystem through
`task.id.task_type`; neither model needs a custom deserializer.
`Application` matches the native nine-field record, including the deprecated
upgrade fields. Both native `std::map` fields remain `BTreeMap` values, decoded
from bounded string-pair object arrays with duplicate-key rejection. Explicit
trailing-field adapters apply native empty-string/map and Invalid defaults;
health and deprecated-field restrictions remain query-level validation.

`Service` matches the native eight-field record rather than consolidating kind
and persistence. Migrate `ServiceKind::Stateful { has_persisted_state }` to
`ServiceKind::Stateful` plus `service.has_persisted_state`. Explicit suffix
adapters default missing status/group fields to Unknown/false; query validation
checks kind/persistence consistency after structural deserialization.
`MessageId` and `ListenInstance` also derive Serde directly, using the shared
GUID field adapter without changing their public field types. Header IDs remain
inline fields in the existing header object; listen controls retain their
ordinary object framing.

Structural helpers remain where native envelopes differ from public results:
`QueryResultEnvelope` carries the result kind, payload pointer, native error
code and paging pointer. Shared derived `ListPayload`/`ItemPayload` schemas
preserve both native list and single-item slots and their profile-specific
requirements. `EmptyObjectArray` rejects populated item-result list slots;
`RawTagged<PagingStatus>` retains paging tags without a custom paging deserializer.
These are not duplicate domain records.
`NamingUri` owns its genuinely different component representation.
No generalized derive framework or changed default Vec/Option mapping is needed.

Validation is above the generic Serde backend. That backend enforces bounds,
framing, metadata and tags; checked semantic types validate after their component
decode, before returning a valid typed value. Record/operation validation still
enforces duplicate node IDs/parameter keys, list health restrictions, deprecated
fields and service kind/persistence consistency. In particular `HealthState`
retains unknown codes structurally, then list validation rejects them; node and
health results continue retaining them. Node paging retains its permissive type-info
policy; application/service paging requires an empty tag. Payload validation
precedes reporting an embedded query error, so malformed required records remain
Protocol rather than becoming fake domain success. Actor/action, Item/List
invariants and owner policy remain outside the serializer. Ordinary failure
decoding requires a nonzero code, accepts native code-only suffixes and rejects
malformed present diagnostics; runtime IPC and other domain-specific error
policies remain separate.

See the [backend contract and compiling example][native-serde],
[transport payloads][serde-messages], [query DTOs][serde-query],
[health reply declaration][serde-health] and [list validation][serde-lists].

[native-serde]: ../../crates/libs/fubst/src/serde.rs
[serde-messages]: ../../crates/libs/fube-transport/src/message.rs#L12-L123
[serde-query]: ../../crates/libs/fube-client/src/query_protocol.rs
[serde-health]: ../../crates/libs/fube-client/src/health_protocol.rs
[serde-lists]: ../../crates/libs/fube-client/src/list_protocol.rs

## 5. Offline and native qualification

Use nextest 0.9.146, pinned in CI. The repository requires at least 0.9.133 for
test-group filters. Ordinary checks need neither onebox nor native client
libraries:

```sh
cargo nextest run --locked -p fube-transport -p fube-client \
  -E 'not group(onebox)' --no-tests fail
cargo clippy --locked -p fube-transport -p fube-client --all-targets -- -D warnings
cargo doc --locked -p fube-transport -p fube-client --no-deps
```

**Live onebox exchanges are the wire-compatibility authority.** Offline tests
exercise parser bounds, malformed input, internal round trips, and connection
lifecycle behavior; they do not establish native protocol correctness.
There are no committed golden packet hex files or normalized capture snapshots.
Native compatibility claims require the self-contained live scenarios below
to pass against the real onebox.

Offline query tests check explicit query names and argument key/value semantics
through one shared request decoder, without treating facade insertion order as
native byte identity. Query declarations are checked against source-derived
record/tag/paging/actor profiles; generic envelope and error-precedence behavior
is tested once in the shared query layer.

Handwritten record layouts remain model-conformance regressions: they detect
DTO mapping changes but share the production Rust codec, so they do not
authenticate their own wire format. Raw-prefix truncations are codec-level
extent tests, not field-by-field validation; record tests retain malformed
fields inside valid extents and absent-versus-malformed suffix checks.
Allocation, ACK, cancellation, and race tests retain their distinct invariants.
The codec checks string metadata against the
[pinned native type definition](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/shared/serialization/inc/FabricSerializationTypes.h#L12-L40),
but source-derived assertions and Rust round trips are not runtime
interoperability evidence.

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

The later shared-adapter/direct-model expansions and focused resolution/
notification implementation were requalified with the five live scenarios on
SF **11.8.121.4**, as well as offline byte comparisons, malformed-input
regressions and independent model peers. Cases/fields not exercised by those
scenarios retain their documented evidence limits.

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
cargo nextest archive --locked -p fube-transport -p fube-client \
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
8 initially Pending overlapping mixed operations on clones (4 ping, 2 existing
names, 1 absent name, 1 node query), ninth local refusal and slot-release reuse,
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
HealthEntityNotFound followed by healthy same-owner reuse, 8 mixed
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

On one shared `FurpcClient` it confirms initial absence, creates the name,
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
same-client reuse, runtime and archive identity. Its required `furpc` section
records the original source generation, native duplicate/missing error origins,
independent HTTP presence/absence, three concurrent Ping/NameExists operations
after errors, and observation timestamps. Every successful write and read checks
the original generation. CI validates this receipt and binds its observation
window as `furpc_naming_writes`. Keep journals private/ignored;
after exact destruction, remove private profiles and extracted consumers.
Never use these fixtures on a shared cluster.

The current furpc name archive passed this lifecycle and the independent receipt
validator on **11.8.121.4**, retaining generation 1 through native errors, write
reuse and concurrent cloned reads. It did not rerun the other native scenarios.

The earlier legacy native-name archive qualified on **11.8.121.4**, five logical nodes in one
Ubuntu container, with **119 offline tests and all three live group scenarios**.
Offline tests cover exact requests, call-local malformed-domain rejection, concurrent
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

The earlier **11.8.121.4** application/service delivery documented four passing
live scenarios and 148 offline tests plus four CLI
parser tests. Offline evidence covers strict suffixes, hostile bounds, enums,
malformed successes, native errors, out-of-order mixed queries, deadlines,
ownership collisions, ambiguous mutation retention and cleanup dependency order.
The current maintained native group has five tests; owned MovePrimary is included
inside the list fixture. The movement now uses the same furpc client as the
list/topology/deployment reads. Its required `movement.furpc` evidence retains
the actual mutation response generation, requires the original list generation,
and records successful concurrent replica/partition/Ping reuse. The existing
`validate_native_movement` still independently requires a single constraint-enforced
request and the actual HTTP/native role swap on the same owned replica identities;
an empty success alone cannot satisfy it.
Current migration results are new executable receipts,
not a relabeling of that earlier four-scenario archive.
Live coverage does not establish Compose/Mesh applications, service groups,
persisted stateful data, upgrade/deletion transitions, partial aggregation,
byte-size paging, divergent unions, other runtime versions or full URI grammar.

### Build once, run the archive on onebox

The Linux Rust job compiles the selected packages' test binaries, including
ignored onebox cases, into a Zstandard-compressed tar archive:

```sh
cargo nextest archive --locked -p fube-transport -p fube-client \
  --archive-file build/native-client-tests.tar.zst
cargo nextest run --archive-file build/native-client-tests.tar.zst \
  -E 'not group(onebox)' --no-tests fail
```

CI uploads that archive after the Linux Rust checks pass. The Linux onebox job
depends only on that producer and downloads and consumes its archive. Both Linux
runners use Ubuntu 24.04, and the producer exports its pinned nextest version so
the onebox consumer installs the same version.

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

### Owned resolution and notification qualification

`onebox_resolution_and_notifications` is the fifth serialized live scenario.
It requires a new disposable onebox, an explicit absolute
`MSSF_NOTIFICATION_CONFIG`, the matching `MSSF_LIST_OWNER`, and the exact
`MSSF_TEST_ARCHIVE_SHA256`. The profile contains `profile` (`clear`),
`allocation`, exact controller `allocation_owner`, `endpoint`, `runtime`,
workload `owner`, and absolute fresh `journal`/`report` paths. The allocation's
private state must match owner/name/runtime and be ready.
The [Rust fixture setup](../../crates/libs/fube-client/tests/support/notification_setup.rs)
can generate a new profile when `MSSF_NOTIFICATION_ALLOCATION` explicitly names
the owned allocation and `MSSF_NOTIFICATION_JOURNAL`/`MSSF_NOTIFICATION_REPORT`
provide absolute fresh output paths. It reads the allocation's actual owner and
runtime, validates every ownership/readiness gate, and refuses to overwrite an
existing profile or evidence. CI supplies only these environment inputs, not
an inline configuration generator. Leave `MSSF_NOTIFICATION_ALLOCATION` unset
to use an existing explicit JSON profile with the same validation.
Never select these mutating fixture tests against a shared or pre-existing
allocation that you do not exclusively own.

```sh
cargo nextest archive --locked -p fube-transport -p fube-client \
  --archive-file build/native-client-tests.tar.zst
export MSSF_TEST_ARCHIVE_SHA256="$(sha256sum build/native-client-tests.tar.zst | cut -d ' ' -f 1)"
mkdir -p build/notification-evidence
export MSSF_NOTIFICATION_CONFIG="$PWD/build/notification-evidence/config.json"
export MSSF_NOTIFICATION_ALLOCATION=your-new-owned-onebox
export MSSF_NOTIFICATION_JOURNAL="$PWD/build/notification-evidence/journal.json"
export MSSF_NOTIFICATION_REPORT="$PWD/build/notification-evidence/report.json"
export MSSF_LIST_OWNER=your-exact-workload-owner
cargo nextest run --archive-file build/native-client-tests.tar.zst \
  --run-ignored only -E 'test(onebox_resolution_and_notifications)' --no-tests fail
```

The scenario journals a unique type/upload/two applications before mutation and
cleans only those exact identities, including after panic/error. It additionally
creates two named partitions inside an owned application, changes one owned
stateless service from five endpoints to one, removes an exact owned primary
replica to observe replacement/versioned updates, and deletes an owned service
to observe empty invalidations. Native results must eventually match independent
HTTP endpoint/role **multisets**; equal/empty address strings are not unique IDs.
A test-only relay drops exactly one successful native processing ACK, observes a
gateway retry, and verifies no repeated endpoint version is delivered.
Production operations never use HTTP or this relay.

The required `furpc_notifications` receipt records three actual, independent
transport generations, confirmed unregister/close counts, initial prefix/exact
updates and empty invalidations. Validators bind those events to the independently
observed owned partition and require the native ACK-loss/retry witnesses.
Initial updates can precede full placement; they are not a five-endpoint snapshot.

On SF **11.8.121.4**, clear TCP, the scenario demonstrated six singleton/int64
partition lookups plus two named lookups, previous-result refresh, native errors
with healthy reuse, three independent notification sessions, exact/prefix/
primary-only registration, updates, endpoint-count/primary changes, invalidation,
unregister and independent close. The relay observed one dropped successful ACK,
one native retry and zero duplicate delivered versions. The original gateway
identity was returned as node N0010; its notification address was intentionally
empty, not replaced with an advertised/dialed private endpoint.

Qualification limits: these implicit-host volatile fixtures publish empty
address strings. Nonempty/structured address preservation is covered offline,
not by an application routing demonstration. Primary-only registration is live
qualified, but isolated content-change delivery with distinct primary addresses
is not. No persisted application-data replication, TLS, authentication,
cross-release compatibility, automatic recovery or transparent failover is
claimed. Profiles, journals, reports and packet-free diagnostic artifacts stay
local/ignored.

### Native ImageStore upload evidence

The focused library run passed **419 tests, with 5 live tests ignored**;
production library check and strict all-target Clippy passed. The CLI's two
subprocess/controlled-native-peer tests also passed. Upload tests cover empty
and full-size/multichunk content, exact routed Delete schemas, postorder markers,
native errors, wrong envelopes, bounded handoff/stale replies, prior-file cleanup
and duplicate commits, and retained-client cancellation/deadline retirement.

**End-to-end qualification passed on an owned SF 11.8.121.4 onebox.** One real
native mssf SDK copy established the predecessor inventory, including a deliberate
obsolete file. Rust CLI replacement matched that inventory minus the obsolete
file and independently matched every local file size and generated zero-byte
`_.dir` marker. Coverage included nested Unicode/space paths, empty
files/directories, **7 MiB** multichunk data and a **132 MiB** sample binary.
sfctl provision/create then succeeded; an independent native runtime observer
resolved/contacted the live application, and deployed Data **SHA256 hashes
matched the local source**. Exact owned-workload cleanup passed.
The SDK adapter invokes the real native CopyApplicationPackage API; sfctl
remains provisioning/observation tooling, not production upload or fallback.

The final scenario uses **one SDK predecessor copy**, not a redundant second SDK
copy/reference root. That earlier redundant native-only call intermittently
exited **139**, with no cause established by source inspection. No SDK fallback,
retry, delay or destructor workaround was added. The successful final scenario
does **not** establish repeated native SDK copy reliability.
The opt-in scenario is
[`tests.onebox.native_upload`](../../tests/onebox/native_upload.py); it does not
replace the existing CI uploader or join the existing live test group.

## 6. Deliberate exclusions

No TLS/rustls, authentication, HTTP operation fallback, Windows-specific
integration, native client wrapper, full URI/API coverage, cached or prefix
resolution, legacy change callbacks, service-group expansion, application
endpoint connections, automatic recovery/replay, or production failover is provided.
The onebox is privileged and unsecured: keep it local with loopback-only
published endpoints. Do not use this profile on a production or shared cluster.

The [production design](design/FabricClient.md) and
[broader implementation plan](design/ImplementationPlan.md) remain separate.
This POC does not complete their production lifecycle or TLS requirements.
