# Rust FabricClient POC

**Status:** experimental native clear-TCP subset. Gateway ping, NameExists,
typed GetNodeList, single-node GetNodeHealth, and native CreateName/DeleteName
have been demonstrated against the repository's Ubuntu onebox running
Service Fabric **11.8.121.4**. This is not a complete FabricClient replacement
and does not support TLS or authentication.

## 1. What is implemented

`mssf-transport` implements bounded Fabric serialization, clear frames and CRCs,
ordered native headers, client-only ListenInstance establishment, and
continuously driven, bounded concurrent Tokio request/reply I/O. `mssf-client`
adds native gateway ping, NameExists, typed node-list pages, node health and
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
`NodeStatus` discriminants. `from_bits` preserves any UInt32 native mask.
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
| `from_bits(u32)` | Sent unchanged. Other bits do not match; exact special-value equality matters: NONE \| ERROR = 9 selects Error, not none. |

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

Tests that need onebox are named `onebox_*` and assigned to the shared
**`onebox` nextest group** in `.config/nextest.toml`. They remain `#[ignore]`
for ordinary Cargo runs. The group permits only one test at a time; any
scenario may run first. Each test prepares its own fresh independent
observations and completes its cleanup before returning. Setup and restoration
are helper functions, not tests with ordering dependencies. Select the group
once; no individual test-name selectors or manual preparation are needed.

On a fresh disposable local onebox, provide absolute private output paths and
a unique fixture owner. The baseline helper uses test-only `reqwest` HTTP
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
Proxy use and redirects are disabled. No additional CI job is needed.
`reqwest` has JSON support only, with default/TLS features disabled, and is a
dev-dependency rather than a production client dependency.

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
# MSSF_TEST_CONFIG / MSSF_TEST_REPORT must also be set, as above.
cargo-nextest nextest run --archive-file "$archive" --workspace-remap "$PWD" \
  --run-ignored all -E 'group(onebox)' --no-tests fail
```

These commands extract and execute prebuilt tests without rebuilding them.
`--workspace-remap` handles a different checkout path. Empty selections fail
rather than silently reporting success. Only the three sanitized native success
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
