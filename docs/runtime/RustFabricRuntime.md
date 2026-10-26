# Pure Rust FabricRuntime design

## 1. Decision and scope

A native-Service-Fabric-library-free Rust runtime is a plausible project, but it
is **not just a Rust implementation of `FabricRuntime.idl`**. It must replace the
application process's hosting client and Reconfiguration Agent Proxy (RAP),
speak the node's private binary protocols, and implement the service lifecycle.
Stateful replication is a separate, substantially larger compatibility tier.
The companion [native architecture](FabricRuntime.md) explains those boundaries.

**Status:** proposed design, not an implementation or interoperability claim.
Research baseline: local Service Fabric source at
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`; repository baseline
`e87b47ad1f36e8fa37f3b392bed7a8b2226bee75`, inspected 2026-10-25.
No Rust runtime was connected to a native node for this investigation. The
source revision is not assumed to match the repository's 11.8.121.4 onebox.

Here **pure Rust runtime** means application-side Rust hosting, lifecycle and
protocol code without `FabricRuntime`/`FabricCommon` libraries, native COM
adapters, a native runtime helper process, or native Service Fabric replication
libraries. The existing native node, placement/reconfiguration authorities,
deployer and OS remain. This does not propose rewriting the cluster, replacing
FabricClient, or making existing C++/managed binaries load a Rust DLL.
Cryptographic-provider and OS dependencies must be declared separately; choosing
a Rust TLS API alone does not establish an entirely Rust dependency tree.

The first target should be an **SF-activated Linux ExeHost with one code package,
ordinary stateless service factories, and an explicitly qualified local IPC
security profile**. Multiple service instances in that process are required;
"single code package" must not become "one partition per process."
Nonactivated hosts, DLL/multicode-package hosts, guest-service activation,
containers and Windows security are separate profiles, not implicit fallbacks.

## 2. Compatibility tiers

| Tier | Deliverable | Explicitly not promised |
|---|---|---|
| H: hosting foundation | Native host registration, runtime registration, activation/node context, liveness and cleanup for one declared host/security profile | Successful factory registration or replica placement by itself |
| S: stateless runtime | Service-type registration; native stateless open/close/abort; partition reporting; endpoint publication; restart and upgrade qualification | Stateful factories, built-in replication, Reliable Collections |
| C: custom stateful runtime | Stateful lifecycle and complete SF-facing custom-replicator contract with epoch/configuration/catch-up/progress semantics | Native replication wire format or mixed Rust/native replication peers |
| N: native-compatible classic replication | Native copy/replication protocol, acknowledgements, queues, epochs and state-provider integration | Transactional replicator, KVS or Reliable Collections compatibility |
| T: transactional/data stack | Explicitly selected transactional APIs, state providers, durability, recovery, backup/restore and upgrade contracts | Automatic compatibility with existing disk formats or every collection |
| A: ABI compatibility | Optional separately specified C/COM ABI facade over qualified functionality | Loading arbitrary existing C++/managed applications merely because symbols exist |

These are capability boundaries, not independent checkboxes. H is a prerequisite
for S/C; C needs a working replicator, not a no-op role handler. N/T require
their own native qualification. A is unnecessary for new Rust applications and
must not force COM ownership rules into the Rust core.

The favorable Linux feasibility finding is that the **default app-host lease
monitor polls node Hosting over IPC**; this traced path does not require a new
per-host native lease-driver registration. The native node still owns leasing.
Initial TTL conversion and deployed-build behavior remain qualification gates,
not reasons to assume liveness is already solved.
[Native lease analysis](FabricRuntime.md#92-default-linux-app-host-lease-monitoring-uses-ipc).

## 3. What this repository can actually reuse

| Existing component | Reusable part | Work still required |
|---|---|---|
| [`mssf-transport` frames][rust-frame] | Bounded clear Fabric frames, CRCs and native header records | IPC-appropriate negotiated/admitted limits and secure profiles; current frame cap is 4 MiB |
| [Native serializer][rust-serde] | Ordered objects, GUIDs, explicit pointer/tag/scope/suffix adapters and allocation bounds | Hosting/RAP schemas, nested native records, enum validation and source-version policy |
| [Connection driver][rust-connection] | Continuous bounded I/O, correlation, deadlines, cancellation and failure fan-out | Generic incoming actor dispatch, one-way sends, IPC identity and runtime-specific reply policies |
| [`mssf-client`][rust-client] | Optional independent management/qualification observer | It is a gateway client, not an application host or RAP |
| [`mssf-ext`][rust-ext] | Examples of replication-facing application concepts | It wraps native `IFabricStateReplicator2`; it is not a native-free engine |
| [`mssf-rplctr`][rust-replicator] | Experimental Rust custom-replicator vocabulary and tonic transport | Epoch, progress, abort, quorum, configuration, build/remove and streams contain TODO/dummy behavior |
| [`kvmap`][rust-sample] | Deployed application packaging and workload ideas | It uses `mssf-core` runtime/context and calls native partition `CreateReplicator` |

In particular, `connect_duplex_clear` is **not generic runtime duplex IPC**.
Its only accepted reverse envelope is the NamingGateway service-notification
protocol; its consuming reply context constructs `ServiceNotificationReply`.
Native runtime commands include different actors, actions and one-way protocol
replies. Do not route them through that notification exception.
[Evidence: reverse dispatch][rust-reverse-dispatch] and
[reply construction][rust-reverse].

The native IPC source defaults to 16 MiB while this Rust codec/frame layer has
4 MiB object/frame bounds. Neither number should be blindly copied to every
connection. Establish limits from the selected profile and real startup/package
payloads; reject oversize inputs explicitly and keep aggregate memory bounded.
[Native defaults][native-ipc-config]; [Rust limits][rust-limits].

No existing Rust component above establishes native runtime interoperability.
The [current client qualification][client-status] concerns gateway operations,
not host registration, RAP or replication.

## 4. Proposed component structure

Add a single `mssf-runtime` crate initially, using `mssf-transport`. Split crates
only when a separately usable boundary emerges. Keep wire DTOs private rather
than exporting node-internal records as the service programming model.
Do not depend on the existing native runtime/COM adapters just to reuse their
public types. Use owned native-free types or a separately verified common type
surface, and verify the resulting dependency/link graph.

```text
Rust service application
    Runtime / activation snapshot / factories / partition handles
                          |
    host supervisor + service-unit lifecycle scheduler
                          |
    hosting client       RAP protocol adapter       reporting clients
         \                      |                        /
          typed private wire DTOs + actor/action dispatch
                              |
    generic bidirectional Fabric IPC session / security profile
                              |
                    existing native node

optional later:
    custom replicator OR native-compatible classic replication engine
        -> own peer transport, copy streams, operation acknowledgements
    transactional runtime/state providers
        -> logging, checkpoints, recovery, backup and restore
```

The node continues to own placement and membership decisions. The Rust process
must honor native reconfiguration commands, not elect itself primary or infer
authority from a management query.

### 4.1 Public Rust API

Expose owned Rust types, `Result`, async lifecycle methods and cancellation.
Factory registration lives for the runtime lifetime; do not invent a
service-type-unregister wire operation to implement a convenient drop guard.
Explicit shutdown is asynchronous; `Drop` can cancel/retire ownership but cannot
claim that remote unregistration completed.

Conceptually:

```text
Runtime::attach(options) -> Runtime
Runtime::activation_context() -> immutable, versioned snapshot
Runtime::node_context() -> owned node information
Runtime::register_stateless(type_name, factory) -> Result<()>
Runtime::shutdown(deadline) -> Result<()>

StatelessFactory::create(service_identity, initialization_bytes)
    -> service instance
StatelessInstance::open(partition, cancellation) -> opaque endpoint string
StatelessInstance::close(cancellation) -> Result<()>
StatelessInstance::abort()
```

These are API sketches, not existing signatures or compilable examples.
Identity includes native partition/instance IDs; initialization bytes stay
opaque. Return the application's actual listening address, after its listener
is ready. Do not make the runtime serialize application traffic or assume
every returned address is FabricTransport.

Partition handles carry the identity and generation of their owner. Calls after
close/invalidation fail explicitly. Registration, factory and instance objects
are `Send`/`Sync` as required by the executor; user callbacks run outside protocol
locks and outside the socket reader.

## 5. Transport and schema work

### 5.1 Generic IPC, not a gateway mode switch

Preserve the existing gateway behavior while introducing an explicit IPC
session profile. Reuse low-level codecs and I/O machinery rather than copying a
second TCP driver, but do not admit arbitrary reverse traffic on gateway
connections.

The IPC session must support:

- Client identity/PID header on requests and one-way traffic sent through the
  IPC client, as native `IpcClient::OnSending` does. Native correlated replies
  through `ReceiverContext::Reply` instead add `RelatesTo` and send directly;
  do not assume all message directions have identical header requirements.
- Continuous incoming requests, one-way commands and correlated replies on the
  same outbound-established TCP connection.
- Actor/action dispatch independent of `ExpectsReply`: absence of that flag
  does not make a native lifecycle command ignorable.
- A consuming correlated-reply capability and a separate one-way send path.
  RAP's semantic reply action is not necessarily a transport `RelatesTo` reply.
- Bounded data/control queues with reserved shutdown/fault/reply capacity and a
  documented overload outcome. Never silently discard a required lifecycle reply.
- Complete native header/timeout/error preservation appropriate to each schema,
  partial-I/O safety and no user callback on the reader.

The relevant lifecycle actor is **RA (12) in both directions**, not RS (13).
`ReplicaOpen`/`ReplicaClose` and semantic replies are one-way messages;
`RAPQuery` is correlated. Include service-description updates, queries and
app-host context/invalidation control in reconnaissance rather than assuming
only open/close can arrive for a stateless deployment.

The native server learns the reverse route from the client's `IpcHeader`; the
application host does not need a second arbitrary node-to-app listening port.
That identity header is not, by itself, authenticated proof of process identity.
See [architecture: IPC](FabricRuntime.md#5-ipc-and-message-routing).

### 5.2 Schema inventory and compatibility

Build a checked inventory before implementing each flow:

| Schema group | Required evidence |
|---|---|
| Host start/finish/unregister | Action, actor, native field order, host context, timeout, node/lease information and error semantics |
| Runtime register/unregister | Full runtime context, runtime identity and code-package identity/instance fields |
| Service-type register | Service-type identity/context and registration sequence semantics |
| Lifecycle | Failover-unit/service/replica descriptors, replica-instance IDs, flags, endpoints and proxy errors |
| Reports and access | Load/fault/health/move identities, sequence/ack behavior where applicable, read/write access rules |
| Context/package updates | Node context, deployed-package paths, resolved endpoints, package versions and change notifications |

Use native `FABRIC_FIELDS_*` declarations plus the actual sender/receiver, not
IDL parameter order or a neighboring query-result schema. Nested objects need
the same treatment. Public ABI `Reserved` extension chains are not the wire's
serialization scopes. Case comparison, signed IDs, URI/GUID representation and
optional suffixes are contract decisions, not Serde defaults.
For example, the 17-field start-registration reply includes logical-directory
maps and nested shared-log settings even for a stateless process. The existing
Serde backend does not support arbitrary bare maps: implement the appropriate
native schema/adapter or bounded structural skip rather than assuming every
native type can simply derive Serialize/Deserialize.

Unknown optional tails may be skipped only when the bounded serializer and
operation schema permit it. Unknown required kinds/actions/versions must fail
the supported profile explicitly. Use a protocol failure reply only where a
source-defined reply exists; otherwise retire the session/host rather than
invent an error action or reproduce native coding assertions on untrusted
input. Never deserialize a new version into plausible zero-valued state.

## 6. Ownership and lifecycle invariants

Use a process-wide host supervisor, a runtime registry and a service-unit table.
Runtime IDs, host IDs and service-unit IDs are distinct. Keep code-package and
replica incarnation/version information; a GUID alone does not fence old work.

For each service unit, serialize the incompatible operations required by the
native state machine. Do not assume the entire process receives one operation
at a time. The table must permit independent partitions to progress concurrently.
The actual transition/reply matrix must be derived from the
[native RAP](FabricRuntime.md#6-service-lifecycle-and-the-rap), not from this
simplified model:

```text
Absent -> Opening -> Open -> Closing -> Closed
             |         |        |
             +---------+--------+----> Aborted / Invalidated
```

Required invariants:

| Invariant | Implementation consequence |
|---|---|
| Native identity and incarnation checked before dispatch and completion | A late factory/open callback cannot recreate an invalidated replica or publish an old endpoint |
| Each command follows its source-defined execute/reply/drop policy | Duplicate/retried commands need native-compatible idempotence, stale rejection or recorded result; some incompatible work is deliberately left for RA retry |
| User cancellation is advisory until termination is observed | Dropping a future cannot be treated as proof that its listener or mutation stopped |
| Close is graceful; abort is terminal | Preserve user-close errors and run required abort cleanup; acknowledge terminal close according to native semantics only after the unit is actually disposed |
| Endpoint publication follows successful open/role work | Never return a configured port as proof of a running listener |
| No callback executes while registry/state locks are held | Reentrant reporting and shutdown must not deadlock |
| Required control traffic remains possible under load | Reserve bounded resources independently of application tasks |
| Loss of authority fences service behavior | Node/lease/host invalidation must stop work within the qualified liveness contract |

Application panics or stuck callbacks are not successful opens/closes. Define
which failures can be reported and isolated, and which require terminating the
host so the node can recover it. A task cancellation mechanism alone cannot stop
arbitrary blocking or unsafe user code.
In particular, native RAP can acknowledge a failed graceful close successfully
after aborting/cleaning the unit. The Rust API must retain the callback failure
for diagnostics without confusing that with a false wire-level close failure
or premature successful acknowledgement.

### 6.1 Disconnect and recovery policy

Native IPC reconnect is not replay of registration or arbitrary commands.
Transport reachability, host registration, runtime registration, service
registration and lease validity are separate states. Reconnecting TCP does not
re-authorize cached service units.

For the first qualified profile, prefer retiring the session and terminating
the owned host on loss of required node/liveness authority rather than inventing
an incomplete in-process recovery protocol. This is an intentional difference
from native transport retry behavior and from the assumption that every native
stateless listener is forcibly stopped on TTL expiry; neither equivalence is
claimed. Test this conservative policy explicitly. Stop admission, fence
callbacks/endpoints, attempt bounded cleanup, then exit; never remain serving
indefinitely because a close callback is stuck. A later reconnect mode must
specify native registration reconciliation and duplicate/stale behavior before
being enabled.

### 6.2 Linux lease monitor

For the initial default-IPC profile, decode the node lease handle as opaque
data and maintain local monotonic expiration state. Implement the native empty
request to actor Hosting (15), without an invented action, and decode its TTL
reply. Associate each reply with the request's send timestamp:

```text
candidate expiration = monotonic request-send time + returned TTL
```

Do not refresh authority from receipt time, extend it on a failed/malformed poll,
or resurrect a closed monitor when a late reply arrives. Poll before expiration
with bounded requests, and connect expiry/invalidation to the supervisor and
stateful access gates. Wall-clock changes must not extend authority.

The inspected start-registration TTL initialization differs from later polling
and appears to omit the send-time conversion. Before advertising compatibility,
measure the deployed native behavior and specify the initial Rust rule; do not
copy the ambiguous conversion or silently call the duration an absolute time.
Until valid initial authority is established, the proposed Rust host must not
serve authoritative work.

`InvalidateLeaseRequest` must close the monitor and produce the defined reply;
it is not itself the native process-exit callback. Node process monitoring,
activated process lifetime and IPC connection loss are separate signals.
Windows direct native-lease queries and explicitly disabled IPC-polling modes
are outside this first profile.

## 7. Context, reporting and security

Activation context is not a bag of environment variables. Preserve the native
distinction between host bootstrap identity and deployment-derived package,
configuration, directory and endpoint state. Read the selected profile's
authoritative deployed artifacts and node-provided data; do not recover
credentials or missing identity through a management-query guess.

Expose immutable versioned package/configuration snapshots. Once package update
callbacks are supported, retain old snapshots while callbacks use them, apply
updates atomically and define callback registration/unregistration races.
Until then, explicitly qualify only the upgrade behaviors the implementation
can honor; do not acknowledge an unsupported in-place package update as applied.

Stateless S includes partition information and source-compatible load, fault,
health and move-cost behavior required by its advertised API. Health reporting
uses correlated IPC to node RA and forwarding to Health Manager; implement its
batching/sequencing and outcomes, not a gateway-query substitute or local log entry.
Methods not implemented must be absent from the Rust API or return a precise
unsupported-capability error before registration/advertisement makes a stronger
promise.

Security is an explicit profile:

| Profile | Design requirement |
|---|---|
| Qualified Linux local IPC | Use the actual source/deployed transport policy and correct host/PID context; remain within its local trust boundary |
| Container/certificate IPC | Provisioned client identity, expected server certificate/thumbprint policy, Fabric-framed security negotiation and protected frames |
| Windows local IPC | Native Windows authentication/signing semantics and identity/ACL rules; not "TCP is loopback, therefore clear is equivalent" |
| Replication or application listeners | Independently configured and authenticated peer channel; host IPC credentials do not secure these automatically |

The inspected source's Unix non-TLS Windows-credential path does not construct a
Windows secure context. That observation is **not a license to downgrade a
configured secure deployment**, nor evidence about all released Linux builds.
Unsupported security requirements must fail before registration.
See [architecture: security](FabricRuntime.md#9-security-and-liveness).

For TLS, ordinary `TlsConnector::connect(socket)` is not the protocol. Native
negotiation initially carries TLS tokens inside clear Fabric messages; the
framing-protected path then encrypts complete Fabric frames. A Rust TLS record
engine would need an explicit memory-I/O adapter and transition state machine,
with authorization and buffered-input handling tested against native peers.
Preserve certificate validation/pinning semantics; do not use an accept-all
verifier to accommodate self-generated certificates. Windows protection modes
are a separate implementation target.
[Protocol detail](../transport/Transport.md#7-security-and-tls-on-the-wire).

## 8. Stateful design choices

Choose one stateful target before starting C. The service-facing abstractions
may look alike while their interoperability and durability obligations differ.

### Custom replicator

Return a complete SF-facing custom replicator from replica open. Implement
epoch/role transitions, current progress, catch-up capability/quorum,
configuration changes, build/remove replica, data-loss response and abort.
The node remains the membership authority; a private peer protocol must honor
its epochs/configurations and fence former primaries.

Rust-only peers may use a purpose-built protocol, but this is a **custom
replicator**, not native `FabricReplicator` compatibility. A generic consensus
library does not automatically map SF's configuration/catch-up contract or
permit a second independent membership authority. Existing native replicated
services cannot join that peer set without a separately designed protocol.
The incomplete `mssf-rplctr` experiment is not a starting correctness oracle.

### Native-compatible classic replicator

Implement the native engine's peer addressing/session identity, copy-context
exchange, copy/replication streams, sequence/epoch validation, acknowledgements,
quorum completion, queue/backpressure policies, retries and reconfiguration
barriers. Add durable state-provider behavior where persistence is promised.
Application acknowledgement and transport receipt must remain distinct.
Qualification requires mixed native/Rust peers in every supported role, not
only all-Rust replicas exchanging handcrafted packets.

### Transactional replicator and data providers

Treat transaction/log formats, state-provider lifecycle, checkpoints/recovery,
truncation, copy, backup/restore and upgrade compatibility as another project.
A transactional facade over an in-memory map is not a substitute for those
contracts. Existing native disk state must not be opened or rewritten until its
format and crash-recovery compatibility are separately proven.
See [architecture: replication](FabricRuntime.md#8-replication-and-storage-are-separate-layers).

## 9. Implementation milestones and acceptance gates

No calendar estimate follows from source inspection alone. The principal size
drivers are RAP fidelity, liveness/security integration, host variants and the
selected stateful tier.

| Milestone | Work | Required exit evidence |
|---|---|---|
| 0: deployed-protocol reconnaissance | Pin an actual native runtime build; compare schemas/profile with source; observe a native ExeHost registration and service lifecycle | Reproducible ownership-scoped native baseline, actor/action/body inventory, authority/liveness behavior and explicit unsupported variants |
| 1: IPC foundation | Generic duplex dispatch/one-way/reply, IPC identity, bounded resources and selected security | Rust/native bidirectional exchange, correct correlation and unsolicited commands, fragmentation and idle disconnect; existing gateway behavior unchanged |
| 2: host/runtime attachment | Bootstrap, contexts, host start/finish, runtime register/unregister and required liveness | Real node recognizes the unique Rust host/runtime, rejects bad/stale identity, and cleans it up after exit; no native SF libraries loaded |
| 3: stateless service | Factory/type registration, open/close/abort, partition/report APIs, endpoint | Native placement creates real instances; independent service resolution and actual RPC agree; close/restart removes/replaces endpoints |
| 4: stateless resilience | Concurrent partitions, stale/duplicate commands, callback failures, process/node failure and supported upgrades | Relocation/restart/upgrade under load with no stale publication, bounded resources, explicit errors and timely loss-of-authority fencing |
| 5: broader host/security profiles | Containers/TLS, Windows, nonactivated/multicode-package or dynamic packages, one selected profile at a time | Each advertised profile passes its own identity, negative authorization, liveness, restart and upgrade matrix |
| 6: chosen stateful tier | C first, then N/T only if required | Native-driven role/configuration/failure tests plus independent replicated-state and durability oracles matching the tier |

Milestone 2 is a useful feasibility checkpoint but not a usable stateless runtime.
Milestone 4 is the first reasonable boundary for claiming the initial stateless
profile, with its precise supported API/upgrade/security limits documented.

## 10. Validation without self-authenticating fixtures

Maintain three distinct evidence categories:

| Evidence | What it establishes |
|---|---|
| Pinned native sender/receiver/layout review | Static correspondence to inspected source, including nested records and message direction |
| Deterministic model tests | State-machine invariants, bounds, cancellation, stale-completion fencing and one-sided mapping regressions |
| Real native qualification with independent expected outcomes | Interoperability and lifecycle behavior for the tested build/profile/scenario |

Round-tripping Rust-generated messages, compiling a type-profile assertion or
matching manually copied action numbers does not establish the third category.
Do not grow another set of whole-envelope fixtures that all share the same
untested codec. Source-derived primitive/layout probes can supplement, not
replace, native evidence.

Native qualification should deploy a uniquely owned application to a disposable
onebox, using management only to set up and observe the workload. Runtime
operations themselves must go through Rust IPC, not REST or a native helper.
Keep existing user applications and cluster resources out of the test's scope.

The initial stateless matrix must include:

- Successful host/runtime/type registration, missing/invalid bootstrap input,
  duplicate/stale registration and native error followed by safe behavior.
- Multiple instances and partitions in one process; factory/open failure,
  close failure, abort while opening, callback completion after invalidation,
  and listener readiness before endpoint publication.
- Independent discovery of returned endpoints and successful application RPC;
  process kill, host restart, native relocation and supported code/config upgrade.
- Native-observed load/health/fault effects rather than "a send returned Ok";
  access/report handles rejected after lifetime ends.
- Disconnect, node restart and liveness expiry with independently measured
  fencing; slow/stuck callbacks, bounded queue exhaustion and partial frames.
- Negative authentication/authorization for each secure profile, including
  unexpected certificates and wrong host identity; no credentials in logs.

For C/N/T, add primary loss, old-primary fencing, epoch/configuration changes,
minority isolation, build/catch-up, interrupted copy, restart/recovery and
data-loss handling. Validate committed application state independently of the
replication implementation. Claimed persistence additionally needs crash tests,
not a graceful-close-only round trip.

Verify the native-free boundary both statically and dynamically: inspect the
dependency/link graph and loaded libraries of the deployed Rust process, and
run its build/tests without the Service Fabric SDK/runtime libraries available.
The node is expected to use native SF libraries; the application must not.

## 11. Remaining feasibility gates

Before promising H/S, establish the deployed build's exact hosting/RAP schemas,
registration/instance behavior and liveness mechanism. Default Linux IPC polling
offers an app-side native-lease-library-free path, but the initial TTL conversion,
deployed policy and activated-process lifetime still need native qualification.
Implementing host registration alone must never be presented as equivalent
lease safety; other platform/profile lease modes remain separate work.

Before promising broader compatibility, decide the exact host/security/upgrade
profiles, whether ABI compatibility is wanted, which stateful tier is required,
and whether native peer or disk-format interoperability is a requirement.
Those decisions change the project by entire subsystems, not a few additional
public methods.

## Source pointers

Native facts and the detailed source map are in
[FabricRuntime architecture](FabricRuntime.md). Repository links below refer to
the inspected implementation; line anchors identify the relevant surfaces.

[rust-frame]: ../../crates/libs/mssf-transport/src/frame.rs#L1-L85
[rust-serde]: ../../crates/libs/mssf-transport/src/serialization/serde.rs#L1-L28
[rust-connection]: ../../crates/libs/mssf-transport/src/connection.rs#L265-L554
[rust-client]: ../../crates/libs/mssf-client/Cargo.toml
[rust-ext]: ../../crates/libs/mssf-ext/src/state_replicator.rs#L17-L69
[rust-replicator]: ../../crates/libs/mssf-rplctr/src/rplctr.rs#L73-L138
[rust-sample]: ../../crates/samples/kvmap/src/kvmap.rs#L210-L259
[rust-reverse]: ../../crates/libs/mssf-transport/src/connection.rs#L119-L164
[rust-reverse-dispatch]: ../../crates/libs/mssf-transport/src/connection.rs#L579-L639
[rust-limits]: ../../crates/libs/mssf-transport/src/serialization.rs#L5-L8
[client-status]: ../transport/RustFabricClient.md
[native-ipc-config]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TransportConfig.h#L90-L115
