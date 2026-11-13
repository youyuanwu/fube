# Standalone container Nodes with an external FM

**Status: networking and shared control foundations implemented; standalone
container profile remains proposed and unqualified.**

**Baseline:** `4847cb10e39ca0b0dbd76a96a8f2dfedbf6a93ff`, inspected 2026-11-13.

**Networking update (2026-11-13):** general native Channels now use
`Endpoint::unsecured_tcp` / `accept_unsecured_tcp` without loopback-only checks.
NodeUp accepts non-loopback metadata, Hosting propagates `NodeContext::ip` to
the application, and `ReplicationEndpointOptions::advertised_ip` supports
wildcard replication listeners while preserving the bound port and actor
identity. Local runtime IPC retains its loopback validation. Offline tests
exercise actual same-host non-loopback TCP, normal public replicator construction
and owned application registration. Networking alone does not provide a
standalone runner or qualify the three-container profile.

**Shared control update (2026-11-13):**
[`fube_node::control`](../../crates/libs/fube-node/src/control/mod.rs) now owns
the reusable retained FM/peer effect adapter, native timers, applied semantic-ACK
tracking and original-session LFUM upload state.
[`control::wire`](../../crates/libs/fube-node/src/control/wire.rs) owns the same
bounded native action/body decode, FM/peer Channel compositions and exact
source/generation-bound Link previously private to the launcher. The local
launcher consumes these shared implementations, not copied replacements.
Its profile-specific inbound admission, parent/stdin supervision, package
selection and physical-cleanup receipts remain in `fube-cluster`. The standalone
runner, source-IP mesh association and three-container qualification remain
unimplemented.

## 1. Goal and agreed scope

Support an external user running three containers, each containing one fube
Node/RA/Hosting process and one application replica. The three replicas belong
to **one volatile partition**, with one Primary, two ActiveSecondaries, and
immutable target/minimum sizes **3/2**. They are not three independent singleton
services. The user supplies the FM and container orchestration.

The initial supported journey is **fresh, healthy startup, application use and
owned shutdown**. The user selected:

| Area | Initial decision |
|---|---|
| FM contract | Native SF-style reliability exchanges. The external FM owns placement, replica identities, epochs and configuration intent; fube executes RA/RAP semantics. |
| Failure scope | Healthy startup only. Whole-container failover, replacement and rejoin are deferred. |
| TCP transport | Plain TCP without authentication or a security opt-in. Remove general TCP loopback-only restrictions; add TLS later. |
| Membership | Three explicitly configured, original Node instances for one deployment. No discovery, resizing or peer rebinding. |
| Application | One trusted, prepackaged volatile stateful service using the existing runtime and public replicator contracts. |
| State | In-memory RA state and volatile application data. No restart recovery, even with a retained work directory. |

This is a new bounded execution profile, not a replacement for the existing
[local FM launcher](MinimalFmCluster.md) or an implementation of native
Federation. It narrows the broader [pure-Rust cluster design](PureRustCluster.md)
to a concrete external-FM use case. Containers/TLS remain excluded from that
older design's implemented profile; this proposal does not retroactively
qualify them.

### Non-goals

Do not implement an FM, a desired-state-to-FM translation service, elections,
automatic promotion, container replacement, reconnect to a lost admitted
session, FM restart, durable recovery, data-loss recovery, rolling upgrades,
multiple partitions, generic application deployment or native FM/Federation
interoperability. Do not expose runtime/Hosting IPC outside its container.

Existing local app-host repair/failover qualification does not establish
whole-container recovery. This profile must not silently enable those
operations merely because the underlying RA supports some of their exchanges.

## 2. Existing foundation and actual gaps

These are facts at the pinned baseline; the networking update above identifies
the first implemented changes. Subsequent sections describe the complete
proposed profile unless explicitly noted otherwise.

| Existing code | Reuse or required change |
|---|---|
| [`Node::new`](../../crates/libs/fube-node/src/local.rs#L414-L437) sets `max_entities = 1`. | The one-replica-per-Node boundary already fits. Keep it. |
| [FM ingress methods](../../crates/libs/fube-node/src/local.rs#L1382-L1507) and [external-effect transfer](../../crates/libs/fube-node/src/local.rs#L593-L603) separate local execution from an FM adapter. | Reuse typed commands and retained effect ownership; no dependency on the local FM is needed. |
| [Configured initial membership](MinimalFmCluster.md#immutable-configured-initial-membership) builds Idles, copies through RAP and admits them through committed native reconfiguration. | Reuse the 3/2 path, not three singleton Opens or direct engine configuration. |
| [Peer binding](../../crates/libs/fube-ra/src/peer.rs#L263-L298) fixes exact original Node instances before entity admission. | Bind the two original peers once. Healthy startup needs no dynamic peer-update API. |
| [The internal node runner](../../crates/tools/fube-cluster/src/node.rs#L949-L987) requires its original launcher parent, loopback listeners and stdin admission. | Supply an independent runner; do not make a fake launcher parent or share the launcher's receipt files between containers. |
| [Dialing](../../crates/libs/fube-transport/src/endpoint.rs#L28-L49), [accepted channels](../../crates/libs/fube-transport/src/accepted_channel.rs#L17-L47) and [channel adoption](../../crates/libs/fube-transport/src/native_channel.rs#L524-L589) enforce loopback. | Remove general TCP loopback-only checks consistently across these layers. Changing a listener address alone is insufficient. |
| [The NodeUp helper](../../crates/libs/fube-ra/src/protocol/bootstrap.rs#L167-L192) also enforces loopback. | Accept valid advertised node addresses without a loopback restriction or a separate security profile. Preserve native field order. |
| [Hosting's node metadata and environment](../../crates/libs/fube-hosting/src/local.rs#L315-L403) hardcode the node IP to loopback. | Supply the configured container-visible node address, while retaining loopback runtime IPC. |
| [Replication endpoint options](../../crates/libs/fube-data/src/extensions.rs#L81-L89) contain a listen address, and [the transport](../../crates/libs/fube-data/src/transport.rs#L316-L325) publishes its bound socket. | Separate bind and advertised addresses through normal public construction. Wildcard binds cannot be advertised as replica endpoints. |
| [RA ownership](../../crates/libs/fube-ra/src/entity.rs#L639-L654) is session-only. | Do not imply that containers or SQLite make it recoverable. |

## 3. Ownership and process architecture

```text
                  User-owned external FM
             placement, identities, epochs, intent
                  /           |           \
             FM control   FM control   FM control
                /             |             \
       Container A       Container B       Container C
       Node/RA/Hosting <-- RA peer mesh --> Node/RA/Hosting
             |                |                |
       local IPC/lease  local IPC/lease  local IPC/lease
             |                |                |
       app + runtime    app + runtime    app + runtime
          Primary       ActiveSecondary  ActiveSecondary
             \________ direct copy/replication ________/
```

Each container runs the same standalone Node binary. Hosting owns its local
application child, registration, finite runtime lease, process group and cleanup.
The FM is not the application's parent and does not fabricate physical cleanup
receipts for remote processes.

The external FM supplies the service description and initial membership. It
retains request/retry ownership and consumes matched semantic replies. It does
not set application roles, manufacture replication ACKs, or treat a container's
running state as replica readiness. Container launch/stop belongs to the user's
orchestrator, whether embedded in that FM or separate.

RA retains native entity serialization, epoch/identity checks, commits, peer
choreography and RAP effects. RAP uses normal public service/replicator callbacks.
Applications retain partition-access checks, provider ownership and their own
client protocol. Neither FM nor RA proxies application data.

### Implementation boundaries

- Add a thin runner package, provisionally `fube-node-runner`, producing a
  `fube-node` binary. Names and configuration below are proposed, not commands
  available today.
- Keep `fube_node::local::Node` as the local lifecycle owner. Extract the
  reusable supported wire dispatch and bounded effect/timer adapter from
  `fube-cluster` into a shared Node control module. The runner composes this
  module; it must not copy a second reliability state machine.
- Keep the launcher's parent/stdin/local-receipt supervision in `fube-cluster`.
  Reuse common control mechanisms without changing its process ownership,
  retries or semantic completion rules.
- Add an explicit external-FM volatile capability profile. Initial Primary
  versus peer-created Idle is selected by the first valid native creation
  request, not by a hardcoded node number or sample-only executable mode.
  Selection does not permit a second entity or adoption of an existing runtime.

### Shared control integration

The composition owns one `control::Adapter` alongside its local `Node`, supplies
the original admitted FM/peer `wire::Link` values, and drives Node work and
adapter progress. Adapter queue/status views are read-only; admitted peer
command and semantic-ACK observations use bounded tracking methods. Socket
writes do not clear retained RA notification/report state: its matched native
reply must apply before the adapter retires that observation.

The same module retains each native retry owner and the frozen LFUM body/request
across lost replies. Production retry defaults are unchanged. Existing
qualification fixtures may explicitly make their retained timer due or hold an
original peer reply through the `qualification` feature; production callers
cannot mutate these queues through the observation API. This is adapter
qualification, not a private data-engine control seam.

The composition still validates its deployment/service/placement profile before
forwarding inbound work to the existing typed Node APIs. Public wire codecs and
source headers do not grant deployment readiness, authentication, process
adoption or permission to enable deferred operations.

## 4. Configuration and container contract

The user provisions one immutable configuration per Node. Validate it before
deployment directories, processes or admitted control work are acquired.

| Configuration | Required contract |
|---|---|
| Deployment and protocol profile | Deployment ID, configuration schema version and pinned compatible fube build/protocol profile. These are deployment metadata, not extra fields in native NodeUp. |
| Local identity | Exact nonzero `NodeInstance`, human-readable name and container-visible node IP. |
| FM authority | One exact FM `NodeInstance`, fixed generation/owner and numeric FM listener address. |
| Peers | Exactly two distinct original `NodeInstance` values with numeric advertised RA listener addresses. Neither may equal the local Node or FM. All three Nodes must have distinct static IPv4 addresses so accepted peer sockets can be associated unambiguously. |
| RA listener | Local bind socket and separately advertised reachable socket. |
| Package | Local package path, application/service/type identity, package version and validated application parameters. No upload/ImageStore service is required. |
| Service | One partition, volatile stateful, target/minimum 3/2, fixed service version. Native requests must match the admitted package and description. |
| Local ownership | Private writable run directory; bounded startup/shutdown budgets and existing finite queue/message limits. |
| Application endpoints | Explicit application and replication bind/advertise settings delivered through declared application configuration or arguments. |

For example, Container A can bind RA, replication and application listeners to
`0.0.0.0:19002`, `0.0.0.0:19003` and `0.0.0.0:19004`, advertising
`10.42.0.11` with those ports. Containers B/C advertise their own private IPs.
The application's Hosting/RAP/lease connection remains an independently chosen
`127.0.0.1` socket inside each container.

The first profile uses static IPv4 addresses reachable on a private container
network, without host-port translation, a service VIP or load balancing between
replica endpoints. DNS churn, NAT mapping and remote-host deployment
qualification are deferred. Reject unspecified, multicast, zero-port and
loopback advertised cross-container addresses; require advertised ports to match
the bound listeners in this no-port-translation profile.

The package/image is supplied by the user. Run without privileged mode, a
Docker-socket mount, host networking or shared work directories. Use a private
writable directory for owned runtime materialization. A container init may
forward signals/reap orphans, but must not replace Hosting's exact child/group
ownership checks. Disable automatic restart/rejoin for this initial profile.

Being deployed inside an OCI container does **not** make this a native SF
container activation. Keep the existing activated ExeHost/runtime profile,
including `Fabric_IsContainerHost=false`; do not bypass the runtime's rejection
of unsupported native container-host/security profiles.

## 5. Networking

### Separate the four traffic paths

| Path | Proposed transport/ownership |
|---|---|
| FM to/from Node | Node dials the configured FM listener; one admitted bidirectional native Channel per Node. |
| RA to/from RA | One bidirectional native Channel per original peer pair. The lower numeric Node ID dials the higher, avoiding duplicate mesh sessions. |
| Runtime/Hosting/RAP/lease | Existing loopback IPC and original finite lease inside the container, unchanged. |
| Copy/replication and application requests | Direct application-owned listeners. FM/RA control Channels do not carry application data. |

All Nodes bind their RA listeners before attempting peer connections. Initial
dial/listen attempts may retry within one bounded startup deadline **before**
session admission. Once admitted, a lost session is terminal for this profile:
no socket replacement, target adoption or transparent replay on a new Channel.

### Associate peer sockets before application startup

The single RA listener accepts only the configured peer connections, not FM
connections. The dialer already knows its intended peer from configuration.
For accepted sockets, use this deterministic routing contract:

1. Each peer dialer binds its outgoing socket to its configured advertised node
   IPv4 address and an ephemeral source port. The no-NAT container network must
   preserve that source IP; the source port is not a peer-listener identity.
2. The acceptor matches the actual socket source IP to exactly one configured
   peer whose numeric Node ID is lower than its own. Reject an unknown source,
   an unexpected connection direction or a second connection for an occupied
   peer slot; never replace the retained original Channel.
3. Reserve that bounded peer slot during the native transport handshake. Failed
   pre-admission establishment releases the slot within the startup deadline.
   Successful establishment binds the Channel and its actual-socket target to
   the configured peer and marks the slot transport-ready.
4. The local mesh barrier completes when both original peer slots are
   transport-ready. It does not wait for a CreateReplica or any other reliability
   message: the Secondary-to-Secondary connection may have no such traffic
   during initial creation.

Do not associate peers by accept order, compare an ephemeral source port to
the configured listener port, or require the initiating native ListenInstance
to advertise an identity it does not currently carry. Source-IP association is
local routing context, not authentication or replica authority. Validate the
exact PToP source/destination and generation on every reliability message before
RA admission. A transport-ready mesh proves neither remote RA bootstrap nor
application/service readiness.

### General TCP behavior

Use ordinary plain-TCP constructors for both loopback and non-loopback
connections. Remove loopback-only checks from general TCP dial, accept and
established-socket adoption; rename loopback-specific APIs and update their
callers consistently. Do not add a parallel private-network API, address-based
security policy or cleartext opt-in.

Preserve actor/action protocol validation, actual-socket target affinity,
decode limits, backpressure, deadlines and owned shutdown. Local runtime IPC
remains loopback inside the container with its existing domain-specific
validation; it does not need to become a remote runtime connection.

Configured connection context binds each Channel to one deployment membership,
exact source/destination Node instances and FM generation. Preserve native
PToP and Generation headers; do not invent a deployment field inside a native
body or claim that a header supplies authentication. Reject duplicate sessions,
foreign identities, stale generations and undeclared peers before RA admission.
Accepting a TCP connection or native ListenInstance hello grants no role.

Node identities, generations and session checks remain necessary for
reliability correctness, not network authentication. TCP traffic is unencrypted
and unauthenticated in this initial version. TLS will be added later; security
mechanisms and security-profile negotiation are not prerequisites for this
delivery.

### Bind versus advertise

Extend normal replicator endpoint construction with an optional advertised
IP address, preserving the actual bound port without port translation. Validate
it with the listener configuration before publication;
never rewrite an already-published endpoint string. Use the same advertised
address in transport ListenInstance and replication endpoint identity, retaining
the original partition/replica/incarnation and transport suffix semantics.
Preserve today's behavior when the option is absent and the bound address is
usable; reject wildcard publication without an explicit advertised address.

Application listeners have the analogous responsibility in application code.
The sample used for acceptance needs explicit bind/advertise configuration,
but the generic Node must not implement an application protocol.

## 6. External FM contract

Use the existing native framing, actor/action/header contracts and reliability
codecs. This is not a new HTTP `set-primary` API. A non-Rust FM must implement
the same bounded wire subset; a Rust FM can reuse the codecs and shared control
adapter. No native Service Fabric library is required by the Rust profile.

| Exchange | Owner and completion rule |
|---|---|
| `NodeUp` / `NodeUpAck` | Node requests fresh bootstrap; FM validates exact configured identity and returns the fixed activated generation/profile. NodeUp contains NodeDescription, not an invented incarnation field. |
| Final empty `ReplicaUp` / `ReplicaUpReply` | Fresh Node inventory handshake. A matching processed reply is required before application authority is admitted. No recovery/nonempty inventory is accepted. |
| Service-type notification/status exchanges | Hosting/RA reports actual registration and availability; FM returns matching semantic ACKs. Registration alone does not create a replica. |
| `AddPrimary` / `AddPrimaryReply` | FM creates exactly one initial Primary. Reply completes its local creation, not the three-member service. |
| `AddReplica` / `AddReplicaReply` | FM asks the Primary to build each exact destination. Primary RA drives peer `CreateReplica/Reply` and actual RAP Open/copy/build. Ready Idle is not active membership. |
| Initial same-Primary `DoReconfiguration` / reply | FM admits both built Idles at unchanged 3/2. Required peer `Activate/Reply` commits and final RAP EndReconfiguration/RA completion precede the final FM reply. |
| Endpoint publication/ACK, when emitted | Consume the normal matched endpoint exchange. Do not publish a client-ready endpoint from registration or an early local-running observation. |
| `ReplicaUp` / reply, when emitted | Process current exact reports with native processed-list semantics. A report is an observation, not permission for deferred recovery operations. |
| `DeleteReplica` / reply | Retire the exact local lifetime during whole-deployment teardown. Semantic deletion and physical cleanup remain separate observations. |

The versioned profile manifest must enumerate the concrete accepted actions,
actors, flags, body limits and error/retry rules from the reused adapter. Shipping
the runner includes that implementer-facing contract and codec fixtures, not
just a list of message names. Its version describes fube's execution subset;
it does not alter native payload schemas or promise arbitrary native-FM support.

For this profile, admit only the initial unchanged-Primary configuration and
its exact retries. Reject promotion, membership removal/replacement, changed
sizes, generation turnover and nonempty reconciliation explicitly. Retain the
existing explicit rejection of deprecated `ChangeConfigurationReply`; it is
not a completion gate. Existing supported local-launcher operations remain
available only under their own profiles.

The adapter owns finite outgoing-effect escrow and native timers. A successful
socket write is not a semantic ACK; an applied RA command is not proof of
physical disposal. Keep original request identities across supported retries.
Do not shorten native production retry defaults for this deployment. Capacity
and unsupported-profile errors must be visible to the FM/operator, never
successful no-ops.

## 7. Healthy startup and service readiness

1. The FM/orchestrator creates a fresh deployment, fixes all three Node instances,
   partition/replica identities, package version, generation and 3/2 description,
   and starts the FM listener and containers.
2. Each runner validates its configuration, owns its private directory, binds its
   RA listener and establishes the original FM and peer Channels. Associate
   peers and complete the transport-ready mesh barrier without waiting for
   reliability traffic. Do not launch the application until fresh bootstrap
   and this mesh barrier are complete.
3. Each Node completes NodeUp/Ack and final empty ReplicaUp/Reply, constructs its
   RA with the fixed peer bindings, and starts its owned application. Hosting
   performs real runtime/type registration and finite lease establishment.
4. The FM waits for fresh bootstrap and actual service-type registration on all
   three original Nodes, processes/ACKs their notifications, then sends
   AddPrimary to its selected initial node. It retains the result until matched
   completion. The initial endpoint may be recorded internally, but is not yet
   a ready three-replica service.
5. The FM issues AddReplica for each other destination, one at a time in this
   initial profile. Wait for actual AddReplicaReply before the next build;
   CreateReplicaReply or local Idle readiness does not settle FM build.
6. With both Idles built, the FM sends one same-Primary DoReconfiguration
   admitting both at immutable 3/2. Every required peer completion must commit
   before final EndReconfiguration and the matched FM reply.
7. The FM publishes the completed membership and Primary endpoint. An independent
   application client proves public serving readiness, an acknowledged nonempty
   write and readback. Secondaries must remain non-writers.

### Native write access versus FM completion

These are separate boundaries:

- In the initial singleton configuration, membership is below minimum two and
  public writes must be denied. Building an Idle alone does not grant active
  membership or override partition access.
- As normal RAP installs configuration, write access follows native partition
  authority, configured minimum and actual configuration-derived quorum. It may
  become valid before all RA activation commits and the final reply reach the
  FM. Do not add an FM-acknowledgment-controlled write gate.
- FM create completion and client-ready endpoint publication wait for every
  required RA commit, final RAP EndReconfiguration and the matched final FM
  reply. A valid application write does not complete that control operation.

Minimum counts configured membership; it is not a quorum override. Losing the
final FM reply can leave create/publication pending while an independent client
with the internally recorded endpoint can already perform valid writes. Retry
that same control operation without revoking legitimate native access merely
because the FM has not observed its completion.

Expose distinct local observations for process/listener live, bootstrap
complete, registration complete, local replica phase and configuration complete.
An app-defined serving probe is separate. Container startup probes must not
require service readiness before the FM can create the service, and FM create
completion must not be inferred from a process probe.

## 8. Failure, shutdown and deferred recovery

| Event | Initial-profile behavior |
|---|---|
| Invalid config, package or unsupported request | Reject explicitly. Preserve and expose any already-acquired ownership for cleanup. |
| Partial startup/build failure | Mark service creation failed or unknown as appropriate. Retain original admitted work; the orchestrator tears down this deployment rather than inventing rollback or replacement. |
| Lost semantic reply on a surviving original Channel | Native retry/replay for the same operation; no second Open/build or extra configuration epoch. |
| Lost FM/peer Channel or a whole container | Mark the affected control operation/topology unknown or unavailable. No automatic promotion, peer rebind, replacement or new session admission. |
| FM crash/restart | No transparent controller recovery. Do not let a fresh FM assume authority over surviving Nodes. |
| Node/container restart | No old-runtime or old-store adoption. A restarted process cannot rejoin the existing deployment under this profile. |
| Volatile application/whole-set loss | No recovered-state or data-preservation claim. Do not acknowledge missing writes or synthesize successful recovery. |

Loss of the FM connection does not itself revoke the local runtime lease,
prove the Primary dead or authorize another Primary. Existing application
serving may continue only while normal partition authority and actual
replication quorum permit it. Conversely, loss of a container must not relax
quorum or make another replica a singleton. Report control uncertainty honestly
without claiming either automatic service recovery or instantaneous fencing.

For graceful shutdown, the FM stops issuing new work and sends normal exact
DeleteReplica requests for the whole deployment. Each Node joins RA/RAP
completion with its independent local cleanup. On local termination signals,
stop new admission, drive owned cleanup and report unresolved work explicitly;
SIGTERM is not a fabricated FM acknowledgment. Do not report clean solely from
process exit, a transport write or DeleteReplicaReply.

The runner handles SIGTERM as the application supervisor and retains exact
process/group cleanup. The orchestrator owns any outer hard-stop deadline and
independently observes container removal; forced termination is not graceful
provider disposal. No shared cross-container receipt filesystem is required.

After failure, the supported reset is externally verified teardown of **all**
old containers/application processes, followed by a fresh deployment and empty
volatile state. A new deployment ID alone does not fence surviving old apps.
Node instances/generation must not be reused to disguise a restart.

Future whole-container recovery needs separate design and qualification for
failure evidence/fencing, fresh node incarnations, peer/session rebinding,
survivor reconciliation and fresh volatile copy/readmission. It cannot reuse
the current app-host-only physical-retirement receipt as proof that a remote
Node or container was retired. None of those mechanisms belongs to this MVP.

## 9. Delivery slices and acceptance

| Slice | Deliverable and exit criterion |
|---|---|
| Shared control contract | Extract the supported Node wire/effect adapter, document the external FM subset and supply bounded codec fixtures. Existing local-launcher tests still pass without ownership or retry changes. |
| TCP and advertised endpoints | Remove general TCP and NodeUp loopback restrictions; add node IP propagation and bind/advertise support. Loopback callers keep working; local runtime IPC stays container-local. |
| Standalone runner | Independently launch the one-partition Node with deterministic source-IP peer association and a transport-ready mesh barrier, real package activation, signals and explicit local status/cleanup. No launcher parent, stdin capability or sample-only service identity is required. |
| Three-container qualification | A native-free fixture FM uses the published contract against three separate network namespaces and demonstrates the complete healthy journey below. |

Acceptance must establish:

- Three unprivileged containers with independent IPs/work roots form one 3/2
  partition through a fixture external FM. Do not use host networking, native
  onebox or a single shared loopback namespace as a substitute.
- Public writes are denied in the initial singleton configuration below minimum.
  Real RAP copy/build and each Secondary activation complete; application
  writes follow native partition authority and actual quorum, then replicate
  and read back through the published Primary endpoint. Use normal
  provider/runtime observations, not private engine hooks or FM-fabricated ACKs.
- Registration/listener readiness cannot prematurely complete create. An exact
  lost build/final reply retries on its original Channel without reopening
  replicas, changing service sizes or duplicating configuration work. Withhold
  the final FM reply after native completion: create and endpoint publication
  remain pending without requiring native write access to remain denied.
- Vary peer connection arrival order and leave Secondary-to-Secondary
  reliability traffic idle. Source-IP association still binds both correct
  peer slots and completes the transport-ready mesh barrier. Unknown source
  IPs and duplicate slots reject; exact PToP/generation checks remain required
  on subsequent reliability traffic.
- Bad advertised endpoints, foreign node instances, wrong generations,
  undeclared peers, duplicate admitted sessions, resize, promotion and attempted
  restart/rejoin reject without widening authority.
- General TCP Channels connect across container addresses without a security
  opt-in and still work on loopback. Update tests that intentionally asserted
  TCP loopback-only behavior; retain local runtime IPC validation and all
  protocol/ownership checks.
- A negative disconnect/container-loss check demonstrates reported uncertainty,
  no automatic promotion/reconnect and no fabricated recovery. This is refusal
  qualification, not a failover/replacement support claim.
- Graceful whole-deployment teardown verifies original app/group/root cleanup
  in each live container and independently verifies container removal. Failed
  startup and forced termination retain honest failed/unknown cleanup results.
- Existing local RA/Hosting/runtime and configured-membership suites remain
  green. Add focused tests for the new profile and TCP endpoint behavior;
  do not rerun native interoperability suites as a proxy for container evidence.

The fixture FM is test/reference integration code, not a new shipped orchestration
service. Publish a runnable example with the standalone runner, package and
external-FM contract only when this acceptance passes. Until then, label the
profile proposed or experimental, not supported containerized HA.
