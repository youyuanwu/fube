# SF API alignment: current contracts and limits

This is the as-built public runtime/data contract, not a claim of whole-SDK,
mssf source or COM ABI interchangeability. This is a **coordinated breaking
migration**: old factories, aggregate contexts, monolithic controls and public
engine adapters have no compatibility aliases/wrappers. See [migration](../Migration.md#breaking-runtime-and-data-api-migration),
[stateless](RustStatelessRuntime.md), [stateful](RustStatefulRuntime.md) and
[data](FubeData.md) guides.

## Reading the four axes

Each row independently classifies **signature/synchrony**, **accepted values**,
**completion**, and **supported capability**. **M** means the specified reference
responsibility matches; **R** is a Rust representation/shape departure; **L** is
an explicit behavior/profile limit; **E** is a fube-specific extension; **U**
means unavailable, not a successful stub. Mixed classifications describe distinct
aspects, not a family-wide parity score. Supported means implemented in the
selected Linux x86-64 volatile trusted-clear profile, **not deployed-qualified
for this migration**. Qualification and source identity are recorded separately
[below](#verification-provenance).

Public source links below lead to declarations **and their Rustdoc**. The
runtime [public contract witnesses][RT], data [public contract witnesses][DT],
runtime [report literals][SL] and [stateful report witnesses][SR] are executable
offline evidence, not native deployment.

### Reference identity and provenance

- Native responsibilities: Microsoft Service Fabric source
  **3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d**:
  [FabricRuntime.idl][N-runtime], [FabricTypes.idl][N-types], and the pinned
  [managed API directory][N-managed]. This source is **not assumed to be the
  source used to build deployed SF 11.8.121.4**.
- Rust shape/vocabulary: inspected mssf-core **0.10.0** fork revision
  **35555883476ac5f78f5f53ef0215b6a32e43b574**:
  [stateless traits][M-stateless], [stateful traits][M-stateful],
  [stateful types][M-stateful-types], [role enum][M-roles], [partition types][M-partition],
  [health][M-health], [metrics][M-metrics] and [runtime wrapper][M-runtime].
  These public pinned fork files were accessible when checked on 2026-10-30.
  The fork manifest identifies upstream [Azure/service-fabric-rs][M-upstream];
  it is a reference checkout, **not a patched workspace dependency**.
  Published [mssf-core 0.10.0 API][M-public] is an additional public reference,
  not proof that fork and registry bytes coincide.
- Canonical contracts follow SF/mssf responsibilities. **Execution,
  native-binding and cleanup integration types were newly written for fube**;
  they are not “ported SDK APIs.” Scheduling now uses ordinary Tokio ownership
  and JoinSets with thin deadline/context/disposal wrappers, not a custom task
  scheduler. Replay, operation origin and epoch-checked diagnostics/admission
  reorganize **preexisting fube behavior outside SDK traits**. The primary-epoch
  mask follows native `ToPrimaryEpoch`; exposing `EpochDomains` is a fube API
  choice. An `extensions` namespace proves neither necessity nor native support.

## Runtime and control mapping

| Current family / method → reference; Rustdoc / regression | Signature / synchrony | Values / acceptance | Completion | Supported capability |
|---|---|---|---|---|
| `IStatelessServiceFactory::create_instance` → native CreateInstance / [mssf stateless][M-stateless]; [Rustdoc][R-api], [RT] | **M** sync fallible boxed instance; **R** explicit boxed future facility elsewhere, no context-only factory | **M** type, URI, borrowed init bytes, partition, public instance ID; **R** UTF-8 String/UUID vs WString/Uri/GUID (D01) | **M** constructed participant or error, not Open success | **L** runtime-supervised constructor; activation provider captured by factory, incarnation not an SDK argument |
| `IStatefulServiceFactory::create_replica` → native CreateReplica / [mssf][M-stateful]; [Rustdoc][R-stateful], [RT] | **M** sync `Result<Box<dyn IStatefulServiceReplica>>` | **M/R** same argument roles, public replica ID, UTF-8/UUID | **M** construction only; no provider/listener activation success implied | **L** volatile native runtime creation, no persisted recovery |
| `Runtime::register_stateless_service_factory` and `register_stateful_service_factory` → [mssf Runtime][M-runtime]; [Rustdoc][R-register], [RT] | **M** sync boxed factories, **R** `&str`/typed Result; former async shape removed (D02) | **L** bounded nonempty name and supported owned execution entry; rejected invalid-context input remains the original recoverable box | **M/L** actual native registration reply + authority, not enqueue; accepted timeout terminal/uncertain | **L** attached runtime lifetime; no type-unregister invention; [C1](#c1-synchronous-contracts-and-owned-execution) |
| `IStatelessServiceInstance::{open,close,abort}` → [native lifecycle][N-runtime] / [mssf][M-stateless]; [Rustdoc][R-api], [lifecycle tests][R-life-tests] | **M** async Open/Close, sync Abort; **R** object-safe `ApiFuture`, Cancellation, opaque String endpoint | **M** partition trait object, successful endpoint may be empty; **R** Arc/owned token vs COM/mssf token | **M** advertised listener ready before returning its address; empty success advertises none, not failure/readiness proof; Close/Abort owned disposal; cancellation is advisory | **L** service owns arbitrary spawned descendants; shared listener lifetime may exceed unit lifetime |
| `IStatefulServiceReplica::{open,change_role,close,abort}` / `OpenMode` → [mssf][M-stateful]; [Rustdoc][R-stateful], [RT] | **M** async lifecycle/sync Abort, boxed primary control | **M** explicit New/Existing/Invalid intent; **L** New only, Existing Unsupported, Invalid InvalidArgument before activation (D03); **R** corrected `Invald` spelling | **M** separate service and replicator addresses; disposal is not dropped observation | **L** no Existing recovery/persistence; address is not access permission |
| `IReplicator::{open,change_role,update_epoch,close,abort}` → native IFabricReplicator / [mssf][M-stateful]; [Rustdoc][R-control], [RT] | **M** async except Abort; **R** futures/UTF-8 | **M** native-selected role/full epoch; **L** supported role/profile only | **M/L** owned barriers and cleanup, not self-election or universal durability | **L** custom control or one built-in native-bound owner |
| `IReplicator::get_current_progress` → [native/mssf][M-stateful]; [Rustdoc][R-control], [native control tests][D-native-tests] | **M** sync fallible scalar | **M/L** initialized role-specific local observation; not remote unknown -1 | **L** built-in primary quorum frontier vs secondary received frontier (D19), not provider local apply | **L** immediate observation, no global network/disk commitment grant |
| `IReplicator::get_catch_up_capability` → [native/mssf][M-stateful]; [Rustdoc][R-control], [D-native-tests] | **M** separate sync scalar, not old paired public getter | **L** eligible retained completed-history lower bound, zero can mean unavailable history | **L** native publication samples private coherent pair; two public calls need not be atomic | **L** not a fabricated remote LSN or ALL attestation (D19/D20) |
| `IPrimaryReplicator::on_data_loss` → [mssf][M-stateful]; [Rustdoc][R-primary], [DT] | **M** async bool | **M** bool means provider state changed, not epoch notification | **M** actual bool propagated through owner and runtime | **L** volatile application reconciliation, not restoration of lost durable state |
| `update_catch_up_replica_set_configuration(current, previous)` → [mssf][M-stateful]; [Rustdoc][R-primary], [disposal tests][D-disposal] | **M** sync `Result<()>`; former async shape removed (D04) | **M** SDK descriptors; **E/L** exact native-issued membership/local votes/incarnations/epochs retained privately (D05) | **M/L** applied owner installation and required physical disposal, not enqueue; timeout uncertain | **L** functional canonical root AND bound view, owned entry required, not permanently Unsupported (C3) |
| `update_current_replica_set_configuration(current)` → [mssf][M-stateful]; [Rustdoc][R-primary], [D-disposal] | **M** sync Result | **M** no previous argument; **L** must match issued projection, cannot shrink quorum from available peers | **M/L** applied/disposed boundary as above | **L** same canonical owner path; no standalone SDK-descriptor authority |
| `wait_for_catch_up_quorum(Write/All, cancellation)` → [mssf][M-stateful]; [Rustdoc][R-primary], [D-native-tests] | **M** async | **M/L** Invalid rejected; current/previous/local/required-target obligations remain distinct | **L** captured assigned fence; native prefix attests only existing completed history for Write, never ACKs new data or satisfies All | **L** no quorum reduction/election; fresh network ACKs required where specified |
| `build_replica(replica, cancellation)` → [mssf][M-stateful]; [Rustdoc][R-primary], [DT], [D-native-tests] | **M** returns async future; **E** immutable authority frozen at the call before the future is returned | **M** clean SDK descriptor; **L** exact native target/node/replica/actor incarnation/revision required | **L** real copy/application-ACK readiness; canceled invocation is not disposal of an established peer | **L** root and bound canonical controls work; unpolled stale future cannot retarget same-ID replacement |
| `remove_replica(id)` → [mssf][M-stateful]; [Rustdoc][R-primary], [D-disposal] | **M** sync ID-only, no public purpose/cancel arguments | **E/L** privately captured exact incarnation and retry-vs-final purpose, not latest-ID lookup | **M/L** physical disposal before success; BuildRetryCleanup is not successful Build (D04/D05) | **L** root removal final; issued retry cleanup avoids final tombstone; removal never changes native quorum membership |

### Partitions, values and reporting

| Current family / method → reference; Rustdoc / regression | Signature / synchrony | Values / acceptance | Completion | Supported capability |
|---|---|---|---|---|
| Stateless `get_partition_info`; stateful `get_partition_information` → [mssf stateless][M-stateless] / [stateful][M-stateful]; [Rustdoc][R-api], [R-partition], [RT] | **M** object-safe sync fallible getter, names intentionally differ | **M/R** ID-bearing singleton/range/named, UUID/UTF-8 | **M** immediate information, no diagnostic lookup needed | **L** named model support, not deployed qualification |
| `ServicePartitionInformation`, Singleton/Int64/Named information → [mssf types][M-partition]; [Rustdoc][R-info], [RT] | **M/R** enum/fields; **R** no mssf `get_partition_id` convenience member | **M** id/low_key/high_key/name; **R** corrected `Int64PartitionInfomation` / `NamedPartitionInfomation`, no typo aliases | **M** value only; Invalid is not usable identity | **L** no authority hidden in descriptors |
| `get_read_status` / `get_write_status`, `ServicePartitionAccessStatus` → [mssf][M-partition]; [Rustdoc][R-partition], [access tests][R-values] | **M** separate sync Results | **M** Invalid/Granted/ReconfigurationPending/NotPrimary/NoWriteQuorum | **L** current lifecycle/native membership plus lifetime/host overlay, may race subsequent call | **L** no report-bucket, published address or diagnostic role grants read/write |
| Stateless `report_load` → [mssf][M-stateless], [FUP load][N-load]; [Rustdoc][R-api], [SL] | **M** sync Result; old async canonical shape removed (D07) | **L** 1–64 unique declared metrics, value < INT_MAX; undeclared rejected vs native filtering (D10) | **L** complete local frame write + authority check, **not RA/FM ACK** | **L** stateful=false secondary/node arrays; bounded local retention distinct from native FM retention/incarnation |
| Stateful `report_load` → [FUP][N-load] / [load merge][N-load-merge]; [Rustdoc][R-partition], [SR] | **M** sync Result | **M** vanilla private **current configuration role** snapshot; **L** Unknown/None/value/profile errors stricter than FUP S_OK/downstream filtering | **L** local complete-frame write, writer-boundary uncertainty; not RA/FM acknowledgment | **M/L** Primary array or ActiveSecondary/IdleSecondary secondary/node arrays, independent of completed service callback/access |
| `report_move_cost` on both partitions → [native partition][N-runtime] / [mssf metrics][M-metrics]; [Rustdoc][R-api], [R-partition], [SL], [SR] | **M** sync Result | **M** Zero/Low/Medium/High 0–3; **U** newer mssf VeryHigh (D21) | **L** same local write and current-configuration-role selection as load | **L** `_MoveCost_` load metric, not imperative move/effective PLB-policy guarantee |
| `report_instance_health` (stateless) → [native health generation][N-health]; [Rustdoc][R-api], [SL] | **M** sync borrowed information only | **M/R** kind/tag **2**, partition + public instance ID; seconds/UTF-8 values below | **L** one exact HM result matched by kind/source/sequence/entity/property (D08), not SDK queue admission | **L** private route/generation, no hidden retry; timeout may already have applied |
| `report_replica_health` (stateful) → [native health generation][N-health]; [Rustdoc][R-partition], [SR] | **M** sync borrowed information only | **M** kind/tag **1**, public replica ID, application replica-instance sentinel **-1**, not RAP incarnation | **L** exact matched HM result, not a fresh durable transaction | **L** independent target layout/key, never stateless DTO reuse |
| `report_partition_health` (both) → [native health generation][N-health]; [Rustdoc][R-api], [R-partition], [SL], [SR] | **M** sync borrowed information only | **M** kind/tag **3**, partition-only entity; distinct field count | **L** exact matched HM result, uncertain timeout, no hidden retry | **L** implemented on both canonical partition objects; no public ambiguous `report_health` |
| `report_{instance,replica,partition}_health2` → native versioned partition interfaces [IDL][N-runtime]; [Rustdoc][R-api], [R-partition], [SL], [SR] | **M** health then optional borrowed send option; **R** native-versioned API not present on inspected mssf base traits | **M/L** Immediate false **and** true accepted by direct-send | **L** true adds no reporting-interval delay; false also direct, not SDK default batching/retries or delivery guarantee (C2) | **L** same target-specific result fencing as base calls |
| `HealthInformation`, `HealthState`, `HealthReportSendOption` → [mssf health][M-health] / [native types][N-types]; [Rustdoc][R-health], [SR] | **M/R** canonical field roles, UTF-8 text, public u32 TTL seconds | **M** TTL0=infinite, sequence0=auto positive, negative invalid; **L** only Ok/Warning/Error, bounded text/reserved System*/DeleteProperty rejection | **L** HM staleness/expiration rules, not source-time TTL or guaranteed sequence history after deletion | **L** former Duration/subsecond convenience is not public; stricter validation/retention individually disclosed (D10) |
| `report_fault`, `FaultType` → [native][N-runtime] / [mssf][M-stateless]; [Rustdoc][R-api], [R-partition], [fault tests][R-fault-tests] | **M** sync Result, Invalid/Permanent/Transient | **M/R** public Permanent1/Transient2 explicitly maps RAP Transient1/Permanent2; Invalid rejected before coalescing | **L** immutable initial write/coalesced result, **not** replacement/recovery ACK | **E/L** later bounded retries/coalescing/permanent supersession and delivery snapshots are separate observations (D09/D21) |
| `Epoch` → [mssf][M-stateful-types] / [native types][N-types]; [Rustdoc][R-epoch], [R-values], [DT] | **M** public field roles; **R** no inherent `primary()` | **M** public INVALID(-1,-1), ZERO(0,0) valid; **L** RAP invalidzero is contextual absence, not global zero replacement (D20) | **M/L** full low bits retained for authority; primary mask only at explicit boundary | **E** `EpochDomains` follows native internal rule, exposing trait is fube choice |
| `ReplicaInformation`, `ReplicaSetConfig`, `ReplicaStatus`, `ReplicaSetQuorumMode`, `ReplicaRole` → [mssf descriptor types][M-stateful-types] / [actual role definition][M-roles]; [Rustdoc][R-descriptors], [R-values], [D-native-tests] | **M/R** SDK-shaped fields, config write_quorum u32, explicit enums | **M** remote unknown LSN -1 vs initialized local0; **R** public Primary2/Idle3/Active4 vs RAP Primary4/Idle2/Secondary3 | **M** descriptor only, never native proof; local votes/PC/CC retained privately | **U/L** ActiveAuxiliary, IdleAuxiliary and PrimaryAuxiliary each unavailable; no enum casts/compat aliases (D05/D20/D21) |
| Native partition `CreateReplicator`, mssf `try_get_com`, mutable native replicator settings → [native][N-runtime] / [mssf][M-stateful]; [current partition Rustdoc][R-partition], [DT] | **U** no such fube SDK members (not methods returning fake success) | **U** no arbitrary COM pointers/settings accepted | **U** no native creation/COM result | **E/L** data-layer `FabricReplicator::create` instead; preserves data → runtime dependency (D11) |
| `Error`, `ErrorKind`, `Result`, `ApiFuture`, `Cancellation` → native HRESULT / [mssf][M-stateless]; [errors][R-error], [RT] | **R** shared typed Rust errors, object-safe boxed futures/owned token (D01/D22) | **L** actual InvalidArgument/Unsupported/context/access/cancel/timeout/resource/native/provider/transport causes, no text guessing | **L** `is_uncertain` and advisory cancel do not mean rollback/disposal; rejected registration box recoverable once | **E/L** arbitrary user error causes are managed captures; release before owner shutdown, not full HRESULT/COM equivalence |

Individual version-skew exclusions, not aliases or capability expansion:

- `ActiveAuxiliary` exists in the [pinned mssf role enum][M-roles] but is
  unavailable in fube's current native profile.
- `IdleAuxiliary` exists in that enum but is unavailable in this profile.
- `PrimaryAuxiliary` exists in that enum but is unavailable in this profile.
- `MoveCost::VeryHigh` exists in [pinned mssf metrics][M-metrics] but is
  unavailable here; accepted native costs remain Zero/Low/Medium/High.

## Data/provider/stream mapping

Native [IFabricStateReplicator/IStateProvider/IOperation/streams][N-runtime]
and managed [provider][N-provider], [state replicator][N-state-replicator],
[operation][N-operation], [payload stream][N-payload-stream] and
[operation stream][N-operation-stream] supply these responsibilities; the
inspected mssf runtime does not itself implement this built-in replication API.

| Current family / method → reference; Rustdoc / regression | Signature / synchrony | Values / acceptance | Completion | Supported capability |
|---|---|---|---|---|
| `FabricReplicator::create`, `get_state_replicator` → managed [FabricReplicator][N-fabric]; [Rustdoc][D-fabric], [DT] | **E/R** fube-data helper takes partition, native integration, provider, optional replay, endpoint, Settings; sync Result<Arc<Self>>; views implement canonical controls | **L** native-issued matching authority required before listener/provider activation, no standalone primary controls | **L** one engine/listener/owner; views share it, not independent construction | **L** managed composition responsibility, **not** native partition-construction location/signature/ABI (D11) |
| `IStateReplicator::replicate` / `ReplicationAdmission` → native Begin/EndReplicate, managed [out-LSN][N-state-replicator]; [Rustdoc][D-replicate], [DT] | **R** async bounded admission yields early LSN + separate completion instead of COM out-parameter | **L** finite limits reject before admission with **no LSN consumed**; current native authority required | **M/L** early admitted identity != quorum completion (D14); accepted work/local apply remain application-owned across disconnect | **L** nonzero ordered sequences in volatile profile, no rollback on drop/cancel |
| `ReplicationCompletion::wait` → native EndReplicate / [managed guidance][N-state-replicator]; [Rustdoc][D-completion], [sample][APP-write], [DT] | **R** separately awaitable observer | **L** included sample applies locally before wait; custom applications own apply/retry policy (D13) | **L** selected current/previous quorum result, not prepare → quorum → ordered local commit parity; cancel/timeout after admission ambiguous | **L** neither universal network commitment nor disk durability; application-owned admission pump survives HTTP disconnect |
| `get_copy_stream` / `get_replication_stream` → native/managed [state replicator][N-state-replicator]; [Rustdoc][D-replicate], [DT] | **M/R** immediate fallible sync Arc<dyn IOperationStream> getters | **L** owner's bounded single consumer, both streams require service ACK | **M** getter != enumeration, receive != apply/ACK | **L** RequireServiceAck=true mandatory, unlike native volatile implicit-ACK default (D15) |
| `IStateProvider::get_last_committed_sequence_number` → [provider][N-provider]; [Rustdoc][D-provider], [MemoryProvider][D-memory], [DT] | **M/R** sync fallible scalar | **L** included provider returns **locally applied history**, possibly ahead of quorum (D12) | **L** canonical name does not strengthen provider state into network commitment | **L** custom provider owns its data/retry semantics; not universal local-apply policy |
| `IStateProvider::update_epoch` → [provider][N-provider]; [Rustdoc][D-provider], [provider tests][D-memory-tests] | **M/R** async epoch/prior-last/cancellation | **M/L** authoritative prior-epoch boundary; original bytes/history/ledger reconciled | **L** ambiguous tail may truncate, not rollback promise for earlier client observations | **L** volatile reconciliation; full provider bool/barrier errors retained |
| `IStateProvider::on_data_loss` → [provider][N-provider]; [Rustdoc][D-provider], [DT] | **M** async bool | **M** actual provider-changed outcome, not unit/epoch-changed shorthand | **M** bool reaches canonical primary/runtime result | **L** no persistence/whole-set restart restoration |
| `get_copy_context` → [provider][N-provider]; [Rustdoc][D-provider], [DT] | **M/R** sync payload-stream factory | **L** volatile built-in engine supplies **None** to get_copy_state, does not negotiate context | **M** factory != enumeration; oracle defensive empty-stream callback is not advertised persisted context support | **L** non-null context rejected by included provider (D18) |
| `get_copy_state(up_to, context)` → [provider][N-provider]; [Rustdoc][D-provider], [D-memory-tests], [DT] | **M/R** sync fallible stream factory, owned optional Arc context | **L** exact requested horizon after local application reaches it; included provider copies history **and request-result ledger**; context None only | **L** coherent pinned exact snapshot, unavailable history errors; never substitute latest/lower boundary | **L** custom provider responsible for its snapshot/retry content; no bidirectional/persisted context |
| `IOperationDataStream::get_next` / `OperationData` → [payload stream][N-payload-stream]; [Rustdoc][D-payload], [DT] | **M/R** async Option<immutable segments>, not COM nullable interface | **M** Some(empty) is data, None EOS; segment/byte bounds explicit | **M** provider payload enumeration has **no ACK** | **L** 1 MiB/1024-segment default maximum and capture disposal bounds (D16/D24) |
| `IOperationStream::get_operation` → [operation stream][N-operation-stream]; [Rustdoc][D-operation], [DT] | **M/R** async Option<Arc<dyn IOperation>>, object-safe | **M** Some(empty operation data) != None; no synthetic application EOS item | **L** internal legacy-null EOS retirement cannot bridge unapplied gaps and adds no service ACK/operation (D16) | **L** destination copy AND replication, bounded one consumer |
| `IOperation::{get_metadata,get_data}` / `OperationMetadata`, `OperationType` → [native metadata][N-types] / [operation][N-operation]; [Rustdoc][D-operation], [DT], [wire tests][D-wire-tests] | **M/R** immediate fallible getters, segmented owned data | **M/L** type/sequence/atomic-group only; Normal=1 and atomic_group_id=-1 supported, not public epoch or atomic groups | **M** getters neither apply nor ACK | **L/U** no public EOS/atomic-group operations; original epoch only explicit origin capability |
| `IOperation::acknowledge` → [operation][N-operation]; [Rustdoc][D-operation], [DT] | **M/R** explicit sync fallible once-only ACK | **L** service must apply first for **both destination streams**, unlike native volatile default | **L** generation/revision/sequence-fenced local protocol ACK, never ACK-on-poll/drop/cancel; stale/duplicate fail | **L** not provider-payload ACK, durable storage or universal quorum knowledge (D15) |
| `ReplayCheckpointProvider`, `ReplayOperation` → **no standard provider callback**; [Rustdoc][D-extensions], [D-memory-tests] | **E** optional separately supplied async capability, not SDK supertrait/discovery | **E/L** exact original LSN/epoch/bytes/segment boundaries or None; never synthesize snapshot write/new LSN | **E/L** charged supervised retry pacing/disposal, unavailable history cannot grant catch-up | **E** preexisting fube replay behavior reorganized; standard-only provider works (D17) |
| `OperationOriginStream`, `OperationWithOrigin`, `EpochCheckedReplication` → **no native operation-epoch member**; [Rustdoc][D-extensions], [DT] | **E** opt-in origin stream and replicate_in_epoch | **E/L** authentic original epoch beside canonical metadata; exact admitted primary-epoch check | **E/L** same single consumer/admission/completion ownership, not new ACK semantics | **E** preexisting fube provenance/epoch checks reorganized, not ported SDK APIs (D17) |
| `ReplicatorDiagnostics` / `Progress`, native binding/invocation/accounting types → **no SDK counterpart**; [Rustdoc][D-extensions], [R-binding], [execution tests][R-execution-tests] | **E** explicit integration/observations, no native_binding/replication_runtime SDK discovery | **E/L** diagnostics cannot mint private issued proof; retained proof revocable (D05/D06) | **E/L** receipt/callback/destructor accounting != authority or cancellation=disposal (D23) | **E/L** real runtime/RAP paths in model hosts remain OFFLINE; clear trusted IPC is not cryptographic authentication |
| `Settings`, `MemoryProvider`, budgets → **no Reliable Collections contract**; [settings][D-settings], [provider][D-memory], [D-memory-tests] | **E** finite fube settings/application provider, not native mutable settings facade | **L** queue/byte/peer/copy/deadline limits, 4096-op/16 MiB provider history+ledger | **L** backpressure/rejection, no silent eviction or durability (D24) | **U** persistence, TLS/Windows peer replication, atomic groups, Reliable/transactional Collections, rolling upgrades/global durability/universal V1 (D25) |

## C1: synchronous contracts and owned execution

Factories, registration, configuration, removal and reports now have synchronous
SDK shape; old D02/D04/D07 async recommendations are **history, not exceptions**.
Signature matching is separate from execution, value and completion parity.
Reserve `Runtime::execution_runtime().try_reserve()` **before** moving captures,
then `permit.run_owned(move || sync_call(...)).await`. Runtime-invoked factories
and data provider callbacks already enter owned execution. Direct arbitrary
async contexts cannot block these waiting calls: they return typed
`InvalidExecutionContext`. Same-engine reentry/locks required by callbacks are
not a safe shortcut. See the [compiling registration example][R-example],
[deadline/context checks][R-entry] and actual [sample registration][APP-register].

An invalid-context registration error retains the **exact original boxed
factory**, recoverable once through `take_rejected_registration_input` and
`RejectedRegistrationInput::{Stateless,Stateful}`. No hidden box destruction or
accepted-work abandonment is implied. Other pre-acceptance destruction remains
caller-owned; accepted timeout can retire authority without rolling back native
effects. Tokio tasks/JoinSets retain owned construction/poll/drop and cleanup
through actual disposal. Abort/observer cancellation alone is not disposal.
Application-supplied scheduler implicit children are **application-owned**;
runtime does not adopt arbitrary descendants or guarantee stalled external
executor progress. Services/applications must stop/join their listeners/tasks.
The [actual sample][APP-register] calls `Runtime::shutdown()` **before** its
final shared `rpc.shutdown_listeners()`; unit shutdown retires routes and owned
work. Do not infer the reverse process-listener ordering.

Accepted cleanup/terminal errors use the same managed-cause release route as
ordinary callback errors; observing/cloning an error does not transfer its final
user-cause destructor to the observer. Capacity/shutdown rejection destroys the
unretained cause under the existing active cleanup fence. Independent cleanup
or publication obligations do not change an accepted error's kind, typed source
or native code. Truly cyclic or uncooperative returned ownership remains subject
to the existing original hard deadline/fence, not a new graceful rejection
policy. Expired cleanup and pending-result fences keep
their original absolute timestamp, including completion during terminal grace;
healthy dormant publication does not keep its construction deadline armed.

## C2: direct-send reporting is not SDK health-client parity

Both Immediate values work by direct-send; true adds no intentional batching
interval and false also sends directly. This is **not** native default
batching/retry behavior. Load/move local-frame completion is not RA/FM ACK;
health waits for one exact HM result, not a fresh durable transaction; fault's
initial immutable result is separate from subsequent retries/coalescing and
observations. Authority checks cannot revoke bytes already beyond the writer
boundary. Native load retention and health incarnation/staleness/expiration
limits remain; close/cancel cannot remotely undo applied reports.

Load/move snapshots vanilla **private current configuration role at admission**,
coherently with existing routing identity ([FUP 1780–1808][N-load]).
Pending demotion uses secondary/node buckets even if service ChangeRole has not
completed; configured Primary can report while completed service role is Unknown.
Bucket selection grants no access. Explicit stricter Unknown/None/value errors
are not native FUP S_OK/downstream-admission parity. The old completed-role
preference was erroneous implementation guidance, **not user policy or a
necessary retained limitation**; it was corrected in `6b397e3`.

## C3: canonical controls and private issued authority

`FabricReplicator::create` requires real runtime/partition-issued integration.
Root and bound **canonical** configuration/build/remove reach the same owner.
Private immutable records preserve node/replica/actor incarnations, generation,
revision, current/previous epochs, local membership and quorums, plus
retry-versus-final removal purpose. Public descriptors/diagnostics cannot
construct these records; retaining a record does not prevent revocation.
There is no standalone non-SDK control authority, diagnostic grant, SDK discovery
member or old-contract-taking public adapter. The
[offline model host][D-model] exercises real runtime registration/lease/RAP
issuance, not deployed SF or globally unforgeable/cryptographically authenticated
authority. Trusted clear transport remains a **trust-profile limitation**.

The helper lives in **fube-data**, not an extra SDK partition method, and returns
one shared control/state facade. Native partition CreateReplicator, mssf
try_get_com, arbitrary native settings and COM source/ABI compatibility are
unavailable, not silently emulated.

## Departure index

This retains the original research IDs while correcting their as-built status.
The independent four-axis rows above are normative; these are navigation, not
an umbrella exception permitting another dialect.

| ID | Current disposition / location |
|---|---|
| D01 | UTF-8/UUID/Cancellation/boxed future and stream representations; factory/lifecycle/error rows. |
| D02 | Sync registration implemented; C1 entry, exact box recovery, native-reply completion limits. |
| D03 | Explicit New/Existing/Invalid; New only, no recovery. |
| D04 | Sync configuration/removal implemented; applied/physical-disposal completion, uncertainty. |
| D05 | Runtime-issued immutable control envelope outside clean SDK descriptors; functional C3. |
| D06 | Diagnostics/progress/access/receipts are observations, not grants. |
| D07 | Sync reports implemented; load/move local-write-only completion. |
| D08 | Target-specific exact health result; direct-send both Immediate values, no batching/retry parity. |
| D09 | Fault initial result vs bounded retry/coalescing/supersession/observations. |
| D10 | Strict metric/text/value/retention profile; canonical seconds/zero-infinite/zero-auto, no public subsecond convenience. |
| D11 | Data-layer helper instead of native partition/COM/settings capability; construction-location/shape/ABI difference. |
| D12 | Included provider's last-committed name reports locally applied history, potentially ahead of quorum. |
| D13 | Primary apply precedes wait, not managed prepare/quorum/ordered local commit parity. |
| D14 | Early LSN separate completion; no LSN on pre-admission rejection; after-admission cancellation ambiguous. |
| D15 | Mandatory once-only service ACK on destination copy and replication; not native volatile default. |
| D16 | Provider payload vs destination operation streams; Some(empty) vs None; internal EOS cannot fill data gaps. |
| D17 | Authentic replay/origin/epoch-checked admission are explicit preexisting fube extensions, not SDK ports. |
| D18 | Exact-horizon included history+ledger, None context only; custom providers own their policy. |
| D19 | Role-specific frontiers/capability/catch-up are not provider apply/global commit; Write != All. |
| D20 | Public invalid(-1,-1)/validzero/internal invalidzero; remote-1/local0; full epoch vs mask. |
| D21 | Public/RAP enum numerics; ActiveAuxiliary, IdleAuxiliary, PrimaryAuxiliary and VeryHigh individually unavailable; actual pinned enum definition and reference typo fixes without aliases. |
| D22 | Typed shared error/cause/native code and uncertainty, not blanket HRESULT/source equivalence. |
| D23 | Finite callback/capture/destructor accounting/watchdog, not cancellation=disposal or automatic descendant joins. |
| D24 | Finite budgets and included application ledger, not Reliable/transactional Collections. |
| D25 | Volatile Linux x86-64 clear trusted subset; individual unsupported capabilities remain explicit. |
| D26 | Source/model/ABI/archive evidence != deployed qualification; exact source-bound verification below. |

## Verification provenance

Counts are **overlapping suites, not additive unique scenarios**. Local raw logs
and ignored workflow files are not durable public prerequisites; reproduce
commands in [Development](../Development.md#native-free-checks), inspect the
linked public tests, and record new immutable source/bundle identities for
future native qualification. No live/deployed qualification was run anywhere
in this API-alignment workflow.

| Source covered | Actual recorded validation | What it does not prove |
|---|---|---|
| `c7cf02feddb3ab0015578178da02e1c8fb002ead` (Phase 2 final corrected bytes) | 363 Rust + 8 negative doctests; workspace all-target check/fmt/diff; strict four-crate lint and affected docs; Python runtime44/data197 (196 pass, one AF_PACKET skip) | Later provider/report changes, current docs bytes, deployed SF |
| `8737eb909bc4311a6633d1f1f73a88c9586a9bf6` (Phase 3 final corrected bytes) | 367 Rust + 17 doctests; workspace all-target/all-feature check; strict four-crate lint/docs/fmt/diff; runtime44/data197 (one skip); oracle build/`--abi-self-test` | ABI self-test used local COM doubles: native_engine_created=false, native_interoperability_qualified=false; not deployment |
| `34bb3194ce574ab046c2f9dff97a8a59377d3442` (historical Phase 4 before load-role correction) | 373 Rust +22 docs and second310 Rust +17 docs; workspace gates/32 doctests; Python414 (413 pass/one skip); fresh archive SHA256 `872a9be0122e1e827303567631fd4bf7c34d49ffe760744b8294b922bbae57a2`, 798 pass/five native exclusions | Its completed-role bucket policy is **superseded**, not user policy or corrected-source acceptance |
| `6b397e335b98cf65160a3f8bacb2b30b9b386344` (historical corrected Phase 4 implementation) | 374 Rust +22 docs and second311 Rust +17 docs; strict workspace all-target/all-feature check/clippy, build bins/examples, warning-denied docs, fmt/diff and32 workspace doctests PASS; Python414 run,413 pass/one AF_PACKET capability skip under umask022; fresh post-review archive SHA256 **`7924b016eee67120134e6039ab87d50eefb2f8407d0f938f941256071638ff3e`**, **799 pass/five intended native-group exclusions**, unchanged `not group(onebox)` | That archive was built from these implementation bytes, **not later documentation/Rustdoc-edited bytes**; no native/live claim |
| `29005d09ff63d26831ef993ca08244bbd260f8b6` (historical Phase 5 documentation/Rustdoc changes based on `6b397e3`, 2026-10-30) | Required format, workspace all-target/all-feature check and strict clippy, workspace bins/examples build, warning-denied workspace docs, diff check **PASS**; **33 workspace doctests PASS** (6 client +9 data +14 runtime +4 transport), including the added public reservation/registration example. Local/source/pinned reference links checked separately; [commands](../Development.md#api-alignment-documentation-gates). | No fresh archive/native outcome inferred from documentation checks; all Rust edits are comments, no executable algorithm/fixture changes; historical archive still belongs to6b397e3 |
| Current API alignment on deployed SF, both runtime three-replica and data profiles | **NOT RUN** | Must use fresh immutable bundles/exact source identities and separately execute both profiles; model/RAP/source/ABI tests do not substitute |

Earlier native qualifications remain historical at the **exact old source and
profile** recorded in [stateless evidence](RustStatelessRuntime.md#recorded-package-update-outcome-2026-10-27),
[empty-state evidence](RustStatefulRuntime.md#executed-evidence-and-limits) and
[data evidence](FubeData.md#executed-qualification). For example data source
`1414adca03635cde661b7e8ec19cf49dfdc113f8` and empty-state ownership repeat
`b093a98f5d5692b584a4b656fadb410238560e86` do not qualify the changed implementation.
Native-source revision/version skew is not resolved by a successful source fixture.

[N-runtime]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricRuntime.idl#L405-L758
[N-types]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/idl/public/FabricTypes.idl
[N-managed]: https://github.com/microsoft/service-fabric/tree/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric
[N-provider]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/IStateProvider.cs
[N-state-replicator]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/IStateReplicator.cs#L17-L139
[N-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/IOperation.cs
[N-payload-stream]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/IOperationDataStream.cs
[N-operation-stream]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/IOperationStream.cs
[N-fabric]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/managed/Api/src/System/Fabric/FabricReplicator.cs#L17-L55
[N-load]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.cpp#L1780-L1808
[N-load-merge]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/LoadBalancing/LoadOrMoveCostDescription.cpp#L121-L142
[N-health]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/HealthReport.cpp#L1318-L1353
[M-upstream]: https://github.com/Azure/service-fabric-rs
[M-public]: https://docs.rs/mssf-core/0.10.0/mssf_core/runtime/index.html
[M-stateless]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/runtime/stateless_traits.rs
[M-stateful]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/runtime/stateful_traits.rs
[M-stateful-types]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/types/runtime/stateful.rs
[M-roles]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/types/common/stateful.rs#L9-L18
[M-partition]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/types/common/partition.rs
[M-health]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/types/runtime/health.rs
[M-metrics]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/types/common/metrics.rs
[M-runtime]: https://github.com/youyuanwu/service-fabric-rs-fork/blob/35555883476ac5f78f5f53ef0215b6a32e43b574/crates/libs/core/src/runtime/runtime_wrapper.rs
[R-api]: ../../crates/libs/fube-runtime/src/api.rs#L84-L150
[R-info]: ../../crates/libs/fube-runtime/src/api.rs#L32-L60
[R-stateful]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L117-L150
[R-control]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L165-L190
[R-primary]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L194-L232
[R-partition]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L314-L359
[R-epoch]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L30-L58
[R-descriptors]: ../../crates/libs/fube-runtime/src/stateful_api.rs#L235-L281
[R-health]: ../../crates/libs/fube-runtime/src/reporting.rs#L29-L72
[R-register]: ../../crates/libs/fube-runtime/src/lib.rs#L566-L623
[R-example]: ../../crates/libs/fube-runtime/src/lib.rs#L12-L31
[R-entry]: ../../crates/libs/fube-runtime/src/execution_owned.rs#L942-L987
[R-error]: ../../crates/libs/fube-runtime/src/error.rs#L19-L75
[R-binding]: ../../crates/libs/fube-runtime/src/stateful_binding.rs#L152-L255
[RT]: ../../crates/libs/fube-runtime/tests/sdk_contracts.rs#L274-L491
[R-values]: ../../crates/libs/fube-runtime/src/stateful_api_tests.rs#L1-L245
[R-life-tests]: ../../crates/libs/fube-runtime/src/lifecycle_tests.rs#L1-L140
[R-fault-tests]: ../../crates/libs/fube-runtime/src/lifecycle_tests.rs#L113-L270
[R-execution-tests]: ../../crates/libs/fube-runtime/src/execution_tests.rs#L113-L565
[SL]: ../../crates/libs/fube-runtime/src/reporting_tests.rs#L335-L518
[SR]: ../../crates/libs/fube-runtime/src/stateful_reporting_tests.rs#L28-L598
[D-fabric]: ../../crates/libs/fube-data/src/fabric_replicator.rs#L19-L70
[D-provider]: ../../crates/libs/fube-data/src/api.rs#L129-L165
[D-payload]: ../../crates/libs/fube-data/src/api.rs#L119-L127
[D-operation]: ../../crates/libs/fube-data/src/api.rs#L167-L209
[D-replicate]: ../../crates/libs/fube-data/src/api.rs#L212-L230
[D-completion]: ../../crates/libs/fube-data/src/api.rs#L232-L262
[D-extensions]: ../../crates/libs/fube-data/src/extensions.rs#L1-L105
[D-settings]: ../../crates/libs/fube-data/src/api.rs#L12-L75
[D-memory]: ../../crates/libs/fube-data/src/provider.rs#L597-L676
[D-memory-tests]: ../../crates/libs/fube-data/src/provider.rs#L678-L1009
[DT]: ../../crates/libs/fube-data/tests/sdk_contracts.rs#L271-L648
[D-native-tests]: ../../crates/libs/fube-data/src/engine_native_tests.rs#L227-L414
[D-disposal]: ../../crates/libs/fube-data/src/engine_native_disposal_tests.rs#L68-L245
[D-wire-tests]: ../../crates/libs/fube-data/src/wire_tests.rs#L1-L40
[D-model]: ../../crates/libs/fube-data/tests/support/rap_model.rs#L227-L270
[APP-write]: ../../crates/samples/rust-runtime-app/src/data.rs#L263-L323
[APP-register]: ../../crates/samples/rust-runtime-app/src/main.rs#L628-L672
