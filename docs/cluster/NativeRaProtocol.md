# Native reliability boundary catalog

## Provenance and reading rules

Native source baseline: **3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d**,
verified with `git rev-parse HEAD` in the read-only local checkout
`/home/user1/code/reference/service-fabric`. This is source research, not a
native execution result or deployed-binary interoperability claim.

All source references below are relative to `src/prod/src/`, unless prefixed
`root:`. Every linked file is pinned to that revision. The short directories
`common/`, `ra/`, and `fm/` mean `Reliability/Failover/{common,ra,fm}/`.
Links identify files; accompanying line ranges identify the specific evidence.
No inference of sender, recipient, or completeness is made from action names.

**Status:** source inventory complete. The [foundation codecs and bounded
entity seam](RaProtocolFoundation.md) plus [stateless removal/close/reporting](RaStatelessRemoval.md)
and [opening/node registration reporting](RaStatelessOpening.md)
are implemented with source-baseline
native-stream evidence; complete handlers/runtime/traces remain staged.
The inventory alone does not establish protocol implementation.
`AddInstance`/`AddInstanceReply`, `RemoveInstance`/`RemoveInstanceReply` and
`ReplicaUp`/`ReplicaUpReply` and
`ServiceTypeNotification`/`ServiceTypeNotificationReply` are the eight selected
action codecs (58 required boundary actions remain unselected);
the bounded entity slice does not implement Hosting or authorize serving.
The implementation mapping below supplies actual Rust mappings and evidence
as each phase lands. A native test anchor is a reference to existing source, not a
claim that the test was run.

### Reconciled counts

* **119 initialized RSMessage registry entries**, all classified in
  [NativeRaRegistry.csv](NativeRaRegistry.csv): 29 RA-to-FM factories, 27 FM-to-RA factories,
  8 RA-to-RA factories, 30 common-to-FM, 22 FM-to-common,
  1 resolver, 1 common-to-RA, 1 RA-to-common.
* **66 required boundary actions**: the 64 explicit RA factory entries plus
  common registration `NodeUp`/`NodeUpAck`. This includes retained legacy,
  deprecated, no-op and sender-only definitions rather than silently deleting
  them. **67 directional contracts** because `DoReconfiguration` is also
  actually sent RA-to-RA during swap continuation.
* **53 other initialized entries** explicitly classified by actual paths:
  administration, resolution, token-change, client-to-RA query/fault, and common
  image reporting. They are not RA–FM/RA–RA state-machine exchanges.
* **2 additional header-only declarations**, `DropReplica`/`DropReplicaReply`,
  have no initializer/accessor in the pinned registry; `DropReplicaAction`
  actually sends `DeleteReplica`. They are accounted for separately below,
  not counted as initialized actions.

Registry evidence: [RSMessage.cpp:32–250][registry],
[RSMessage.h:24–173,245–298,333–334][registry-h]. Factory names are metadata,
not proof of actual direction: `GenerationProposalReply` and `GFUMTransfer`
are correlated request replies, while `DoReconfiguration` has two senders.

## Dispatch, identity, headers, and common processing

### Header contracts

| Contract | Native evidence and exact meaning |
|---|---|
| Factory headers | [RSMessage.cpp:288–299][registry] adds Actor, Action, HighPriority and sets `Idempotent=true`; constructor defaults at [RSMessage.h:13–21][registry-h]. Registry `true` means **default high priority**, not a request/reply or idempotence flag. |
| Actor values | [Transport/Actor.h:38–41][actor]: FMM=10, FM=11, RA=12, RS=13. The registry actor is the **destination actor**, not the sender identity. |
| Header schemas | [ActorHeader.h:21–24][actor-header] = `actor_:Actor::Enum`; [ActionHeader.h:26–29][action-header] = `action_:wstring`; [HighPriorityHeader.h:25–27][priority-header] = `bool`; [TimeoutHeader.h:30–33][timeout-header] = `TimeSpan`; [FabricActivityHeader.h:35–38][activity-header] = `ActivityId`. IDs: [MessageHeaderId.h:24–32,55,67][header-ids] Actor=0x8001, Action=0x8002, HighPriority=0x8009, Generation=0x8018, FabricActivity=0x8020. |
| Generation | [common/GenerationHeader.h:11–42,62–64][generation-header] serializes `(GenerationNumber generation_, bool isForFMReplica_)`; `ReadFromMessage` asserts presence. [GenerationNumber.h:56–60][generation-number] = `(int64 generation_, Federation::NodeId owner_)`. It is not a single counter or transport-session generation. |
| FM/FMM routing | [FederationWrapper.cpp:162–195,394–414][federation] rewrites Actor to FMM on FMM sends/requests. [ra/Communication.FMTransport.h:118–135][fm-transport] chooses FM/FMM by owner; owner of FM-service FT is FMM. Ordinary FM is not interchangeable with FMM. |
| FM→RA partition messages | [fm/SendMessageAction.cpp:28–47][fm-send] adds `(FM.Generation, body.FT.IsForFMReplica)` GenerationHeader. `AddInstance` is Actor RA, high priority true, with this header. |
| RA→RA partition messages | [ra/Infrastructure.SendMessageActions.h:53–88][send-actions] adds the **sender RA's send generation for that owner**, then sends to a specific NodeInstance. Includes peer `DoReconfiguration`. |
| RA→FM entity replies | [ra/Communication.FMTransport.h:45–63][fm-transport], [ra/Infrastructure.MessageUtility.h:15–23][message-utility] construct body/factory headers and entity transport-test property; **these helpers do not add GenerationHeader**. Do not invent a mandatory generation header on `AddInstanceReply` from symmetry with the request. |
| Transport wrapper details | [FederationWrapper.cpp:31–75,176–230][federation] supplies route timeouts, validates target node incarnation on direct sends, and may set `Idempotent=false` on direct P2P. The test-only partition property is not another serialized body field. Activity headers are optional for trace correlation ([ra/MessageHandler.cpp:32–41][handler]); don't require one merely because a message has an activity ID in logging. |

### RA admission and metadata

[ra/Communication.MessageMetadataTable.cpp:21–85][metadata] defines:

* `N`: target RA, creates=false, process-during-node-close=false, staleness=None.
* `NC`: target RA, creates=false, close=true, staleness=None.
* `F`: target FT, creates=false, close=false, staleness=FTFailover.
* `FC`: target FT, creates=true, close=false, staleness=FTFailover.
* `D`: deprecated; default metadata is deprecated=true
  ([Communication.MessageMetadata.h:30–36][metadata-type]).
* `RR`/`OP`: correlated registration/rebuild reply or request bypassing the
  ordinary RA metadata dispatcher, explicitly traced below.
* `—`: not an RA receive action in the pinned metadata table.

`FC` means an **empty map entry may be allocated**, not that a replica has been
committed or runtime-created. Decode, owner readiness, get-or-create and job
construction are at [MessageHandler.cpp:44–93][handler]. Unknown/deprecated
actions, lifecycle mismatch or queue-full are rejected before enqueue
(:144–183). The one-way receiver accepts its transport context before semantic
processing because higher layers retry (:214–244). This is **not**
`AddInstanceReply` or any other semantic completion.

Lifecycle is rechecked on queue execution (:450–463). FT work rechecks
generation under the entity lock, except generation zero
([MessageContext.h:96–153][message-context]); it also runs FT staleness checks.
The pinned implementation invokes the same staleness predicate twice; this
does not define two different versions of the contract.

FT failover checks at
[infrastructure.FailoverUnitEntityTraits.cpp:17–35,135–283][traits]:

* No existing FT: permit the creating handler to decide.
* Open/close pending rejects ordinary traffic; RemoveInstance/DeleteReplica
  are exceptions to pending-open/pending-close checks. This is separate from
  **node closing** metadata: both remain `FC`, close=false.
* Closed+locally-deleted create intent (`AddInstance`, `AddPrimary`,
  `CreateReplica`) is treated like absence.
* Default: reject older current epoch and, for an open FT, absent replica or
  older instance using the body replica's NodeId.
* AddReplica/RemoveReplica require **equal** epoch.
* CreateReplicaReply compares primary epoch.
* RemoveInstance/DeleteReplica skip generic epoch/instance comparison and
  have their own force/identity/epoch checks in DeleteLocalReplica.
* A newer instance for the same replica ID is a test assertion except for
  CreateReplica, AddPrimary, GetLSNReply.

Source is the transport context `FromInstance` (:237 in [MessageHandler][handler]),
retained in MessageContext; reply routing uses that source. The common checks
do **not** contain a blanket “source must be current FM/current primary”
predicate: generation, body node/epoch/instance, phase and per-handler rules
are the evidenced checks. Do not claim cryptographic source authentication
or invent a stronger native source check here.

FM admission is [FailoverManager.cpp:873–981][fm]: read status and readiness;
GenerationUpdateReject is accepted during rebuild, LFUMUpload/ChangeNotification
on the request path are exceptions to readiness. Partition bodies are decoded
before task scheduling. [FailoverUnitMessageTask.cpp:94–230][fm-task] rejects
missing FT except ReplicaDropped/DeleteReplicaReply, stale configuration/data
loss epochs, and absent/older replica instances. It passes actual `from` to
the cache/task; individual algorithms add their own checks. Replies must not
be replaced with route acknowledgments.

## Required boundary exchange inventory

Each row accounts for **each action separately** even where request/reply share
a row. Schema identifiers are expanded in the schema sections. `RA` as actor
always means the recipient is RA; `FM*` means registry Actor FM, rewritten to
FMM when sent to the FM-service owner. `reply:` is a correlated reply path,
not re-dispatch to the actor advertised in its registry factory.

All rows retain full native semantics as staged obligations. `C1` denotes the
selected first codec pair; `C2` denotes removal/reporting codecs and bounded
stateless execution; `C3` denotes service-type notification codecs and bounded
node coordination; `S` means
other staged protocol work; `L`
means retained legacy contract; `D` means native explicit rejection; `G` means
an observed pinned-source endpoint gap. C1/C2/C3 have the bounded implementation below;
the other contracts remain staged.

### Rust implementation and test mapping

| Inventory class | Rust mapping at catalog delivery | Required delivery/admission gate |
|---|---|---|
| C1: AddInstance/AddInstanceReply | Public schemas/headers: [protocol.rs](../../crates/libs/fube-ra/src/protocol.rs); selected [entity execution](../../crates/libs/fube-ra/src/entity.rs#L733-L1148) and [native opening policy/scenario mappings](RaStatelessOpening.md). [Protocol tests](../../crates/libs/fube-ra/tests/protocol.rs), [entity tests](../../crates/libs/fube-ra/tests/entity.rs), [native source-stream gate](../../tests/ra_protocol/README.md). | 36 total bidirectional native-stream/schema-adapter cases (43 types/129 fields), including all prior 29 cases, and selected entity barriers. Full Hosting/transport/native trace qualification and excluded lifecycle branches remain required; no deployed capability admission. |
| C2: RemoveInstance/RemoveInstanceReply, ReplicaUp/ReplicaUpReply | [Native-shaped schemas](../../crates/libs/fube-ra/src/protocol.rs), [native helper closure:93–747](../../crates/libs/fube-ra/src/lifecycle.rs#L93-L747), [exact native/scenario mappings and limits](RaStatelessRemoval.md). | Stateless Delete/Obliterate/close and one-FT non-last FM report/ack profile only. Trusted simulated Hosting/RAP cleanup; initial/last upload, batching, FMM, live delivery, GC/restart and broad stateful lifecycle remain staged. RemoveReplica/DeleteReplica actions are not enabled by shared schema/helpers. |
| C3: ServiceTypeNotification/ServiceTypeNotificationReply | [VersionedServiceType, ServiceTypeInfo, ServiceTypeNotification and distinct factories/decoders](../../crates/libs/fube-ra/src/protocol.rs), [bounded node ledger](../../crates/libs/fube-ra/src/notification.rs#L1-L421), [behavior/evidence limits](RaStatelessOpening.md#shared-node-registration-notifications), [protocol tests](../../crates/libs/fube-ra/tests/protocol.rs), [independent native-stream/helper cases](../../tests/ra_protocol/README.md). | Request Actor FM=11/no generation; reply Actor RA=12/actual FM GenerationHeader; both priority=false. Generic receive-generation lower-bound admission precedes key+instance-only ack. Native UTF-16-unit startup-C-locale identity accepts non-ASCII names and retains actual returned spelling. Dynamic work/retry timers and captured sends preserve native-visible histories. Schema/helper evidence does not establish live Hosting/FM delivery or full original-handler execution. |
| S: all other active boundary contracts | No Rust codec/handler/trace implementation asserted. The CSV's per-action body, direction and evidence map each missing implementation individually. | Port exact referenced schema and sender/receiver semantics, corresponding native tests and normalized traces before capability admission. No successful stubs or permanent omission. |
| L: retained legacy | No Rust wire implementation asserted; retained current/native legacy behavior must be tested explicitly. | Preserve source-baseline old action definitions/handling versus current ReplicaUp replacement distinctions. |
| D: deprecated ChangeConfigurationReply | No Rust dispatcher asserted. | Rejection/deprecation test, not successful no-op. |
| G: PartitionNotification source endpoint gap | No Rust handler invented to conceal native gap. | Resolve against native evidence and qualify exact counterpart before capability admission. |
| Nonboundary / declaration-only names | Explicit CSV classification and exclusion evidence; no invented RA exchange. | Registry/header reconciliation must remain complete. |

Offline catalog integrity tests live in
[`test_ra_catalog.py`](../../tests/offline/cluster/test_ra_catalog.py).
They run without native source. The explicit `audit_source(native_path)` also
reconciles all initialized actions, header declarations and RA metadata against
the exact pinned checkout; it does not build native code.

### Partition commands and reconfiguration

| Actions; actor; direction | Actual sender → receiver; metadata; schema | Semantic completion, preconditions, retry; native tests; status |
|---|---|---|
| `AddInstance`; RA; FM→RA. `AddInstanceReply`; FM*; RA→FM | [fm/SendMessageAction.cpp:125–131][fm-send], [fm/Replica.cpp:632–649][fm-replica] → [ra/MessageHandler.cpp:475–485][handler], [ReconfigurationAgent.cpp:1639–1724][ra-main]; FC; `ReplicaMessageBody`. Reverse: RA :894–902,1645–1719 → [FM:960–967][fm], [fm/FailoverUnitMessageProcessor.cpp:22–43][fm-processor]; `ReplicaReplyMessageBody`; —. | Stateless InBuild work generated by [PendingTask.cpp:180–201][pending]; throttled retry [Replica.cpp:661–667][fm-replica]. Actual success means runtime open completed or identical ready instance already exists, not request admission/store insertion. Detailed decision table below. Tests [Test.StateMachine.cpp:3552–3605,5529–5538,6836,7195–7197][ra-tests], [AddFailoverUnitChecks:95–115][add-check-tests], [FM StateMachineTask.Test.cpp:474][fm-tests]. **C1**. |
| `RemoveInstance`; RA; FM→RA. `RemoveInstanceReply`; FM*; RA→FM | [fm/SendMessageAction.cpp:135–150][fm-send] → [MessageHandler.cpp:486–496][handler], [RA:1726–1729,777–829][ra-main]; FC; `DeleteReplicaMessageBody` (not bare ReplicaMessageBody). Reverse [RA:698–706][ra-main] → [FM:961][fm], [fm-task:151–154][fm-task]; `ReplicaReplyMessageBody`. | Closed identity/epoch update precedes own guards; lower instance accepted. Closed always Delete, open force Obliterate; captured Delete completion selects RemoveInstanceReply, not generic force success. [PendingTask.cpp:38–45,184–195][pending] retries until removal. Actual stateless removal [Close:367–384][close-tests]; shared helper [DeleteReplica:214–275,347–415][delete-tests] includes misleadingly named nonforce DeleteReplica inputs. [Port mappings](RaStatelessRemoval.md). **C2**; complete lifecycle/runtime remains staged. |
| `AddPrimary`; RA; FM/FMM→RA. `AddPrimaryReply`; FM*; RA→owner | [fm-send:163–169][fm-send] → [handler:497–507][handler], [RA:1731–1821][ra-main]; FC; `ReplicaMessageBody`. Reverse [RA:906–917,1771–1819][ra-main] → [FM:963][fm], [fm-processor:46–62][fm-processor]; `ReplicaReplyMessageBody`. | Persisted FM replica rebuild may require TryDropFMReplica; create uses Primary role and empty replica deactivation info. Ready duplicate replies; newer requested replica closes old local replica first; closed/stale handling includes dropped report. FM requires creating-primary, up and matching node instance. PendingTask :165–175 retries. Tests [Rebuild:188–217][rebuild-tests], [ReplicaLifecycle:1342][lifecycle-tests]. **S**. |
| `AddReplica`; RA; FM→primary RA. `AddReplicaReply`; FM*; primary RA→owner | [fm-send:173–179][fm-send] → [handler:508–518][handler], [RA:1823–2051][ra-main]; F; `ReplicaMessageBody`. Reverse [RA:1893,2905,2935,4089][ra-main], [FailoverUnit.cpp:1345–1352][ft] → [FM:962][fm], [fm-processor:64–89][fm-processor]; `ReplicaReplyMessageBody`. | Equal epoch; local primary/allowed swap build, open/up and remote replica incarnation/state gates. Primary first sends CreateReplica to peer, then RAP build; build/activation completion controls FM reply. FM only completes an up InBuild replica with reconfiguration primary available; failures can drop replica. Tests [Reconfiguration:61–117][reconfiguration-tests], [ReplicaLifecycle:1342][lifecycle-tests]. **S**. |
| `RemoveReplica`; RA; FM→primary RA. `RemoveReplicaReply`; FM*; primary RA→owner | [fm-send:183–189][fm-send] → [handler:519–529][handler], [RA:2053–2140,3098–3135][ra-main]; F; `ReplicaMessageBody`. Reverse [RA:2077,2103,2137,4148][ra-main] → [FM:964][fm], [fm-processor:201–214][fm-processor]; `ReplicaReplyMessageBody`. | Equal epoch and primary/local-open checks; removes remote idle replica from primary/replicator, **not** synonym for deleting that replica on its node. In-create can be removed locally; other states require RAP idle-replica removal. PendingTask :124–130 retries pending removals. **S**; native lifecycle/reconfiguration tests above. |
| `DeleteReplica`; RA; FM/FMM→replica RA. `DeleteReplicaReply`; FM*; RA→owner | [fm-send:193–217][fm-send] → [handler:541–551][handler], [RA:2240–2243,776–834][ra-main]; FC; `DeleteReplicaMessageBody`. Reverse [RA:696–709][ra-main] → [FM:965][fm], [fm-task:167–170,207][fm-task], [fm-processor:260–266][fm-processor]; `ReplicaReplyMessageBody`. | Tombstone creation, own replica ID, force/epoch behavior; force uses Obliterate, normal uses Delete. Closed stale state still updates identity according to UpdateInstanceForClosedReplica. Reply can be processed when FM FT absent. Tests [DeleteReplica][delete-tests], [Close:346–350][close-tests]. **S**. |
| `DoReconfiguration`; RA; **FM/FMM→RA and old-primary RA→new-primary RA**. `DoReconfigurationReply`; FM*; RA→owner | FM [fm-send:153–159][fm-send]; peer [RA:3435–3451][ra-main], [FT:3497–3541][ft] → [handler:530–540][handler], [RA:2142–2238][ra-main]; FC; **`DoReconfigurationMessageBody : ConfigurationMessageBody`**. Reverse [FT:3487–3495][ft] → [FM:974–976][fm], [fm-task:187–190][fm-task]; `ConfigurationReplyMessageBody`. | Peer swap continuation carries phase0Duration, full FT/config/service fields. Closed/missing FT becomes tombstone and reports dropped, not a successful live replica. Open must actually have local replica and matching incarnation. FT phase algorithms drive reply after completion; peer continuation does **not** redirect final reply to peer. Retry [FT:3710–3791][ft]; tests [DoReconfiguration:41–169][doreconfig-tests], [Reconfiguration:61–117][reconfiguration-tests]. **S**; preserve both actual directions. |
| `ChangeConfiguration`; FM*; RA→owner. `ChangeConfigurationReply`; RA; owner→RA | [FT:3457–3468][ft] → [FM:969–972][fm], [fm-task:179–182][fm-task], [fm-processor:531–662][fm-processor]; `ConfigurationMessageBody`. Reverse [fm-send:229–236][fm-send]; `FailoverUnitReplyMessageBody`; **D** at [metadata:46][metadata], rejected [handler:161–165][handler]. | Request reports candidate/configuration information, processed only in suitable reconfiguration/source state by FM; ChangeConfigurationReply is **not** an RA progress gate in this baseline. Retain its definition and explicit deprecation behavior. Tests [DoReconfiguration:137–154,221][doreconfig-tests]. **S / D**. |
| `DatalossReport`; FM*; RA→owner; no dedicated reply action | [FT:3470–3484][ft] → [FM:969–972][fm], [fm-task:175–178][fm-task], [fm-processor:663–end][fm-processor]; `ConfigurationMessageBody`; —. | Contains full configuration/service; retransmitted while current DataLossVersion has not advanced past version being reported. [FT:3776][ft] includes it in retries. It is not a generic fault event. Native phase/get-LSN/reconfiguration tests; **S**. |
| `ServiceBusy`; RA; FM/FMM→RA; no semantic reply | [fm/FailoverUnitJobQueue.cpp:42–56][busy] → [metadata:49][metadata], [handler:316–319][handler]; N; **empty** body. | Queue-throttled FM notification; RA branch explicitly says it does not process it yet. Preserve as a documented no-op, not a substitute for any command reply. **S (native no-op)**. |

### Replica reporting and retained old actions

| Actions; actor/direction | Actual sender → receiver; metadata; schemas | Completion/retry/owner semantics; native test/status |
|---|---|---|
| `ReplicaUp`; FM*; RA→FM/FMM. `ReplicaUpReply`; RA; owner→RA | [Communication.FMMessageBuilder.cpp:22–40,75–87][fm-builder], [Node.PendingReplicaUploadStateProcessor.cpp:124–135][upload-processor], [FT:4739][ft] → [fm/CommonJobQueue.cpp:35–38][fm-queue], [FM:2328–2337][fm], [ReplicasUpdateOperation.cpp:144–201,343–421][replicas-update]. Reverse :410–421 → [handler:262–265][handler], [RA:3287–3408][ra-main]; NC. **Both use `ReplicaUpMessageBody`**. | Carries normal **and dropped** `vector<FailoverUnitInfo>`, last flag, FM/FMM flag. Reply lists are per-FT semantic processing/deletion instructions, not merely echoed received IDs. FM validates owner vs FM-service FT, waits for NodeUp incarnation, commits final node ReplicaUploaded before last=true reply. RA handles both lists and only acknowledges last after all entity work succeeds and RA is open. Retried by FMMessageRetry plus pending-upload processor. Tests [ReplicaUpReply:208–309][replicaup-tests], [LastReplicaUp:296–456][last-tests], [ReplicaUploadAcknowledgement:164][ack-tests]. **C2** for codecs and one-FT non-last removal reporting/ack; full upload/batch/FMM execution remains staged. |
| `ReplicaEndpointUpdated`; FM*; RA→owner. `ReplicaEndpointUpdatedReply`; RA; owner→RA | [fm-builder:38–39][fm-builder] → [FM:955–958][fm], [fm-task:139–142][fm-task], [fm-processor:310–358][fm-processor]. Reply [fm-send:239–245][fm-send] → [handler:552–562][handler], [RA:1630–1637][ra-main]; F. `ReplicaMessageBody` / `ReplicaReplyMessageBody`. | Endpoint-publish state and incarnation/epoch gates; reply stops the corresponding endpoint publication stage, not all FT work. Tests [EndpointPublish][endpoint-tests]. **S**. |
| `ReplicaDropped`; FM*; legacy RA→owner. `ReplicaDroppedReply`; RA; owner→legacy RA | Retained FM receive [FM:955–958][fm], [fm-task:113–114,135–138][fm-task], [fm-processor:269–308][fm-processor]; `ReplicaMessageBody`. Reply [fm-send:220–227][fm-send]; `ReplicaReplyMessageBody`. **No direct current RA sender** of this action found; `SendReplicaDroppedMessage` directly constructs **ReplicaUp** with a dropped FT list ([FT:4720–4742][ft]); pending FM retry stages also use the ReplicaUp builder. **No RA metadata/dispatch for ReplicaDroppedReply**, despite residual [RA:1601–1612][ra-main] processor. | Keep distinction: current dropped reporting uses dropped entries inside ReplicaUp, not this old action. Native FM missing-FT behavior is retained. Incoming legacy reply is unknown to current RA and rejected rather than processed by residual method. **L**, not declared `Deprecated` by native metadata. Current semantic replacement tests [ReplicaUpReply:233–250][replicaup-tests]. |
| `ReplicaDown`; FM*; legacy RA→owner. `ReplicaDownReply`; RA; owner→legacy RA | Retained FM receive [fm-queue:39–42][fm-queue], [FM:2340–2351][fm]; `ReplicaListMessageBody`. Reply [fm/ReplicaDownOperation.cpp:126–137][down-operation]; `ReplicaListMessageBody`. No current direct RA sender/receive metadata found; builder [fm-builder:29–35][fm-builder] routes down stage through ReplicaUp. | Do not label the symbolic FMMessageStage::ReplicaDown or scenario helper `ProcessReplicaDownReply` as a wire action: current replies arrive as ReplicaUpReply. **L**, not native Deprecated metadata. Current tests [ReplicaUpReply:252–309][replicaup-tests]. |

ReplicaUp **owner/flag nuances are source-specific**: batched builder and
empty-final sender populate isFromFMM from target owner
([fm-builder:85–86][fm-builder], [upload-processor:133–134][upload-processor]).
The direct `SendReplicaDroppedMessage` helper instead constructs
`(empty normal list, dropped list, false, false)` at [FT:4735][ft], while its
TransportUtility still routes to the FT owner. This is the actual pinned
constructor behavior, not an implied license to normalize every send site.
The receive side's pending-stage termination is also semantic:
[FT:5734–5788][ft] requires Dropped state to clear dropped stage, down state
and applicable instance checks to clear down stage, and exact instance to
clear up stage. A delayed “up” reply cannot clear a newer down/dropped stage.

### Required RA–RA lifecycle (not Federation membership)

All eight explicitly registered peer actions have Actor **RA**, default high
priority=true, and the per-owner send GenerationHeader supplied by
SendMessageToRAAction. The same entity job/FTFailover checks apply to them.
They exchange **replica reliability state**, independently of the transport
used to deliver them. The ninth peer direction, `DoReconfiguration` swap
continuation, is already included above.

| Actions and actual direction | Sender → receiver; metadata; schema | Preconditions, semantic completion and retry; native tests; status |
|---|---|---|
| `CreateReplica`; primary/old-primary RA→remote replica RA. `CreateReplicaReply`; remote RA→requesting RA | [FT:2437–2480][ft] chooses an up, InCreate, remote configuration replica while local is Primary or swapping out; [RA:3453–3468][ra-main] builds request → [handler:565–575][handler], [RA:2245–2423][ra-main]; **FC**; `RAReplicaMessageBody` (ReplicaMessageBody base + DI). Reply [RA:918–929,2260,2314,2408][ra-main] → [handler:611–621][handler], [RA:2851–2939][ra-main]; **F**; `ReplicaReplyMessageBody`. | Upgrade/deactivation admission check may reject as RANotReadyForUse or silently defer upgrading node; local newer replica closes old first, closed equal/older instance reports dropped/StaleRequest; allowed requested role Idle/Secondary. Creates locally as Idle, records requesting FromInstance for eventual reply; primary-epoch change may require restart/drop and rebuilding. Ready duplicate may reply immediately; initial reply follows actual open. On primary, require open local Primary/allowed swap build, up/exact remote instance. Success advances InCreate→InBuild and triggers RAP build; **CreateReplicaReply is not AddReplicaReply**. Failure is sent to FM for idle add and aborts creation. [FT:3710–3720,2437–2506][ft] retries lifecycle while state remains pending. Tests [ReplicaLifecycle:1342][lifecycle-tests], [Rebuild:219–248][rebuild-tests], [Reconfiguration:61–117][reconfiguration-tests]. **S**. |
| `GetLSN`; primary-candidate RA→other replica RA. `GetLSNReply`; replica RA→requesting RA | [FT:2204–2268][ft] phase1 send, [RA:3418–3433][ra-main] generic request helper → [handler:576–586][handler], [RA:2425–2492][ra-main]; **FC**; `ReplicaMessageBody`. Reply [RA:3541–3577][ra-main] normal/not-found helper and :2476–2484 closed base-only variant → [handler:622–632][handler], [RA:2942–2957][ra-main], [FT:519–645][ft]; **F**; `GetLSNReplyMessageBody : ReplicaReplyMessageBody` (closed variant base only). | Missing FT returns **NotFound without durable FT insert**, despite creating empty entry permission. Closed same replica replies Success+Dropped using **requested instance**, not newer local instance, for older-peer compatibility; closed different replica returns NotFound. Open replica updates epoch/PC as needed, records FromInstance and asks RAP status. Reply accepted only Success/NotFound, local open/not-closing, phase1, known replica RAReplyPending and exact instance. NotFound marks unknown LSN; Dropped updates durable dropped state; otherwise deactivation epoch/catchup progress used. Phase1 quorum/LSN selection drives continuation, not a generic ack. Retry via pending message stage until phase progress clears it. Tests [Test.StateMachine.GetLsn.cpp:110–189][getlsn-tests], [GetLSNReply.cpp:70–90][getlsn-reply-tests]. **S**. |
| `Deactivate`; coordinating primary/old-primary RA→other replica RA. `DeactivateReply`; other RA→requesting RA | [FT:2409–2434,2566–2593,2649–2700][ft] (deactivation-info update, forced restart, phase3); [RA:3487–3503][ra-main] → [handler:587–597][handler], [RA:2495–2682][ra-main]; **F**; `DeactivateMessageBody : ConfigurationMessageBody` + isForce/DI. Reply [RA:691,2540,2675][ra-main] → [handler:633–643][handler], [RA:2960–2989][ra-main], [FT:1110–1239][ft]; **F**; `ReplicaReplyMessageBody`. | Closed FT updates CC epoch and replies with incoming replica instance if matching replica ID exists in list. Open requires actual open/not-InCreate, local NodeInstance member and nonstale incarnation/config. Force is remote restart/deactivate, **does not** use normal deactivation-epoch protocol; persisted vs nonpersisted close differs. Normal traffic updates DI and intermediate/previous configuration, may close excluded S/N replica and reply only after close. Successful reply drives DeactivateCompleted/phase progress; non-success does not clear pending. Phase3 sends only remote up RAReplyPending, not creating, not still building unless ToBeDeactivated, not restarting. Retry stops on message-stage/phase transitions/quorum conditions, not delivery receipt. [Reconfiguration:61–117][reconfiguration-tests], [GetLsn tests:137–155][getlsn-tests]. **S**. |
| `Activate`; coordinating primary/old-primary RA→other replica RA. `ActivateReply`; other RA→requesting RA | [FT:2828–2875][ft], [RA:3470–3484][ra-main] → [handler:598–608][handler], [RA:2685–2849][ra-main]; **F**; `ActivateMessageBody : ConfigurationMessageBody` + DI. Reply [RA:2726,2844,3982][ra-main] → [handler:644–654][handler], [RA:2992–3021][ra-main], [FT:1242–1357][ft]; **F**; `ReplicaReplyMessageBody`. | Remote CC member, up, RAReplyPending, not create/still-build/restart at send. Closed receiver echoes incoming instance for matching replica ID after updating epoch. Open receiver requires local open/not-closing, membership/instance; cannot be activated as Primary; always considers DI update after staleness gates. Pending RAP role update is retried and **sender FromInstance updated** because coordinator may change. Role/config change may require RAP UpdateConfiguration before reply; unchanged local case can complete immediately. Successful ActivateReply clears appropriate pending stage and checks phase4 or finishes build/AddReplicaReply; does not by itself mean whole partition reconfigured. Tests [Reconfiguration:61–117,148–end][reconfiguration-tests], [DoReconfiguration:156–169][doreconfig-tests]. **S**. |

### Common registration, rebuild and generation ownership

| Actions; actor/direction | Actual sender → receiver; metadata; schemas | Semantic completion, retry, native evidence/status |
|---|---|---|
| `NodeUp`; FM*; common node→FM/FMM. `NodeUpAck`; RS registry actor; correlated owner→common→RA | [ReliabilitySubsystem.cpp:588–596][rs], [common/NodeUpOperation.cpp:14–50][nodeup] → [FM:1077–1079,1739–1769][fm]. Ack is consumed by `SendReceiveToFMOperation` then `RA.ProcessNodeUpAck` [RA:1501–1528][ra-main], [NodeUpAckProcessor.cpp:37–154][nodeup-processor]; OP. `NodeUpMessageBody` / `NodeUpAckMessageBody`. | Independent FM/FMM request operations/readiness. NodeUp carries packages/node/anyReplicaFound; ack carries upgrades, FabricVersionInstance, activation bool+sequence, GenerationHeader. Update receive/send/proposed generations; process all owner FTs before setting ready and pending replica upload. FMM cannot initiate fabric upgrade via NodeUpAck. Retry [SendReceiveToFMOperation.cpp:34–129][send-receive] until **expected ack action**; wrong-action/transport success is not completion. Tests [NodeUpAckProcessing][nodeup-tests], [Test.Unit.NodeUpOperationFactory.cpp][nodeup-factory-tests]. **S**, required despite common rather than RA factory. |
| `GenerationProposal`; RA; FM/FMM→common RA actor preprocessor. `GenerationProposalReply`; FM registry actor; correlated RA/common→proposer | [fm/RebuildContext.cpp:76–95][rebuild] → [ReliabilitySubsystem.cpp:318–388][rs] → [GenerationStateManager.cpp:295–311][generation-state]; RR, bypass metadata. Request **empty** + GenerationHeader; reply `GenerationProposalReplyMessageBody`. | New generation updates per-owner proposed generation. Reply records requested and highest current proposed generation plus ErrorCode. FM consumes via multi-reply context (:132–239 in RebuildContext), retries/restarts proposal if greater generation found or decode fails. Replies are not normal one-way FM messages. **S**; [Rebuild tests:250–end][rebuild-tests]. |
| `GFUMTransfer`; FM registry actor; **former FMM/common saved state→new FMM**, correlated alternative to GenerationProposalReply | Saved state created [FM:737–771][fm]; actual send [RS:374–380][rs] when FMM proposal and savedGFUM exist; receive [rebuild:167–186][rebuild] → [FM:2545–2608][fm]; RR; `fm::GFUMMessageBody`. | Clears savedGFUM after handing to reply; restores global state and completes update/recover rather than normal LFUM rebuild. Contains NodeInfo vector and **full fm::FailoverUnit**, not a FailoverUnitInfo list. Neither “RA requests GFUM” nor ordinary RA peer lifecycle. **S**, within required registration/rebuild inventory. |
| `GenerationUpdate`; RA; FM/FMM→RA. `GenerationUpdateReject`; FM*; RA→proposer/owner | [rebuild:282–314][rebuild] → [handler:258–261][handler], [RA:4200–4214][ra-main], [generation-state:46–287][generation-state]; N. **FM body `GenerationUpdateMessageBody`; FMM body empty.** Reject body `GenerationNumber`; send :272–282 → [FM:2610–2618][fm], [rebuild:368–387][rebuild]. | Receive generation only updates if proposed generation matches. FM path may first examine/abort stale FM-service local replica using FMServiceEpoch, under FT lock, before LFUM snapshot; FMM handles its separate domain. Reject restarts generation negotiation, not a generic negative ack. Retry broadcast until pending-node LFUM progress completes. **S**, [Rebuild:250–end][rebuild-tests]. |
| `LFUMUpload`; FM*; RA/common→owner. `LFUMUploadReply`; RA registry actor; correlated owner→common→RA | [generation-state:359–end][generation-state], [RS:599–646][rs], [common/RALFUMUploadOperation.cpp:14–39][lfum-operation] → [FM:2620–2647][fm], [rebuild:389–end][rebuild]. OP. Request `LFUMMessageBody`; reply **empty** plus echoed request generation+FM/FMM GenerationHeader. | FMM LFUM contains at most FM-service FT; ordinary FM LFUM excludes it. Body generation/node/list/anyReplicaFound all matter. Retry operation expects LFUMUploadReply; RA advances **send generation only when reply generation equals receive generation** ([generation-state:318–337][generation-state]). **S**, [Rebuild tests][rebuild-tests]. |

### Node/service controls, upgrades, load

| Actions; actor/direction | Sender → receiver; metadata; body | Semantic completion/retry/test; staged obligation |
|---|---|---|
| `ServiceTypeEnabled` / `ServiceTypeDisabled`; FM; RA→FM. `ServiceTypeEnabledReply` / `ServiceTypeDisabledReply`; RA; FM→RA | [Node.ServiceTypeUpdateProcessor.cpp:236–258][service-update] → [fm-queue:43–50][fm-queue], [FM:2363–2465][fm]. Reply :2457–2465 → [handler:277–284][handler], [service-update:277–end][service-update]; N. `ServiceTypeUpdateMessageBody` / `ServiceTypeUpdateReplyMessageBody`. | Vector of ServiceTypeIdentifier plus sequence; node/service updates and node sequence commit precede FM reply; RA removes only corresponding pending update at matching sequence; stale replies do not stop newer retry. FM only, not FMM. [ServiceTypeEnabledDisabled:164–306][service-type-tests]. **S**. |
| `ServiceTypeNotification`; FM; RA→FM. `ServiceTypeNotificationReply`; RA; FM→RA | [FMTransport:65–74][fm-transport] called by Hosting service-type map → [fm-queue:31–34][fm-queue], [FM:2478–2513][fm]. Reply → [handler:308–311][handler], [RA:1326–1334][ra-main]. N; `ServiceTypeNotificationRequestMessageBody` / `ServiceTypeNotificationReplyMessageBody`. | Full versioned type/code-package identity; reply contains only processed infos, pending infos continue retry. Distinct from enabled/disabled sequence protocol. Both factories priority=false; request normal FM/no generation, reply RA/actual FM generation. [ServiceTypeNotification:158–311][service-notification-tests]. **C3** codecs; live node/runtime integration remains staged. |
| `PartitionNotification`; FM; RA→FM. `PartitionNotificationReply`; RA; intended FM→RA | Actual RA send [service-update:261–275][service-update]; full-source search finds **no FM receive branch and no reply construction** in this baseline. FM one-way fallthrough [FM:1014–1058][fm] has no PartitionNotification. RA receive [handler:312–315][handler], [service-update:287–325][service-update]; N. Request `PartitionNotificationMessageBody`; reply decoded as `ServiceTypeUpdateReplyMessageBody`. | Disabled partition list plus sequence; RA retries/removes by sequence. Native RA tests synthesize the reply [PartitionEnabledDisabled:220–306][partition-tests]. **G**: sender/receiver contract required, but a working native FM endpoint is not evidenced. This is an explicit known source gap, not an omitted or invented handler. |
| `NodeActivateRequest`; RA; FM/FMM→RA. `NodeActivateReply`; FM*; RA→sender owner | [fm/NodeCache.cpp:1912–1923][node-cache] → [handler:267–270][handler], [Node.NodeDeactivationMessageProcessor.cpp:21–29,42–59][activation]; N. Request `NodeActivateRequestMessageBody`; reply `NodeActivateReplyMessageBody`; send :117–126 → [fm-queue:51–54][fm-queue], [FM:1288–1296][fm]. | Request embeds owner bool+int64 sequence. Must have that owner's NodeUpAck readiness. FM activation can update both FM/FMM local activation state; FMM activation only itself. Activate reply immediate after state request (including stale-request ack), not proof all reopened replicas are serving. [NodeActivationDeactivation:176–273][activation-tests]. **S**. |
| `NodeDeactivateRequest`; RA; FM/FMM→RA. `NodeDeactivateReply`; FM*; RA→sender owner | [fm/DeactivateNodesContext.cpp:366–379][deactivate-context] → [handler:271–274][handler], [activation:31–40,42–96][activation]; N. Request `NodeDeactivateRequestMessageBody`; reply `NodeDeactivateReplyMessageBody`; [Node.NodeDeactivationState.cpp:117–170][activation-state] → [fm-queue:55–58][fm-queue], [FM:1277–1285][fm]. | Owner/sequence readiness checks. Reply **after all replicas are closed**, with state/sequence rechecks; not immediate admission. Closing monitor and sequence staleness at :195–204. Retry owned by FM deactivation context. [NodeActivationDeactivation:212–250][activation-tests]. **S**. |
| `NodeUpdateServiceRequest`; RA; FM/FMM→RA. `NodeUpdateServiceReply`; FM*; RA→owner | [fm/UpdateServiceContext.cpp:99–110][update-context] → [handler:304–307][handler], [RA:1285–1324,3139–3230][ra-main]; N. Request `NodeUpdateServiceRequestMessageBody`; reply `NodeUpdateServiceReplyMessageBody`; [FM:1034–1036,2352–2361][fm]. | Enumerates owner FTs, validates service identity/instance/update-version, finishes multi-entity work then replies. Reply is service name/instance/update-version, no generic ErrorCode. [UpdateServiceDescription:318–344,393–403][update-tests]. **S**. |
| `NodeUpgradeRequest`; RA; FM→RA. `NodeUpgradeReply`; FM; RA→FM | [fm/Upgrade.cpp:483–486,179–216][upgrade] → [handler:287–290][handler], [RA:1418–1465][ra-main]; N. `UpgradeDescription` (ApplicationUpgradeSpecification), **not the administrative UpgradeRequestMessageBody wrapper**. Reply [FMTransport:76–85][fm-transport] → [FM:1022–1024,2274–2299][fm]; `NodeUpgradeReplyMessageBody`. | RA ready, app-upgrade table open; atomic version-table/FT enumeration under upgrade lock; version/instance-aware upgrade processing and completed-instance reply replay. Reply includes application ID, instance and full ReplicaUp body. FM context retries pending nodes. [Upgrade.UpgradeMessageProcessor.cpp:324–474][upgrade-processor], [Test.Unit.UpgradeMessageProcessor.cpp][upgrade-tests]. **S**. |
| `CancelApplicationUpgradeRequest`; RA; FM→RA. `CancelApplicationUpgradeReply`; FM; RA→FM | [fm/ApplicationUpgradeContext.cpp:200–211][application-upgrade-context] → [handler:291–294][handler], [RA:1469–1488][ra-main]; N. `UpgradeDescription`. Reply [FMTransport:98–107][fm-transport] → [FM:1030–1032,2313–2326][fm]; `NodeUpgradeReplyMessageBody`. | Cancel instance barriers; [UpgradeMessageProcessor:245–322][upgrade-processor] says cancellation reply waits for matching active upgrade cancellation completion; not transport cancellation. [Test.StateMachine.CancelUpgradeContext.cpp][cancel-tests]. **S**. |
| `NodeFabricUpgradeRequest`; RA; FM→RA. `NodeFabricUpgradeReply`; FM; RA→FM | [upgrade:425–428,179–216][upgrade] → [handler:296–299][handler], [RA:1337–1397][ra-main]; N. `FabricUpgradeSpecification`. Reply [FMTransport:87–96][fm-transport] → [fm-queue:59–62][fm-queue], [FM:2259–2272][fm]; `NodeFabricUpgradeReplyMessageBody`. | RA readiness and FabricVersionInstance staleness classifier; upgrade-not-required returns matching semantic reply, upgrade-required enters upgrade state machine, incompatible unexpected case ignored/test-asserted. [Test.Unit.Upgrade.FabricUpgradeStalenessChecker.cpp][fabric-check-tests]. **S**. |
| `CancelFabricUpgradeRequest`; RA; FM→RA. `CancelFabricUpgradeReply`; FM; RA→FM | [fm/FabricUpgradeContext.cpp:289–300][fabric-upgrade-context] → [handler:300–303][handler], [RA:1399–1416][ra-main]; N. `FabricUpgradeSpecification`. Reply [FMTransport:109–118][fm-transport] → [FM:1026–1028,2300–2311][fm]; `NodeFabricUpgradeReplyMessageBody`. | Ready/version/instance-aware cancellation; reply is FabricVersionInstance, not bool/ErrorCode. Upgrade/cancel processor retains old/new instance ordering. [CancelUpgradeContext][cancel-tests], [UpgradeMessageProcessor tests][upgrade-tests]. **S**. |
| `ReportLoad`; FM; RA→FM; no dedicated reply | [RA:3693–3725][ra-main] forwards local RAP report after decode → [FM:1018–1020,2516–2543][fm]; `ReportLoadMessageBody`; —. | Best-effort reports include legacy reports vector, modern LoadOrMoveCostDescription vector and sender StopwatchTime; do not reduce to a single metric or interpret a route ack as reliable per-report completion. Local IPC source is RAMessage ReportLoad, different registry. Resource usage conversion is adjacent. [Test.StateMachine.ResourceMonitorProcessing.cpp][load-tests]. **S**. |

Additional completion details:

* **ChangeConfiguration retry:** after choosing a different primary,
  [FT:2884–2915][ft] captures the request description/list and sends it.
  An equal-epoch retried DoReconfiguration while result is ChangeConfiguration
  resends ChangeConfiguration ([FT:1364–1379][ft]). FM changes epoch/primary
  and may enqueue a fresh DoReconfiguration
  ([fm-processor:624–660][fm-processor]); it also emits the legacy
  ChangeConfigurationReply, but current RA rejects that deprecated action.
  Do not make the deprecated reply a required RA retry-completion gate.
* **NodeUpdateService completion:** [RA:3207–3230][ra-main] suppresses reply
  if any entity actually marked updated has a failed commit, waiting for FM
  retry. Unchanged/no-FT work still can reply; “received request” is not an
  unconditional semantic ack.
* **GenerationProposal admission:** common preprocessing still invokes
  [RA:4189–4198][ra-main]; when RA is not open, the body error becomes
  RANotReadyForUse rather than silently reporting successful participation.
  LFUMUpload at [rebuild:389–end][rebuild] requires active matching generation
  and matching pending-node incarnation before consuming reports.

## Serialized schema graph

Field order below is the **macro order**, never member declaration order,
constructor parameter order, JSON order or trace-text order. A colon `base +`
means inherited serialization **runs before a distinct derived type scope**.
[Common/FabricSerializationHelper.h:264–294][serialization] calls
`__super::Write/Read`, then StartType, then the declared fields, then EndType.
The primitive macro at :4310–4320 writes the primitive directly, without
introducing a FabricSerializable object scope.

**Width distinction:** C++ enum declarations do not establish serialized
width. [Common/FabricSerializationHelper.h:44–62][serialization] serializes
**scalar enum fields as LONG64**, including ReplicaRole, ReplicaStates, partition kind,
metric weight, policy kind, Actor and ErrorCodeValue. ErrorCode therefore
becomes the enum's **signed 64-bit primitive serialization**, even though its
stored C++ enum/HRESULT value is 32-bit. Negative HRESULTs are sign-extended;
do not zero-extend their u32 bit patterns. Integer serialization is the native
stream's compression, not a fixed eight-byte error payload.

This scalar rule does not override `ArrayUtility`: a vector of a type marked
`DEFINE_AS_BLITTABLE` without a specific array utility is serialized as raw
byte array, `count * sizeof(T)` ([serialization:134–164,4997–5001][serialization]).
In particular `vector<NodeDeactivationIntent::Enum>` in GFUM is this array
case, not repeated scalar LONG64. Serializable object vectors instead use
`DEFINE_USER_ARRAY_UTILITY` and WriteSerializableArray.

### Body roots (all boundary actions)

Each root's member declarations in the cited header give the C++ types; nested
schemas are listed below. `FT`, `R`, `SD`, `DI`, `E` abbreviate
FailoverUnitDescription, ReplicaDescription, ServiceDescription,
ReplicaDeactivationInfo, ErrorCode.

| Body type | Exact serialized fields and definition |
|---|---|
| ReplicaMessageBody | `(FT fudesc_, R replica_, SD service_)` [common/ReplicaMessageBody.h:51–56][replica-body]. |
| ReplicaReplyMessageBody | `(FT fudesc_, R replica_, E errorCode_)` [common/ReplicaReplyMessageBody.h:37–43][replica-reply]. **No ServiceDescription field.** |
| DeleteReplicaMessageBody | `ReplicaMessageBody base + (bool isForce_)`; [common/DeleteReplicaMessageBody.h:10,52–55][delete-body]. |
| RAReplicaMessageBody | `ReplicaMessageBody base + (DI deactivationInfo_)`; [common/RAReplicaMessageBody.h:10,32–34][ra-replica-body]. |
| ConfigurationMessageBody | `(FT fudesc_, vector<R> replicas_, SD serviceDesc_)`; [common/ConfigurationMessageBody.h:62–68][config-body]. Declaration order differs. |
| ConfigurationReplyMessageBody | `(FT fudesc_, vector<R> replicas_, E errorCode_)`; [common/ConfigurationReplyMessageBody.h:42–48][config-reply]. |
| DoReconfigurationMessageBody | **ConfigurationMessageBody base + `(TimeSpan phase0Duration_)`**, default MaxValue; [common/DoReconfigurationMessageBody.h:10–22,42–45][doreconfig-body]. Dropping base or derived scope loses protocol fields. |
| ActivateMessageBody | `ConfigurationMessageBody base + (DI deactivationInfo_)`; [common/ActivateMessageBody.h:10,33–36][activate-body]. |
| DeactivateMessageBody | `ConfigurationMessageBody base + (bool isForce_, DI deactivationInfo_)`; [common/DeactivateMessageBody.h:10,38–43][deactivate-body]. |
| GetLSNReplyMessageBody | `ReplicaReplyMessageBody base + (DI deactivationInfo_)`; [common/GetLSNReplyMessageBody.h:10,32–35][getlsn-body]. **Closed GetLSN success path sends a base ReplicaReplyMessageBody** ([RA:2476–2484][ra-main]); normal/NotFound helper sends derived (:3554–3577). Preserve default/missing derived-scope behavior as a native conformance case, not a fabricated all-default derived fixture. |
| FailoverUnitMessageBody | `(FT fudesc_)`; [common/FailoverUnitMessageBody.h:29–33][ft-body]. Defined family, not the current AddInstance body. |
| FailoverUnitReplyMessageBody | `(FT fudesc_, E errorCode_)`; [common/FailoverUnitReplyMessageBody.h:33–38][ft-reply]. Used retained ChangeConfigurationReply. |
| ReplicaUpMessageBody | `(vector<FailoverUnitInfo> replicaList_, vector<FailoverUnitInfo> droppedReplicas_, bool isLastReplicaUpMessage_, bool isFromFMM_)`; [common/ReplicaUpMessageBody.h:56–76][replicaup-body]. Owner bool is meaningful both directions. |
| ReplicaListMessageBody | `(map<FailoverUnitId, ReplicaDescription> replicas_)`; [common/ReplicaListMessageBody.h:36–40][replicalist-body]. Retained ReplicaDown pair. |
| NodeUpMessageBody | `(NodeDescription nodeDescription_, vector<ServicePackageInfo> packages_, bool anyReplicaFound_)`; [common/NodeUpMessageBody.h:103–108][nodeup-body]. |
| NodeUpAckMessageBody | `(vector<UpgradeDescription> upgrades_, FabricVersionInstance fabricVersionInstance_, bool isNodeActivated_, int64 nodeActivationSequenceNumber_)`; [common/NodeUpAckMessageBody.h:54–61][nodeupack-body]. |
| GenerationProposalReplyMessageBody | `(GenerationNumber proposedGeneration_, GenerationNumber currentGeneration_, ErrorCode error_)`; [common/GenerationProposalReplyMessageBody.h:42–47][proposal-body]. |
| GenerationUpdateMessageBody | `(Epoch fmServiceEpoch_)`; [common/GenerationUpdateMessageBody.h:31–35][genupdate-body]. FMM sends no body. |
| LFUMMessageBody | `(GenerationNumber generation_, NodeDescription node_, vector<FailoverUnitInfo> failoverUnitInfos_, bool anyReplicaFound_)`; [common/LFUMMessageBody.h:51–57][lfum-body]. |
| fm::GFUMMessageBody | `(GenerationNumber generation_, vector<fm::NodeInfo> nodes_, fm::FailoverUnit failoverUnit_, int64 endLookupVersion_, SD serviceDescription_)`; [fm/GFUMMessageBody.h:77–85][gfum-body]. |
| ServiceTypeUpdateMessageBody | `(vector<ServiceTypeIdentifier> serviceTypes_, uint64 sequenceNumber_)`; [common/ServiceTypeUpdateMessageBody.h:38–43][type-update-body]. |
| ServiceTypeUpdateReplyMessageBody | `(uint64 sequenceNumber_)`; [common/ServiceTypeUpdateReplyMessageBody.h:25–29][type-reply-body]. Also PartitionNotificationReply receive type. |
| ServiceTypeNotificationRequestMessageBody / ReplyMessageBody | Both `(vector<ServiceTypeInfo> infos_)`; [request:29–32][type-notification-body], [reply:29–32][type-notification-reply-body]. Rust `ServiceTypeNotification { infos: ObjectArray<Vec<ServiceTypeInfo>> }`, reply alias, distinct actions/factories. |
| PartitionNotificationMessageBody | `(vector<PartitionId> disabledPartitions_, uint64 sequenceNumber_)`; [common/PartitionNotificationMessageBody.h:38–42][partition-body]. PartitionId alias is defined in common types; no FM receive implementation was found. |
| NodeActivateRequestMessageBody / NodeDeactivateRequestMessageBody | `(int64 sequenceNumber_, bool isFromFmm_)`; [activate:32–36][nodeactivate-body], [deactivate:35–39][nodedeactivate-body]. |
| NodeActivateReplyMessageBody / NodeDeactivateReplyMessageBody | `(int64 sequenceNumber_)`; [activate:28–31][nodeactivate-reply-body], [deactivate:24–28][nodedeactivate-reply-body]. |
| NodeUpdateServiceRequestMessageBody | `(SD serviceDescription_)`; [common/NodeUpdateServiceRequestMessageBody.h:37–40][service-request-body]. |
| NodeUpdateServiceReplyMessageBody | `(wstring serviceName_, uint64 serviceInstance_, uint64 serviceUpdateVersion_)`; [common/NodeUpdateServiceReplyMessageBody.h:41–47][service-reply-body]. |
| UpgradeDescription | `(ApplicationUpgradeSpecification specification_)`; [common/UpgradeDescription.h:50–53][upgrade-body]. |
| NodeUpgradeReplyMessageBody | `(ApplicationIdentifier applicationId_, uint64 instanceId_, ReplicaUpMessageBody replicas_)`; [common/NodeUpgradeReplyMessageBody.h:43–49][upgrade-reply-body]. Both node application upgrade and cancel replies. |
| FabricUpgradeSpecification | `(FabricVersion fabricVersion_, uint64 instanceId_, UpgradeType::Enum upgradeType_, bool isMonitored_, bool isManual_)`; [ServiceModel/FabricUpgradeSpecification.h:48–59][fabric-upgrade-body]. |
| NodeFabricUpgradeReplyMessageBody | `(FabricVersionInstance versionInstance_)`; [common/NodeFabricUpgradeReplyMessageBody.h:55–59][fabric-upgrade-reply-body]. Also cancel reply. |
| ReportLoadMessageBody | `(vector<ReportLoadInfo> reports_, vector<LoadOrMoveCostDescription> reportsNew_, StopwatchTime senderTime_)`; [common/ReportLoadMessageBody.h:54–59][load-body]. |
| GenerationUpdateReject | Body is directly `GenerationNumber` as constructed [generation-state:272–274][generation-state]. |
| Bodyless | ServiceBusy, GenerationProposal, LFUMUploadReply; FMM's GenerationUpdate only. A bodyless request/reply is not a body object containing an empty status. |

### Full AddInstance / AddInstanceReply nested field/type contract

This subsection is sufficient to identify **all serialized reachable types**
for the selected pair, including nonempty policy/scaling collections.
Primitive field values are still encoded by FabricSerialization, not a
fixed-width packed C struct.

1. **FailoverUnitDescription** [common/FailoverUnitDescription.h:71–80][ft-description]:
   `FailoverUnitId failoverUnitId_`,
   `ConsistencyUnitDescription consistencyUnitDescription_`,
   `Epoch ccEpoch_`, `Epoch pcEpoch_`,
   `int32 targetReplicaSetSize_`, `int32 minReplicaSetSize_`.
   * [FailoverUnitId.h:106–109][ft-id]: `(Guid guid_)`.
   * [ConsistencyUnitDescription.h:90–97][cu-description]:
     `(ConsistencyUnitId consistencyUnitId_, int64 lowKeyInclusive_,
     int64 highKeyInclusive_, wstring partitionName_,
     FABRIC_SERVICE_PARTITION_KIND partitionKind_)`.
     [ConsistencyUnitId.h:202,220][cu-id]: `(Guid guid_)`.
   * [Epoch.h:60,71–72][epoch]: `(int64 configurationVersion_, int64 dataLossVersion_)`.
     **Configuration before data loss**, opposite constructor parameter order.
2. **ReplicaDescription** [common/ReplicaDescription.h:119–164][replica-description],
   exact 12-field order:
   `(Federation::NodeInstance federationNodeInstance_, int64 replicaId_,
   int64 instanceId_, ReplicaRole::Enum currentConfigurationRole_,
   ReplicaRole::Enum previousConfigurationRole_, bool isUp_,
   int64 lastAcknowledgedLSN_, int64 firstAcknowledgedLSN_,
   ReplicaStates::Enum state_, ServicePackageVersionInstance packageVersionInstance_,
   wstring serviceLocation_, wstring replicationEndpoint_)`.
   * [ServiceModel/federation/NodeInstance.h:46,97–100][node-instance]:
     `(NodeId id_, uint64 instanceId_)`.
   * [NodeId.h:249–253][node-id] uses `FABRIC_SERIALIZE_AS_BYTEARRAY`,
     not `FABRIC_FIELDS`. It wraps `LargeInteger`; [Common/LargeInteger.h:189–194][large-integer]
     layout is **uint64 high_, uint64 low_**. The macro writes `sizeof(NodeId)`
     bytes ([serialization:118–132][serialization]); preserve byte order/layout
     according to native executed fixture evidence, not UUID text.
   * Version closure is item 4 below.
3. **ServiceDescription** [ServiceModel/reliability/failover/ServiceDescription.h:200–206,227–254][service-description],
   exact 25-field order:

   | # | Member | C++ type |
   |---|---|---|
   | 1 | name_ | wstring |
   | 2 | instance_ | uint64 |
   | 3 | type_ | ServiceTypeIdentifier |
   | 4 | applicationName_ | wstring |
   | 5 | packageVersionInstance_ | ServicePackageVersionInstance |
   | 6 | partitionCount_ | int32 |
   | 7 | targetReplicaSetSize_ | int32 |
   | 8 | minReplicaSetSize_ | int32 |
   | 9 | isStateful_ | bool |
   | 10 | hasPersistedState_ | bool |
   | 11 | initializationData_ | vector<byte> |
   | 12 | isServiceGroup_ | bool |
   | 13 | serviceCorrelations_ | vector<ServiceCorrelationDescription> |
   | 14 | placementConstraints_ | wstring |
   | 15 | scaleoutCount_ | int32 |
   | 16 | metrics_ | vector<ServiceLoadMetricDescription> |
   | 17 | defaultMoveCost_ | uint32 |
   | 18 | replicaRestartWaitDuration_ | TimeSpan |
   | 19 | quorumLossWaitDuration_ | TimeSpan |
   | 20 | updateVersion_ | uint64 |
   | 21 | placementPolicies_ | vector<ServicePlacementPolicyDescription> |
   | 22 | standByReplicaKeepDuration_ | TimeSpan |
   | 23 | servicePackageActivationMode_ | ServicePackageActivationMode::Enum |
   | 24 | serviceDnsName_ | wstring |
   | 25 | scalingPolicies_ | vector<ServiceScalingPolicyDescription> |

4. **Type/version closure**:
   * [ServiceTypeIdentifier.h:85,93–94][service-type-id]:
     `(ServicePackageIdentifier packageIdentifier_, wstring serviceTypeName_)`.
   * [ServicePackageIdentifier.h:69,77–78][package-id]:
     `(ApplicationIdentifier applicationIdentifier_, wstring servicePackageName_)`.
   * [ApplicationIdentifier.h:51,58–59][app-id]:
     `(wstring applicationTypeName_, ULONG/u32 applicationNumber_)`.
   * [ServicePackageVersionInstance.h:64,67–68][package-version-instance]:
     `(ServicePackageVersion version_, uint64 instanceId_)`.
   * [ServicePackageVersion.h:49,52–53][package-version]:
     `(ApplicationVersion appVersion_, RolloutVersion value_)`.
   * [ApplicationVersion.h:38,57][app-version]: `(RolloutVersion value_)`.
   * [RolloutVersion.h:44,62–63][rollout-version]:
     `(ULONG/u32 major_, ULONG/u32 minor_)`.
5. **Nonempty service collections**:
   * [ServiceCorrelationDescription.h:38,51–52][correlation]:
     `(wstring serviceName_, FABRIC_SERVICE_CORRELATION_SCHEME scheme_)`.
   * [ServiceLoadMetricDescription.h:49,65–68][metric]:
     `(wstring name_, FABRIC_SERVICE_LOAD_METRIC_WEIGHT weight_,
     uint32 primaryDefaultLoad_, uint32 secondaryDefaultLoad_)`.
   * [ServicePlacementPolicyDescription.h:47,60–61][placement]:
     `(wstring domainName_, FABRIC_PLACEMENT_POLICY_TYPE type_)`.
   * [ServiceScalingPolicyDescription.h:72–75][scaling-policy]:
     `(ScalingTriggerSPtr trigger_, ScalingMechanismSPtr mechanism_)`.
     This order is opposite member declarations. These are **polymorphic**
     pointers with native Fabric type information, not plain enums.
   * [ScalingTrigger.h:48–68][scaling-trigger] base `(ScalingTriggerKind::Enum kind_)`;
     [AveragePartitionLoadScalingTrigger.h:73–78][partition-trigger] derived
     `(wstring metricName_, double lowerLoadThreshold_, double upperLoadThreshold_,
     uint32 scaleIntervalInSeconds_)`;
     [AverageServiceLoadScalingTrigger.h:78–84][service-trigger] derived same
     plus `(bool useOnlyPrimaryLoad_)`.
   * [ScalingMechanism.h:44–64][scaling-mechanism] base
     `(ScalingMechanismKind::Enum kind_)`;
     [InstanceCountScalingMechanism.h:60–64][instance-mechanism] derived
     `(int32 maximumInstanceCount_, int32 minimumInstanceCount_, int32 scaleIncrement_)`;
     [AddRemoveIncrementalNamedPartitionScalingMechanism.h:58–62][partition-mechanism]
     derived `(int32 maximumPartitionCount_, int32 minimumPartitionCount_, int32 scaleIncrement_)`.
     Activators/kind/type-information definitions are in these base headers
     and matching `.cpp` files; a selected unsupported polymorphic shape must
     be explicitly unsupported, never silently replaced by an empty list.
6. **ErrorCode**, reply only: [Common/ErrorCode.h:80–96][error] uses
   `FABRIC_PRIMITIVE_FIELDS_01(value_)`: **only ErrorCodeValue::Enum**, **not**
   `message_`, thread ID, read flag or overwrite-check bookkeeping.
   [Common/FabricSerializationHelper.h:4310–4320][serialization] makes it a
   primitive adapter, not an object containing a diagnostic string.
7. **Scalar/container adapters:** TimeSpan uses primitive `(int64 ticks_)`
   ([Common/TimeSpan.h:146,157][timespan]); Guid uses primitive GUID
   ([Common/Guid.h:130][guid]). String Write/Read uses the stream String
   representation with character count
   ([serialization:4923–4958][serialization]), byte vectors use byte array
   (:4968–4994), serializable vectors use array utilities (:183–229,4997–5032),
   maps write count then key/value entries (:4429–4498).
   Shared polymorphic pointers use stream WritePointer/ReadPointer with
   `T::FabricSerializerActivator` (:4613–4697).
   The type information is **raw enum kind bytes of sizeof(kind)**,
   separately from the enum's LONG64 field:
   [ScalingTrigger.cpp:41–79][trigger-implementation],
   [ScalingMechanism.cpp:41–69][mechanism-implementation].
   The native stream implements serializable framing at
   `src/prod/shared/serialization/src/FabricSerializableStream.cpp:320–728`,
   signed 64-bit primitive at :1043–1052 and String at :1130–1147.
   These exact native paths, rather than a parallel handwritten format,
   are the selected-pair native-oracle source requirements.

### Selected-pair defaults, enum values, and error values

* `ReplicaMessageBody()`/`ReplicaReplyMessageBody()` default-construct their
  members ([replica-body:15–16][replica-body],
  [replica-reply:15–16][replica-reply]); they do not carry a private “valid”
  or success-substitution discriminator.
* FT default: zero epochs and target/min=0
  [FailoverUnitDescription.cpp:12–17][ft-description-cpp].
  Epoch default `(0,0)` ([epoch:13–16][epoch]).
  Consistency description default keys=0, empty name, singleton
  ([cu-description:16–19][cu-description]); GUID/ID default constructors
  remain the native defaults.
* Replica default ([ReplicaDescription.cpp:12–24][replica-description-cpp]):
  replicaId=instanceId=**-1**, current/previous roles None, state Ready,
  isUp=true, first/last LSN=**-1**, empty locations, package version
  **ServicePackageVersionInstance::Invalid**. InvalidReplicaId=-1,
  UnknownLSN=-2 ([common/Constants.cpp:37,47][constants]); invalid LSN=-1
  [prebuilt/sdk/FabricTypes.h:94][fabric-types]. Unknown and invalid LSN
  are not interchangeable.
* Role enum [ReplicaRole.h:13–20][role]:
  Unknown=0, None=1, Idle=2, Secondary=3, Primary=4.
  State enum [ReplicaStates.h:12–18][states]:
  StandBy=0, InBuild=1, Ready=2, Dropped=3.
* Service default [ServiceDescription.cpp:27–46][service-description-cpp]:
  scalar counts/instance/update=0, strings/collections empty, stateful,
  persisted, service-group=false, scaleout=0, move cost LOW,
  all three wait durations **TimeSpan::MinValue**, activation SharedProcess.
  Service's default package version is **default-constructed**, unlike
  ReplicaDescription's explicit Invalid version.
* [RolloutVersion.cpp:12–19][rollout-defaults]: default=**1.0**, Invalid=0.0.
  [ApplicationVersion.cpp:12–18][app-defaults],
  [ServicePackageVersion.cpp:15–22][package-defaults],
  [ServicePackageVersionInstance.cpp:15–20][version-defaults]:
  version default propagates 1.0 rollout components; Invalid propagates 0.0,
  instance=0. **Do not collapse all package defaults to Invalid.**
* Error default Success/S_OK=0 ([ErrorCode.cpp:19–25][error-cpp],
  [ErrorCodeValue.h:18][errors]). Internal values are tied to the pinned
  enum sequence, not public SDK error-name guesses:
  `RANotReadyForUse=0x80071d19` (-2147017447),
  `StaleRequest=0x80071d4a` (-2147017398),
  `RAServiceTypeNotRegistered=0x80071d18`,
  `RAProxyCouldNotOpenStatelessService=0x80071d1f`.
  Derivation: FIRST_INTERNAL_ERROR_CODE_MINUS_ONE =
  FABRIC_E_LAST_RESERVED_HRESULT−188, increment through enum
  ([errors:336–338,503–513,553][errors];
  [FabricTypes.h:120][fabric-types] last reserved=0x80071d4b).
  Other genuine open/Hosting failures are propagated rather than translated
  to generic command statuses; these four are not an exhaustive error domain.
* Public partition/correlation/metric/policy enums are the SDK definitions
  [prebuilt/sdk/FabricTypes.h:391–397,538–569,1436–1444][fabric-types]:
  partition Invalid=0/Singleton=1/Int64Range=2/Named=3;
  metric weight Zero=0/Low=1/Medium=2/High=3;
  move cost Zero=0/Low=1/Medium=2/High=3;
  correlation Invalid=0/Affinity=1/AlignedAffinity=2/NonAlignedAffinity=3;
  placement Invalid=0/InvalidDomain=1/RequiredDomain=2/
  PreferredPrimaryDomain=3/RequiredDomainDistribution=4/
  NonPartiallyPlaceService=5.
* Activation [ServicePackageActivationContext.h:10–16][activation-mode]:
  SharedProcess=0, ExclusiveProcess=1. Scaling trigger
  [ScalingTriggerKind.h:12–17][trigger-kind]:
  Invalid=0, AveragePartitionLoad=1, AverageServiceLoad=2. Mechanism
  [ScalingMechanismKind.h:12–17][mechanism-kind]:
  Invalid=0, PartitionInstanceCount=1, AddRemoveIncrementalNamedPartition=2.
* TimeSpan extrema are signed int64 min/max
  ([Common/TimeSpan.cpp:13–14][timespan-implementation]);
  don't replace MinValue with zero or -1.

### Additional schema closure for all other boundary bodies

All fields are in serialized order. Nested scalar enums retain their declared
native values; there is no license to omit fields because they are “FM data”.

| Type | Ordered schema and source |
|---|---|
| ReplicaDeactivationInfo | `(Epoch deactivationEpoch_, int64 catchupLSN_)`; [common/ReplicaDeactivationInfo.h:44,63–64][deactivation-info]. |
| FailoverUnitInfo | `(SD serviceDescription_, FT failoverUnitDescription_, Epoch icEpoch_, bool isReportFromPrimary_, vector<ReplicaInfo> replicas_, bool isLocalReplicaDeleted_)`; [common/FailoverUnitInfo.h:80–94][ft-info]. |
| ReplicaInfo | `(ReplicaDescription replicaDescription_, ReplicaRole::Enum icRole_)`; [common/ReplicaInfo.h:68–72][replica-info]. |
| ServicePackageInfo | `(ServicePackageIdentifier packageId_, ServicePackageVersionInstance versionInstance_)`; [NodeUpMessageBody.h:35–39][nodeup-body]. |
| NodeDescription | `(wstring nodeUpgradeDomainId_, vector<Uri> nodeFaultDomainIds_, map<wstring,wstring> nodePropertyMap_, map<wstring,uint32> nodeCapacityRatioMap_, map<wstring,uint32> nodeCapacityMap_, FabricVersionInstance versionInstance_, wstring nodeName_, wstring nodeType_, wstring ipAddressOrFQDN_, ULONG/u32 clusterConnectionPort_, ushort/u16 httpGatewayPort_)`; [common/NodeDescription.h:75–89][node-description]. |
| FabricVersionInstance | `(FabricVersion version_, uint64 instanceId_)`; [Common/FabricVersionInstance.h:46,52–53][fabric-version-instance]. |
| FabricVersion | `(FabricCodeVersion codeVersion_, FabricConfigVersion configVersion_)`; [Common/FabricVersion.h:50,58–59][fabric-version]. |
| FabricCodeVersion / FabricConfigVersion | `(uint32 majorVersion_, minorVersion_, buildVersion_, hotfixVersion_)` / `(wstring value_)`; [code:55,61–64][fabric-code-version], [config:44,50][fabric-config-version]. |
| ApplicationUpgradeSpecification | `(ApplicationIdentifier applicationId_, ApplicationVersion applicationVersion_, uint64 instanceId_, UpgradeType::Enum upgradeType_, bool isMonitored_, vector<ServicePackageUpgradeSpecification> packageUpgrades_, wstring applicationName_, vector<ServiceTypeRemovalSpecification> removedTypes_, bool isManual_, map<ServicePackageIdentifier,ServicePackageResourceGovernanceDescription> spRGSettings_, map<ServicePackageIdentifier,CodePackageContainersImagesDescription> cpContainersImages_, vector<wstring> networks_)`; [ServiceModel/ApplicationUpgradeSpecification.h:91–103,127–138][app-upgrade]. |
| ServicePackageUpgradeSpecification | `(wstring packageName_, RolloutVersion packageRolloutVersion_, bool isCodePackageInfoIncluded_, vector<wstring> codePackageNames_)`; [ServiceModel/ServicePackageUpgradeSpecification.h:40–47][package-upgrade]. |
| ServiceTypeRemovalSpecification | `(wstring packageName_, RolloutVersion packageRolloutVersion_, set<wstring> typeNames_)`; [ServiceModel/ServiceTypeRemovalSpecification.h:41–46][type-removal]. |
| ServicePackageResourceGovernanceDescription | `(bool IsGoverned, uint32 notUsed_, uint32 MemoryInMB, double cpuCores_)`; [ServiceModel/ServicePackageResourceGovernanceDescription.h:58–69][resource-governance]. `notUsed_` is still serialized. |
| CodePackageContainersImagesDescription | `(vector<wstring> containersImages_)`; [ServiceModel/CodePackageContainersImagesDescription.h:33–36][images-description]. |
| ServiceTypeInfo | `(VersionedServiceTypeIdentifier versionedServiceTypeId_, wstring codePackageName_)`; [common/ServiceTypeInfo.h:34–38][type-info]. Rust `ServiceTypeInfo { versioned, code_package }`. |
| VersionedServiceTypeIdentifier | `(ServiceTypeIdentifier id_, ServicePackageVersionInstance servicePackageVersionInstance_)`; [ServiceModel/VersionedServiceTypeIdentifier.h:36,55–57][versioned-type]. Rust `VersionedServiceType { id: ServiceType, version: VersionInstance }`. |
| ReportLoadInfo | `(FailoverUnitId failoverUnitId_, ReplicaRole::Enum replicaRole_, vector<LoadMetric> loadMetrics_)`; [common/ReportLoadInfo.h:72–78][report-load-info]. `isStateful_` member is **not** serialized. |
| LoadMetric | `(wstring name_, uint32 value_)`; [Reliability/LoadBalancing/LoadMetric.h:36–41][load-metric]. |
| LoadOrMoveCostDescription | `(Guid failoverUnitId_, wstring serviceName_, bool isStateful_, vector<LoadMetricStats> primaryEntries_, vector<LoadMetricStats> secondaryEntries_, map<NodeId,vector<LoadMetricStats>> secondaryEntriesMap_)`; [Reliability/LoadBalancing/LoadOrMoveCostDescription.h:89–104][load-description]. `isReset_` is not serialized. |
| LoadMetricStats | `(wstring name_, uint32 lastReportValue_, uint32 count_, double weightedSum_, double sumOfWeight_, StopwatchTime lastReportTime_)`; [Reliability/LoadBalancing/LoadMetricStats.h:46–57][load-stats]. |
| fm::NodeInfo | `StoreData base + (NodeInstance nodeInstance_, DateTime lastUpdated_, bool isUp_, bool isReplicaUploaded_, bool isNodeHealthReportedDuringDeactivate_, bool isNodeStateRemoved_, NodeDescription nodeDescription_, wstring actualUpgradeDomainId_, uint64 sequenceNumber_, fm::NodeDeactivationInfo deactivationInfo_, int64 healthSequence_, bool isNodeHealthReportedDuringUpgrade_, bool isPendingDeactivateNode_, bool isPendingFabricUpgrade_, vector<ApplicationIdentifier> pendingApplicationUpgrades_, DateTime nodeUpTime_, DateTime nodeDownTime_)`; [fm/NodeInfo.h:41–58,191–217][node-info]. DisabledPartitions/stale-cache members are not in the 17-field list. |
| fm::NodeDeactivationInfo | `(NodeDeactivationIntent::Enum intent_, NodeDeactivationStatus::Enum status_, vector<wstring> batchIds_, int64 sequenceNumber_, vector<NodeDeactivationIntent::Enum> intents_, DateTime startTime_)`; [fm/NodeDeactivationInfo.h:27–33,114–128][node-deactivation-info]. Pending safety-check wrappers are not serialized here. |
| fm::FailoverUnit | `StoreData base + (FT failoverUnitDesc_, wstring serviceName_, FailoverUnitFlags::Flags flags_, int64 lookupVersion_, DateTime lastUpdated_, vector<fm::Replica> replicas_, int64 updateVersion_, int64 healthSequence_, FailoverUnitHealthState::Enum currentHealthState_, wstring extraDescription_, DateTime reconfigurationStartTime_, DateTime placementStartTime_, int64 lastQuorumLossTime_)`; [fm/FailoverUnit.h:279–292,423–474][fm-ft]. |
| fm::Replica | `(ReplicaDescription replicaDesc_, ReplicaFlags::Enum flags_, DateTime lastUpdated_, uint64 serviceUpdateVersion_, int64 lastBuildTime_, DateTime lastUpDownTime_)`; [fm/Replica.h:215,294–308][fm-replica-h]. |
| fm::StoreData | Base derives FabricSerializable but declares **no FABRIC_FIELDS**; operationLSN and persistenceState are not an inherited on-wire payload; [fm/StoreData.h:12–37][store-data]. |
| Common::Uri | `(UriType::Enum type_, wstring scheme_, wstring authority_, UriHostType::Enum hostType_, wstring host_, int32 port_, wstring path_, wstring query_, wstring fragment_, vector<wstring> pathSegments_)`; [Common/Uri.h:87,110–119][uri]. UriType AuthorityAbEmpty=0/Absolute=1/Rootless=2/Empty=3; UriHostType None=0/IPv4=1/IPv6=2/RegName=3 (:13–20). Node fault domains are full Uri objects, not a lone URI string. |
| Common::ActivityId | `(Guid guid_, uint64 index_)`; [Common/ActivityId.h:69–73][activity-id]. Applies to optional FabricActivityHeader. |
| TimeSpan / DateTime / StopwatchTime | Primitive int64 ticks, no object wrapper; [TimeSpan.h:146–157][timespan], [DateTime.h:77–85][datetime], [StopwatchTime.h:51–60][stopwatch]. |
| FailoverUnitFlags::Flags | Primitive `(int32 value_)`, not LONG64 enum; [common/FailoverUnitFlags.h:60–64][ft-flags]. Flags None=0/Stateful=1/Persisted=2/NoData=4/ToBeDeleted=8/SwappingPrimary=16/Orphaned=32/Upgrading=64 (:16–26). |
| PartitionId | `typedef FailoverUnitId PartitionId`; [ServiceModel/reliability/failover/PartitionId.h:10][partition-id], therefore its Guid schema above, not an assumed bare Guid. |

Additional enum schema references:

* [NodeDeactivationIntent.h:12–19,31][deactivation-intent]:
  None=0, Pause=1, Restart=2, RemoveData=3, RemoveNode=4; explicitly blittable
  for vectors.
* [NodeDeactivationStatus.h:14–21,33][deactivation-status]:
  SafetyCheckInProgress=0, SafetyCheckComplete=1, DeactivationComplete=2,
  ActivationInProgress=3, None=4.
* [common/ReplicaFlags.h:12–27][replica-flags]: integer bitmask enum,
  None=0 through ToBeDroppedForNodeDeactivation=0x1000, with each native
  bit named there (retained GFUM field).
* [fm/FailoverUnitHealthState.h:14–29][ft-health]: native values 0–12,
  Invalid through PlacementStuckAtZeroReplicaCount (retained GFUM field).
* [ServiceModel/UpgradeType.h:12–20][upgrade-type]:
  Invalid=0, Rolling=1, Rolling_ForceRestart=2, Rolling_NotificationOnly=3.

Scalar adapters, aliases and enum declarations are in the pinned definitions
referenced above. Detailed scalar widths/SDK values and platform-specific
primitive representation are conformance inputs, not evidence that native
code ran. See the selected-pair section for the precise nested types that the
first codec must cover.

## Exact selected stateless entity requirements

### Native AddInstance decision flow

1. The source FM creates AddInstance only for a **stateless** FT replica in
   **InBuild**, after deletion/to-be-dropped branches; PendingTask :180–201.
   AddInstanceAction targets that replica's FederationNodeInstance and uses
   `CreateReplicaMessageBody(true)`. This selects the service package **target
   version for that node upgrade domain**, and the 4-argument body constructor
   overrides **both** service and replica package versions
   ([Replica.cpp:632–648][fm-replica], [ReplicaMessageBody.h:27–36][replica-body]).
2. RA must be open, owner-ready (post-NodeUpAck), decode the **whole body**,
   validate generation and FT staleness before execution. An empty map entry
   is permitted by FC but not durable state or runtime success.
3. [RA:4313–4345][ra-main] rejects fabric-upgrading node **silently** (no error
   reply), and owner-deactivated node with **RANotReadyForUse** echoing incoming
   FT/replica. Native tests [AddFailoverUnitChecks:95–115][add-check-tests].
4. Create missing FT from incoming FT ID/consistency description/service;
   update service description when valid; mark upload knowledge
   ([RA:1658–1662,4279–4293][ra-main]). A bounded slice cannot skip identity/
   epoch/service-version checks merely because no Hosting adapter exists.
5. If local FT **Open**:
   * incoming replica ID greater than local: start Drop close of old replica,
     **no AddInstance success**;
   * same instance: if local **Ready**, enqueue Success reply using local
     FT/replica; if not ready, no success; return;
   * other cases do not gain a newly invented success path.
6. If local FT **Closed**:
   * incoming instance greater: require local role None; create role None with
     default ReplicaDeactivationInfo, start native open retry;
   * equal instance: send dropped-state notification, not success;
   * incoming instance older: send dropped-state notification **and**
     AddInstanceReply **StaleRequest**, with current FT and requested replica.
   Entire flow [RA:1664–1724][ra-main].
7. [RA:750–773][ra-main] calls EnableUpdate and native StartCreateLocalReplica.
   Role None is explicitly stateless (test-assert not stateful)
   [FT:300–305][ft]. Open retry requires readiness and resolves registration
   before deferred RAP request [RA:850–879][ra-main],
   [FT:5143–5194][ft]. RAServiceTypeNotRegistered means **continue retry**,
   not immediate successful instance or generic failed request.
8. Native success sets local state **Ready** for stateless and clears open
   pending after actual RAP open success
   [FT:5250–5335][ft]. FinishOpenLocalReplica selects AddInstanceReply for
   stateless [RA:883–902][ra-main]. The bounded foundation has no RAP/Hosting
   completion, therefore must not turn its own store commit into this success.
   Fatal open errors propagate the native error path; retry-threshold behavior
   is at [FT:5209–5279][ft], not a fixed “any error is StaleRequest” rule.
9. FM receives ReplicaReplyMessageBody, ignores if replica missing/not
   InBuild/not Up; otherwise CompleteBuildReplica. Success records endpoints,
   package version and Ready; failure marks dropped
   [fm-processor:22–43,92–136][fm-processor]. An AddInstanceReply is not merely
   an operation-ticket disposition.

### Native entity execution and tests

* Map get-or-create allocates an empty entry without persistence
  [Infrastructure.EntityMap.h:51–84][entity-map].
* Private write-state and commit description are handled through the locked
  entity; execute list acquires entity lock, processes handlers, then starts
  commit and only after EndCommit invokes FinishProcess/actions
  [EntityJobItemListExecutorAsyncOperation.h:48–163][entity-executor].
  Specifically [Infrastructure.EntityState.h:84–107][entity-state] clones the
  current snapshot for write lock acquisition; committed readers retain the
  published snapshot. [Infrastructure.LockedEntityPtr.h:171–181][locked-entity]
  places the proposed snapshot in the commit description. Durable update
  cannot be downgraded by a subsequent EnableInMemoryUpdate (:185–209).
* Map's durable path serializes proposed state, awaits entry commit, then
  calls `UpdateMapOnCommitComplete(error.IsSuccess())`
  [entity-map:228–286][entity-map]. On failure that function returns without
  replacing state; on success it deletes/sets committed data
  (:339–369). Memory-only path skips store and updates the map (:230–236).
  In-memory inserts are forbidden (:178).
* Job actions run only if handler was not dropped and commit succeeded;
  otherwise they are cancelled. Native queue actions execute **while the
  entity lock is held**, justified by native set-membership ordering; this is
  not permission for arbitrary callbacks under the Rust store lock.
  [Infrastructure.EntityJobItem.h:232–292][entity-job].
* Commit failures can cause fail-fast for job descriptions that require it,
  excluding RAStoreNotUsable/ObjectClosed/NotPrimary
  [entity-job:295–314][entity-job]. Do not claim every native failure is merely
  a returned error, or every native error means an unknown commit outcome.
* [Infrastructure.StateMachineActionQueue.cpp:23–56][action-queue] prevents
  consuming a queue twice and separates PerformAction/CancelAction.
  This supports executor-local at-most-once dispatch, **not** network exactly-once.

Focused source test anchors:

| Invariant | Native tests |
|---|---|
| Empty map allocation not persistence; failed insertion stays Created | [Test.Unit.Infrastructure.EntityMap.cpp:512–535][entity-map-tests]. |
| Failed delete stays Inserted; failed update keeps old values | [EntityMap tests:564–589][entity-map-tests]. |
| Explicit in-memory update changes memory; does not rewrite persisted field | [EntityMap tests:591–598,1019–1025][entity-map-tests]. |
| Read lock retains old snapshot while write work changes/commits | [EntityMap tests:1193–1215][entity-map-tests]. |
| Single writer violation asserts | [EntityMap tests:1175–1182][entity-map-tests]. |
| Commit failure cancels action queue | [Test.Unit.Infrastructure.EntityJobItem.cpp:474–490][entity-job-tests]. |
| Queue execution still holds entity lock | [EntityJobItem tests:492–506][entity-job-tests]. |
| Work completion occurs after lock release, even failure path completes work | [EntityJobItem tests:532–542,555–571][entity-job-tests]. |
| New same-entity work remains unscheduled until current entity lock released | [Test.Unit.Infrastructure.EntityScheduler.cpp:249–282][scheduler-tests]. |
| Fail-fast policy and closing/not-primary exclusions | [EntityJobItem tests:443–472][entity-job-tests]. |
| Selected request stale epoch, incarnation, closed older instance, open/close transition | [Test.StateMachine.cpp:3552–3605][ra-tests]. |
| Selected request node-not-ready no reply | [Test.StateMachine.cpp:5529–5538][ra-tests]. |

**Evidence boundary:** these native tests were read, not executed.
Unknown store completion, caller cancellation ownership, retaining an
operation ticket/fence until resolution, and eventual progress after actual
outcome are **the foundation's Rust execution requirements**, tested by
[the selected entity barriers](../../crates/libs/fube-ra/tests/entity.rs), not
claims established by a similarly named native test. The native definite
commit-result publication/effect ordering above is the source grounding.

## Whole-registry exclusions, aliases, and native endpoint gaps

The accompanying CSV lists all 119 initializers with line, actual actor,
classification and evidence key. Its 53 nonboundary entries reconcile as:

* `ChangeNotification`, `ChangeNotificationAck`, `ChangeNotificationChallenge`:
  common token-range update operation ([ChangeNotificationOperation.cpp:20–77][change-operation])
  ↔ [FM:1914–1968][fm]; not RA replica lifecycle.
* Create/Update/UpdateSystem/DeleteService and application create/delete/update:
  ServiceAdminClient/common request operations ↔ FM common queue
  ([ServiceAdminClient.cpp:380–529][admin], [fm-queue:106–141][fm-queue]).
  Reply action/actor asymmetries are preserved: DeleteApplicationReply,
  CreateApplicationReply and UpdateApplicationReply use **common-to-FM
  factories even when actually returned as correlated FM replies**.
* ServiceTableRequest/ServiceTableUpdate: resolver request/broadcast
  ([ServiceResolverImpl.cpp:72][resolver], [FM:2221–2256][fm],
  [fm/FMServiceLookupTable.cpp:224][lookup]); actor ServiceResolver for update,
  not a peer RA configuration.
* DeactivateNode/ActivateNode, DeactivateNodes, NodeDeactivationStatus,
  RemoveNodeDeactivation, NodeStateRemoved, RecoverPartitions/Partition/
  ServicePartitions/SystemPartitions: common **administrative** operations,
  not NodeActivate/DeactivateRequest RA state-machine exchanges. FM
  dispatch [FM:1089–1124][fm], processing :1161–1573; callers
  [admin:220–285,805–862,1290–1294,1546–1561][admin].
* QueryRequest/QueryReply: common FM query and separately a **client→RA**
  query path, which reuses action but replaces routing actor; actual RA
  dispatch [handler:411–414][handler] and responses
  [Query.QueryUtility.cpp:50–76][query-utility]. Not RA↔FM protocol merely
  because both implement the same action name.
* ReportFaultRequest/Reply: actual actions **ClientReportFaultRequest /
  ClientReportFaultReply**, actors RA/RS, client→RA request-response
  [handler:415–418][handler], [RA:1592][ra-main].
  Not RAP's local RAReportFault nor RA→FM DatalossReport.
* ApplicationUpgradeRequest/Reply are wire actions **UpgradeRequest /
  UpgradeReply**, common administration ↔ FM
  [fm-queue:122–125][fm-queue], [FM:1694–1736][fm].
  FabricUpgradeRequest/Reply similarly administrative; distinct
  from NodeFabricUpgradeRequest/Reply.
* ServiceDescriptionRequest/Reply, ResetPartitionLoad/Reply,
  ToggleVerboseServicePlacementHealthReporting/Reply: common administrative
  operations ([admin:986–1102][admin], [FM:1129–1139,1634–1670,2861–2919][fm]).
  Native ResetPartitionLoad and Toggle handlers actually build replies
  using the **request** factory at :2903 and :2917; their separate Reply
  definitions have no production references in the reliability tree.
* AvailableContainerImages: common ReliabilitySubsystem→FM image reporting,
  [RS:528–542][rs] → [FM:1042–1044,2957–end][fm];
  body [AvailableContainerImagesMessageBody.h:37–41][images-body]
  `(wstring nodeId_, vector<wstring> availableImages_)`.
  This is explicit common adjunct reporting, not an omitted RA handler.
* `DropReplica`/`DropReplicaReply`: only static declarations
  [RSMessage.h:333–334][registry-h]. No initializer or accessor; actual
  [DropReplicaAction at fm-send:193–199][fm-send] sends **DeleteReplica**.
  Do not mint two new wire actions from header declarations.
* PartitionNotification's missing FM endpoint and retained ReplicaDown/
  ReplicaDropped receive-only remnants are documented above. Only
  ChangeConfigurationReply is explicitly `Deprecated` in RA metadata.

Replacing Federation delivery changes none of the required 66 action
contracts, the peer continuation direction, generation domains, body fields,
or semantic retry completion. The native `#pragma region Federation messages`
label in metadata denotes a transport path; it does not remove reliability
ownership of GetLSN/CreateReplica/Deactivate/Activate.

## Native documentation and verification provenance

Native reference docs are plain Markdown: [root README.md:5–6,40–105][native-readme],
`docs/README.md:1` calls docs a placeholder; architecture navigation is
`docs/architecture/README.md`. README documents `./runbuild.sh` (Docker-based
full native build) and approximately 70 GB build space. RA production target
is `objects_FailoverRA`, test subdirectory at
[ra/CMakeLists.txt:192–195][ra-cmake]. The Boost executable uses
`${exe_FailoverRA.Test}` and explicitly includes EntityMap/EntityJobItem tests
([ra/test/CMakeLists.txt:8–10,72–75][ra-test-cmake]). These facts are not a
request to build the full reference tree.

The inventory's native research did not execute full RA/FM native tests.
The later [codec producer/consumer](../../tests/ra_protocol/README.md) compiles
and executes only the native serialization subset, with disclosed schema and
platform adapters. No full SF tree build, running FM/RA or publication is claimed.

## Explicit remaining evidence limits

1. Source inventory/classification is complete; **protocol implementation is
   not**. C1/C2 have the bounded mappings above; all other boundary codecs/handlers/
   traces and the rest of C1/C2 lifecycle/runtime behavior remain staged obligations.
2. PartitionNotification has an actual RA sender and RA reply handler, but no
   evidenced FM receiver/reply producer at the pinned revision. Native RA
   synthetic tests do not establish this missing endpoint.
3. Base-only closed GetLSNReply versus derived reply deserialization needs
   executed conformance evidence before a byte-level interoperability claim.
4. The selected pair now has executed native-produced inputs and native decoder
   field comparisons through the source-baseline stream/schema-adapter oracle,
   including nonempty/inherited scaling and malformed inputs. Full original
   production C++ RA classes/handlers and independently pinned deployed-binary
   interoperability remain unverified, separate evidence gates.

[registry]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RSMessage.cpp#L32-L250
[registry-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RSMessage.h
[actor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/Actor.h
[actor-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ActorHeader.h
[action-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/ActionHeader.h
[priority-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/HighPriorityHeader.h
[timeout-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/TimeoutHeader.h
[activity-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/FabricActivityHeader.h
[header-ids]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Transport/MessageHeaderId.h
[generation-header]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/GenerationHeader.h#L11-L42
[generation-number]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/GenerationNumber.h
[federation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/FederationWrapper.cpp
[fm-transport]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Communication.FMTransport.h#L45-L63
[fm-send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/SendMessageAction.cpp#L28-L47
[send-actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.SendMessageActions.h#L53-L88
[message-utility]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.MessageUtility.h
[handler]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/MessageHandler.cpp#L44-L93
[metadata]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Communication.MessageMetadataTable.cpp#L21-L85
[metadata-type]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Communication.MessageMetadata.h
[message-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/MessageContext.h#L96-L153
[traits]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/infrastructure.FailoverUnitEntityTraits.cpp#L135-L283
[fm]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverManager.cpp
[fm-task]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnitMessageTask.cpp
[ra-main]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L1639-L1724
[fm-replica]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/Replica.cpp
[fm-processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnitMessageProcessor.cpp
[pending]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/PendingTask.cpp
[ft]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/FailoverUnit.cpp
[busy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnitJobQueue.cpp
[fm-builder]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Communication.FMMessageBuilder.cpp
[upload-processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.PendingReplicaUploadStateProcessor.cpp
[fm-queue]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/CommonJobQueue.cpp
[replicas-update]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/ReplicasUpdateOperation.cpp
[down-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/ReplicaDownOperation.cpp
[rs]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ReliabilitySubsystem.cpp
[nodeup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpOperation.cpp
[nodeup-processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/NodeUpAckProcessor.cpp
[send-receive]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/SendReceiveToFMOperation.cpp
[rebuild]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/RebuildContext.cpp
[generation-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/GenerationStateManager.cpp
[lfum-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RALFUMUploadOperation.cpp
[service-update]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.ServiceTypeUpdateProcessor.cpp
[activation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.NodeDeactivationMessageProcessor.cpp
[activation-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.NodeDeactivationState.cpp
[node-cache]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/NodeCache.cpp
[deactivate-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/DeactivateNodesContext.cpp
[update-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/UpdateServiceContext.cpp
[upgrade]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/Upgrade.cpp
[upgrade-processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Upgrade.UpgradeMessageProcessor.cpp
[application-upgrade-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/ApplicationUpgradeContext.cpp
[fabric-upgrade-context]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FabricUpgradeContext.cpp
[serialization]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricSerializationHelper.h
[replica-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaMessageBody.h#L15-L56
[replica-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaReplyMessageBody.h#L15-L43
[delete-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/DeleteReplicaMessageBody.h
[ra-replica-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/RAReplicaMessageBody.h
[config-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ConfigurationMessageBody.h
[config-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ConfigurationReplyMessageBody.h
[doreconfig-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/DoReconfigurationMessageBody.h#L10-L45
[activate-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ActivateMessageBody.h
[deactivate-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/DeactivateMessageBody.h
[getlsn-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/GetLSNReplyMessageBody.h
[ft-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitMessageBody.h
[ft-reply]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitReplyMessageBody.h
[replicaup-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaUpMessageBody.h#L56-L76
[replicalist-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaListMessageBody.h
[nodeup-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpMessageBody.h
[nodeupack-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpAckMessageBody.h
[proposal-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/GenerationProposalReplyMessageBody.h
[genupdate-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/GenerationUpdateMessageBody.h
[lfum-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/LFUMMessageBody.h
[gfum-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/GFUMMessageBody.h
[type-update-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ServiceTypeUpdateMessageBody.h
[type-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ServiceTypeUpdateReplyMessageBody.h
[type-notification-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ServiceTypeNotificationRequestMessageBody.h
[type-notification-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ServiceTypeNotificationReplyMessageBody.h
[partition-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/PartitionNotificationMessageBody.h
[nodeactivate-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeActivateRequestMessageBody.h
[nodedeactivate-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeDeactivateRequestMessageBody.h
[nodeactivate-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeActivateReplyMessageBody.h
[nodedeactivate-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeDeactivateReplyMessageBody.h
[service-request-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpdateServiceRequestMessageBody.h
[service-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpdateServiceReplyMessageBody.h
[upgrade-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/UpgradeDescription.h
[upgrade-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeUpgradeReplyMessageBody.h
[fabric-upgrade-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/FabricUpgradeSpecification.h
[fabric-upgrade-reply-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeFabricUpgradeReplyMessageBody.h
[load-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReportLoadMessageBody.h
[ft-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitDescription.h#L71-L80
[ft-description-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitDescription.cpp
[ft-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/FailoverUnitId.h
[cu-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ConsistencyUnitDescription.h
[cu-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ConsistencyUnitId.h
[epoch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/Epoch.h
[replica-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaDescription.h#L119-L164
[replica-description-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaDescription.cpp
[role]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaRole.h
[states]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaStates.h
[node-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/federation/NodeInstance.h
[node-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/federation/NodeId.h
[large-integer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/LargeInteger.h
[service-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceDescription.h#L200-L254
[service-description-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceDescription.cpp
[service-type-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceTypeIdentifier.h
[package-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageIdentifier.h
[app-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationIdentifier.h
[package-version-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageVersionInstance.h
[package-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageVersion.h
[app-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationVersion.h
[rollout-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/RolloutVersion.h
[correlation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceCorrelationDescription.h
[metric]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceLoadMetricDescription.h
[placement]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePlacementPolicyDescription.h
[scaling-policy]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ServiceScalingPolicyDescription.h
[scaling-trigger]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingTrigger.h
[partition-trigger]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/AveragePartitionLoadScalingTrigger.h
[service-trigger]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/AverageServiceLoadScalingTrigger.h
[scaling-mechanism]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingMechanism.h
[instance-mechanism]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/InstanceCountScalingMechanism.h
[partition-mechanism]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/AddRemoveIncrementalNamedPartitionScalingMechanism.h
[error]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ErrorCode.h#L80-L96
[error-cpp]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ErrorCode.cpp
[errors]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ErrorCodeValue.h
[timespan]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/TimeSpan.h
[guid]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Guid.h
[constants]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/Constants.cpp
[fabric-types]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/prebuilt/sdk/FabricTypes.h
[rollout-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/RolloutVersion.cpp
[app-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationVersion.cpp
[package-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageVersion.cpp
[version-defaults]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageVersionInstance.cpp
[deactivation-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaDeactivationInfo.h
[ft-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitInfo.h
[replica-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaInfo.h
[node-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/NodeDescription.h
[fabric-version-instance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricVersionInstance.h
[fabric-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricVersion.h
[fabric-code-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricCodeVersion.h
[fabric-config-version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/FabricConfigVersion.h
[app-upgrade]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationUpgradeSpecification.h
[package-upgrade]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageUpgradeSpecification.h
[type-removal]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServiceTypeRemovalSpecification.h
[resource-governance]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageResourceGovernanceDescription.h
[images-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/CodePackageContainersImagesDescription.h
[type-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ServiceTypeInfo.h
[versioned-type]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/VersionedServiceTypeIdentifier.h
[report-load-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReportLoadInfo.h
[load-metric]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/LoadBalancing/LoadMetric.h
[load-description]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/LoadBalancing/LoadOrMoveCostDescription.h
[load-stats]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/LoadBalancing/LoadMetricStats.h
[node-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/NodeInfo.h
[node-deactivation-info]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/NodeDeactivationInfo.h
[fm-ft]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnit.h
[fm-replica-h]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/Replica.h
[store-data]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/StoreData.h
[entity-map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityMap.h#L228-L286
[entity-executor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItemListExecutorAsyncOperation.h
[entity-job]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h
[action-queue]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.StateMachineActionQueue.cpp
[entity-map-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.Infrastructure.EntityMap.cpp
[entity-job-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.Infrastructure.EntityJobItem.cpp#L443-L571
[ra-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.cpp
[add-check-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.AddFailoverUnitChecks.cpp
[fm-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/StateMachineTask.Test.cpp
[delete-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.DeleteReplica.cpp
[lifecycle-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ReplicaLifecycle.cpp
[rebuild-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.Rebuild.cpp
[doreconfig-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.DoReconfiguration.cpp
[reconfiguration-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.Reconfiguration.cpp
[close-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.Close.cpp
[replicaup-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ReplicaUpReply.cpp
[last-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.LastReplicaUp.cpp
[ack-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ReplicaUploadAcknowledgement.cpp
[endpoint-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.EndpointPublish.cpp
[nodeup-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.NodeUpAckProcessing.cpp
[nodeup-factory-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.NodeUpOperationFactory.cpp
[service-type-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ServiceTypeEnabledDisabled.cpp
[service-notification-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ServiceTypeNotification.cpp
[partition-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.PartitionEnabledDisabled.cpp
[activation-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.NodeActivationDeactivation.cpp
[update-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.UpdateserviceDescription.cpp
[upgrade-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.UpgradeMessageProcessor.cpp
[cancel-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.CancelUpgradeContext.cpp
[fabric-check-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.Upgrade.FabricUpgradeStalenessChecker.cpp
[load-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.ResourceMonitorProcessing.cpp
[change-operation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ChangeNotificationOperation.cpp
[admin]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ServiceAdminClient.cpp
[resolver]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ServiceResolverImpl.cpp
[lookup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FMServiceLookupTable.cpp
[query-utility]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Query.QueryUtility.cpp
[images-body]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/AvailableContainerImagesMessageBody.h
[native-readme]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/README.md
[ra-cmake]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/CMakeLists.txt
[ra-test-cmake]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/test/CMakeLists.txt
[getlsn-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.GetLsn.cpp
[getlsn-reply-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.GetLSNReply.cpp
[trigger-implementation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingTrigger.cpp#L41-L79
[mechanism-implementation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingMechanism.cpp#L41-L69
[activation-mode]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageActivationContext.h#L10-L16
[trigger-kind]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingTriggerKind.h#L12-L17
[mechanism-kind]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/ScalingMechanismKind.h#L12-L17
[timespan-implementation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/TimeSpan.cpp#L13-L14
[uri]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Uri.h#L87
[activity-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/ActivityId.h#L69-L73
[datetime]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/DateTime.h#L77-L85
[stopwatch]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/StopwatchTime.h#L51-L60
[ft-flags]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/FailoverUnitFlags.h#L16-L64
[partition-id]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/PartitionId.h#L10
[entity-state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityState.h#L84-L107
[locked-entity]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.LockedEntityPtr.h#L171-L209
[scheduler-tests]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.Infrastructure.EntityScheduler.cpp#L249-L282
[deactivation-intent]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/NodeDeactivationIntent.h#L12-L31
[deactivation-status]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/reliability/failover/NodeDeactivationStatus.h#L14-L33
[replica-flags]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/common/ReplicaFlags.h#L12-L27
[ft-health]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/fm/FailoverUnitHealthState.h#L14-L29
[upgrade-type]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/UpgradeType.h#L12-L20
