# Volatile data qualification fixture

The existing executable, service type and default empty/stateless profiles are
unchanged. Create a stateful service with initialization bytes **exactly**
`fube-data-v1` to select this profile for that replica's lifetime. Configuration
refresh does not change it. This is a trusted-network, bounded, volatile test
fixture, not a durable store or native interoperability qualification.

Current integration uses canonical synchronous fallible factories/registration,
object-safe partitions and `OpenMode::New`, split common/primary controls and
one `fube_data::FabricReplicator` with a state facade. Provider getters/copy
factories are sync; enumeration/epoch/data-loss callbacks and lifecycle/build/
catch-up are async. No old factories/contexts/Engine adapters or compatibility
aliases remain. See the [API alignment and provenance matrix](../../../../docs/runtime/SfApiAlignment.md)
and [migration guide](../../../../docs/Migration.md#breaking-runtime-and-data-api-migration).
The [actual main](main.rs#L628-L672) reserves execution before moving factories,
awaits owned registration, later awaits Runtime shutdown **before** final shared
RPC/peer-listener cleanup. Application-owned children/listeners are not
automatically adopted by the runtime.

The published endpoint is `/data/<unique-owner-path>/service`. Its siblings:

- `service`: authoritative ready-primary read; native lifetime, roles, epoch
  and read access are checked independently of engine readiness.
- `diagnostics`: lifetime-only observation, including on secondaries; not an
  authoritative read.
- `write?id=<id>&key=<key>&value=<value>`: identified mutation on the authorized
  ready primary. GET/query encoding follows strict UTF-8 percent decoding
  (`+` means space). Duplicate/unknown parameters and malformed encoding fail.
- `test?ack_delay_ms=<0..2000>`: **qualification-only**, delays actual
  application ACKs after provider apply, never sends receipt-only apply ACKs.
  Set `0` to reset. This route needs replica lifetime, not Primary access.
- `test?copy_delay_ms=<0..100>`: qualification-only provider enumeration pacing,
  under unchanged engine callback deadlines; set `0` to reset. Exactly one
  hook parameter is permitted. It does not delay native control or replace TCP.

Requests retain the existing 4 KiB total HTTP-header bound (including URL).
Decoded IDs are 1–128 bytes, keys 1–1024 bytes; values must fit that request.
The common `MemoryProvider` mutation/copy payload is used without a COM bridge.
Its 4096-operation/16 MiB history/ledger rejects new identities at capacity;
identical retries return the original sequence without new admission.

Write JSON: `outcome` (`committed`, `ambiguous`, `rejected`), `request_id`,
`duplicate`, and `sequence` **only if accepted**; optional `error`.
HTTP statuses are respectively 200, 202, 503; malformed operations/queries use
400, absent owner routes 404. Accepted writes are applied locally before waiting
up to five seconds for quorum; timeout/cancellation is ambiguous, never undo.
An identical retry of a locally applied but uncommitted write stays ambiguous.
The standard-named provider last-committed getter therefore means local applied
history, not quorum/disk commitment. Admission/local apply stays owned across
HTTP disconnect; pre-admission rejection consumes no LSN. Both destination copy
and replication pumps apply then explicitly ACK once; provider payload
enumeration has no ACK, Some(empty) is not None EOS.

Read/diagnostic JSON includes `profile:"fube-data-v1"`, `engine_kind:"rust"`,
`partition`, `replica`, native integer `incarnation`, distinct GUID
`engine_incarnation`, `node`, `host`, `pid`, `service_address`,
`replication_address`, `native_epoch:{data_loss_number,configuration_number}`,
`read_status`, `write_status`, `state` (the common `MemoryProvider.snapshot()`:
`last`, `copy_in_progress`, `values`, ID-to-sequence `results`, `history_count`,
`retained_bytes`, `epoch_data_loss`, `epoch_configuration`), `engine`
(`extensions::EngineSnapshot`, including role/progress/copy_done/ready/fault), native action
counters and `packages`. Additional role, creation-generation, ACK-delay and
pump-fault evidence is diagnostic only. Fabric TCP replication uses a separate
per-replica `node-IP:0` listener, never these HTTP routes.
Rust diagnostics expose actual received `observed_primary` and configured peer
addresses/incarnations for independent wire/identity correlation.
