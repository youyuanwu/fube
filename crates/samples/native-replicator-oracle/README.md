# Native classic replicator oracle — Phase 1 foundation

This **test-only, native-backed dependency profile** is not a production Rust
runtime or a working native replication deployment. Its executable runs local
COM ABI checks without loading native libraries, registering a service, or
allocating cluster resources:

```sh
cargo check --locked -p native-replicator-oracle --all-targets
cargo test --locked -p native-replicator-oracle
cargo run --locked -p native-replicator-oracle -- --abi-self-test
```

The fixture uses `windows_core::implement` through the existing `mssf-pal` 0.10
alias, with the published `IFabricOperationData_Impl` for owned segments.
`windows-interface` 0.100 (already used by `mssf-pal`'s `windows-core` 0.100)
generates three local nullable interface declarations with the **published IIDs
and method layouts**: context callback, data-stream EOS and volatile provider
copy context. There is no local IUnknown, allocation, vtable construction or
reference-count implementation. Tests also declare a nullable operation-stream
fixture; actual native operation streams still use the published interface.

These adapters preserve
successful **S_OK/null EOS**, empty non-null operations, zero-length segments,
COM identity and reference ownership. Null is represented only by `Option` or
raw ABI pointers. Do not call the generated non-null `EndGetNext`,
`EndGetOperation`, or `GetCopyContext` projections for nullable results; use the
local helpers instead. Returned native operations still require explicit
`Acknowledge` **after** successful application, never merely after receipt.

Enumeration and fixed-provider callbacks complete synchronously, publish their
result before invoking a reentrant callback, and allow only one outstanding
Begin/End per object. A foreign context, double End, overlapping Begin, invalid
output pointer, or unsupported snapshot boundary fails explicitly. Callbacks
are optional; completed-operation cancellation cannot undo the result.
Unconsumed contexts and data are released with their owner.

The context follows the typed bridge ownership pattern, but deliberately does
not use `mssf_core::sync::BridgeContext`: the published 0.10 helper requires a
non-null callback and asynchronous executor, reports asynchronous completion,
and assumes the caller supplies its exact concrete context type to End. This
synchronous fixture instead accepts absent callbacks and validates foreign and
already-consumed contexts before taking a result. It never downcasts a foreign
context or constructs a null value of a non-null interface type. Nullable ABI
methods validate output pointers; the generated `IFabricOperationData` thunk,
like the published bindings, requires a valid descriptor-output pointer.

The immutable provider exposes one exact snapshot boundary, unchanged data-loss
responses, null volatile copy context and monotonic epoch recording. It rejects
epoch boundaries requiring reconciliation and cannot apply writes, reconcile
tails, build a versioned identity ledger or copy another boundary. Application
payloads are opaque segments at this phase, not a qualified shared-map format.
Each operation is limited to 1024 segments/1 MiB and enumerations to 1024 items.

`create_native_replicator` compiles the real partition `CreateReplicator` route,
sets **both** RequireServiceAck and its flag, and retains both returned native
interfaces. It requires an actual node-supplied partition. There is deliberately
no synthetic partition or fake native replication success. Native factory,
replica Open/EndOpen registration, owned asynchronous pumps/disposal and real
native peer qualification remain later-phase work. The optional private
`IFabricDisposable` interface is not advertised.

The registry bindings load FabricRuntime/Common/Client dynamically through
`mssf-core`; an empty ELF dependency list does not prove native freedom.
Production `fube-data` and the normal runtime must not depend on this crate.

ABI source guidance: Microsoft Service Fabric classic
`ComProxyAsyncEnumOperation.cpp` (`EndGetNext` null EOS),
`ComOperationStream.GetOperationOperation.cpp` (nullable operation EOS),
and the published `IFabricOperationDataStream`, `IFabricOperationStream`,
`IFabricStateProvider` and `FABRIC_OPERATION_DATA_BUFFER` declarations.
Microsoft copyright and MIT attribution are retained in the source files;
see the repository root [Microsoft MIT notice](../../../LICENSE.service-fabric-apps-rs)
and [LICENSE](../../../LICENSE).

Typed bridge reference: `service-fabric-apps-rs` commit
`16914836baaae1641275de8eb92d945cd8b923a3`,
`crates/libs/mssf-ext/src/{state_provider,stream,data,operation}.rs`.
Unlike that reference's stream EOS conversion, this fixture never creates an
interface from a null pointer. Unlike its copy-state bridge, a null volatile
copy-context input is handled without `unwrap`.
