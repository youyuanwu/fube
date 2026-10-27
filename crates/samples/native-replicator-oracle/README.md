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

The fixture uses `windows_core::implement` through the locked published
`mssf-pal` 0.10 alias, with `IFabricStateProvider_Impl`,
`IFabricAsyncOperationContext_Impl` and `IFabricOperationData_Impl`.
Provider/context duplicate declarations have been removed. The partition,
state replicator, operation data, stream consumers and async context consumers
all use published interfaces. There is no handwritten IUnknown, allocation or
reference-count implementation.

The user explicitly authorized one minimal exception on 2026-10-27: a local
**macro-generated nullable stream adapter** for the native S_OK/null EOS ABI.
The two-method data-stream declaration and analogous test-only operation-stream
fixture are confined here. All provider/context/data definitions reuse published
interfaces. This is not a general COM framework or a production API.

These retained adapters preserve **S_OK/null EOS**, empty non-null operations,
zero-length segments, COM identity and reference ownership. Null is represented
only by `Option` or raw ABI pointers. Do not call the generated non-null
`EndGetNext` or `EndGetOperation` projections for nullable results; use the local
receive helpers, which call the published vtable types. Returned native
operations still require explicit `Acknowledge` **after** successful application,
never merely after receipt.

Enumeration and fixed-provider callbacks complete synchronously, publish their
result before invoking a reentrant callback, and allow only one outstanding
Begin/End per object. Provider callbacks receive their context as an argument
before the generated Begin thunk writes the caller's output. A foreign context,
double End, overlapping Begin or unsupported snapshot boundary fails explicitly.
Callbacks are optional; when absent, the generated context's `get_Callback`
returns `E_POINTER`, not S_OK/null or a fabricated callback. Completed-operation
cancellation cannot undo the result. Unconsumed contexts and data are released
with their owner.

The context follows the typed bridge ownership pattern, but deliberately does
not use `mssf_core::sync::BridgeContext`: the published 0.10 helper requires a
non-null callback and asynchronous executor, reports asynchronous completion,
and assumes the caller supplies its exact concrete context type to End. This
synchronous fixture instead accepts absent callbacks and validates foreign and
already-consumed contexts before taking a result. It never downcasts a foreign
context or constructs a null value of a non-null interface type. Retained
nullable stream methods validate output pointers. Generated provider/context/data
thunks require valid output pointers, and their failure outputs are unspecified;
the fixture does not add custom vtables to change those binding behaviors.

The immutable provider exposes one exact snapshot boundary, unchanged data-loss
responses, a non-null empty copy-context stream and monotonic epoch recording.
Native volatile replication never requests copy context; `GetCopyState` accepts
the null volatile copy-context input without `unwrap` and rejects non-null
inputs. `GetCopyContext` itself can use the generated non-null projection. It rejects
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

## Why the minimal nullable stream adapter exists

The locked **mssf-com 0.10.0** registry source
`src/Windows/ServiceFabric/FabricRuntime/mod.rs` contains:

- Lines **8753–8762**: `IFabricOperationDataStream_Impl::EndGetNext` returns
  `windows_core::Result<IFabricOperationData>`, not `Result<Option<...>>`.
- Lines **8789–8810**: its generated ABI thunk writes the returned interface on
  `Ok`, then returns S_OK; `Err` converts directly to an HRESULT without writing
  the output. There is no nullable successful branch.
- Lines **8731–8750**: the published `IFabricOperationDataStream_Vtbl` already has
  the correct raw output-pointer layout. This permits the existing nullable
  **receive** helper, but cannot change the implementation trait or its thunk.
- Lines **8889–8900**: `IFabricOperationStream_Impl::EndGetOperation` has the
  analogous non-null return, explaining the retained test-only stream fixture.

In the locked **windows-core 0.100.0**, `src/unknown.rs:11–12` stores `IUnknown`
as `NonNull<c_void>`, and `src/interface.rs:78–85` requires `from_raw` to receive
an owned pointer to a valid interface vtable. Constructing a null
`IFabricOperationData` is invalid, not an EOS encoding. The authorized reference
`mssf-ext/src/stream.rs:58` does exactly that and cannot be copied;
`state_provider.rs:109` also unwraps the nullable copy-context input.

Public generated-source reference:
[Azure/service-fabric-rs FabricRuntime](https://github.com/Azure/service-fabric-rs/blob/main/crates/libs/com/src/Windows/ServiceFabric/FabricRuntime/mod.rs)
(line references above are for the installed 0.10.0 source, not moving `main`).

The authorized adapter retains the exact published IID, full two-method order,
calling convention, HRESULT and output-pointer layout, but represents nullable
interface outputs as `Option`. Both vtables and IUnknown ownership are generated
by the existing typed macros. Tests invoke it through the published raw vtable,
including poisoned output pointers, null EOS, non-null empty data, native errors,
reentrant callbacks, cancellation and ownership release. No bindings fork,
vendoring, reference-repository edits or upstream changes are needed.

No null interface construction, dynamic vtable rewriting, monkey patch,
E_NOTIMPL EOS, or output-unwritten S_OK workaround was introduced. Passing local
tests does not establish native copy enumeration/interoperability; both actual
native directions remain mandatory later qualification gates.
