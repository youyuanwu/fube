# RA source-baseline codec oracle

This is an explicit producer/consumer gate, not a running native FM/RA test.
Native revision: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
See [the catalog](../../docs/cluster/NativeRaProtocol.md) for all contracts.

## What actually runs

The producer compiles the **unmodified** native `FabricStream.cpp`,
`FabricSerializable.cpp`, `FabricSerializableStream.cpp`,
`FabricSerializationTypes.cpp`, `ByteCompressor.h` and
`Common/FabricSerializationHelper.h`. It mechanically extracts field macros
and member declarations for 39 schema types/123 members, including the selected
Add, inherited DeleteReplica, reply and ReplicaUp/Reply bodies.
The native helper macros generate their actual Write/Read methods. Nested
fields are not inferred from Rust or encoded as handwritten wire bytes.

`native/ktl.h` is a test-only allocator/container/raw seekable byte-storage
support layer, **not KTL/PAL**. It contains no Fabric metadata, compression,
scope or wire decoding algorithm. Schema adapters omit unrelated production
methods/dependencies and expose fields for fixture construction/comparison.
`scaling.inc` supplies fixture polymorphic activation with pinned kind tags.
The actual stream and field macros retain native encoding/decoding behavior.
Clang MS extensions/delayed templates and a two-byte WCHAR profile are explicit.
The fixture avoids system-width wchar string functions under `-fshort-wchar`.

This is **source-baseline native-stream conformance with schema adapters**,
not the complete production C++ RA classes, KTL/PAL qualification, a native
handler trace test, or interoperability with the SF 11.8.121.4 qualification
binaries. Neither runtime nor Federation compatibility follows from it.

The C++ producer independently constructs expected values and emits 29 inputs:
default/rich AddInstance, default/error AddInstanceReply, default/rich Remove
(force false/true), default/rich removal reply, explicit-empty/rich ReplicaUp
(both nonempty lists, all last/fromFMM combinations), FM/FMM generation,
actor/action/priority headers. The two reply actions share a native body type.
Rust decodes and checks every field, then encodes
independently constructed expected values; the actual native decoder compares
all fields, not just bytes. Native object padding need not be canonical.
Nondefault cases cover GUID/raw NodeId, high-bit instances, negative LSN/epochs,
UTF-16/surrogates, enums/errors, all service collections and null/Invalid/both
derived scaling variants. Rust rejects every proper prefix of rich body
fixtures including both rich force bodies; native rejects four truncated bodies.
Non-polymorphic Delete inheritance invokes base rich traversal and native
base/derived scopes, not a flattened or nested replacement. ReplicaUp's empty
native constructor leaves scalar booleans uninitialized; the empty fixture
explicitly initializes them, and is not labeled a native-default guarantee.
ReplicaInfo's rich fixture initializes its intermediate role explicitly.

## Explicit production

Requires Linux x86-64, Python 3.11+, Rust workspace toolchain, Clang with MS
extensions, and the clean pinned native source. No full SF build/installation.
Compile the Rust example with the current source identity; the producer
rejects an unbound/stale executable:

```sh
mkdir -p build/ra-stateless-removal-tmp
export TMPDIR="$PWD/build/ra-stateless-removal-tmp"
FUBE_RA_SOURCE_DIGEST=$(python3 -c 'from tests.ra_protocol.producer import source_digest; print(source_digest()[0])') \
  cargo build --locked -p fube-ra --example ra_conformance
python3 -m tests.ra_protocol.producer \
  --native /path/to/clean/service-fabric \
  --out /absolute/new/bundle --cxx /path/to/clang++ \
  --rust "$PWD/target/debug/examples/ra_conformance"
```

Producer compilation is intentional, never a consumer fallback. It records
compiler/flags, native input hashes, generated schema, repository HEAD and
source-input hashes, and SHA256 for the complete prebuilt inventory.
Source identity includes the RA/store/transport Rust sources, manifests/lockfile
and producer/consumer/schema/platform inputs. HEAD identifies the source base;
source_digest also binds dirty tracked/new work during development.

## Compiler-free acceptance

Transfer the completed bundle and independently expected HEAD/source digest.
No native source, SF install, compiler, Rust/Cargo or package installation:

```sh
env PATH=/nonexistent /usr/bin/python3 -m tests.ra_protocol.consumer \
  --bundle /absolute/bundle --out /absolute/new/rust-output \
  --expected-head COMMIT --expected-source SOURCE_DIGEST
```

The bundle has exactly 29 `native/*.bin`, `schema.h`, `native-oracle`,
`ra-conformance` and `manifest.json` (33 files total). No other files are
accepted. The consumer verifies exact coverage/type/member counts, regular files, hashes,
provenance and both compiled source identities **before** execution. Missing,
modified, wrong-source and symlink inputs fail; it imports no producer and
cannot generate replacement inputs. The manifest is integrity/provenance
metadata, not a cryptographic signature or hostile-producer trust system.
Offline validator tests deliberately use model bytes and make no native
production/execution claim:

```sh
python3 -m unittest tests.offline.cluster.test_ra_protocol_inputs -v
cargo test --locked -p fube-ra
```

Current-removal production must use a fresh bundle bound to the current
implementation HEAD/source digest. `build/ra-native-final-v2` was produced for
previous work and is not this slice's production. The producer's HEAD names the
commit base and source digest also binds dirty sources during development;
final delivery produces again at the clean reviewed implementation head.
