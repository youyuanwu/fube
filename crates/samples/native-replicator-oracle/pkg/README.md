# Test-only data packages

`launch-mixed.py` execs exactly one prebuilt engine, preserving the SF activation
environment. The injected `Fabric_NodeName` must be one of the five actual
onebox names: N0010/native, N0020/Rust, N0030/native, N0040/Rust, N0050/Rust.
There is no role override, native-engine substitute or compilation fallback.
The mixed package declares both types but has **no default services**: the
native branch registers only the stateful type. The Rust package preserves
default stateless and empty-state services; data is a separately created service.

From a clean source checkout, build once upstream:

```sh
python3 scripts/fube_data_test.py --build-bundle build/data-bundle
```

Qualification never invokes Cargo. Use a new allocation name and evidence path:

```sh
python3 scripts/fube_data_test.py data-owned --prebuilt build/data-bundle \
  --profile full --report build/data-evidence.json
```

`--profile rust` runs the Rust data scenarios; `mixed` runs both classic native
directions in one partition restricted to N0010/N0020/N0030. `full` also requires
the preserved full empty-state/stateless suite. That integration is currently
run through the real runtime `qualify(full=True, stateful="three")` on the same
original allocation after data deployment cleanup. An explicit deployment policy
replaces only package preparation, provisioning, creation and teardown; the native
configuration upgrade, reporting, authority, queries, aborts and restart checks
are unchanged. Both minted legacy versions receive the same immutable ownership
parameters, separate pinned upload reservations and independently captured native
ImageStore generations. The actual native application ID and creation capability
must survive the config-only upgrade. Teardown revalidates both versions and the
original application before deletion, then independently confirms application,
type and upload-reservation absence. Missing proofs retain resources with recovery
details and fail `legacy_full`; an expected qualification status alone cannot pass.
The standalone legacy CLI/default cleanup is unchanged and is never called by
this full entrypoint. The strict nested v3 bundle remains required; no templates
in the stored bundle are modified and there is no Cargo fallback.
`--existing-owner` explicitly retains a caller-owned allocation.

## Cleanup ownership and recovery

The harness exclusively reserves upload staging inside the verified original
allocation before copying `source/.` into it. It prepares a private, unpredictable,
unpublished directory under the verified parent descriptor, retaining the original
directory and owner-file descriptors **before** publishing with atomic
`renameat2(RENAME_NOREPLACE)`. It never opens the published target to mint ownership.
Reservation captures the directory and parent `(device, inode)` and an exclusively
created owner-file capability and its inode. Publication collisions (including
files and dangling symlinks) grant no authority over the target; only the captured
private original can be cleaned. Unavailable atomic publication fails closed.
Partial copies are removed only through the original descriptor-bound reservation;
same-name directories, files, symlinks or copied owner tokens are not adopted.
Parent identity, directory ancestry and the original owner file are rechecked
before cleanup; substitutions are retained with original-identity recovery details.
This keeps the existing trusted-root/native-allocation boundary and permissions;
it does not attempt to defend a private unpublished namespace against hostile root.

Each package has a freshly minted type capability in its immutable application
manifest. Old or prepopulated ownership-parameter templates are rejected during
bundle loading, before allocation. An existing provisioned ImageStore manifest
path is also a collision, even if the application-type query reports absence.
Successful provisioning must be corroborated by the native `GetApplicationManifest`
content fingerprint, version and capability **and** native ImageStore file generation
for `Store/{type}/ApplicationManifest.{version}.xml`, not the uploaded build package.
The generation is the three native `FileVersion` fields
`(EpochDataLossNumber, VersionNumber, EpochConfigurationNumber)`. Manifest and file
generation must remain stable across a second read, and teardown must match the
captured generation. Identical manifest bytes or a copied capability cannot
authorize cleanup of a different observed generation. Creation
supplies a separate run-minted application parameter. After an accepted create,
before scenarios, the harness captures the actual native `ApplicationId` from
`App.*.xml` reached through native deployed-application `WorkDirectory` metadata,
requiring agreement across deployed nodes. The REST `Id` is merely the encoded
application name and is **not** an instance identifier. A name/type/version or
even a copied creation parameter alone never authorizes application deletion.
Teardown rechecks the captured immutable native instance and type fingerprint/generation;
observed replacements are retained rather than deleted.

Definitive AlreadyExists/collision errors grant no capability, even if a foreign
resource has exactly the expected names or markers. Startup timeouts/lost
responses with no previously captured original identity are intentionally
retained, not adopted from subsequent queries. Evidence records outcomes,
available original identities, and precise recovery reasons. Unresolved
application identity also retains its type. Staging cleanup remains independent.
The public application-type query does **not** supply a creation timestamp; the
previous claim of a public `EX3.Metadata.ProvisionTimestamp` was incorrect.
The harness uses native **file** generation, never a manufactured registry
timestamp. Missing, malformed, ambiguous or unavailable generation metadata
retains the type, records precise recovery information and forces FAIL; there is
no content-only fallback or adoption of a later generation. Definitive rejection
still grants no authority even if all metadata matches.

Read-only API/source evidence for this method (not native qualification):

- [Get Image Store Content, REST 6.2](https://learn.microsoft.com/en-us/rest/api/servicefabric/sfclient-api-getimagestorecontent)
  documents `GET /ImageStore/{contentPath}` and an example listing the actual
  `Store\{type}\ApplicationManifest.{version}.xml` with all three `FileVersion` fields.
  [`FileVersion`](https://learn.microsoft.com/en-us/rest/api/servicefabric/sfclient-model-fileversion)
  documents the native creation/update epoch and version, serialized as strings.
- Native [`StoreLayoutSpecification.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/Management/ImageModel/StoreLayoutSpecification.cpp)
  (`GetApplicationManifestFile`, lines 29–41) defines that provisioned path.
  [`ApplicationProvisionOperation.cs`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/managed/Api/src/System/Fabric/Management/ImageBuilder/ApplicationProvisionOperation.cs#L1042-L1051)
  uploads the application manifest there during provision.
- [`ProcessListRequestAsyncOperation.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/Management/FileStoreService/ProcessListRequestAsyncOperation.cpp#L147)
  returns the file metadata's `CurrentVersion`, not its content hash or name.
  [`ProcessUploadRequestAsyncOperation.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/Management/FileStoreService/ProcessUploadRequestAsyncOperation.cpp)
  obtains `GetNextFileVersion()`; [`RequestManager.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/Management/FileStoreService/RequestManager.cpp#L1607-L1610)
  allocates the native epoch pair and incremented version number.
- [`ImageBuilderProxy.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/Management/ClusterManager/ImageBuilderProxy.cpp#L3394-L3434)
  includes the provisioned application manifest in type cleanup.
  [`ApplicationTypeQueryResult.cpp`](https://github.com/microsoft/service-fabric/blob/master/src/prod/src/ServiceModel/ApplicationTypeQueryResult.cpp)
  exposes name/version/parameters/status/definition kind, not a provisioning timestamp.

Actual 11.8.121.4 ImageStore/API behavior and native interoperability qualification
remain **pending**. Source inspection and offline tests do not establish live
cleanup success or Phase 4/5 completion. These observations are native identity
snapshots, not an instance-conditional lifecycle lock; do not externally recreate,
upgrade or unprovision resources during a run. A replacement before the first
identity observation or after the last check cannot be ruled out by these public
snapshot APIs; no atomic generation-conditional deletion is claimed.

The Rust profile also creates five SharedProcess stateless siblings to distinguish
per-replica removal/Close from process-wide loss. Initial data uses 300 commits;
each live copy attempts 200 further writes and requires at least 100 observed
commits inside the native InBuild/copy-horizon window. Ledger saturation runs
last, verifies rejection at the finite 4096-ID bound, and retries old identities
without clearing their partition-lifetime protection.

Evidence includes every required scenario, native placement and deployment,
application ACK tests, hashes, mapping classifications and cleanup. Unsupported
wire fault/data-loss probes are recorded as **MISSING**, never silently passed:
the overall result remains FAIL until all required observations exist. Local
unit tests validate harness contracts, not actual native interoperability.
Build source, review head and CI head/merge identities are distinct fields;
unset review/CI identities remain null. Native replication addresses/peer lists
that are not public are null, never inferred from the HTTP address.

The bundled observer must support data-aware `--stateful` and native
`--move-primary`. Both engines must implement real `ack_delay_ms` and primary
`copy_delay_ms` test hooks. Missing native requested-copy-horizon telemetry or
Rust observed-peer identity also leaves the affected direction MISSING; HTTP
state convergence alone is not substituted for those observations.
