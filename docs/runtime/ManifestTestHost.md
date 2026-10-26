# Manifest-based standalone native test host

This is the deliberately narrow package profile of the
[Rust standalone host](RustTestHost.md#2-implemented-mvp-and-usage), not a deployer,
node, cluster or sandbox. It retains the Linux x86_64, pinned **SF 11.8.121.4**,
trusted-loopback/no-TLS profile and existing minimum runtime API support.
Only trusted packages and application code may run.

## Package and invocation

The input is an ordinary package directory:

```text
package/
  ApplicationManifest.xml
  <ServiceManifestName>/
    ServiceManifest.xml
    <CodePackageName>/
      <Program and regular dependencies>
```

After building `mssf-test-host`, supply existing native/dependency/run directories:

```sh
target/debug/mssf-test-host manifest \
  NATIVE_DIRECTORY DEPENDENCY_DIRECTORY PACKAGE_DIRECTORY RUN_PARENT \
  close fabric:/MyApplication Count=1
```

The final positional items are `close|abort`, the application URI, and zero or more
declared `NAME=VALUE` overrides. Values containing spaces must be quoted for the
**calling shell** so the entire assignment arrives as one CLI argument; the host
never invokes a shell for application arguments. Duplicate/unknown overrides fail.
Directory paths may be relative to the caller's working directory. Native/dependency
directories are canonicalized before launch, so loader paths remain absolute when
the child changes to its manifest Work or CodePackage directory.
Use actual declared parameter names instead of `Count` for your package.
Legacy five-position `bootstrap|close|abort` invocation is unchanged.

The manifest CLI automatically opens the default service, prints `MSSF_LIFECYCLE`
with its actual endpoint, closes/aborts it, then cleans up its owned child. It is
not an interactive host that leaves the service running for external clients.
`MSSF_REPORT` contains learned identities, mapped pinned libraries, actual endpoint,
`lifecycle_completed`, observed `runtime_unregistered`, and separate cleanup status.

## Small library API

[`run_manifest`](../../crates/libs/mssf-test-host/src/lib.rs#L402) accepts
`ManifestOptions`, `Scenario::{Close, Abort}` and a synchronous observer returning
the host's `Result<()>`. Options contain native/dependency/package/run-parent
directories, application URI and an ordered vector of parameter pairs.
[`validate_manifest`](../../crates/libs/mssf-test-host/src/lib.rs#L31) validates
the package without native libraries, materialization or process creation.

Observers receive `LifecycleEvent { stage, endpoint, pid }` first at `Opened`, then
at `Closed` or `Aborted`. Native semantic success precedes each observation; child
liveness is checked before and after. The endpoint is an opaque application string,
including possibly empty. Observers may independently probe the application while
Open and prove shutdown after the terminal callback; the host imposes no IP:port,
ping or diagnostic-marker convention. The
[acceptance example](../../crates/libs/mssf-test-host/examples/manifest_acceptance.rs)
shows fixture-specific ping/refusal, intentionally outside general host admission.

Observers are trusted synchronous code and **must enforce their own I/O/work
deadlines**. The host checks the scenario deadline before/after each observer but
cannot preempt a blocking callback. Callback errors fail the run and still invoke
owned cleanup. Native errors/timeouts are not made successful by cleanup.

There is no assumed cooperative application shutdown protocol. General manifest
Close/Abort means completed **service lifecycle**, not process exit or runtime
release. A still-running app is killed only through its owned process group and
reaped; `cleanup.forced=true` is expected. `runtime_unregistered=false` means no
actual unregister was observed, not graceful release inferred from termination.
Run directories are retained for inspection, not automatically removed.
Linux cleanup retains an exited leader's waitable identity until its process group
has no running members, including ordinary helpers; it then reaps the direct child.
`cleanup.process_group_drained` reports that separate observation. Helpers are not
mistakenly left alive just because the leader exited; helper zombie reaping belongs
to their actual parent/adopter. Escaped groups and abrupt supervisor-death recovery
beyond direct-child parent-death containment remain outside qualification.

## Exact supported XML subset

Both manifests require namespace `http://schemas.microsoft.com/2011/01/fabric`.
The following table is exhaustive. Optional means zero or one unless noted.
Every other element/attribute, foreign namespace, non-whitespace structural text,
processing instruction or DTD/custom entity declaration is rejected before launch.
XML declarations, comments, built-in/numeric character references and scalar text
are supported. XML is namespace-aware; unknown features are never silently removed.

| Element | Supported attributes / children |
|---|---|
| ApplicationManifest | Required ApplicationTypeName, ApplicationTypeVersion; optional Parameters, exactly one ServiceManifestImport and DefaultServices |
| Parameters | Zero or more empty Parameter with required unique Name, DefaultValue |
| ServiceManifestImport | Exactly one empty ServiceManifestRef with ServiceManifestName, ServiceManifestVersion |
| DefaultServices | Exactly one Service |
| Service | Name, optional ServicePackageActivationMode=SharedProcess (default SharedProcess); exactly one StatelessService |
| StatelessService | ServiceTypeName, InstanceCount=1; exactly one empty SingletonPartition |
| ServiceManifest | Required Name, Version matching import and package directory; exactly one ServiceTypes, CodePackage; optional Resources |
| ServiceTypes | Exactly one empty StatelessServiceType with ServiceTypeName matching default service |
| CodePackage | Required Name, Version; exactly one EntryPoint/ExeHost |
| ExeHost | Exactly one Program text; optional Arguments and WorkingFolder text; no attributes |
| WorkingFolder | Work (native default when omitted) or CodePackage; CodeBase is unsupported |
| Resources | Optional Endpoints |
| Endpoints | Zero or more empty Endpoint with unique Name, Protocol=tcp, Type=Internal, explicit Port=1..65535 |

All supported scalar attributes/text can use a whole-value `[ParameterName]`
reference except parameter declarations and namespace declarations. Defaults and
overrides are literal/nonrecursive; embedded/unknown/unresolved references fail.
Overrides apply to both manifests before path lookup and identity comparison.
The supplied application URI is a separate caller value, not parameter-expanded.

Identifiers/versions/names are 1–128 ASCII alphanumeric/underscore/hyphen/dot bytes,
excluding `.` and `..`. Default service Name is one such segment. Application URI
is `fabric:/` followed by one or more safe segments. Program is a relative
slash-separated safe path inside the code package. All copied file/directory names
use safe segments; symlinks, traversal, devices and nonregular files fail. Program
must exist and be executable on Linux. Packages are trusted and must remain
immutable while validating/copying; this is not race-proof hostile-file isolation.

Limits: 1MiB per manifest; code tree 4096 entries, 64 levels and 512MiB; composed
native filename components at most 255 bytes. XML-invalid override characters fail.
Arguments are at most 16KiB/128 ASCII-whitespace-separated tokens, without quotes,
backslashes, non-XML controls or non-ASCII whitespace. There is no shell expansion,
quote grouping or escape processing. Native Linux uses whitespace tokenization
([source][native-args]); this deliberately smaller contract rejects confusing input.

Unsupported features include stateful/multiple services or types, multiple code
packages/imports, DLL hosting, UseImplicitHost/guest execution, setup entrypoints,
placement constraints/policies, exclusive activation, config/data packages,
environment overrides, principals/certificates/TLS, diagnostics configuration,
resource governance, service templates/groups, extensions, dynamic ports, input/
HTTP/HTTPS endpoints, endpoint bindings and unlisted policies. Empty Parameters,
Resources and Endpoints wrappers are allowed; other mandatory wrappers are not
optional substitutes for a supported feature.
The stateful kvmap and guest/placement smoke packages do **not** run unchanged.

## Native materialization and readiness

The [materializer](../../crates/libs/mssf-test-host/src/deployment.rs#L96) writes:

```text
deployment/<ApplicationTypeName>_App1/
  App.1.0.xml
  <ServiceManifestName>.Package.Current.xml
  <ServiceManifestName>.Manifest.<ServiceManifestVersion>.xml
  <ServiceManifestName>.Endpoints.txt
  Application.Sids.txt
  <ServiceManifestName>.<CodePackageName>.<CodePackageVersion>/
  work/  log/  temp/
```

All native XML scalars are escaped. Native application/service rollouts remain
`1.0`, activation version instance `1.0:1.0:1`; application-type/manifest/code
versions are independent validated metadata, not rollout substitutions.
The host emits required empty native diagnostics/principals/certificates elements;
this does not accept user policies/configuration. Required SID and endpoint files
exist even when empty. Static endpoint metadata is nine semicolon-separated fields
per comma-separated record, for example `Control;tcp;Internal;32123;;;;;127.0.0.1`.
Static metadata **does not reserve, bind or schedule ports**. The application's
returned Open endpoint is independent of that activation resource description.

Admission matches actual host/PID/runtime and resolved package/type identities.
After type registration and finite lease activity, Open is a one-way RA12 command;
success requires its identity-matched semantic reply. No sample text is required.
Native registration inserts an invalid local factory before contacting the host;
lookup queues pending validation after acknowledgment
([factory table][native-factory], [registration order][native-registration]).
The profile relies on that source-defined admission, not sleeps or outer retries.
`FABRIC_E_SERVICE_TYPE_NOT_REGISTERED` can also originate in application Create/Open
or a wrong factory kind, so **all nonzero semantic errors fail**, including that
HRESULT. A numeric match alone is not safe retry provenance.

The existing 120-second operation, bounded transport/logging and 10-second owned
reap limits remain. No node/cluster, global loader modification or ABI stub is used.
General health/load/fault/report APIs, reconnect, stateful storage, lease fencing,
escaped descendants and broader SDK/security/platform profiles remain unqualified.

## Native qualification

On 2026-10-25, with the existing extracted pinned Ubuntu22 runtime and private ABI
files on Linux x86_64:

```sh
python3 scripts/native_runtime_test.py manifest-close \
  --diagnostics-directory build/manifest-test-host-evidence
python3 scripts/native_runtime_test.py manifest-abort \
  --diagnostics-directory build/manifest-test-host-evidence
```

The helper packages the built app with the
[real manifest fixture](../../crates/samples/stateless-test-app/manifests).
Close uses defaults: ManifestHostApplication 2.1, EchoPackage 3.2, Executables 4.3,
ManifestEchoType/DefaultEcho. Abort overrides type, default service and executable
arguments to OverriddenEchoType/OverriddenEcho. The app asserts actual native factory
type/service/empty initialization/instance/singleton data and activation code/manifest
versions plus the Control endpoint metadata. Manifest mode emits no `MSSF_APP` and
has no stdin shutdown contract. Caller ping/refusal and live-child checks pass in
both cases. An additional native Close returned `opaque:manifest-fixture` unchanged,
without a network probe; a direct general CLI run also passed.

| Scenario | PID / real Open endpoint | Result |
|---|---|---|
| Manifest Close/defaults | 377012 / `127.0.0.1:34347` | semantic Close, caller ping/refusal before forced reap |
| Manifest Abort/overrides | 378777 / `127.0.0.1:39735` | semantic Abort, caller ping/refusal before forced reap |
| Opaque Close | 379192 / `opaque:manifest-fixture` | actual native opaque reply, forced/reaped |
| General CLI Close | 379568 / `127.0.0.1:37839` | Opened/Closed observations, no fixture probes, forced/reaped |
| Empty-resource CLI Close | 381025 / `127.0.0.1:41875` | no parameters/arguments/endpoints, required empty files accepted, forced/reaped |

Native evidence is local/ignored: the five-scenario `build/manifest-test-host-evidence`,
plus `build/manifest-test-host-opaque-evidence` and `build/manifest-test-host-cli-evidence`.
The empty-resource variant is in `build/manifest-test-host-empty-endpoint-evidence`.
These contain bounded logs/raw reports, not copies of native libraries.
Existing bootstrap/close/abort also passed after these changes.

After final-review fixes to group cleanup and caller-relative loader paths, all
five scenarios passed again: `build/manifest-test-host-final-evidence`. The final
manifest Close/Abort PIDs were 386066/386176, endpoints `127.0.0.1:38727`/
`127.0.0.1:43639`; both had forced/reaped cleanup and process_group_drained=true.
Opaque Close passed again under `build/manifest-test-host-final-opaque-evidence`
(PID386339). A direct CLI command with relative native/dependency/package/run paths
passed under `build/manifest-test-host-relative-evidence` (PID386358,
`127.0.0.1:38171`). Native-free regression coverage now includes 29 Rust tests.

Qualification does not include the mssf-com 0.10 activation `get_ApplicationName`
binding: generated Rust uses an out-struct return for FABRIC_URI whereas native
returns a pointer. An initial assertion exposed its empty result; that unrelated
dependency was not patched or claimed supported. Exact host runtime context and
the native factory service URI independently verify the application identity.

Standalone [CI](../../.github/workflows/native-runtime.yaml) runs both manifest
scenarios without a node. It retains ten explicitly named log/report files for
five scenarios, at most 1MiB/file (10MiB total), for seven days; no native packages,
deployment trees or environment dumps are uploaded. Rust/parser/state tests and
Python runner tests are **not** substitutes for these real native executions.
The implementation push passed standalone CI
[run 40240835140](https://github.com/youyuanwu/service-fabric-apps-rs/actions/runs/40240835140),
including all five scenarios and bounded artifact upload.

[native-args]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ProcessActivator.Linux.cpp#L442-L448
[native-factory]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceFactoryRegistrationTable.cpp#L162-L251
[native-registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceFactoryManager.cpp#L51-L174
