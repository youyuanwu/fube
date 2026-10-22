# Pure Rust FabricClient implementation plan

**Status:** proposed; all implementation phases are pending.

An initial [clear-TCP Rust POC](../RustFabricClient.md) now demonstrates native
gateway ping and NameExists against Ubuntu onebox runtime 11.8.121.4. It is a
deliberately narrower implementation, now with a continuously driven, bounded
concurrent client, restricted naming inputs, and no TLS or automatic recovery. The production
phases below remain pending; partial POC work does not satisfy their full gates.
The POC uses real onebox exchanges as its compatibility oracle, not committed
hex packet snapshots; its offline tests cover internal behavior and safety.
Its Linux CI builds a nextest archive once, then transfers it to the onebox
runner; tests needing onebox share the `onebox` group. See the POC guide for
the current archive/group commands rather than the future phase examples below.

## 1. Overview

Implement the native FabricClient gateway protocol in Rust, using Tokio for TCP
and scheduling, rustls for whole-frame TLS, and an explicit Fabric binary codec.
Deliver a small, demonstrated compatible client before expanding API coverage.

The implementation follows the [design](FabricClient.md) and
[message-format specification](../FabricClientMessageFormat.md). The
[native behavior specification](../FabricClient.md) defines gateway operations;
[Transport.md](../Transport.md) supplies shared transport behavior.

**The available native test environment is onebox. A native Service Fabric C++
build is not a prerequisite.** The initial compatibility oracle is the real
gateway accepting Rust requests and emitting replies that Rust correctly
decodes. Source analysis guides implementation; committed packet hex snapshots
are not a correctness oracle and a native fixture build is not a prerequisite.

This plan creates no crates, starts no cluster, and changes no runtime settings.
Paths, test targets, and commands below describe future implementation work.
In phase descriptions, `mssf-transport/...` and `mssf-client/...` abbreviate
paths under `crates/libs/`.

## 2. Current state and constraints

| Existing asset | Consequence |
|---|---|
| Source-derived specifications pinned to native commit `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` | Use them for schemas, but record the actual onebox runtime separately; source and deployed versions are not assumed identical. |
| Workspace members include `crates/libs/*` | New transport/client crates fit the existing layout. |
| Workspace already supplies Tokio, bytes, tokio-util, and tracing | Reuse these dependencies rather than introducing another runtime. |
| Existing `mssf-*` dependencies call native APIs | They are not production dependencies of the new pure Rust wire client. |
| A onebox runtime is available, but its test profile has not been qualified here | Inspect endpoint, runtime version, security, credentials, and fixture state before live testing. |
| Repository container-cluster tooling is unsecured | It is an optional clear-TCP environment, not proof of TLS support and not required in place of the user's onebox. |
| No demonstrated complete native Service Fabric build | Do not require a C++ fixture executable or a rebuilt native client. |
| Rustls/provider production approval remains open | Use explicit provider selection; isolated experimentation does not constitute production approval. |

The repository's [manifest template](../../../docker/linux-cluster/ClusterManifest.template.xml)
uses native client port **19000** and HTTP port **19080**. Inspect the actual
onebox configuration rather than assuming these defaults. The native protocol
cannot be tested by pointing the Rust client at the HTTP endpoint.

The [Ubuntu single-container onebox](../../UbuntuOnebox.md) provides a disposable
repository-local clear-TCP environment with `just onebox-build` and
`just onebox create NAME`. It publishes native TCP at `127.0.0.1:19000` and HTTP
at `127.0.0.1:19080`; the multi-container workflow publishes only HTTP.
Lifecycle readiness is not qualification of the future Rust client's protocol,
and neither unsecured environment qualifies TLS.

The existing [CI workflow](../../../.github/workflows/ci.yaml) and
[justfile](../../../justfile) remain intact. Add isolated package-level tests
without making the new crates depend on the existing native-backed applications.

## 3. Desired end state and non-goals

The first useful slice is a cloneable Rust client that establishes a gateway,
executes `NameExists`, reports real errors, and safely handles concurrent
requests, cancellation, and disconnects. Clear TCP is an explicitly configured
development mode. A secure compatibility claim additionally requires live
certificate TLS against a secured native gateway.

Subsequent phases add typed queries, partition resolution, claims over TLS,
priority connections, and notification synchronization.

Out of scope:

- Building Service Fabric from C++ source or making native SDK installation a
  prerequisite for offline Rust tests.
- Platform-specific authentication or alternate message-protection formats.
- HTTP as a production fallback for native operations.
- COM ABI compatibility, application remoting/listeners, or internal cluster
  replication/routing.
- Complete `FabricClient.idl` coverage in the first release.
- A Serde wire backend or custom derive macro before handwritten schemas work.
- Initial health-report batching, file transfer, or protocol-1.2 upload claims.
- Changing or restarting a shared onebox without explicit operator authority.

Root `README.md` must remain without links to these transport documents.
Any future PAW artifacts stay local and ignored.

## 4. Execution order and phase status

- [ ] **Phase 1: Foundation and onebox contract** - Create isolated crates and define the live-test profile.
- [ ] **Phase 2: Fabric object codec** - Implement bounded serialization with internal consistency and safety checks.
- [ ] **Phase 3: Frames, headers, and controls** - Implement complete message envelopes and parser semantics.
- [ ] **Phase 4: Clear-TCP gateway establishment** - Prove native controls and ping against onebox.
- [ ] **Phase 5: NameExists and request lifecycle** - Deliver the first useful operation with safe recovery.
- [ ] **Phase 6: Certificate TLS** - Add the rustls driver and qualify a secured native profile.
- [ ] **Phase 7: Queries and resolution** - Add useful reads, paging, and version-aware discovery.
- [ ] **Phase 8: Claims and notifications** - Add later TLS authentication and duplex session behavior.
- [ ] **Phase 9: Compatibility qualification** - Enforce offline CI and recorded onebox release gates.
- [ ] **Phase 10: Documentation** - Publish usage, coverage, evidence limits, and an as-built record.

Dependencies:

```text
1 -> 2 -> 3 -> 4 -> 5 -> 7
                  5 -> 6
        5,6,7 -> 8
        completed release scope -> 9 -> 10
```

Start the small TLS interoperability spike after phase 3, alongside the clear
client work, when a secured onebox is available; phase 6's complete live
NameExists gate additionally depends on phase 5. If the only onebox is unsecured,
continue clear-TCP and offline work but leave live TLS qualification blocked.
Do not mark phase 6 complete from a Rust-to-Rust TLS test.

### Phase candidates

These are deferred follow-up work, not requirements for the first slice:

- [ ] Fabric-specific derive macro after repeated schemas justify it.
- [ ] Optional Serde adapter over the proven codec.
- [ ] Additional management method families, one reviewed family at a time.
- [ ] Separate health-reporting and file-transfer designs.
- [ ] TLS 1.3 after native final-flight/post-handshake compatibility is proven.
- [ ] Native serializer producer/consumer harness if a suitable build becomes available.

## 5. Shared testing contract

### 5.1 Onebox test configuration

Add test-only configuration loading in
`crates/libs/mssf-client/tests/common/mod.rs`. Use one explicit
`MSSF_TEST_CONFIG` path to a private JSON file, not hard-coded local endpoints or
credentials. Its contract includes:

| Field group | Required information |
|---|---|
| Environment | Profile (`clear`, `x509`, or `claims`), native gateway addresses, expected/observed runtime version. |
| Semantic fixtures | A known existing naming URI, a guaranteed absent URI, and the independent management observation establishing those facts. |
| TLS profiles | Expected server identity, CA roots, client certificate/key paths where required, and explicit trust/revocation policy. |
| Claims profile | Token-provider/test-token-file configuration and expected authorization behavior. |
| Evidence output | An ignored output directory for sanitized live result reports. |

Keep private keys, tokens, and credential-bearing configuration outside source
control. Derive no security fallback from a failed connection.

Live integration tests are explicitly ignored in ordinary offline `cargo test`
runs. Dedicated commands select a profile-specific test target with `--ignored`.
Within that selected target, missing configuration, wrong profile, inaccessible
gateway, or unmet fixture prerequisites are failures, not skipped successes.
The report verifies that the expected test cases actually ran.

### 5.2 Evidence tiers

| Evidence | Role and limitation |
|---|---|
| Native source/schema analysis | Guides implementation; does not establish runtime wire correctness. |
| Rust model/property/fuzz tests | Exercise malformed input, resource bounds, and lifecycle races; not an independent native oracle. |
| Real onebox exchanges | Required interoperability proof for each claimed runtime/profile/operation. |
| Existing native client, if usable | Optional result/error comparison using prebuilt libraries/tools; no native rebuild required. |
| Full native serializer harness | Optional future diagnostics; not a replacement for real onebox qualification. |

Management/HTTP tools can establish fixture state and expected public results.
They do not prove the Rust native encoding by themselves, and HTTP status codes
must not be treated as identical to native error enums.

### 5.3 Test state and live evidence

Prepare synthetic naming/service state directly in a test-owned onebox using
independent management observations. Keep private profiles and sanitized live
reports in ignored output directories. Record observed runtime/security
profile, independently known expected state, and the cases that actually ran.

Do not commit packet hex files or normalized captures as correctness fixtures.
Offline generated inputs serve internal round-trip, malformed-input, and
resource-bound tests only. Unexercised native cases remain unqualified.
Local traffic inspection may help debugging, but must not expose tokens,
keys, or sensitive operation bodies or become a substitute for live proof.

## Phase 1: Foundation and onebox contract

### Changes required

- **`crates/libs/mssf-transport/Cargo.toml`, `src/lib.rs`**: Create the shared
  transport crate with the existing workspace edition/style and minimal
  dependencies. No native Service Fabric/COM/OpenSSL protocol dependencies.
- **`crates/libs/mssf-client/Cargo.toml`, `src/lib.rs`**: Create the client
  facade crate depending on the new transport crate.
- **Root `Cargo.toml` and lockfile**: Register needed shared path dependencies,
  retain the existing member glob, and let Cargo resolve only the required
  dependency changes without unrelated upgrades.
- **`.github/workflows/ci.yaml`**: Add the narrow Linux offline package job as
  soon as the crates exist; phase 9 extends its qualification coverage.
- **`crates/libs/mssf-client/tests/common/mod.rs`**: Define the explicit
  test-profile contract and sanitized report schema.
- **`crates/libs/mssf-client/tests/onebox_clear.rs`**: Start with an ignored
  preflight test that checks configured prerequisites and TCP reachability.
  Report that reachability alone is not gateway-protocol readiness.
- **Package tests**: Ensure the crates compile/test without loading a native
  Service Fabric library or requiring onebox.

Do not add rustls/provider dependencies until phase 6 needs them. Review the
dependency graph and the all-Rust provider requirement before selecting a
compatible maintained version set. Native crypto dependencies are not an
implicit approved relaxation.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport -p mssf-client` passes without a cluster.
- [ ] `cargo tree -p mssf-client` shows no production path to native-backed
  `mssf-core`, `mssf-com`, `mssf-pal`, or a native TLS protocol backend.
- [ ] `cargo test -p mssf-client --test onebox_clear -- --ignored` fails when
  required configuration is absent and produces a sanitized preflight report
  when configured correctly.

Operator verification:

- [ ] Identify the actual native endpoint, runtime version, and security mode.
- [ ] Establish existing/absent naming fixtures through an available management
  tool without destructive cluster changes.
- [ ] Record whether secured native testing is currently available; absence
  blocks TLS qualification, not codec development.

## Phase 2: Fabric object codec

### Changes required

- **`mssf-transport/src/serialization/`**: Add bounded cursors, explicit
  `FabricEncode`/`FabricDecode` traits, metadata, object extents/scopes,
  compressed integers, arrays, pointers, and unknown-data handling.
- **Wire value types in that module**: Represent UTF-16 code units, GUID layout,
  TimeSpan ticks, enum codes, inline MessageId, nested ActivityId, and exact
  container specializations. Avoid deriving the native format through Serde.
- **`mssf-transport/tests/codec.rs`**: Exercise scalar/container/object
  consistency, boundary values, malformed input, and resource limits.

Implement the profiled eight-byte object header explicitly, not through Rust
memory layout. Preserve unknown data when re-emission is required. Do not add
unsupported external/no-copy buffer transport.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport --test codec` covers both encoding and decoding.
- [ ] Boolean metadata, sign boundaries including signed 64, UTF-16 surrogate
  pairs, GUID byte order, empty string/vector/array distinctions, inline versus
  nested IDs, and TimeSpan sentinels preserve internal values and reject
  malformed input; their native interpretation requires live onebox coverage.
- [ ] Depth 64 versus 65 and count-limit boundaries are checked before
  unbounded allocation; integer/object-size overflow and truncated input fail.
- [ ] Unknown trailing fields/scopes survive the supported re-emission path.
- [ ] Property tests and a bounded fuzz target exercise decoder progress and
  resource checks; Rust round trips are not reported as native proof.

Review verification:

- [ ] Every bootstrap type has a traced native field order and helper mapping.
- [ ] Padding and ABI assumptions are explicit; no unavailable C++ fixture
  executable is required to proceed.

## Phase 3: Frames, headers, and controls

### Changes required

- **`mssf-transport/src/frame.rs`, `message.rs`, `headers/`**: Add fixed framing,
  CRCs, ordered header records, known schemas, unknown/deleted record handling,
  and immutable owned message buffers.
- **`mssf-transport/tests/headers_frames.rs`**: Exercise generated messages,
  header/frame boundaries, malformed input, and checked extent handling.
- **Control schemas**: Add SecurityNegotiation, ServerAuth, ListenInstance,
  ConnectionAuth, and the claims indication/body needed by later TLS work.
- **Header access**: Distinguish first-match typed lookup from last-visible
  effective native common properties; do not use an information-losing map.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport --test headers_frames` passes.
- [ ] All split positions, byte-at-a-time input, concatenated frames, and EOF
  cases preserve boundaries; invalid lengths/CRCs are rejected.
- [ ] Header area lengths at the 65,535-byte boundary are validated; the encoder
  rejects an oversized area before writing its frame.
- [ ] Generated messages have consistent frame/header/object extents;
  compatibility is qualified by the live ping gate, not a golden packet.
- [ ] Duplicate/deleted headers and malformed later common occurrences follow
  the documented semantics.

Review verification:

- [ ] Frame, record, and object lengths are separate checked quantities.
- [ ] Control actors are not passed to ordinary gateway response decoding.

## Phase 4: Clear-TCP gateway establishment

### Changes required

- **`mssf-transport/src/connection.rs`**: Implement the Tokio-owned socket driver,
  bounded ordered output, partial writes, buffering, and control dispatch.
- **`mssf-transport/src/request_reply.rs`**: Add bootstrap correlation and a
  pending-request entry registered before any request bytes can be sent.
- **`mssf-client/src/gateway.rs`, `protocol/`**: Implement native client-only
  ListenInstance behavior, protocol 1.1, PingRequest, correlated reply handling,
  and GatewayDescription decoding.
- **`mssf-transport/tests/connection.rs`**: Exercise stream fragmentation,
  control ordering, backpressure, deadlines, and shutdown with a model peer.
- **`mssf-client/tests/onebox_clear.rs`**: Extend the live target from socket
  reachability to real transport setup and gateway ping.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport --test connection` passes with controlled
  time and no dependency on a real cluster.
- [ ] `cargo test -p mssf-client --test onebox_clear -- --ignored` obtains a
  correctly correlated native ping reply on the configured onebox.
- [ ] Protocol-version and nested gateway/node identity fields decode correctly;
  reply handling does not require the action literal `"PingReply"`.
- [ ] A sanitized live ping result records actual runtime and independently
  observed node identity, without relying on captured-byte snapshots.

Operator verification:

- [ ] Connect to the native endpoint, not the HTTP port.
- [ ] Compare decoded gateway identity with independent onebox observations.
- [ ] Missing required control/body information fails explicitly rather than
  fabricating a successful gateway description.

## Phase 5: NameExists and request lifecycle

### Changes required

- **`mssf-client/src/client.rs`, `naming.rs`, `error.rs`, `protocol/`**: Add the
  builder, explicit security selection, `connect`, `close`, request options,
  native NamingUri encoding, and `NameExists`.
- **`gateway.rs` and `request_reply.rs`**: Complete bounded admission,
  end-to-end monotonic deadlines, connection generations, ID allocation,
  recovery/backoff, cancellation, and conservative delivery outcomes.
- **`mssf-client/tests/naming.rs`, `lifecycle.rs`**: Add semantic and concurrency
  tests; inject ClientOperationFailure and Fault model-peer cases for internal
  behavior. Their native error-path qualification requires live onebox cases.
- **`onebox_clear.rs`**: Add known-existing and guaranteed-absent names using the
  profile's independently established fixtures.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-client --test naming --test lifecycle` passes.
- [ ] The live clear target returns true and false for the expected fixtures,
  without converting a remote failure into false.
- [ ] Concurrent/out-of-order replies complete only their intended requests;
  duplicates, late replies, timeouts, and disconnect races complete each local
  request at most once and release pending entries/permits.
- [ ] Admission and overall deadlines remain bounded across reconnects.
- [ ] A partially transmitted/encoded frame is drained or the connection closes;
  cancellation never deletes a suffix from the stream.
- [ ] No ambiguous submitted request is automatically replayed as a mutation.

Operator verification:

- [ ] Confirm observed naming results against the fixture-management tool.
- [ ] Exercise connection loss through a test-owned proxy or peer fault
  injection, not by restarting a shared onebox.
- [ ] Record which operation errors were observed live versus injected offline;
  do not equate a local invalid-input error with native failure coverage.

## Phase 6: Certificate TLS

This phase is required for a secure compatibility claim. Its isolated driver
tests can proceed without a secured onebox, but its live exit gate cannot.

### Changes required

- **Workspace/new crate manifests**: Add rustls with an explicitly reviewed
  crypto provider and compatible versions/features. Keep experimental provider
  use restricted to isolated test credentials until production approval.
- **`mssf-transport/src/security.rs`, `connection.rs`**: Implement Fabric-wrapped
  handshake output, independent receive/transmit handoff, whole-frame TLS,
  repeated negotiation verification, and promised authorization-status gates.
- **Trust configuration**: Implement mutual-certificate TLS 1.2 and CA plus
  expected server identity first. Unsupported certificate/revocation policies
  fail explicitly. Keep resumption and early data disabled.
- **`mssf-transport/tests/security.rs`**: Use a test-only Rust TLS peer and
  runtime-generated synthetic keys/certificates; this is not a production
  FabricTransport listener or a native interoperability oracle.
- **`mssf-client/tests/onebox_tls.rs`**: Add a dedicated ignored live target
  requiring an `x509` profile and an actually secured native gateway.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport --test security` covers final wrapped
  handshake output, adjacent raw TLS bytes in the same read, multi-record tokens,
  partial writes, alerts/close_notify, and bounded receive/output buffers.
- [ ] No normal operation bypasses certificate validation, first-protected
  verification, or a promised connection-status gate.
- [ ] `cargo test -p mssf-client --test onebox_tls -- --ignored` completes native
  TLS setup, controls, ping, and NameExists.
- [ ] Wrong server identity, untrusted/expired certificates, absent/wrong client
  certificate, and missing required revocation support fail as specified.
- [ ] The dependency graph matches the approved provider profile; no silent
  native-backend selection or trust bypass is introduced.

Operator verification:

- [ ] Record native runtime, TLS version/suite, identity policy, and credential
  role without recording secrets.
- [ ] If only an unsecured onebox exists, report live TLS as blocked/unqualified;
  never count mock tests or clear TCP as satisfying this gate.
- [ ] Provisioning a secured environment is a separate operator-controlled
  prerequisite, not an implicit mutation of the existing onebox.

## Phase 7: Typed queries and partition resolution

### Changes required

- **`mssf-client/src/query.rs`, `protocol/`**: Add a typed node-list query,
  exact argument-map encoding, polymorphic results, processing errors, and paging.
- **`resolution.rs`**: Add uncached partition resolution with native partition
  key/previous-version semantics, then a bounded version-aware cache.
- **`mssf-client/tests/query_resolution.rs`**: Test wrapper discrimination,
  paging, unknown variants, stale updates, deletion, and partition-key cases.
- **Live targets**: Add read-only cases to the matching clear/TLS onebox profiles.
  Use an existing disposable test service or explicitly prepared fixtures.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-client --test query_resolution` passes.
- [ ] Live node results match management observations on the same runtime.
- [ ] Result processing failures and a verified invalid-query-argument case are
  surfaced rather than returned as successful empty lists.
- [ ] Page tokens and result ordering/version semantics survive request
  boundaries; no implicit switch to the internal query-body schema occurs.
- [ ] Cached/forced-refresh resolution preserves partition/endpoint/version
  information and rejects stale replacement according to native comparisons.

Operator verification:

- [ ] Compare resolved service endpoints with known fixture deployment state.
- [ ] Do not claim paging, deletion, or partition-kind coverage unless the
  corresponding deterministic fixture/case actually ran.

## Phase 8: Claims and notification sessions

These are later capabilities, not prerequisites for the first NameExists slice.
Complete and qualify each independently before publishing its API as supported.

### Changes required

- **Security/token-provider interfaces**: Add claims over the proven TLS driver,
  the correct ClientAuth binding, metadata policy, token deadlines, claims-first
  release ordering, and later denial handling.
- **`gateway.rs` and transport target management**: Add independently secured
  normal/high-priority connections associated with the current gateway generation.
- **`notifications.rs`, `protocol/`**: Add identity, filters, session connection,
  version/deletion synchronization, duplex routing, and acknowledgments.
- **Tests**: Add offline `claims_notifications.rs`, extend transport security
  cases, and add `onebox_claims.rs` plus notification cases for qualified profiles.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-client --test claims_notifications` passes.
- [ ] `cargo test -p mssf-client --test onebox_claims -- --ignored` proves valid,
  invalid, and expired-token behavior on a configured claims-enabled gateway.
- [ ] Initial connection success is not mistaken for token acceptance.
- [ ] Reverse notifications cannot complete ordinary pending requests; replies
  use the original target/generation and correlation ID.
- [ ] Queue overflow surfaces lag/desynchronization; version advancement and
  acknowledgment cannot silently skip an unadmitted event.
- [ ] Reconnect/filter restoration, duplicate pages, deletion, slow consumers,
  and priority-connection faults preserve synchronization semantics.

Operator verification:

- [ ] Exercise only test-owned services/filter state and controlled connection
  faults. A single onebox does not prove cross-node failover or rolling-upgrade
  behavior; qualify those separately when an environment exists.
- [ ] If claims or service-change fixtures are unavailable, mark those features
  unqualified rather than claiming coverage from another profile.

## Phase 9: Compatibility qualification and CI

### Changes required

- **`.github/workflows/ci.yaml`**: Extend the isolated Linux offline job for the
  new packages without rewriting existing application/cluster jobs. Keep it
  running on each relevant change as the qualified scope grows.
- **Profile-specific live targets/reporting**: Support explicit local runs and,
  where an operator-configured onebox runner exists, a dedicated live job.
  Do not assume a public CI runner already has a native gateway.
- **Fixtures and fuzz targets**: Add reviewed native response regressions and
  focused fuzz campaigns for codec/frame/state inputs.
- **Coverage record**: Maintain runtime/profile/method/result-case evidence;
  source-derived, model-tested, and native-observed are distinct statuses.

### Success criteria

Automated:

- [ ] `cargo test -p mssf-transport -p mssf-client` passes with no native runtime.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy -p mssf-transport -p mssf-client --all-targets -- -D warnings`
  passes.
- [ ] Every configured release profile runs its explicit onebox test target;
  missing prerequisites or zero matched/required cases fail qualification.
- [ ] Bounded fuzz runs complete without panics, hangs, unchecked resource growth,
  or dispatch before required security/readiness gates.
- [ ] Dependency/provider and supported certificate policies receive approval
  before any production-ready all-Rust/TLS claim.

Operator verification:

- [ ] Attach sanitized reports from the actual onebox runtime, not only mocks.
- [ ] Do not claim historical runtime compatibility, multiple-node failover,
  full native ABI coverage, or untested TLS profiles from onebox-only evidence.
- [ ] Native compilation remains optional; its absence is documented as an
  evidence limitation, not replaced with fabricated golden fixtures.

## Phase 10: Documentation

### Changes required

- **`docs/transport/design/FabricClient.md`**: Reconcile implementation choices
  and any measured deviations with the design.
- **`docs/transport/RustFabricClient.md`**: Add actual build/use examples,
  configuration, onebox test setup, error/delivery semantics, and a supported
  runtime/security/method matrix.
- **New crate READMEs and Rust API docs**: Explain transport/client boundaries,
  security requirements, and explicit unsupported features.
- **As-built record**: During a PAW implementation, write local ignored
  `.paw/work/<work-id>/Docs.md`; keep public usage/evidence in project docs.
  Do not add root README transport links or commit `.paw` artifacts.
- **Specifications**: Correct discovered source-format inaccuracies with
  evidence, without presenting deployed-runtime observations as universal rules.

### Success criteria

Automated:

- [ ] `cargo doc -p mssf-transport -p mssf-client --no-deps` succeeds.
- [ ] Package tests include public doctests where examples are executable.
- [ ] Relative Markdown links resolve. There is currently no dedicated Markdown
  documentation build; do not invent a passing docs-build command.

Operator verification:

- [ ] Usage examples run against the documented qualified profile.
- [ ] Clearly separate implemented, onebox-qualified, experimental, blocked,
  and deferred capabilities.
- [ ] Publish an accurate first-slice compatibility claim, not "full FabricClient
  replacement."

## References

- [Rust client design](FabricClient.md)
- [Byte-level native message format](../FabricClientMessageFormat.md)
- [Native FabricClient behavior](../FabricClient.md)
- [Shared transport/security](../Transport.md)
- [Workspace dependencies](../../../Cargo.toml)
- [Pinned Rust toolchain](../../../rust-toolchain.toml)
- [Existing CI](../../../.github/workflows/ci.yaml)
- [Linux container-cluster lifecycle](../../LinuxContainerCluster.md), optional
  development environment rather than the required onebox provisioning mechanism.
- [Ubuntu single-container onebox](../../UbuntuOnebox.md), a disposable clear-TCP
  environment with localhost native/HTTP gateways.
- [Native and HTTP endpoint declarations](../../../docker/linux-cluster/ClusterManifest.template.xml)
