# Minimal FM Cluster: deferred MVP work

- Work ID: minimal-fm-cluster
- Core outcome: Launch one foreground in-memory FM and two separate owned
  RA+Hosting processes; explicitly place, observe, contact and delete the fixed
  stateless service, with verified physical cleanup.
- Decision source: The operator explicitly approved this bounded scope on
  2026-11-04. These exclusions do not remove existing supported features or
  authorize bypassing correctness, validation, authority or cleanup.

## Decisions

### FM-001: Broader cluster orchestration and service families
- Status: DEFERRED
- Reason: Stateful reconfiguration, PLB, FMM, controller HA, separate Naming/HM
  services, upgrades and autoscale are not required to prove explicit stateless
  placement on two local nodes.
- MVP alternative: One in-memory FM, caller-selected placement, one fixed
  stateless service profile; preserve other native families as staged/unsupported.
- Revisit when: An explicitly approved outcome requires stateful services,
  scheduling, availability, discovery/health services or upgrades.
- Evidence: Existing [native protocol catalog](../cluster/NativeRaProtocol.md)
  distinguishes selected stateless exchanges from the larger registry;
  [architecture proposal](../cluster/PureRustCluster.md) is not an implemented
  parity contract. The 2026-11-04 scope approval explicitly excludes these
  families; required direct bootstrap/report dependencies remain in scope.

### FM-002: Automatic recovery and durable cluster authority
- Status: DEFERRED
- Reason: Automatic restart/failover, reconnect-driven replacement and durable
  recovery add authority and fencing obligations beyond the ephemeral journey.
- MVP alternative: Retain pending/unknown/error and exact old authority; require
  observed physical teardown/fencing before a fresh deployment identity.
  Link loss never proves that an app has stopped.
- Revisit when: A new approved recovery outcome defines durable authority and
  independently verifiable fencing requirements.
- Evidence: The approved first profile is in-memory and foreground-owned.
  Existing [local Hosting](../cluster/HostingLocalActivation.md) and
  [RA/RAP lifecycle](../cluster/StatelessRaRap.md) distinguish physical cleanup
  from semantic/network completion; they do not supply automatic FM failover.

### FM-003: Arbitrary packages, additional profiles and generic infrastructure
- Status: DEFERRED
- Reason: General provisioning, configuration variants, a generic server/router
  and a new transport backend do not validate the fixed stateless placement
  outcome and increase the verification/ownership surface.
- MVP alternative: One preconfigured profile and numeric-loopback trusted-local
  links; reuse existing transport, codecs, store, RA, Hosting and runtime.
- Revisit when: An approved second profile or concrete consumer cannot use the
  existing components and explicitly bounded fixed-profile interface.
- Evidence: Existing [Hosting profile](../cluster/HostingLocalActivation.md)
  has strict supported/unsupported package validation. The 2026-11-04 scope
  approval preserves those checks and strict Endpoint/Channel/client construction.

### FM-004: Full M1 and native FM/Federation parity qualification
- Status: DEFERRED
- Reason: A native-free two-node stateless demonstration is not proof of full
  M1, native FM/Federation interoperability or completion of all staged actions.
- MVP alternative: Source-pin required semantics, separately verify codec
  evidence, run real native-free qualification, and preserve required existing
  native runtime/client/data regressions when shared paths change.
- Revisit when: A separately approved interoperability outcome defines the
  additional native exchanges and maintained release-execution evidence.
- Evidence: [NativeRaProtocol](../cluster/NativeRaProtocol.md) explicitly
  separates catalog completeness, selected schemas and deployed evidence.
  This deferral does not waive required compatibility regressions.
