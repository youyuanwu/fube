# Reusable Stateless Lifecycles: deferred MVP work

- Work ID: reusable-stateless-lifecycle
- Core outcome: Create, independently contact, delete and recreate the fixed
  stateless service on the same live foreground FM and two original nodes,
  after verified predecessor instance cleanup.

## Decisions

### RSL-001: Recycle the app host and package for every service instance
- Status: DEFERRED
- Reason: A fresh stateless service instance does not require replacing its
  application-host process or package. Doing so would introduce Activation
  replacement, package-version rotation and new ownership transitions unrelated
  to the approved service-instance outcome.
- MVP alternative: Preserve the original live host/runtime/package and require
  positive original-runtime disposal provenance, original acquisition release,
  retired predecessor work and independent old-listener closure before reusing
  its fixed partition with fresh replica and RA instance identities. App-process
  and root cleanup remain required at final deployment shutdown.
- Revisit when: An approved outcome requires app-host recycling, executable
  replacement or a new package/runtime incarnation.
- Evidence: Production runtime awaits owned service disposal before its Close
  reply (`crates/libs/fube-runtime/src/lifecycle.rs`); the fixed app joins its
  original listener (`crates/samples/cluster-stateless-app/src/main.rs`).
  Production Local Node retains the original Hosting acquisition and releases
  that exact capability (`crates/libs/fube-node/src/local.rs`). Activation is
  one-start and owns process/package cleanup
  (`crates/libs/fube-hosting/src/local.rs`). The user-approved 2026-11-04 scope
  is reusable stateless lifecycles without FM/node restart, not package upgrades
  or general host recovery. Native SF pin
  `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` separates
  `FailoverUnitProxy.CloseInstanceAsyncOperation` from host teardown.

The prior [minimal-cluster decisions](./minimal-fm-cluster.md) remain in force:
no automatic reconnect/recovery/failover, arbitrary profiles, stateful
orchestration or full M1/native FM/Federation parity claim. Required correctness
and compatibility qualification are not deferred.

### RSL-002: Normalize included cluster-test formatting
- Status: PROPOSED
- Reason: Implementation-review advisory IR-002 identified indentation and
  compressed assertions in the textually included cluster test file, not a
  functional, assertion-quality or required-check gap. The read-only MVP triage
  classified this as optional readability polish; it is not an approved change
  or a completed fix.
- MVP alternative: Preserve the compiled, inspected lifecycle assertions and
  current execution evidence. Workspace formatting checks pass but do not
  normalize every textually included file; do not imply otherwise.
- Revisit when: A separately approved readability cleanup is requested, or an
  authorized behavioral edit already touches the same test portions.
- Evidence: Source review of `crates/tools/fube-cluster/src/tests.rs`, included
  by `src/lib.rs`, at `b14fc7ceb93ea9a24158f0e70416dbcc891ed73a`
  found substantive stale-proof, two-input-join, replay, history and overflow
  assertions. They passed in the maintained archived suite and focused cluster
  execution. IR-002 remains a nonblocking proposal, not accepted deferral,
  user approval, permission to remove tests or a waiver of required formatting.
