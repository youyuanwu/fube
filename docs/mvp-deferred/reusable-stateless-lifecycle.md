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
