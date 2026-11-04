# Bounded native-free stateless cluster

This tool owns one in-memory FM and exactly two child processes running the
existing local RA/Hosting/runtime. It supports only the fixed
`ClusterStatelessApp/ClusterService/Code/ClusterStatelessType` package. This is
not native Federation interoperability, full M1, recovery, failover or a general
service host.

Build both executables, then run:

```sh
fube-cluster --root /absolute/private/profile-root \
  --app /absolute/cluster-stateless-app
```

The root is operator-owned and private (0700). Its parent must already exist.
All materialized packages, runtime roots, node controls and scratch stay beneath
it. Only trusted numeric-loopback communication is supported. There is no
arbitrary package or executable input in the FM protocol.
The supplied fixed app executable is bounded to 256 MiB. EOF also requests
orderly shutdown (including after operation-identity capacity is exhausted).

After the `ready` JSON line, stdin accepts one JSON object per line:

```json
{"op":"create-1","action":"create","node":1}
{"op":"inspect-1","action":"status"}
{"op":"delete-1","action":"delete","node":1}
{"op":"shutdown","action":"quit"}
```

Create/delete return `pending` after submission, not serving success. Issue a
new status operation to observe later progress. Retry an identical operation ID
to retrieve its retained outcome; reuse with different content is rejected.
Status operation retries return their original snapshot. At most 64 operation
identities are retained without eviction, with 4096-byte input lines, 64-KiB
output objects and strict
unknown-field rejection. Each node gets one creation lifetime, even after
delete/failure/loss. A reported endpoint is numeric loopback; independently send
exactly `identity\n` and read its bounded JSON/newline identity response.

Native NodeUp/Ack and the final empty ReplicaUp/Reply precede RA construction.
FM learns the actual generated package version from the original node's bounded
private startup metadata, not a lifecycle ACK. Required Add/Remove and selected
report/type exchanges use existing native factories, full schemas, generation
and recipient actors. Native PToP header 0x8013 carries exact sender/recipient
NodeInstance (pinned SF `3988b451`, `Federation/PToPHeader.h`,
`PToPActor.h`, `Transport/MessageHeaderId.h`). No incarnation field is invented
inside NodeUp. A one-use launch capability on the original child's stdin admits
one original Channel/session; it is local trusted admission, not authentication
for an untrusted multi-user network or evidence of readiness. No reconnect is
provided.

Link loss leaves desired authority retained and observation unknown; the app can
still be serving. Stop uses independent original-child control. Nodes retain
their original Activation/app capabilities, perform real disposal/physical
cleanup, close executor/store and publish source-bound receipts. The foreground
reaps its original child/group handles and independently verifies app/runtime
absence. Unsupported/staged effects and semantic obligations are counted,
retained and never acknowledged merely because a write or send pass finished.
Original external-effect escrow is 32 slots; reply receipts, scheduling
completions and semantic pending maps have separate finite bounds.

**Always reuse the same profile root for this ownership domain.** An exclusive
lock and atomically published dirty marker precede startup. Only verified
original cleanup and physical removal publish a clean predecessor handoff,
matching the separate authority marker and retaining both source-bound cleanup
receipts. A pre-activation child may explicitly prove no Activation was acquired;
missing/abruptly lost children cannot supply that receipt.
Owner/node loss or missing proof leaves the marker dirty and refuses relaunch;
there is no force/reset option. A different root is a different ownership/trust
domain, not a supported way around uncertain old authority. Do not remove the
small interlock files to bypass failed cleanup.

## Qualification contract

No nested Cargo invocation, native fixture or implicit build happens in tests.
The parent runner builds both binaries first and provides existing owned roots:

```sh
export FUBE_CLUSTER_APP="$CARGO_TARGET_DIR/debug/cluster-stateless-app"
export FUBE_CLUSTER_NODE_EXE="$CARGO_TARGET_DIR/debug/fube-cluster"
export FUBE_CLUSTER_TEST_ROOT="/absolute/task-owned/runs"
cargo test --locked -p fube-cluster -- --test-threads=1
```

Missing executables/root are failures, not silently passing skips. Unit tests
protect exact semantic identities, bootstrap/catalog admission, duplicate and
capacity behavior. Real executable-node tests fault the original Channel and
original OwnedApp capability, checking uncertainty with a live endpoint, stale
generation rejection, sibling isolation, partial-start rollback, lost-node
receipt refusal and clean-vs-dirty relaunch. The CLI test launches the real
foreground executable and both real nodes; its endpoint oracle is captured from
original launch/activation metadata before create. It verifies all actual app
identity fields, rejects a swapped sibling, observes disposal and independently
checks process/runtime removal. Dirty lost-owner fixtures intentionally retain
their failed-handoff domain and evidence. These tests are native-free evidence, not
deployed native FM/Federation qualification.
