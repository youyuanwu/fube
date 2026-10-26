# Migration provenance

This repository imports the Fabric client, transport, runtime/host documentation,
standalone stateless host and native-runtime fixture from
[`youyuanwu/service-fabric-apps-rs`](https://github.com/youyuanwu/service-fabric-apps-rs).

The agreed source range is **inclusive**:

- First commit: `f06e8b28f3e6930c8f336c645843f0c634c8aa49` (initial Rust client POC).
- Last commit: `3fad8553f532589031235c66d64a652de051cf44` (manifest test host).

The first imported commit contains the selected files as they existed at the
first revision, including prerequisite transport documentation and onebox
support that predate it. All 28 commits in the range are retained with their
authors, author/committer dates, and original messages, followed by
`Source-Repository` and `Source-Commit` trailers. Filtered trees/parents change
commit IDs and invalidate original signatures; no signature preservation is
claimed. Old PR numbers in imported commit messages refer to the source repo.

The filtered history was merged without rewriting fube's existing initial
commit or license. For example:

```sh
git log --all --grep='Source-Commit: f06e8b28f3e6930c8f336c645843f0c634c8aa49'
git log --follow -- crates/libs/mssf-client/src/lib.rs
```

Post-import integration removes unrelated workspace members/dependencies,
replaces app-specific CI/build recipes, and updates documentation links.
The client, transport, host, and native-app source code is otherwise unchanged.
The original MIT notice is retained in `LICENSE.service-fabric-apps-rs`.

## Repository boundary

fube owns the native-wire client/transport and standalone host, their complete
tests/designs, native acceptance scripts/workflow, and a simple single-container
Linux onebox. Shared lifecycle scripts, bootstrap/probe code, and regression
fixtures are included so onebox remains self-contained; historical names and
resource identities remain stable. No multi-container Dockerfile, multi-container
CI job, kvmap, or replicator implementation is imported.

The original repository retains `kvmap`, `mssf-ext`, `mssf-rplctr`, their packaging
and Windows deployment workflow, and the multi-container cluster. Shared
infrastructure stays there as needed; moved client/host code and onebox-specific
entry points are removed there. No generated cluster state, native libraries,
build output, or local workflow artifacts are migrated.
