"""Resolved production-graph and supported-consumer boundary regressions."""
import json
import os
from pathlib import Path
import subprocess
import unittest
import re


ROOT = Path(__file__).resolve().parents[2]


class RuntimeReplicationBoundary(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        metadata = subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=ROOT, env={**os.environ, "CARGO_BUILD_JOBS": "1"},
            check=True, capture_output=True, text=True,
        )
        cls.metadata = json.loads(metadata.stdout)
        cls.ids = {package["name"]: package["id"] for package in cls.metadata["packages"]}
        cls.nodes = {node["id"]: node for node in cls.metadata["resolve"]["nodes"]}

    def normal_dependencies(self, package):
        return {
            dependency["pkg"]
            for dependency in self.nodes[package]["deps"]
            if any(kind["kind"] is None for kind in dependency["dep_kinds"])
        }

    def production_closure(self, name):
        seen, pending = set(), [self.ids[name]]
        while pending:
            package = pending.pop()
            if package not in seen:
                seen.add(package)
                pending.extend(self.normal_dependencies(package) - seen)
        return seen

    def test_replication_is_below_runtime_and_core_is_neutral(self):
        self.assertIn(self.ids["fube-data"], self.normal_dependencies(self.ids["fube-runtime"]))
        self.assertNotIn(self.ids["fube-runtime"], self.production_closure("fube-data"))
        for package in ("fube-runtime", "fube-data"):
            self.assertNotIn(self.ids[package], self.production_closure("fube-core"))
        for source in (ROOT / "crates/libs/fube-data/src").glob("*.rs"):
            self.assertNotRegex(
                source.read_text(),
                r"\b(fube_runtime|NativeRuntimeIntegration|NativeInvocation|ReplicaDiagnostics|NativeControlBinding)\b",
                str(source),
            )

    def test_shared_rap_harness_has_no_direct_data_dependency_or_api(self):
        self.assertNotIn(
            self.ids["fube-data"], self.normal_dependencies(self.ids["fube-rap-test-support"])
        )
        for package in ("fube-core", "fube-data", "fube-runtime", "fube-txn", "rust-runtime-app"):
            self.assertNotIn(self.ids["fube-rap-test-support"], self.production_closure(package))
        for source in (ROOT / "crates/libs/fube-rap-test-support/src").glob("*.rs"):
            self.assertNotRegex(source.read_text(), r"\bfube_data\b", str(source))

    def test_transaction_consumers_have_no_lower_implementation_hooks(self):
        package = next(p for p in self.metadata["packages"] if p["name"] == "fube-txn")
        for dependency in package["dependencies"]:
            self.assertNotIn("qualification", dependency["features"])
            self.assertNotIn("test-hooks", dependency["features"])
            if dependency["name"] == "fube-rap-test-support":
                self.assertEqual(dependency["kind"], "dev")
        root = ROOT / "crates/libs/fube-txn"
        for source in (*root.joinpath("src").rglob("*.rs"), *root.joinpath("tests").rglob("*.rs")):
            self.assertNotRegex(
                source.read_text(),
                r"fube_data::(?:hosting|engine|FabricReplicator)|\bReplicatorDiagnostics\b|"
                r"feature\s*=\s*\"(?:qualification|test-hooks)\"",
                str(source),
            )

    def test_factory_endpoint_never_published_by_live_owner_views(self):
        hosting = (ROOT / "crates/libs/fube-data/src/hosting.rs").read_text()
        facade = (ROOT / "crates/libs/fube-data/src/fabric_replicator.rs").read_text()
        runtime = (ROOT / "crates/libs/fube-runtime/src/replication.rs").read_text()
        self.assertIn("Result<(Arc<FabricReplicator>, HostingEndpoint)>", hosting)
        self.assertRegex(hosting, r"pub struct HostingEndpoint\s*\{\s*engine:")
        self.assertNotRegex(facade, r"\bHostingEndpoint\b")
        public_facade_impl = hosting.split("impl FabricReplicator {", 1)[1].split(
            "impl HostingEndpoint {", 1
        )[0]
        self.assertNotRegex(public_facade_impl, r"pub fn (configure|build|remove)\b")
        self.assertRegex(runtime, r"\n\s+hosting: HostingEndpoint,")
        for source in (ROOT / "crates/libs/fube-data/src").glob("*.rs"):
            self.assertNotRegex(
                source.read_text(), r"(?:pub|pub\(crate\)) fn \w+\([^)]*\)[^{;]*->\s*(?:Arc<)?HostingEndpoint",
                str(source),
            )
        # Real rustdoc compile-fail witnesses accompany structural graph checks:
        # they refer to existing public types/methods, not missing imports.
        for method in ("configure", "build", "remove"):
            self.assertIn(f"r.diagnostics().{method}(", runtime)
        self.assertEqual(len(re.findall(r"compile_fail,E0599", runtime)), 3)


if __name__ == "__main__":
    unittest.main()
