import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ENTRYPOINT = Path(__file__).resolve().parents[1] / "docker/linux-cluster/node-entrypoint.py"
TEMPLATE = ENTRYPOINT.with_name("ClusterManifest.template.xml")


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux node entrypoint")
class LinuxNodeEntrypointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("linux_node_entrypoint", ENTRYPOINT)
        cls.node = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.node)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.identity = {
            "cluster_id": "test-cluster",
            "node_name": "n0",
            "node_ip": "172.30.80.11",
            "image_id": "sha256:test",
            "runtime_version": "11.8.121.4",
            "volume_name": "test-n0-data",
        }

    def manifest(self, version="1"):
        from string import Template

        return Template(TEMPLATE.read_text()).substitute(
            CLUSTER_NAME="test-cluster",
            N0_IP="172.30.80.11",
            N1_IP="172.30.80.12",
            N2_IP="172.30.80.13",
            FSS_PRIMARY_SECRET="test-primary",
            FSS_SECONDARY_SECRET="test-secondary",
        ).replace('Version="1"', f'Version="{version}"', 1)

    def write_record(self, phase="allocated", **changes):
        record = {**self.identity, "phase": phase, **changes}
        self.node.save_identity(self.root, record)
        return record

    def write_deployment(self, version="1"):
        folder = self.root / "Data/n0/Fabric"
        folder.mkdir(parents=True)
        (folder / "Fabric.Package.current.xml").write_text("<ServicePackage />")
        (folder / "ClusterManifest.current.xml").write_text(self.manifest(version))
        return folder

    def test_missing_volume_identity_is_not_implicit_bootstrap(self):
        with self.assertRaisesRegex(ValueError, "Missing volume identity"):
            self.node.read_identity(self.root, self.identity)

    def test_allocated_empty_volume_is_accepted(self):
        record = self.write_record()
        self.assertEqual(self.node.read_identity(self.root, self.identity), record)

    def test_wrong_node_image_volume_or_cluster_is_rejected(self):
        for field in self.identity:
            with self.subTest(field=field):
                self.write_record(**{field: "wrong"})
                with self.assertRaisesRegex(ValueError, "Volume identity"):
                    self.node.read_identity(self.root, self.identity)

    def test_partial_bootstrap_and_unmarked_data_are_preserved(self):
        self.write_record("configuring")
        with self.assertRaisesRegex(ValueError, "Interrupted bootstrap"):
            self.node.read_identity(self.root, self.identity)
        self.write_record()
        (self.root / "Data").mkdir()
        with self.assertRaisesRegex(ValueError, "contains data"):
            self.node.read_identity(self.root, self.identity)
        self.assertTrue((self.root / "Data").is_dir())

    def test_manifest_requires_non_scale_min_unique_local_identity(self):
        path = self.root / "manifest.xml"
        path.write_text(self.manifest())
        self.node.validate_manifest(path, self.identity)
        for before, after in [
            ('IsScaleMin="false"', 'IsScaleMin="true"'),
            ('Name="test-cluster"', 'Name="other-cluster"'),
            ('NodeName="n0"', 'NodeName="other"'),
            ("172.30.80.12", "172.30.80.11"),
            ('NodeName="n1"', 'NodeName="n2"'),
        ]:
            with self.subTest(change=before):
                path.write_text(self.manifest().replace(before, after))
                with self.assertRaises(ValueError):
                    self.node.validate_manifest(path, self.identity)

    def test_restart_uses_current_manifest_not_bootstrap(self):
        record = self.write_record("configured")
        folder = self.write_deployment(version="2")
        with patch.object(self.node, "MANIFEST", self.root / "nonexistent-bootstrap.xml"):
            with patch.object(self.node, "run") as run:
                self.node.prepare_deployment(self.root, self.identity, record)
        run.assert_called_once_with(
            [self.node.CODE / "FabricDeployer.sh"], cwd=self.node.CODE
        )
        self.assertIn('Version="2"', (folder / "ClusterManifest.current.xml").read_text())
        self.assertEqual(json.loads((self.root / "identity.json").read_text())["phase"],
                         "configured")

    def test_serialized_manifest_may_omit_default_false_scale_min(self):
        path = self.root / "manifest.xml"
        path.write_text(self.manifest().replace(' IsScaleMin="false"', ""))
        self.node.validate_manifest(path, self.identity)

    def test_missing_or_duplicate_local_deployment_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "exactly the assigned"):
            self.node.validate_deployment(self.root, self.identity)
        self.write_deployment()
        other = self.root / "Data/n1/Fabric"
        other.mkdir(parents=True)
        (other / "Fabric.Package.current.xml").write_text("<ServicePackage />")
        with self.assertRaisesRegex(ValueError, "exactly the assigned"):
            self.node.validate_deployment(self.root, self.identity)

    def test_pending_target_information_does_not_run_deployer(self):
        record = self.write_record("configured")
        self.write_deployment()
        (self.root / "Data/TargetInformation.xml").write_text("<TargetInformation />")
        with patch.object(self.node, "run") as run:
            with self.assertRaisesRegex(ValueError, "explicit recovery"):
                self.node.prepare_deployment(self.root, self.identity, record)
        run.assert_not_called()

    def test_failed_create_records_interrupted_state(self):
        record = self.write_record()
        manifest = self.root / "manifest.xml"
        manifest.write_text(self.manifest())
        with patch.object(self.node, "MANIFEST", manifest):
            with patch.object(self.node, "run", side_effect=RuntimeError("deployer failed")):
                with self.assertRaisesRegex(RuntimeError, "deployer failed"):
                    self.node.prepare_deployment(self.root, self.identity, record)
        self.assertEqual(json.loads((self.root / "identity.json").read_text())["phase"],
                         "configuring")

    def test_fresh_ownership_does_not_follow_runtime_binary_symlinks(self):
        folder = self.write_deployment()
        target = self.root / "installed-runtime"
        target.write_text("runtime")
        (folder / "Fabric").symlink_to(target)
        with patch.object(self.node.os, "chown") as chown:
            self.node.own_fresh_deployment(self.root / "Data", [999, 999])
        self.assertTrue(chown.called)
        for call in chown.call_args_list:
            self.assertEqual(call.kwargs, {"follow_symlinks": False})
            self.assertNotEqual(call.args[0], target)

    def test_loopback_and_invalid_node_names_are_rejected(self):
        environment = {"SF_" + key.upper(): value for key, value in self.identity.items()}
        self.assertEqual(self.node.expected_identity(environment), self.identity)
        for field, value in [("SF_NODE_IP", "127.0.0.1"), ("SF_NODE_IP", "0.0.0.0"),
                             ("SF_NODE_NAME", "../n0")]:
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    self.node.expected_identity({**environment, field: value})

    def wrapper(self, command, auxiliary=None):
        source = (
            "import runpy,subprocess,sys;"
            f"entry=runpy.run_path({str(ENTRYPOINT)!r});"
            f"aux=[subprocess.Popen({auxiliary!r})] if {auxiliary is not None!r} else [];"
            f"sys.exit(entry['supervise']({command!r},aux))"
        )
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", source],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        return process

    def test_supervisor_forwards_sigterm(self):
        command = [sys.executable, "-u", "-c",
                   "import time;time.sleep(.2);print('ready',flush=True);time.sleep(60)"]
        process = self.wrapper(command)
        self.assertEqual(process.stdout.readline().strip(), "ready")
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, (stdout, stderr))

    def test_unexpected_host_or_auxiliary_exit_is_a_failure(self):
        for command, auxiliary in [
            ([sys.executable, "-c", "pass"], None),
            ([sys.executable, "-c", "import time;time.sleep(60)"],
             [sys.executable, "-c", "pass"]),
        ]:
            with self.subTest(auxiliary=auxiliary):
                process = self.wrapper(command, auxiliary)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 1, (stdout, stderr))
                self.assertIn("ERROR:", stderr)


if __name__ == "__main__":
    unittest.main()
