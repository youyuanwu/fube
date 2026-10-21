import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from linux_cluster import TEMPLATE, check_subnet, generate, prepare


class LinuxClusterHarnessTests(unittest.TestCase):
    def generate(self, project="test-cluster", subnet="172.30.80.0/24"):
        return generate(project, subnet, "sha256:node", "node@sha256:pinned",
                        Path("/validation/ClusterManifest.xml"))

    def test_three_unique_nodes_and_private_storage(self):
        compose, inventory, manifest = self.generate()
        self.assertEqual(set(compose["services"]), {"n0", "n1", "n2", "tools"})
        self.assertEqual(len({node["node_ip"] for node in inventory["nodes"]}), 3)
        self.assertEqual(len({node["volume_name"] for node in inventory["nodes"]}), 3)
        self.assertFalse(inventory["ssh_enabled"])
        for node in inventory["nodes"]:
            service = compose["services"][node["node_name"]]
            self.assertNotIn("ports", service)
            self.assertEqual(service["image"], "node@sha256:pinned")
            self.assertTrue(service["volumes"][1]["read_only"])
            self.assertEqual(service["environment"]["SF_IMAGE_ID"], "sha256:node")
        namespace = {"sf": "http://schemas.microsoft.com/2011/01/fabric"}
        infrastructure = ET.fromstring(manifest).find("sf:Infrastructure/sf:Linux", namespace)
        self.assertEqual(infrastructure.get("IsScaleMin"), "false")
        self.assertEqual(len(infrastructure.findall("sf:NodeList/sf:Node", namespace)), 3)
        self.assertTrue(all(node.get("IsSeedNode") == "true" for node in infrastructure[0]))

    def test_generated_credentials_are_not_reused_or_in_inventory(self):
        _, first_inventory, first_manifest = self.generate()
        _, second_inventory, second_manifest = self.generate()
        self.assertNotEqual(first_manifest, second_manifest)
        self.assertNotEqual(first_inventory["manifest_sha256"],
                            second_inventory["manifest_sha256"])
        self.assertNotIn("PasswordSecret", json.dumps(first_inventory))
        self.assertNotIn('Value="Foo"', first_manifest)

    def test_rejects_route_and_docker_network_collisions(self):
        for existing in ("172.30.0.0/16", "172.30.80.0/25", "172.30.80.11/32"):
            with self.subTest(existing=existing):
                with self.assertRaisesRegex(ValueError, "overlaps"):
                    check_subnet("172.30.80.0/24", [existing])
        check_subnet("172.30.80.0/24", ["172.18.0.0/16", "::1/128"])

    def test_rejects_public_loopback_or_too_small_subnets(self):
        for subnet in ("8.8.8.0/24", "127.0.0.0/24", "172.30.80.0/29",
                       "0.0.0.0/24", "169.254.0.0/24"):
            with self.subTest(subnet=subnet):
                with self.assertRaises(ValueError):
                    self.generate(subnet=subnet)

    def test_rejects_invalid_project_name(self):
        for project in ("", "../cluster", "cluster name", "UPPER"):
            with self.subTest(project=project):
                with self.assertRaisesRegex(ValueError, "project name"):
                    self.generate(project=project)

    def test_existing_output_is_rejected_before_docker_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch("linux_cluster.output") as output:
                with self.assertRaisesRegex(ValueError, "already exists"):
                    prepare("cluster", "172.30.80.0/24", "image", Path(temporary))
                output.assert_not_called()

    def test_smoke_explicit_port_is_outside_dynamic_application_range(self):
        namespace = {"sf": "http://schemas.microsoft.com/2011/01/fabric"}
        smoke = ET.parse(TEMPLATE.parent / "smoke/LinuxClusterSmokePackage/ServiceManifest.xml")
        template = ET.parse(TEMPLATE)
        endpoint = smoke.find("sf:Resources/sf:Endpoints/sf:Endpoint", namespace)
        dynamic = template.find("sf:NodeTypes/sf:NodeType/sf:Endpoints/sf:ApplicationEndpoints",
                                namespace)
        port = int(endpoint.get("Port"))
        self.assertFalse(int(dynamic.get("StartPort")) <= port <= int(dynamic.get("EndPort")))
        self.assertEqual(smoke.find("sf:CodePackage/sf:EntryPoint/sf:ExeHost/sf:Arguments",
                                    namespace).text, str(port))

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux file permissions")
    def test_prepare_pins_image_protects_manifest_and_only_allocates(self):
        responses = [
            "[]", "network-id", '[{"IPAM":{"Config":[]}}]',
            '[{"Id":"sha256:node","RepoDigests":["node@sha256:pinned"]}]', "", "",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "new-run"
            with patch("linux_cluster.output", side_effect=responses):
                with patch("linux_cluster.subprocess.run") as run:
                    prepare("test-cluster", "172.30.80.0/24", "node", destination)
            self.assertEqual((destination / "ClusterManifest.xml").stat().st_mode & 0o777,
                             0o600)
            self.assertEqual(run.call_count, 4)
            self.assertIn("create", run.call_args_list[0].args[0])
            self.assertNotIn("up", run.call_args_list[0].args[0])
            for call in run.call_args_list[1:]:
                identity = json.loads(call.kwargs["input"])
                self.assertEqual(identity["phase"], "allocated")
                self.assertIn("test-cluster_", identity["volume_name"])
            volume = Path(temporary) / "volume"
            volume.mkdir()
            seed = run.call_args_list[1].args[0][-1].replace(
                "'/var/lib/sf-node'", repr(str(volume))
            )
            identity = run.call_args_list[1].kwargs["input"]
            first = subprocess.run([sys.executable, "-O", "-c", seed], input=identity,
                                   capture_output=True, text=True, timeout=5)
            self.assertEqual(first.returncode, 0, first.stderr)
            original = (volume / "identity.json").read_bytes()
            replay = subprocess.run([sys.executable, "-O", "-c", seed], input=identity,
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(replay.returncode, 1)
            self.assertIn("refusing identity reuse", replay.stderr)
            self.assertEqual((volume / "identity.json").read_bytes(), original)

    def test_existing_node_volume_is_rejected_before_creating_resources(self):
        responses = [
            "[]", "network-id", '[{"IPAM":{"Config":[]}}]',
            '[{"Id":"sha256:node","RepoDigests":["node@sha256:pinned"]}]',
            "test-cluster_n0-data",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "new-run"
            with patch("linux_cluster.output", side_effect=responses):
                with patch("linux_cluster.subprocess.run") as run:
                    with self.assertRaisesRegex(ValueError, "identity reuse"):
                        prepare("test-cluster", "172.30.80.0/24", "node", destination)
                    run.assert_not_called()
            self.assertFalse(destination.exists())

    def test_locally_built_image_is_pinned_by_image_id(self):
        responses = [
            "[]", "network-id", '[{"IPAM":{"Config":[]}}]',
            '[{"Id":"sha256:local","RepoDigests":[]}]', "", "",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "local-run"
            with patch("linux_cluster.output", side_effect=responses), \
                    patch("linux_cluster.subprocess.run"):
                prepare("test-cluster", "172.30.80.0/24", "node", destination)
            inventory = json.loads((destination / "inventory.json").read_text())
            self.assertEqual(inventory["image"], "sha256:local")


if __name__ == "__main__":
    unittest.main()
