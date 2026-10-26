import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

if not sys.platform.startswith("linux"):
    raise unittest.SkipTest("Ubuntu onebox lifecycle tests require a Linux host")

import linux_cluster_ctl as ctl
import linux_cluster_diagnostics as diagnostics
import onebox_ctl as onebox


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(
        name, ctl.REPO / "docker/linux-cluster" / filename
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


node = load_module("onebox_node_entrypoint", "node-entrypoint.py")
probe = load_module("onebox_cluster_probe", "cluster-probe.py")
NS = {"sf": "http://schemas.microsoft.com/2011/01/fabric"}


class OneboxTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / "test"
        self.directory.mkdir()
        self.cluster = onebox.Onebox(self.directory, "test")
        self.cluster.state = {
            "schema": 1, "name": "test", "project": "sf-onebox-test", "owner": "a" * 32,
            "image": "sha256:" + "b" * 64, "subnet": onebox.DEFAULT_SUBNET,
            "runtime_version": ctl.RUNTIME, "manifest_version": "1",
            "phase": "ready", "baseline": "complete", "publish_gateway": True,
            "shutdown_timeout": ctl.SHUTDOWN_TIMEOUT,
        }
        state = self.cluster.state
        _, _, self.xml = onebox.generate(
            self.cluster.project, state["subnet"], state["image"], state["image"],
            self.directory / "ClusterManifest.xml",
        )
        compose, nodes = onebox.definition(state, self.directory)
        ctl.write(self.directory / "ClusterManifest.xml", self.xml)
        ctl.write_json(self.directory / "compose.json", compose)
        self.cluster.save(nodes=nodes, manifest_sha256=ctl.digest(
            self.directory / "ClusterManifest.xml"
        ))

    def test_single_container_private_ports_external_volume_and_five_node_topology(self):
        compose, nodes = onebox.definition(self.cluster.state, self.directory)
        self.assertEqual(set(compose["services"]), {"onebox"})
        self.assertEqual(compose["volumes"], {
            "data": {"external": True, "name": "sf-onebox-test_data"},
        })
        service = compose["services"]["onebox"]
        self.assertEqual(service["ports"], [
            "127.0.0.1:19080:19080", "127.0.0.1:19000:19000",
        ])
        self.assertEqual(service["image"], self.cluster.state["image"])
        self.assertEqual(service["stop_grace_period"], f"{ctl.SHUTDOWN_TIMEOUT}s")
        self.assertEqual(service["environment"]["SF_LAYOUT"], "onebox")
        self.assertNotIn("HOME", service["environment"])
        self.assertEqual(service["labels"][ctl.OWNER_LABEL], "a" * 32)
        self.assertTrue(service["privileged"])
        self.assertTrue(service["volumes"][1]["read_only"])
        self.assertNotIn("network_mode", service)
        self.assertEqual(len(nodes), 1)
        manifest = ET.fromstring(self.xml)
        infrastructure = manifest.find("sf:Infrastructure/sf:Linux", NS)
        self.assertEqual(infrastructure.get("IsScaleMin"), "true")
        members = infrastructure.findall("sf:NodeList/sf:Node", NS)
        self.assertEqual({item.get("NodeName") for item in members}, set(onebox.NODES))
        self.assertEqual({item.get("IPAddressOrFQDN") for item in members}, {"172.30.91.11"})
        endpoints = manifest.findall("sf:NodeTypes/sf:NodeType/sf:Endpoints/*", NS)
        ports = [item.get("Port") for item in endpoints if item.get("Port") is not None]
        self.assertEqual(len(ports), 30)
        self.assertEqual(len(set(ports)), 30)
        ranges = [set(range(int(item.get("StartPort")), int(item.get("EndPort")) + 1))
                  for item in endpoints if item.get("StartPort") is not None]
        self.assertEqual(sum(map(len, ranges)), len(set().union(*ranges)))
        self.assertFalse(set(map(int, ports)) & set().union(*ranges))

    def test_frozen_inputs_and_separate_cluster_namespace(self):
        self.cluster.load()
        self.assertNotEqual(ctl.Cluster(self.directory, "test").project, self.cluster.project)
        self.assertTrue(self.cluster.image_tag.startswith("sf-apps-onebox:cluster-"))
        compose = json.loads((self.directory / "compose.json").read_text())
        compose["services"]["onebox"]["ports"][0] = "0.0.0.0:19080:19080"
        ctl.write_json(self.directory / "compose.json", compose)
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.cluster.load()

    def test_legacy_onebox_retains_frozen_shutdown_grace_period(self):
        del self.cluster.state["shutdown_timeout"]
        compose = json.loads((self.directory / "compose.json").read_text())
        compose["services"]["onebox"]["stop_grace_period"] = "45s"
        ctl.write_json(self.directory / "compose.json", compose)
        self.cluster.save()
        self.cluster.load()
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "compose") as execute:
            self.cluster.down()
        self.assertEqual([call.args for call in execute.call_args_list],
                         [("stop", "--timeout", "45"), ("down", "--timeout", "45")])

    def test_credentials_are_generated_once_and_retained(self):
        state = self.cluster.state
        _, _, second = onebox.generate(
            self.cluster.project, state["subnet"], state["image"], state["image"],
            self.directory / "ClusterManifest.xml",
        )
        self.assertNotEqual(self.xml, second)
        self.cluster.load()
        self.assertEqual((self.directory / "ClusterManifest.xml").read_text(), self.xml)
        self.assertNotIn("PasswordSecret", json.dumps(state))
        self.assertEqual(self.cluster.path.stat().st_mode & 0o777, 0o600)

    def test_up_retains_one_volume_and_does_not_start_a_tools_container(self):
        self.cluster.save(baseline="not-started")
        with patch.object(self.cluster, "retain_image"), \
                patch.object(self.cluster, "resources"), \
                patch.object(self.cluster, "subnet_check"), \
                patch.object(self.cluster, "helper") as helper, \
                patch.object(self.cluster, "allocate") as allocate, \
                patch.object(self.cluster, "compose") as compose, \
                patch.object(self.cluster, "wait") as wait, \
                patch.object(self.cluster, "establish_baseline") as baseline:
            result = self.cluster.up(900, ["onebox"])
        allocate.assert_not_called()
        baseline.assert_not_called()
        helper.assert_called_once_with(self.cluster.state["nodes"][0], ctl.READ_VOLUME, readonly=True)
        self.assertEqual([call.args for call in compose.call_args_list], [
            ("up", "-d", "--no-build", "--pull", "never", "onebox"),
            ("restart", "--timeout", str(ctl.SHUTDOWN_TIMEOUT), "onebox"),
        ])
        wait.assert_called_once_with(False, stable=0)
        self.assertEqual(self.cluster.state["baseline"], "not-started")
        self.assertEqual(result["nodes"], list(onebox.NODES))
        self.assertEqual(result["client_endpoint"], "127.0.0.1:19000")

    def test_probe_does_not_require_a_baseline_unless_explicitly_requested(self):
        for argument, required in ((None, False), (True, True), (False, False)):
            with self.subTest(argument=argument), \
                    patch.object(self.cluster, "exec", return_value='{"ready":true}') as execute:
                self.cluster.probe(argument)
                self.assertIs(json.loads(execute.call_args.kwargs["data"])["baseline_required"],
                              required)

    def test_retained_baseline_is_not_cleared_or_resubmitted(self):
        for journal in ("not-started", "submitting", "accepted", "complete"):
            with self.subTest(journal=journal):
                self.cluster.save(baseline=journal)
                with patch.object(self.cluster, "retain_image"), \
                        patch.object(self.cluster, "resources"), \
                        patch.object(self.cluster, "subnet_check"), \
                        patch.object(self.cluster, "helper"), \
                        patch.object(self.cluster, "compose"), \
                        patch.object(self.cluster, "wait"), \
                        patch.object(self.cluster, "establish_baseline") as baseline:
                    self.cluster.up(900)
                baseline.assert_not_called()
                self.assertEqual(self.cluster.state["baseline"], journal)

    def test_status_executes_probe_in_the_existing_container(self):
        containers = [{
            "Config": {"Labels": {"com.docker.compose.service": "onebox"}},
            "State": {"Status": "running", "ExitCode": 0},
        }]
        with patch.object(self.cluster, "resources", return_value=(containers, [], [])), \
                patch.object(self.cluster, "probe", return_value={"ready": True}), \
                patch.object(self.cluster, "compose") as compose:
            self.assertTrue(self.cluster.status()["ready"])
            compose.assert_not_called()
        with patch.object(ctl, "run", return_value="") as run:
            self.cluster.exec("sfctl", "cluster", "health")
        self.assertIn("onebox", run.call_args.args[0])
        self.assertNotIn("tools", run.call_args.args[0])

    def test_down_retains_data_and_destroy_requires_exact_confirmation(self):
        volume = self.cluster.state["nodes"][0]["volume_name"]
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "compose") as compose:
            self.assertTrue(self.cluster.down()["data_preserved"])
            self.assertTrue(all("--volumes" not in call.args for call in compose.call_args_list))
        self.cluster.load()
        self.assertEqual(self.cluster.state["phase"], "down")
        with patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "exact cluster name"):
                self.cluster.destroy("wrong")
            run.assert_not_called()
        with patch.object(self.cluster, "resources", return_value=([], [{"Name": volume}], [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose"), patch.object(ctl, "run") as run:
            self.assertTrue(self.cluster.destroy("test")["data_deleted"])
            run.assert_called_once_with(["docker", "volume", "rm", volume])
        self.cluster.load()
        self.assertEqual(self.cluster.state["phase"], "destroyed")
        self.assertFalse((self.directory / "ClusterManifest.xml").exists())

    def test_missing_or_foreign_retained_volume_is_not_replaced(self):
        with patch.object(ctl, "run", side_effect=["", ""]):
            with self.assertRaisesRegex(ValueError, "Missing retained volume"):
                self.cluster.resources()
        volume = self.cluster.state["nodes"][0]["volume_name"]
        with patch.object(ctl, "run", side_effect=[
            "", volume, json.dumps([{"Name": volume, "Labels": {ctl.OWNER_LABEL: "foreign"}}]), "",
        ]):
            with self.assertRaisesRegex(ValueError, "foreign resource"):
                self.cluster.resources()

    def test_create_refuses_existing_allocation(self):
        with patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "already exists"):
                self.cluster.create(onebox.DEFAULT_SUBNET, "image", 900)
            run.assert_not_called()

    def test_create_rejects_the_multi_container_image(self):
        image = {
            "Os": "linux", "Architecture": "amd64",
            "Config": {"Labels": {ctl.IMAGE_LABEL: "1"}},
        }
        fresh = onebox.Onebox(self.directory.parent / "fresh", "fresh")
        with patch.object(ctl, "run", return_value=json.dumps([image])):
            with self.assertRaisesRegex(ValueError, "cluster image first"):
                fresh.create(onebox.DEFAULT_SUBNET, "wrong-image", 900)
        self.assertFalse(fresh.directory.exists())

    def test_diagnostics_use_the_onebox_namespace_and_omit_credentials(self):
        with patch.object(diagnostics, "docker", return_value="") as docker:
            report = diagnostics.collect(
                "test", self.directory.parent, project_prefix=onebox.Onebox.project_prefix,
            )
        self.assertEqual(report["project"], "sf-onebox-test")
        docker.assert_called_once_with(
            "ps", "-aq", "--filter", "label=com.docker.compose.project=sf-onebox-test",
        )
        self.assertNotIn("PasswordSecret", json.dumps(report))

    def test_cli_defaults_and_explicit_subnet(self):
        for arguments, subnet in (([], onebox.DEFAULT_SUBNET), (["--subnet", "172.29.99.0/24"],
                                                               "172.29.99.0/24")):
            with self.subTest(subnet=subnet), patch.object(sys, "argv", [
                "onebox_ctl.py", "--state-root", str(self.directory.parent), "create", "cli-test",
                *arguments,
            ]), patch.object(ctl, "check_local_docker"), patch.object(ctl.os, "umask"), \
                    patch.object(sys, "stdout", new_callable=io.StringIO), \
                    patch.object(onebox.Onebox, "create", return_value={"ready": True}) as create:
                self.assertEqual(onebox.main(), 0)
                create.assert_called_once_with(subnet, "sf-apps-onebox:" + ctl.RUNTIME, 900)

    def test_entrypoint_layout_and_all_five_retained_deployments(self):
        identity = self.cluster.state["nodes"][0]
        environment = {"SF_" + key.upper(): value for key, value in identity.items() if key != "phase"}
        expected = node.expected_identity(environment)
        self.assertEqual(expected["layout"], "onebox")
        node.validate_manifest(self.directory / "ClusterManifest.xml", expected)
        for name in onebox.NODES:
            folder = self.directory / "Data" / name / "Fabric"
            folder.mkdir(parents=True)
            (folder / "Fabric.Package.current.xml").write_text("<ServicePackage />")
            (folder / "ClusterManifest.current.xml").write_text(self.xml)
        node.validate_deployment(self.directory, expected)
        (self.directory / "Data/N0050/Fabric/Fabric.Package.current.xml").unlink()
        with self.assertRaisesRegex(ValueError, "exactly the assigned"):
            node.validate_deployment(self.directory, expected)

    def test_entrypoint_rejects_wrong_layout_address_members_or_duplicate_ports(self):
        identity = self.cluster.state["nodes"][0]
        manifest = self.directory / "ClusterManifest.xml"
        for before, after in (
            ('IsScaleMin="true"', 'IsScaleMin="false"'),
            ('NodeName="N0050"', 'NodeName="N0040"'),
            ('Port="19010"', 'Port="19000"'),
            ('NodeTypeRef="N0050Ref"', 'NodeTypeRef="N0040Ref"'),
            ('IPAddressOrFQDN="172.30.91.11"', 'IPAddressOrFQDN="172.30.91.12"'),
        ):
            with self.subTest(change=before):
                manifest.write_text(self.xml.replace(before, after))
                with self.assertRaises(ValueError):
                    node.validate_manifest(manifest, identity)

    def observations(self, address, path, deadline):
        members = probe.expected_members(self.xml)
        if path == "/Nodes":
            return {"Items": [{
                "Name": name, "IpAddressOrFQDN": member["node_ip"], "Type": member["node_type"],
                "NodeStatus": "Up", "HealthState": "Ok", "CodeVersion": ctl.RUNTIME,
                "ConfigVersion": "1", "IsSeedNode": True,
                "FaultDomain": member["fault_domain"], "UpgradeDomain": member["upgrade_domain"],
                "NodeDeactivationInfo": {"NodeDeactivationStatus": "None"},
            } for name, member in members.items()]}
        if path == "/$/GetClusterManifest":
            return {"Manifest": self.xml}
        if path == "/$/GetClusterHealth":
            return {"AggregatedHealthState": "Ok"}
        if path == "/$/GetUpgradeProgress":
            return {"UpgradeState": "RollingForwardCompleted", "CodeVersion": ctl.RUNTIME,
                    "ConfigVersion": "1", "RollingUpgradeMode": "Monitored"}
        if path == "/Applications":
            return {"Items": []}
        return {"Items": [
            {"NodeName": name, "ReplicaStatus": "Ready", "HealthState": "Ok",
             "ReplicaRole": "Primary" if index == 0 else "ActiveSecondary"}
            for index, name in enumerate(onebox.NODES[:3])
        ]}

    def test_readiness_checks_all_five_gateways_and_three_replica_subset(self):
        with patch.object(probe, "get", side_effect=self.observations) as get:
            result = probe.snapshot(self.cluster.state, self.xml)
        self.assertTrue(result["ready"], result["problems"])
        self.assertEqual(set(result["gateways"]), set(onebox.NODES))
        addresses = {call.args[0] for call in get.call_args_list if call.args[1] == "/Nodes"}
        self.assertEqual(addresses, {
            "172.30.91.11", "172.30.91.11:19082", "172.30.91.11:19084",
            "172.30.91.11:19086", "172.30.91.11:19088",
        })

    def test_bootstrap_upgrade_placeholder_does_not_block_onebox_readiness(self):
        def get(address, path, deadline):
            if path == "/$/GetUpgradeProgress":
                return {
                    "UpgradeState": "RollingForwardCompleted", "CodeVersion": "0.0.0.0",
                    "ConfigVersion": "", "RollingUpgradeMode": "Invalid",
                }
            return self.observations(address, path, deadline)

        with patch.object(probe, "get", side_effect=get):
            result = probe.snapshot(self.cluster.state, self.xml, baseline_required=False)
            self.assertTrue(result["ready"], result["problems"])
            self.assertTrue(all(gateway["baseline"] == "uninitialized"
                                for gateway in result["gateways"].values()))
            self.assertFalse(probe.snapshot(self.cluster.state, self.xml)["ready"])

    def test_no_baseline_does_not_bypass_node_version_or_health_checks(self):
        def get(address, path, deadline):
            result = self.observations(address, path, deadline)
            if path == "/Nodes":
                result["Items"][0].update(changes)
            return result

        for changes in (
            {"CodeVersion": "0.0.0.0"}, {"ConfigVersion": "other"},
            {"NodeStatus": "Down"}, {"HealthState": "Warning"},
        ):
            with self.subTest(changes=changes), patch.object(probe, "get", side_effect=get):
                self.assertFalse(probe.snapshot(
                    self.cluster.state, self.xml, baseline_required=False,
                )["ready"])

    def test_one_unreachable_native_gateway_fails_readiness(self):
        def get(address, path, deadline):
            if address.endswith(":19088"):
                raise OSError("gateway unavailable")
            return self.observations(address, path, deadline)

        with patch.object(probe, "get", side_effect=get):
            result = probe.snapshot(self.cluster.state, self.xml)
        self.assertFalse(result["ready"])
        self.assertIn("error", result["gateways"]["N0050"])

    def test_probe_uses_the_requested_gateway_port_without_http_proxy(self):
        for address, authority in (
            ("172.30.91.11", "172.30.91.11:19080"),
            ("172.30.91.11:19088", "172.30.91.11:19088"),
        ):
            with self.subTest(address=address), \
                    patch.object(probe.urllib.request, "build_opener") as build:
                opener = build.return_value
                opener.open.return_value.__enter__.return_value = io.StringIO('{"Items":[]}')
                self.assertEqual(probe.get(address, "/Nodes", time.monotonic() + 10), {"Items": []})
                self.assertEqual(opener.open.call_args.args[0].full_url,
                                 f"http://{authority}/Nodes?api-version=6.0")
                self.assertEqual(build.call_args.args[0].proxies, {})

    def test_duplicate_or_foreign_system_replica_is_not_healthy(self):
        def get(address, path, deadline):
            result = self.observations(address, path, deadline)
            if "/GetReplicas" in path:
                result["Items"][2]["NodeName"] = replacement
            return result

        for replacement in ("N0010", "foreign"):
            with self.subTest(node=replacement), patch.object(probe, "get", side_effect=get):
                self.assertFalse(probe.snapshot(self.cluster.state, self.xml)["ready"])

    def test_healthy_standby_is_allowed_but_never_counts_as_an_active_replica(self):
        standby = {
            "NodeName": "N0040", "ReplicaRole": "IdleSecondary",
            "ReplicaStatus": "Standby", "HealthState": "Ok",
        }

        def get(address, path, deadline):
            result = self.observations(address, path, deadline)
            if "/GetReplicas" in path:
                result["Items"].append(dict(standby))
            return result

        with patch.object(probe, "get", side_effect=get):
            self.assertTrue(probe.snapshot(self.cluster.state, self.xml)["ready"])
            for changes in (
                {"ReplicaStatus": "InBuild"}, {"HealthState": "Warning"},
                {"NodeName": "foreign"}, {"NodeName": "N0010"}, {"ReplicaRole": "Primary"},
            ):
                with self.subTest(changes=changes):
                    original = dict(standby)
                    standby.update(changes)
                    self.assertFalse(probe.snapshot(self.cluster.state, self.xml)["ready"])
                    standby.update(original)


if __name__ == "__main__":
    unittest.main()
