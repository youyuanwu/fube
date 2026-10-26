import copy
from contextlib import nullcontext
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

if not sys.platform.startswith("linux"):
    raise unittest.SkipTest("Linux cluster lifecycle tests require a Linux host")

import linux_cluster_ctl as ctl
import linux_cluster_diagnostics as diagnostics
from linux_cluster import generate


PROBE = ctl.REPO / "docker/linux-cluster/cluster-probe.py"
spec = importlib.util.spec_from_file_location("linux_cluster_probe", PROBE)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class CommandLineTests(unittest.TestCase):
    def test_create_default_subnet_and_explicit_override(self):
        for arguments, expected in (
            ([], "172.30.90.0/24"),
            (["--subnet", "172.29.99.0/24"], "172.29.99.0/24"),
        ):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as temporary:
                with patch.object(sys, "argv", [
                    "linux_cluster_ctl.py", "--state-root", temporary, "create", "test",
                    *arguments,
                ]), patch.object(sys, "stdout", new_callable=io.StringIO), \
                        patch.object(ctl.os, "umask"), patch.object(ctl, "check_local_docker"), \
                        patch.object(ctl.Cluster, "create", return_value={"ready": True}) as create:
                    self.assertEqual(ctl.main(), 0)
                create.assert_called_once_with(expected, "sf-apps-node:11.8.121.4", 900)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / "test"
        self.directory.mkdir(mode=0o700)
        self.cluster = ctl.Cluster(self.directory, "test")
        self.cluster.state = {
            "schema": 1, "name": "test", "project": "sf-linux-test", "owner": "a" * 32,
            "image": "sha256:" + "b" * 64, "subnet": "172.29.99.0/24",
            "runtime_version": ctl.RUNTIME, "manifest_version": "1",
            "phase": "ready", "baseline": "complete", "publish_gateway": True,
            "shutdown_timeout": ctl.SHUTDOWN_TIMEOUT,
        }
        compose, nodes = ctl.definition(self.cluster.state, self.directory)
        self.cluster.state["nodes"] = nodes
        _, _, manifest = generate("sf-linux-test", "172.29.99.0/24",
                                  self.cluster.state["image"], self.cluster.state["image"],
                                  self.directory / "ClusterManifest.xml")
        ctl.write(self.directory / "ClusterManifest.xml", manifest)
        ctl.write_json(self.directory / "compose.json", compose)
        self.cluster.save(manifest_sha256=ctl.digest(self.directory / "ClusterManifest.xml"))

    def test_external_volumes_immutable_image_and_on_demand_tools(self):
        compose, nodes = ctl.definition(self.cluster.state, self.directory)
        for name, volume in compose["volumes"].items():
            self.assertEqual(volume, {"external": True, "name": "sf-linux-test_" + name})
        for node in nodes:
            service = compose["services"][node["node_name"]]
            self.assertEqual(service["image"], self.cluster.state["image"])
            self.assertEqual(service["labels"], {ctl.OWNER_LABEL: "a" * 32})
            self.assertEqual(service["stop_grace_period"], f"{ctl.SHUTDOWN_TIMEOUT}s")
            self.assertNotIn("profiles", service)
            self.assertNotIn("network_mode", service)
            if node["node_name"] == "n0":
                self.assertEqual(service["ports"], ["127.0.0.1:19080:19080"])
            else:
                self.assertNotIn("ports", service)
        tools = compose["services"]["tools"]
        self.assertEqual(tools["profiles"], ["admin"])
        self.assertNotIn("ports", tools)
        self.assertEqual(tools["environment"]["HOME"], "/tmp/sfctl-home")
        self.assertTrue(tools["volumes"][0]["read_only"])

    def test_legacy_allocation_retains_frozen_shutdown_grace_period(self):
        del self.cluster.state["shutdown_timeout"]
        compose = json.loads((self.directory / "compose.json").read_text())
        for name in ctl.NODES:
            compose["services"][name]["stop_grace_period"] = "45s"
        ctl.write_json(self.directory / "compose.json", compose)
        self.cluster.save()
        self.cluster.load()
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "compose") as execute:
            self.cluster.down()
        self.assertEqual([call.args for call in execute.call_args_list],
                         [("stop", "--timeout", "45"), ("down", "--timeout", "45")])
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose") as execute:
            self.cluster.destroy("test")
        execute.assert_called_once_with("down", "--timeout", "45")

    def test_compose_command_deadline_exceeds_container_shutdown_deadline(self):
        with patch.object(ctl, "run") as run:
            self.cluster.compose("stop", "--timeout", str(ctl.SHUTDOWN_TIMEOUT))
        self.assertGreater(run.call_args.kwargs["timeout"], ctl.SHUTDOWN_TIMEOUT)

    def test_legacy_allocation_retains_unpublished_gateway(self):
        del self.cluster.state["publish_gateway"]
        compose, _ = ctl.definition(self.cluster.state, self.directory)
        self.assertTrue(all("ports" not in service for service in compose["services"].values()))
        ctl.write_json(self.directory / "compose.json", compose)
        self.cluster.save()
        self.cluster.load()
        self.assertNotIn("publish_gateway", self.cluster.state)

    def test_edited_gateway_binding_rejected(self):
        compose = json.loads((self.directory / "compose.json").read_text())
        compose["services"]["n0"]["ports"] = ["0.0.0.0:19080:19080"]
        ctl.write_json(self.directory / "compose.json", compose)
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.cluster.load()

    def test_private_atomic_state_and_frozen_inputs(self):
        self.cluster.load()
        self.assertEqual(self.cluster.path.stat().st_mode & 0o777, 0o600)
        manifest = self.directory / "ClusterManifest.xml"
        manifest.write_text(manifest.read_text().replace('Value="3"', 'Value="4"'))
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.cluster.load()

    def test_edited_compose_or_inventory_rejected(self):
        compose = json.loads((self.directory / "compose.json").read_text())
        compose["services"]["n0"]["network_mode"] = "host"
        ctl.write_json(self.directory / "compose.json", compose)
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            self.cluster.load()
        self.cluster.state["nodes"][0]["volume_name"] = "foreign-volume"
        self.cluster.save()
        with self.assertRaisesRegex(ValueError, "allocation differs"):
            self.cluster.load()

    def test_lock_exclusion_and_unsafe_names(self):
        with ctl.locked(self.root, "test"):
            with self.assertRaisesRegex(ValueError, "holds this cluster"):
                with ctl.locked(self.root, "test"):
                    self.fail("Competing lock was accepted")
        for name in ("../other", "TEST", "", "a" * 41, "two names", "-start"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                ctl.validate_name(name)
        (self.root / "linked").symlink_to(self.directory)
        with self.assertRaisesRegex(ValueError, "symlink"):
            with ctl.locked(self.root, "linked"):
                self.fail("Symlink was accepted")

    def test_missing_retained_volume_never_replaced(self):
        with patch.object(ctl, "run", side_effect=["", ""]):
            with self.assertRaisesRegex(ValueError, "Missing retained volume"):
                self.cluster.resources()

    def test_default_subnet_rejects_route_and_docker_network_collisions(self):
        self.cluster.state["subnet"] = ctl.DEFAULT_SUBNET
        network = {"Name": "other", "IPAM": {"Config": [{"Subnet": ctl.DEFAULT_SUBNET}]}}
        for responses in (
            [json.dumps([{"dst": ctl.DEFAULT_SUBNET}]), ""],
            ["[]", "network-id", json.dumps([network])],
        ):
            with self.subTest(responses=responses), patch.object(ctl, "run", side_effect=responses):
                with self.assertRaisesRegex(ValueError, "overlaps"):
                    self.cluster.subnet_check()

    def test_foreign_volume_rejected(self):
        volume = self.cluster.state["nodes"][0]["volume_name"]
        with patch.object(ctl, "run", side_effect=[
            "", volume, json.dumps([{"Name": volume, "Labels": {ctl.OWNER_LABEL: "other"}}]), "",
        ]):
            with self.assertRaisesRegex(ValueError, "foreign resource"):
                self.cluster.resources(require_volumes=False)

    def test_wrong_container_image_rejected(self):
        container = {
            "Name": "sf-linux-test-n0-1", "Image": "wrong",
            "Config": {"Labels": {ctl.OWNER_LABEL: "a" * 32,
                                  "com.docker.compose.service": "n0"}},
        }
        with patch.object(ctl, "run", side_effect=["container", json.dumps([container]), "", ""]):
            with self.assertRaisesRegex(ValueError, "container or image"):
                self.cluster.resources(require_volumes=False)

    def test_down_preserves_all_volumes_and_allocation_progress(self):
        for phase in ("ready", "allocating"):
            self.cluster.save(phase=phase)
            with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                    patch.object(self.cluster, "compose") as compose:
                result = self.cluster.down()
            self.assertTrue(result["data_preserved"])
            self.assertEqual([call.args for call in compose.call_args_list],
                             [("stop", "--timeout", str(ctl.SHUTDOWN_TIMEOUT)),
                              ("down", "--timeout", str(ctl.SHUTDOWN_TIMEOUT))])
            self.assertEqual(self.cluster.state["phase"], "allocating" if phase == "allocating" else "down")

    def test_failed_shutdown_is_reported_after_preserving_data(self):
        container = {
            "Config": {"Labels": {"com.docker.compose.service": "n1"}},
            "State": {"ExitCode": 137},
        }
        with patch.object(self.cluster, "resources", return_value=([container], [], [])), \
                patch.object(self.cluster, "compose"):
            with self.assertRaisesRegex(ValueError, "not clean"):
                self.cluster.down()
        self.assertEqual(self.cluster.state["phase"], "down")

    def test_destroy_requires_exact_confirmation_before_any_command(self):
        with patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "exact cluster name"):
                self.cluster.destroy("other")
            run.assert_not_called()

    def test_destroy_only_removes_owned_volumes_and_leaves_tombstone(self):
        volumes = [{"Name": node["volume_name"]} for node in self.cluster.state["nodes"]]
        with patch.object(self.cluster, "resources", return_value=([], volumes, [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose") as compose, patch.object(ctl, "run") as run:
            result = self.cluster.destroy("test")
        self.assertTrue(result["data_deleted"])
        compose.assert_called_once_with("down", "--timeout", str(ctl.SHUTDOWN_TIMEOUT))
        self.assertEqual([call.args[0] for call in run.call_args_list],
                         [["docker", "volume", "rm", volume["Name"]] for volume in volumes])
        self.assertFalse((self.directory / "ClusterManifest.xml").exists())
        self.assertTrue(self.cluster.path.exists())
        self.cluster.load()
        self.assertEqual(self.cluster.state["phase"], "destroyed")
        with patch.object(ctl, "run") as run:
            self.cluster.destroy("test")
            run.assert_not_called()

    def test_create_after_destroy_has_new_ownership_and_credentials(self):
        original = dict(self.cluster.state)
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose"):
            self.cluster.destroy("test")
        image = {
            "Id": original["image"], "Os": "linux", "Architecture": "amd64",
            "Config": {"Labels": {ctl.IMAGE_LABEL: "1"}},
        }
        with patch.object(ctl, "run", side_effect=[json.dumps([image]), "", "", "", ""]), \
                patch.object(self.cluster, "subnet_check"), \
                patch.object(self.cluster, "up", return_value={"ready": True}) as up:
            self.cluster.create("172.29.99.0/24", "image", 900)
        up.assert_called_once_with(900)
        self.assertNotEqual(self.cluster.state["owner"], original["owner"])
        self.assertNotEqual(self.cluster.state["manifest_sha256"], original["manifest_sha256"])
        self.assertEqual(self.cluster.state["baseline"], "not-started")
        self.assertIs(self.cluster.state["publish_gateway"], True)
        self.assertEqual(self.cluster.state["shutdown_timeout"], ctl.SHUTDOWN_TIMEOUT)
        compose = json.loads((self.directory / "compose.json").read_text())
        self.assertEqual(compose["services"]["n0"]["ports"], ["127.0.0.1:19080:19080"])

    def test_interrupted_destroy_is_resumable_after_one_volume_removed(self):
        volumes = [{"Name": node["volume_name"]} for node in self.cluster.state["nodes"]]
        failure = subprocess.CalledProcessError(1, ["docker", "volume", "rm"], stderr="in use")
        with patch.object(self.cluster, "resources", return_value=([], volumes, [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose"), \
                patch.object(ctl, "run", side_effect=["", failure]):
            with self.assertRaises(subprocess.CalledProcessError):
                self.cluster.destroy("test")
        self.cluster.load()
        self.assertEqual(self.cluster.state["phase"], "destroying")
        with patch.object(self.cluster, "resources", return_value=([], volumes[1:], [])), \
                patch.object(self.cluster, "image_tag_id", return_value=""), \
                patch.object(self.cluster, "compose"), patch.object(ctl, "run") as run:
            self.cluster.destroy("test")
        self.assertEqual(run.call_count, 2)

    def test_partial_destroy_never_resumes_as_fresh_cluster(self):
        self.cluster.save(phase="destroying")
        with patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "cannot resume"):
                self.cluster.up(60)
            run.assert_not_called()

    def test_complete_or_accepted_baseline_is_not_submitted_twice(self):
        for state in ("active", "complete"):
            self.cluster.save(baseline="submitting")
            with patch.object(self.cluster, "probe", return_value={
                "gateways": {name: {"baseline": state} for name in ctl.NODES}
            }), patch.object(self.cluster, "sfctl") as sfctl:
                self.cluster.establish_baseline()
                sfctl.assert_not_called()
            self.assertEqual(self.cluster.state["baseline"], "accepted" if state == "active" else "complete")

    def test_ambiguous_submission_is_not_retried(self):
        self.cluster.save(baseline="submitting")
        with patch.object(self.cluster, "probe", return_value={
            "gateways": {name: {"baseline": "uninitialized"} for name in ctl.NODES}
        }), patch.object(self.cluster, "sfctl") as sfctl:
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                self.cluster.establish_baseline()
            sfctl.assert_not_called()

    def test_provisioned_versions_are_reused_before_first_submission(self):
        self.cluster.save(baseline="not-started")
        with patch.object(self.cluster, "probe", return_value={
            "gateways": {name: {"baseline": "uninitialized"} for name in ctl.NODES}
        }), patch.object(self.cluster, "exec"), patch.object(self.cluster, "sfctl", side_effect=[
            "", json.dumps([{"codeVersion": ctl.RUNTIME}]), '[{"configVersion":"1"}]', "",
        ]) as sfctl:
            self.cluster.establish_baseline()
        calls = [call.args[:2] for call in sfctl.call_args_list]
        self.assertNotIn(("cluster", "provision"), calls)
        self.assertNotIn(("application", "upload"), calls)
        self.assertEqual(calls.count(("cluster", "upgrade")), 1)
        self.assertEqual(self.cluster.state["baseline"], "accepted")

    def test_tools_cleanup_runs_even_after_deadline_or_failure(self):
        with patch.object(self.cluster, "compose") as compose:
            with self.assertRaisesRegex(ValueError, "probe failed"):
                with self.cluster.tools():
                    self.cluster.deadline = 0
                    raise ValueError("probe failed")
        self.assertEqual(compose.call_args.args, ("rm", "--stop", "--force", "tools"))

    def test_status_does_not_start_nodes_or_tools_when_stopped(self):
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "compose") as compose:
            result = self.cluster.status()
            compose.assert_not_called()
        self.assertFalse(result["ready"])

    def test_replayed_create_refuses_existing_cluster_before_docker(self):
        with patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "already exists"):
                self.cluster.create("172.29.99.0/24", "image", 900)
            run.assert_not_called()

    def test_up_checks_retained_identities_and_waits_without_allocating(self):
        with patch.object(self.cluster, "resources"), patch.object(self.cluster, "subnet_check"), \
                patch.object(self.cluster, "retain_image") as retain, \
                patch.object(self.cluster, "allocate") as allocate, \
                patch.object(self.cluster, "helper") as helper, \
                patch.object(self.cluster, "compose") as compose, \
                patch.object(self.cluster, "tools", return_value=nullcontext()), \
                patch.object(self.cluster, "wait") as wait, \
                patch.object(self.cluster, "establish_baseline") as baseline:
            result = self.cluster.up(900, ["n2"])
        self.assertTrue(result["ready"])
        baseline.assert_called_once_with()
        self.assertEqual(self.cluster.state["baseline"], "complete")
        retain.assert_called_once()
        allocate.assert_not_called()
        self.assertEqual(helper.call_count, 3)
        self.assertTrue(all(call.args[1] == ctl.READ_VOLUME and call.kwargs["readonly"]
                            for call in helper.call_args_list))
        self.assertEqual(compose.call_args_list[0].args,
                         ("up", "-d", "--no-build", "--pull", "never", *ctl.NODES))
        self.assertEqual(compose.call_args_list[1].args,
                         ("restart", "--timeout", str(ctl.SHUTDOWN_TIMEOUT), "n2"))
        self.assertEqual(wait.call_args_list[0].args, (False,))
        self.assertEqual(wait.call_args_list[1].args, (True,))

    def test_default_probe_still_requires_the_multi_container_baseline(self):
        with patch.object(self.cluster, "exec", return_value='{"ready":true}') as execute:
            self.cluster.probe()
        self.assertIs(json.loads(execute.call_args.kwargs["data"])["baseline_required"], True)

    def test_wait_requires_full_stability_window_and_resets_after_unhealthy_observation(self):
        observations = [{"ready": ready, "problems": [], "gateways": {}}
                        for ready in (True, False, True, True, True)]
        with patch.object(self.cluster, "probe", side_effect=observations) as snapshot, \
                patch.object(ctl.time, "monotonic", side_effect=[0, 20, 40, 69.99, 70]), \
                patch.object(ctl.time, "sleep"):
            self.cluster.wait(True)
        self.assertEqual(snapshot.call_count, 5)

    def test_allocation_race_cannot_seed_a_foreign_volume(self):
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(ctl, "run", side_effect=[
                    "", json.dumps([{"Labels": {ctl.OWNER_LABEL: "other"}}]),
                ]), patch.object(self.cluster, "helper") as helper:
            with self.assertRaisesRegex(ValueError, "collided"):
                self.cluster.allocate()
            helper.assert_not_called()

    def test_retained_image_tag_protects_the_frozen_id_from_public_tag_rebuilds(self):
        with patch.object(ctl, "run", side_effect=["", ""]) as run:
            self.cluster.retain_image()
        self.assertEqual(run.call_args.args[0], [
            "docker", "image", "tag", self.cluster.state["image"], self.cluster.image_tag,
        ])
        with patch.object(ctl, "run", return_value=self.cluster.state["image"]) as run:
            self.cluster.retain_image()
            self.assertEqual(run.call_count, 1)
        with patch.object(ctl, "run", return_value="sha256:foreign") as run:
            with self.assertRaisesRegex(ValueError, "frozen image identity"):
                self.cluster.retain_image()
            self.assertEqual(run.call_count, 1)

    def test_destroy_removes_only_the_cluster_image_tag_not_the_source_tag_or_id(self):
        with patch.object(self.cluster, "resources", return_value=([], [], [])), \
                patch.object(self.cluster, "image_tag_id", return_value=self.cluster.state["image"]), \
                patch.object(self.cluster, "compose"), patch.object(ctl, "run") as run:
            self.cluster.destroy("test")
        run.assert_called_once_with(["docker", "image", "rm", self.cluster.image_tag])

    def test_volume_seed_is_repeatable_but_never_overwrites_data(self):
        volume = self.root / "volume"
        volume.mkdir()
        node = self.cluster.state["nodes"][0]
        seed = ctl.SEED_VOLUME.replace("'/var/lib/sf-node'", repr(str(volume)))
        arguments = [sys.executable, "-O", "-c", seed]
        for _ in range(2):
            result = subprocess.run(arguments, input=json.dumps(node), text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        original = (volume / "identity.json").read_bytes()
        (volume / "retained-data").write_text("keep")
        result = subprocess.run(arguments, input=json.dumps(node), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((volume / "identity.json").read_bytes(), original)
        self.assertEqual((volume / "retained-data").read_text(), "keep")


class DiagnosticsTests(unittest.TestCase):
    def test_only_allowlisted_metadata_is_exported(self):
        observation = {
            "ready": False, "problems": ["private error detail"],
            "gateways": {"n0": {
                "error": "private error detail", "baseline": "complete",
                "Manifest": "PRIVATE_CREDENTIAL",
                "health": {"AggregatedHealthState": "Warning",
                           "HealthEvents": [{"Description": "PRIVATE_CREDENTIAL"}]},
                "upgrade": {"UpgradeState": "RollingForwardCompleted",
                            "UpgradeStatusDetails": "PRIVATE_CREDENTIAL"},
                "nodes": {"Items": [{"Name": "n0", "NodeStatus": "Up",
                                     "private": "PRIVATE_CREDENTIAL"}]},
            }},
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "test"
            directory.mkdir()
            (directory / "state.json").write_text(json.dumps({
                "phase": "ready", "credentials": "PRIVATE_CREDENTIAL",
            }))
            (directory / "last-status.json").write_text(json.dumps(observation))
            (directory / "ClusterManifest.xml").write_text("PRIVATE_CREDENTIAL")
            container = {
                "Name": "sf-linux-test-n0-1", "Image": "sha256:node",
                "Config": {"Env": ["PASSWORD=PRIVATE_CREDENTIAL"]},
                "State": {"Status": "running", "Running": True, "ExitCode": 0,
                          "Health": {"Log": ["PRIVATE_CREDENTIAL"]}},
            }
            statistics = {"Name": "sf-linux-test-n0-1", "MemUsage": "800MiB / 16GiB",
                          "private": "PRIVATE_CREDENTIAL"}
            with patch.object(diagnostics, "docker", side_effect=[
                "id0", json.dumps([container]), json.dumps(statistics),
            ]) as docker:
                report = diagnostics.collect("test", root)
        encoded = json.dumps(report)
        self.assertNotIn("PRIVATE_CREDENTIAL", encoded)
        self.assertNotIn("private error detail", encoded)
        self.assertFalse(report["errors"])
        self.assertEqual(report["observation"]["problem_count"], 1)
        self.assertTrue(report["observation"]["gateways"]["n0"]["query_failed"])
        self.assertEqual(report["containers"][0]["state"]["Status"], "running")
        self.assertEqual(docker.call_args_list[0].args,
                         ("ps", "-aq", "--filter", "label=com.docker.compose.project=sf-linux-test"))

    def test_no_containers_never_falls_back_to_global_docker_stats(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(diagnostics, "docker", return_value="") as docker:
            report = diagnostics.collect("test", Path(temporary))
        self.assertFalse(report["allocation"]["available"])
        self.assertEqual(report["containers"], [])
        self.assertEqual(docker.call_count, 1)

    def test_partial_allocation_and_diagnostic_failures_are_explicit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "test").mkdir()
            (root / "test/state.json").write_text("{")
            with patch.object(diagnostics, "docker", side_effect=subprocess.TimeoutExpired("docker", 20)):
                report = diagnostics.collect("test", root)
        self.assertEqual(report["errors"], [
            "state.json: JSONDecodeError", "Docker diagnostics: TimeoutExpired",
        ])


class DockerContextTests(unittest.TestCase):
    def test_remote_docker_host_is_rejected_before_connecting(self):
        with patch.dict(ctl.os.environ, {"DOCKER_HOST": "ssh://other-host"}, clear=True), \
                patch.object(ctl, "run") as run:
            with self.assertRaisesRegex(ValueError, "remote Docker"):
                ctl.check_local_docker()
            run.assert_not_called()

    def test_explicit_context_takes_precedence_over_docker_host(self):
        with patch.dict(ctl.os.environ, {
            "DOCKER_HOST": "ssh://unused", "DOCKER_CONTEXT": "local",
        }, clear=True), patch.object(ctl, "run", side_effect=[
            '[{"Endpoints":{"docker":{"Host":"unix:///var/run/docker.sock"}}}]', "linux/x86_64",
        ]) as run:
            ctl.check_local_docker()
            self.assertEqual(run.call_args_list[0].args[0], ["docker", "context", "inspect", "local"])

    def test_wrong_daemon_platform_is_rejected(self):
        with patch.dict(ctl.os.environ, {"DOCKER_HOST": "unix:///var/run/docker.sock"}, clear=True), \
                patch.object(ctl, "run", return_value="linux/aarch64"):
            with self.assertRaisesRegex(ValueError, "x86-64"):
                ctl.check_local_docker()


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        _, inventory, self.xml = generate(
            "sf-linux-test", "172.29.99.0/24", "image", "image", Path("/manifest.xml")
        )
        self.state = {**inventory, "runtime_version": ctl.RUNTIME}
        self.nodes = [{
            "Name": node["node_name"], "IpAddressOrFQDN": node["node_ip"],
            "NodeStatus": "Up", "HealthState": "Ok", "CodeVersion": ctl.RUNTIME,
            "ConfigVersion": "1", "IsSeedNode": True, "Type": "LinuxNodeType",
            "FaultDomain": f"fd:/host0/c{index}", "UpgradeDomain": f"UD{index}",
            "NodeDeactivationInfo": {"NodeDeactivationStatus": "None"},
        } for index, node in enumerate(inventory["nodes"])]
        self.upgrade = {
            "UpgradeState": "RollingForwardCompleted", "CodeVersion": ctl.RUNTIME,
            "ConfigVersion": "1", "RollingUpgradeMode": "Monitored",
        }
        self.overrides = {}

    def get(self, address, path, deadline):
        if path in self.overrides:
            return self.overrides[path]
        if path == "/Nodes":
            return {"Items": self.nodes}
        if path == "/$/GetClusterManifest":
            return {"Manifest": self.xml}
        if path == "/$/GetClusterHealth":
            return {"AggregatedHealthState": "Ok"}
        if path == "/$/GetUpgradeProgress":
            return self.upgrade
        if path == "/Applications":
            return {"Items": []}
        return {"Items": [
            {"NodeName": name, "ReplicaStatus": "Ready", "HealthState": "Ok",
             "ReplicaRole": "Primary" if index == 0 else "ActiveSecondary"}
            for index, name in enumerate(ctl.NODES)
        ]}

    def snapshot(self, baseline_required=True):
        with patch.object(probe, "get", side_effect=self.get):
            return probe.snapshot(self.state, self.xml, baseline_required)

    def test_requires_healthy_nodes_gateways_and_system_replicas(self):
        self.assertTrue(self.snapshot()["ready"])
        self.nodes[0]["NodeStatus"] = "Down"
        self.assertFalse(self.snapshot()["ready"])

    def test_rejects_wrong_identity_version_seed_or_disabled_node(self):
        changes = {"IpAddressOrFQDN": "127.0.0.1", "CodeVersion": "other",
                   "ConfigVersion": "2", "IsSeedNode": False, "Type": "other",
                   "FaultDomain": "fd:/other", "UpgradeDomain": "UD4",
                   "NodeDeactivationInfo": {"NodeDeactivationStatus": "Completed"}}
        original = copy.deepcopy(self.nodes)
        for key, value in changes.items():
            with self.subTest(field=key):
                self.nodes = copy.deepcopy(original)
                self.nodes[0][key] = value
                self.assertFalse(self.snapshot()["ready"])

    def test_pagination_and_extra_members_never_pass(self):
        self.overrides["/Nodes"] = {"Items": self.nodes, "ContinuationToken": "more"}
        self.assertFalse(self.snapshot()["ready"])
        self.overrides.clear()
        self.nodes.append(dict(self.nodes[0], Name="n3"))
        self.assertFalse(self.snapshot()["ready"])

    def test_health_warnings_and_unhealthy_applications_fail(self):
        self.overrides["/$/GetClusterHealth"] = {"AggregatedHealthState": "Warning"}
        self.assertFalse(self.snapshot()["ready"])
        self.overrides.clear()
        self.overrides["/Applications"] = {"Items": [{"Status": "Ready", "HealthState": "Error"}]}
        self.assertFalse(self.snapshot()["ready"])

    def test_missing_system_replica_or_second_primary_fails(self):
        path = f"/Partitions/{probe.PARTITIONS['Naming']}/$/GetReplicas"
        self.overrides[path] = {"Items": [{"NodeName": "n0", "ReplicaStatus": "Ready", "ReplicaRole": "Primary"}]}
        self.assertFalse(self.snapshot()["ready"])
        self.overrides[path] = {"Items": [
            {"NodeName": name, "ReplicaStatus": "Ready", "ReplicaRole": "Primary"} for name in ctl.NODES
        ]}
        self.assertFalse(self.snapshot()["ready"])

    def test_bootstrap_placeholder_is_not_a_completed_native_baseline(self):
        self.upgrade.update(CodeVersion="0.0.0.0", ConfigVersion="", RollingUpgradeMode="Invalid")
        self.assertTrue(self.snapshot(False)["ready"])
        self.assertFalse(self.snapshot(True)["ready"])
        self.assertEqual(probe.baseline_state(self.upgrade, ctl.RUNTIME, "1"), "uninitialized")

    def test_rollback_or_different_upgrade_is_not_success(self):
        for change in ({"UpgradeState": "RollingBackCompleted"}, {"ConfigVersion": "2"},
                       {"RollingUpgradeMode": "UnmonitoredAuto"}):
            upgrade = dict(self.upgrade, **change)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "Unexpected native upgrade"):
                probe.baseline_state(upgrade, ctl.RUNTIME, "1")

    def test_semantic_comparison_ignores_only_proven_defaults_and_order(self):
        xml = '<Linux IsScaleMin="false"><Node IsSeedNode="false" NodeName="n3"/><Node NodeName="n0" IsSeedNode="true"/></Linux>'
        reordered = '<Linux><Node IsSeedNode="true" NodeName="n0"/><Node NodeName="n3"/></Linux>'
        self.assertEqual(probe.semantic_manifest(xml), probe.semantic_manifest(reordered))
        self.assertNotEqual(probe.semantic_manifest(xml),
                            probe.semantic_manifest(reordered.replace('IsSeedNode="true"', 'IsSeedNode="false"')))

    def test_manifest_setting_drift_or_unreachable_gateway_fails(self):
        self.overrides["/$/GetClusterManifest"] = {"Manifest": self.xml.replace('Value="3"', 'Value="4"')}
        self.assertFalse(self.snapshot()["ready"])
        with patch.object(probe, "get", side_effect=OSError("connection refused")):
            result = probe.snapshot(self.state, self.xml)
        self.assertFalse(result["ready"])
        self.assertTrue(any("connection refused" in error for error in result["problems"]))


if __name__ == "__main__":
    unittest.main()
