import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from tests import run, suites
from tests.onebox.client_runtime import __main__ as client_runtime
from tests.onebox.native_data import __main__ as native_data


class PartitionTests(unittest.TestCase):
    def invoke(self, runner, results=None):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "archive.tar.zst"
            archive.write_bytes(b"test archive")
            arguments = ["--name", "owned", "--owner", "a" * 32, "--prebuilt", str(root / "bundle"),
                         "--bundle-tar", str(root / "bundle.tar"),
                         "--producer", str(root / "producer.json"), "--report-dir", str(root / "proof"),
                         "--archive", str(archive), "--config-dir", str(root / "private"),
                         "--fixture-owner", "ci-100-1"]
            with patch.object(run.subprocess, "run", side_effect=results,
                              return_value=subprocess.CompletedProcess([], 0)) as execute, \
                    patch("tests.onebox.shared.fixture.run",
                          side_effect=lambda selected, args, execute: execute(selected, args, suites.digest())), \
                    contextlib.redirect_stdout(io.StringIO()) as output, \
                    contextlib.redirect_stderr(output):
                status = runner.main(arguments)
            receipt = json.loads((root / "proof/suite-outcomes.json").read_text())
            return status, receipt, execute.call_args_list, output.getvalue()

    def test_client_runtime_runs_complete_rust_profile(self):
        status, receipt, calls, _ = self.invoke(client_runtime)
        self.assertEqual(status, 0)
        self.assertEqual(list(receipt["suites"]), ["native_clients", "rust_data", "legacy"])
        commands = [call.args[0] for call in calls]
        self.assertEqual(commands[0][commands[0].index("-E") + 1], "group(onebox)")
        self.assertEqual(commands[0][-2:], ["--run-ignored", "all"])
        self.assertEqual(commands[1][commands[1].index("--profile") + 1], "rust")
        self.assertEqual(commands[2][2:4], ["tests.onebox.shared.ci", "legacy"])
        self.assertIn("--producer", commands[2])
        self.assertEqual(calls[0].kwargs["env"]["MSSF_TEST_ARCHIVE_SHA256"],
                         receipt["suites"]["native_clients"]["archive_sha256"])
        self.assertEqual(receipt["manifest_sha256"], suites.digest())

    def test_native_data_runs_complete_native_profile(self):
        status, receipt, calls, _ = self.invoke(native_data)
        self.assertEqual(status, 0)
        self.assertEqual(list(receipt["suites"]), ["native_data"])
        command = calls[0].args[0]
        self.assertEqual(command[command.index("--profile") + 1], "native-data")
        self.assertEqual(command[command.index("--existing-owner") + 1], "a" * 32)
        self.assertEqual(calls[0].kwargs["timeout"], suites.MANIFEST.suites["native_data"]["timeout"])

    def test_legacy_keeps_strict_adapter_and_failure_exit(self):
        status, receipt, calls, output = self.invoke(client_runtime, [
            subprocess.CompletedProcess([], 3), subprocess.TimeoutExpired(["child"], 1),
            subprocess.CompletedProcess([], 0)])
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 3, "a failed suite must not skip the following whole suites")
        self.assertEqual(receipt["status"], "FAIL")
        self.assertEqual(receipt["suites"]["native_clients"]["outcome"], "failure")
        self.assertEqual(receipt["suites"]["rust_data"], "failure")
        self.assertEqual(receipt["suites"]["legacy"], "success")
        self.assertEqual(len(receipt["errors"]), 2)
        self.assertNotIn("a" * 32, output)

    def test_partition_cannot_select_another_profile(self):
        for runner, other in ((client_runtime, "native-data"), (native_data, "client-runtime")):
            with self.subTest(partition=runner.__package__), \
                    patch.object(run.subprocess, "run") as execute, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    runner.main(["--partition", other])
                self.assertEqual(error.exception.code, 2)
                execute.assert_not_called()

    def test_report_collision_rejects_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "suite-outcomes.json"
            report.write_text("original")
            with patch.object(run.subprocess, "run") as execute, contextlib.redirect_stderr(io.StringIO()):
                status = run.main(["--suite", "native_data", "--name", "owned", "--owner", "capability",
                                   "--prebuilt", temporary, "--report-dir", temporary])
            self.assertEqual(status, 1)
            execute.assert_not_called()
            self.assertEqual(report.read_text(), "original")

    def test_empty_unittest_selection_fails_without_launching(self):
        with patch.object(run.unittest.TestLoader, "discover", return_value=unittest.TestSuite()), \
                patch.object(run.subprocess, "run") as execute, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(run.main(["--suite", "cluster_models"]), 1)
        execute.assert_not_called()


class ManifestTests(unittest.TestCase):
    def source(self):
        return tomllib.loads(suites.PATH.read_text())

    def test_reassigning_a_whole_suite_generates_another_worker(self):
        value = self.source()
        value["partitions"]["client-runtime"]["suites"].remove("legacy")
        value["partitions"]["legacy"] = {"id": "lg", "timeout_minutes": 90, "suites": ["legacy"]}
        manifest = suites.Manifest(value)
        matrix = manifest.matrix()["include"]
        self.assertEqual([entry["route"] for entry in matrix], ["client-runtime", "native-data", "legacy"])
        self.assertEqual([entry["archive"] for entry in matrix], [True, False, False])
        self.assertEqual(set(manifest.coverage()[0]), set(suites.MANIFEST.coverage()[0]))
        self.assertEqual(manifest.coverage()[1], suites.MANIFEST.coverage()[1])

    def test_invalid_assignments_and_unknown_runner_options_fail(self):
        mutations = (
            lambda v: v["partitions"]["native-data"]["suites"].append("missing"),
            lambda v: v["partitions"]["native-data"]["suites"].append("rust_data"),
            lambda v: v["partitions"]["client-runtime"]["suites"].remove("legacy"),
            lambda v: v["suites"]["native_data"].update(command="echo success"),
            lambda v: v["suites"]["rust_data"].update(profile="missing"),
            lambda v: v["suites"]["rust_data"].update(profile="mixed"),
            lambda v: v["suites"]["rust_data"].update(timeout=True),
            lambda v: v["suites"]["legacy"].update(report="../cleanup.json"),
            lambda v: v["scenario_groups"]["common"].append("legacy_full"),
        )
        for mutation in mutations:
            value = copy.deepcopy(self.source())
            mutation(value)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "suite manifest"):
                suites.Manifest(value)
