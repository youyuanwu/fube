"""Preparation/preflight model tests; not native interoperability evidence."""

import hashlib
import io
import pathlib
import shutil
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from tests import ROOT
from tests.native_runtime import runner as profile


class PreparationTests(unittest.TestCase):
    def test_hash_mismatch_preserves_existing_archive_and_does_not_extract(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            package = root / f"servicefabric_{profile.VERSION}.deb"
            package.write_bytes(b"invalid existing user input")
            with patch.object(profile.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    profile.prepare(root)
                run.assert_not_called()
            self.assertEqual(package.read_bytes(), b"invalid existing user input")
            self.assertFalse((root / "runtime").exists())

    def test_wrong_library_hash_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            for name in profile.LIBRARY_HASHES:
                (root / name).write_bytes(b"not a native library")
            with self.assertRaisesRegex(ValueError, "library hash"):
                profile.qualify_libraries(root)

    def test_hash_reader_streams_real_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            file = pathlib.Path(temporary) / "data"
            file.write_bytes(b"fixture")
            self.assertEqual(profile.digest(file), hashlib.sha256(b"fixture").hexdigest())


class DiagnosticTests(unittest.TestCase):
    def test_every_scenario_writes_bounded_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            for scenario in profile.SCENARIOS:
                profile.save_diagnostics(root, scenario, b'MSSF_REPORT {"real":"payload"}\n')
                for suffix in ("log", "json"):
                    self.assertLessEqual((root / f"{scenario}.{suffix}").stat().st_size,
                                         profile.DIAGNOSTIC_LIMIT)
            self.assertEqual(len(list(root.iterdir())), len(profile.SCENARIOS) * 2)

    def test_unknown_diagnostic_scenarios_cannot_write_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "not-created"
            for scenario in ("../escape", "/absolute", "unknown"):
                with self.assertRaisesRegex(ValueError, "unsupported diagnostic scenario"):
                    profile.save_diagnostics(root, scenario, b"failure")
            self.assertFalse(root.exists())

    def test_bounded_tail_and_original_report_not_runtime_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            output = (b"x" * (profile.DIAGNOSTIC_LIMIT + 100)
                      + b'\nMSSF_REPORT {"scenario":"close","cleanup":{"reaped":true}}\n')
            profile.save_diagnostics(root, "close", output)
            self.assertEqual((root / "close.log").read_bytes(),
                             output[-profile.DIAGNOSTIC_LIMIT:])
            self.assertEqual((root / "close.json").read_bytes(),
                             b'{"scenario":"close","cleanup":{"reaped":true}}\n')
            self.assertEqual(sorted(path.name for path in root.iterdir()),
                             ["close.json", "close.log"])

    def test_failure_without_report_removes_stale_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "abort.json").write_bytes(b'{"stale":true}')
            profile.save_diagnostics(root, "abort", b"native scenario FAILED\n")
            self.assertFalse((root / "abort.json").exists())
            self.assertEqual((root / "abort.log").read_bytes(), b"native scenario FAILED\n")

    def test_oversize_report_fails_and_keeps_bounded_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            with self.assertRaisesRegex(ValueError, "size limit"):
                profile.save_diagnostics(root, "bootstrap",
                                         b"MSSF_REPORT " + b"x" * profile.DIAGNOSTIC_LIMIT)
            self.assertEqual((root / "bootstrap.log").stat().st_size,
                             profile.DIAGNOSTIC_LIMIT)
            self.assertFalse((root / "bootstrap.json").exists())

    def test_host_nonzero_exit_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            result = subprocess.CompletedProcess(["host"], 17, b"native failed\n")
            with patch.object(profile.subprocess, "run", return_value=result) as run:
                with redirect_stdout(io.StringIO()) as console:
                    self.assertEqual(profile.run_host(["host"], root, root, "close"), 17)
            self.assertIn("native failed", console.getvalue())
            self.assertEqual((root / "close.log").read_bytes(), result.stdout)
            self.assertEqual(run.call_args.kwargs["timeout"], 140)

    def test_timeout_retains_partial_output_and_still_raises(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            timeout = subprocess.TimeoutExpired(["host"], 140, output=b"partial native log\n")
            with patch.object(profile.subprocess, "run", side_effect=timeout):
                with redirect_stdout(io.StringIO()):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        profile.run_host(["host"], root, root, "abort")
            self.assertEqual((root / "abort.log").read_bytes(), b"partial native log\n")
            self.assertFalse((root / "abort.json").exists())

    def test_no_diagnostics_preserves_streaming_behavior(self):
        result = subprocess.CompletedProcess(["host"], 3)
        with patch.object(profile.subprocess, "run", return_value=result) as run:
            self.assertEqual(profile.run_host(["host"], pathlib.Path("."), None, "close"), 3)
            self.assertNotIn("stdout", run.call_args.kwargs)
            self.assertNotIn("stderr", run.call_args.kwargs)


class ManifestFixtureTests(unittest.TestCase):
    def test_packaging_copies_only_manifests_and_selected_executable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            repo = ROOT
            fake_repo = root / "repo"
            (fake_repo / "target/debug").mkdir(parents=True)
            binary = fake_repo / "target/debug/stateless-test-app"
            binary.write_bytes(b"fixture executable")
            binary.chmod(0o755)
            shutil.copytree(repo / "crates/samples/stateless-test-app/manifests",
                            fake_repo / "crates/samples/stateless-test-app/manifests")
            first = profile.manifest_fixture(fake_repo, root, binary)
            second = profile.manifest_fixture(fake_repo, root, binary)
            self.assertNotEqual(first, second)
            self.assertEqual(sorted(p.relative_to(first).as_posix() for p in first.rglob("*") if p.is_file()),
                             ["ApplicationManifest.xml", "EchoPackage/Executables/native-echo",
                              "EchoPackage/ServiceManifest.xml"])
            copied = first / "EchoPackage/Executables/native-echo"
            self.assertEqual(copied.read_bytes(), binary.read_bytes())
            self.assertEqual(copied.stat().st_mode & 0o777, 0o755)


class PrebuiltTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.repo = self.root / "repo"
        for name, relative in profile.BINARY_OUTPUTS.items():
            path = self.repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(name.encode())
            path.chmod(0o755)
        shutil.copytree(ROOT / "crates/samples/stateless-test-app/manifests",
                        self.repo / "crates/samples/stateless-test-app/manifests")
        git = patch.object(profile, "git", side_effect=lambda *args: "" if args[0] == "status" else "a" * 40)
        git.start()
        self.addCleanup(git.stop)
        repo = patch.object(profile, "ROOT", self.repo)
        repo.start()
        self.addCleanup(repo.stop)
        self.bundle = self.root / "bundle"
        with patch.object(profile.subprocess, "run", side_effect=AssertionError("must not compile")):
            profile.package_bundle(self.bundle)

    def test_every_scenario_uses_prebuilt_executables_without_compiling(self):
        for scenario in profile.SCENARIOS:
            with self.subTest(scenario=scenario), \
                    patch.object(profile, "qualify_libraries"), \
                    patch.object(profile.subprocess, "run",
                                 return_value=subprocess.CompletedProcess(["ldd"], 0, stdout="")) as run, \
                    patch.object(profile, "run_host", return_value=17) as host:
                root = self.root / scenario
                self.assertEqual(profile.main([
                    scenario, "--root", str(root), "--prebuilt", str(self.bundle)]), 17)
                self.assertEqual([call.args[0][0] for call in run.call_args_list], ["ldd"])
                command = host.call_args.args[0]
                executable = "manifest_acceptance" if scenario.startswith("manifest-") else "fube-test-host"
                self.assertEqual(command[0], str(self.bundle / executable))
                if scenario.startswith("manifest-"):
                    package = pathlib.Path(command[3])
                    self.assertEqual((package / "EchoPackage/Executables/native-echo").read_bytes(),
                                     (self.bundle / "stateless-test-app").read_bytes())
                else:
                    self.assertEqual(command[3], str(self.bundle / "stateless-test-app"))

    def test_missing_prebuilt_fails_before_preparing_or_compiling(self):
        with patch.object(profile.subprocess, "run") as run, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                profile.main(["close", "--root", str(self.root / "not-created")])
            self.assertEqual(error.exception.code, 2)
            run.assert_not_called()
        self.assertFalse((self.root / "not-created").exists())

    def test_changed_bundle_fails_before_native_preflight_without_fallback(self):
        for failure in ("bytes", "mode", "missing", "extra", "symlink", "source"):
            directory = self.root / failure
            shutil.copytree(self.bundle, directory)
            binary = directory / "fube-test-host"
            if failure == "bytes":
                binary.write_bytes(b"tampered")
            elif failure == "mode":
                binary.chmod(0o644)
            elif failure == "missing":
                binary.unlink()
            elif failure == "extra":
                (directory / "unexpected").write_bytes(b"x")
            elif failure == "symlink":
                binary.unlink()
                binary.symlink_to(self.bundle / "fube-test-host")
            else:
                metadata = (directory / "build-info.json").read_text()
                (directory / "build-info.json").write_text(metadata.replace("a" * 40, "b" * 40))
            with self.subTest(failure=failure), patch.object(profile.subprocess, "run") as run:
                with self.assertRaises((ValueError, OSError)):
                    profile.main(["close", "--prebuilt", str(directory)])
                run.assert_not_called()

    def test_missing_build_output_does_not_invoke_cargo(self):
        (self.repo / profile.BINARY_OUTPUTS["fube-test-host"]).unlink()
        with patch.object(profile.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "missing executable build output"):
                profile.package_bundle(self.root / "missing-output")
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
