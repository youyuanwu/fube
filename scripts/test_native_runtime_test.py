"""Preparation/preflight model tests; not native interoperability evidence."""

import hashlib
import io
import pathlib
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import native_runtime_test as profile


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


if __name__ == "__main__":
    unittest.main()
