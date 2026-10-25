"""Preparation/preflight model tests; not native interoperability evidence."""

import hashlib
import pathlib
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
