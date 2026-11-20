import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.onebox import native_upload


class NativeUploadProofTests(unittest.TestCase):
    def test_existing_or_relative_report_is_rejected_before_allocation_access(self):
        with tempfile.TemporaryDirectory() as temporary:
            existing = Path(temporary) / "report.json"
            existing.write_text("original receipt")
            for path in (existing, Path("relative-report.json")):
                with self.subTest(path=path), \
                        patch.object(native_upload.runtime, "Allocation") as allocate:
                    with self.assertRaisesRegex(RuntimeError, "fresh absolute"):
                        native_upload.qualify(SimpleNamespace(report=path))
                    allocate.assert_not_called()
                    self.assertEqual(existing.read_text(), "original receipt")

    def test_deployed_hash_receipt_retains_actual_map_and_rejects_mismatch(self):
        expected = {"nested/file.bin": "a" * 64}
        allocation = Mock()
        allocation.execute.return_value = json.dumps(expected)
        self.assertEqual(native_upload.deployed_data(allocation, "/owned/Data", expected), expected)
        allocation.execute.return_value = json.dumps({"nested/file.bin": "b" * 64})
        with self.assertRaisesRegex(RuntimeError, "deployed Data hashes differ"):
            native_upload.deployed_data(allocation, "/owned/Data", expected)
