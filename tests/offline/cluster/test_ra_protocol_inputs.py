"""Bundle validator models, NOT native serialization/interoperability fixtures."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.ra_protocol.consumer import CASES, REVISION, consume, validate


class InputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.bundle / "native").mkdir()
        self.inputs = {"model-only": "not-a-native-fixture"}
        self.source = hashlib.sha256(json.dumps(self.inputs, sort_keys=True).encode()).hexdigest()
        files = {"schema.h", "native-oracle", "ra-conformance"} | {
            f"native/{name}.bin" for name in CASES
        }
        for name in files:
            (self.bundle / name).write_bytes(b"validator model only")
        self.manifest = {
            "format": 1, "native_revision": REVISION, "head": "a" * 40,
            "source_digest": self.source, "source_inputs": self.inputs,
            "native_sources": {"model-only": "not-execution"},
            "schema_types": 39, "schema_fields": 123, "target": "Linux-x86_64-short-wchar",
            "evidence": "source-baseline-native-stream-with-schema-adapters",
            "cases": sorted(CASES),
            "files": {name: hashlib.sha256((self.bundle / name).read_bytes()).hexdigest()
                      for name in files},
        }
        self.save()

    def save(self):
        (self.bundle / "manifest.json").write_text(json.dumps(self.manifest))

    def test_inventory_model_is_valid_not_an_execution_claim(self):
        self.assertEqual(len(validate(self.bundle, "a" * 40, self.source)["cases"]), 29)

    def test_exact_schema_counts_and_extra_files_are_rejected(self):
        for key in ["schema_types", "schema_fields"]:
            value = self.manifest[key]
            self.manifest[key] = value + 1
            self.save()
            with self.assertRaises(ValueError):
                validate(self.bundle, "a" * 40, self.source)
            self.manifest[key] = value
        self.save()
        (self.bundle / "unexpected.bin").write_bytes(b"not in inventory")
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)

    def test_wrong_head_or_source_fails_before_execution(self):
        with patch("tests.ra_protocol.consumer.subprocess.run") as execute:
            for head, source in [("b" * 40, self.source), ("a" * 40, "0" * 64)]:
                with self.assertRaises(ValueError):
                    consume(self.bundle, self.root / "output", head, source)
            execute.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_missing_and_changed_artifacts_have_no_fallback(self):
        (self.bundle / "native/priority.bin").unlink()
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)
        (self.bundle / "native/priority.bin").write_bytes(b"changed")
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)

    def test_symlink_input_rejected_even_with_matching_digest(self):
        target = self.root / "target"
        target.write_bytes((self.bundle / "schema.h").read_bytes())
        (self.bundle / "schema.h").unlink()
        (self.bundle / "schema.h").symlink_to(target)
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)

    def test_manifest_coverage_and_source_input_tamper_fail(self):
        self.manifest["cases"].remove("rich-request")
        self.save()
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)
        self.manifest["cases"] = sorted(CASES)
        self.manifest["source_inputs"]["model-only"] = "changed"
        self.save()
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)

    def test_wrong_compiled_identity_fails_before_output(self):
        with patch("tests.ra_protocol.consumer.subprocess.check_output", return_value="unbound"):
            with self.assertRaises(ValueError):
                consume(self.bundle, self.root / "output", "a" * 40, self.source)
        self.assertFalse((self.root / "output").exists())

    def test_duplicate_or_reordered_case_inventory_is_rejected(self):
        for cases in [sorted(CASES) + [sorted(CASES)[0]], sorted(CASES, reverse=True)]:
            self.manifest["cases"] = cases
            self.save()
            with self.assertRaises(ValueError):
                validate(self.bundle, "a" * 40, self.source)
