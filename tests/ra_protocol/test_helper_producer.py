"""Helper evidence guards; actual-native execution is an explicit producer run."""
import unittest
from pathlib import Path
from unittest.mock import patch

from . import helper_producer, producer


class HelperProducerTests(unittest.TestCase):
    def test_adapter_and_rust_example_changes_invalidate_source_identity(self):
        baseline, inputs = producer.source_digest()
        for relative in [
            "tests/ra_protocol/helpers/adapter.h",
            "tests/ra_protocol/helpers/main.cpp",
            "tests/ra_protocol/helper_producer.py",
            "crates/libs/fube-ra/examples/ra_helper_conformance.rs",
        ]:
            with self.subTest(path=relative):
                original = producer.digest
                changed = producer.ROOT / relative
                with patch.object(
                    producer, "digest",
                    side_effect=lambda path: "changed" if path == changed else original(path),
                ):
                    self.assertNotEqual(producer.source_digest()[0], baseline)
                self.assertIn(relative, inputs)

    def test_stale_explicit_rust_is_rejected_before_output_creation(self):
        native = Path("/reference")
        rust = producer.ROOT / "target/debug/examples/ra_helper_conformance"
        out = producer.ROOT / "build/helper-guard-must-not-be-created"
        with (
            patch.object(helper_producer, "clean_revision"),
            patch.object(helper_producer.platform, "system", return_value="Linux"),
            patch.object(helper_producer.platform, "machine", return_value="x86_64"),
            patch.object(helper_producer, "source_digest", return_value=("current", {})),
            patch.object(Path, "is_file", return_value=True),
            patch.object(helper_producer.subprocess, "check_output", return_value="stale\n"),
            patch.object(Path, "mkdir") as mkdir,
        ):
            with self.assertRaisesRegex(ValueError, "current FUBE_RA_SOURCE_DIGEST"):
                helper_producer.produce(native, out, Path("/clang"), rust)
            mkdir.assert_not_called()

    def test_native_reference_cannot_be_an_output_directory(self):
        native = Path("/reference")
        with patch.object(helper_producer, "clean_revision") as revision:
            for out in [native, native / "helper-output"]:
                with self.subTest(out=out), self.assertRaisesRegex(ValueError, "native reference"):
                    helper_producer.produce(native, out, Path("/clang"))
            revision.assert_not_called()


if __name__ == "__main__":
    unittest.main()
