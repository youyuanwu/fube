"""Bundle validator models, NOT native serialization/interoperability fixtures."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.ra_protocol.consumer import CASES, REVISION, consume, validate
from tests.ra_protocol import schema


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
            "schema_types": 73, "schema_fields": 233, "target": "Linux-x86_64-short-wchar",
            "native_constants": {"PToP": 0x8013, "PToPDirect": 0},
            "evidence": "source-baseline-native-stream-with-schema-adapters",
            "cases": sorted(CASES),
            "files": {name: hashlib.sha256((self.bundle / name).read_bytes()).hexdigest()
                      for name in files},
        }
        self.save()

    def save(self):
        (self.bundle / "manifest.json").write_text(json.dumps(self.manifest))

    def test_inventory_model_is_valid_not_an_execution_claim(self):
        manifest = validate(self.bundle, "a" * 40, self.source)
        self.assertEqual(len(manifest["cases"]), 101)
        self.assertEqual(len(manifest["files"]) + 1, 105)

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

    def test_notification_coverage_and_tamper_fail_before_execution(self):
        names = [
            "empty-service-type-notification", "rich-service-type-notification",
            "empty-service-type-notification-reply", "partial-service-type-notification-reply",
            "action-ServiceTypeNotification", "action-ServiceTypeNotificationReply", "priority-false",
        ]
        with patch("tests.ra_protocol.consumer.subprocess.check_output") as identity:
            for name in names:
                with self.subTest(case=name):
                    self.manifest["cases"].remove(name)
                    self.save()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(CASES)
                    self.save()
                    path = self.bundle / f"native/{name}.bin"
                    original = path.read_bytes()
                    path.unlink()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(b"tampered notification fixture")
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            identity.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_symlink_input_rejected_even_with_matching_digest(self):
        target = self.root / "target"
        target.write_bytes((self.bundle / "schema.h").read_bytes())
        (self.bundle / "schema.h").unlink()
        (self.bundle / "schema.h").symlink_to(target)
        with self.assertRaises(ValueError):
            validate(self.bundle, "a" * 40, self.source)

    def test_bootstrap_coverage_and_tamper_fail_before_execution(self):
        names = [
            "fresh-node-up", "rich-node-up", "fresh-node-up-ack", "rich-node-up-ack",
            "action-NodeUp", "action-NodeUpAck", "actor-13", "direct-ptop", "rich-ptop",
        ]
        with patch("tests.ra_protocol.consumer.subprocess.check_output") as identity:
            for name in names:
                with self.subTest(case=name):
                    self.manifest["cases"].remove(name)
                    self.save()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(CASES)
                    self.save()
                    path = self.bundle / f"native/{name}.bin"
                    original = path.read_bytes()
                    path.unlink()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(b"tampered bootstrap fixture")
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            identity.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_delete_replica_coverage_and_tamper_fail_before_execution(self):
        names = [
            "default-delete-replica", "rich-delete-replica", "default-delete-replica-reply",
            "rich-delete-replica-reply-0", "rich-delete-replica-reply--2147017398",
            "action-DeleteReplica", "action-DeleteReplicaReply",
        ]
        with patch("tests.ra_protocol.consumer.subprocess.check_output") as identity:
            for name in names:
                with self.subTest(case=name):
                    self.manifest["cases"].remove(name)
                    self.save()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(CASES)
                    self.save()
                    path = self.bundle / f"native/{name}.bin"
                    original = path.read_bytes()
                    path.unlink()
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(b"tampered DeleteReplica fixture")
                    with self.assertRaises(ValueError):
                        consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            identity.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_native_header_id_and_direct_actor_contract_fail_before_execution(self):
        with patch("tests.ra_protocol.consumer.subprocess.check_output") as identity:
            for constants in [
                {}, {"PToP": 0x8014, "PToPDirect": 0}, {"PToP": 0x8013, "PToPDirect": 1},
            ]:
                self.manifest["native_constants"] = constants
                self.save()
                with self.assertRaises(ValueError):
                    consume(self.bundle, self.root / "output", "a" * 40, self.source)
            identity.assert_not_called()
        self.assertFalse((self.root / "output").exists())

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


class SchemaExtractionTests(unittest.TestCase):
    def test_shared_header_macros_and_native_collection_types_are_not_flattened(self):
        # Source model only; this does not produce native serialization fixtures.
        header = """
class ServicePackageInfo : public Serialization::FabricSerializable {
    FABRIC_FIELDS_01(version_);
    uint64 version_;
};
class NodeUpMessageBody : public Serialization::FabricSerializable {
    FABRIC_FIELDS_04(packages_, properties_, names_, port_);
    unsigned short port_ = 0;
    std::set<std::wstring> names_;
    std::map<std::wstring, uint> properties_;
    std::vector<ServicePackageInfo> packages_;
};
"""
        models = [
            ("ServicePackageInfo", "Shared.h", ""),
            ("NodeUpMessageBody", "Shared.h", ""),
        ]
        with patch.object(schema, "MODELS", models), patch.object(Path, "read_text", return_value=header):
            extracted = schema.extract(Path("/unused-source-model"))
        self.assertEqual(extracted[0][4], [("version_", "uint64", "ULONG64")])
        self.assertEqual(
            [(field, cpp) for field, _, cpp in extracted[1][4]],
            [
                ("packages_", "std::vector<ServicePackageInfo>"),
                ("properties_", "std::map<std::wstring, ULONG>"),
                ("names_", "std::set<std::wstring>"),
                ("port_", "USHORT"),
            ],
        )
        with self.assertRaisesRegex(ValueError, "unmapped native field type"):
            schema.adapted("std::map<std::wstring, UnsupportedNativeType>")

    def test_native_constants_require_unique_authoritative_values(self):
        values = {
            "MessageHeaderId.h": "enum Enum { PToP = 0x8013, };",
            "PToPActor.h": "enum Enum { Direct = 0, };",
        }
        with patch.object(Path, "read_text", side_effect=lambda path: values[path.name], autospec=True):
            self.assertEqual(schema.native_constants(Path("/unused-source-model")),
                             {"PToP": 0x8013, "PToPDirect": 0})
            for text in ["enum Enum { PToP = 0x8014, };", "PToP = 0x8013, PToP = 0x8013,", ""]:
                values["MessageHeaderId.h"] = text
                with self.assertRaises(ValueError):
                    schema.native_constants(Path("/unused-source-model"))
