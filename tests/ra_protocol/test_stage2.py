"""Offline schema/receipt guards; model bytes never claim native execution."""
import hashlib
import json
import csv
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from . import consumer, schema


class StageTwoCatalogTests(unittest.TestCase):
    def test_selected_contracts_keep_registry_classes_and_codec_only_endpoint_limits(self):
        root = Path(__file__).resolve().parents[2]
        with (root / "docs/cluster/NativeRaRegistry.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        records = {row["action"]: row for row in rows}
        self.assertEqual(len(rows), 119)
        self.assertEqual(sum(row["boundary"] == "true" for row in rows), 66)
        self.assertEqual(sum("selected" in row["status"] for row in rows), 48)
        for action in consumer.STAGE_TWO_ACTIONS:
            with self.subTest(action=action):
                row = records[action]
                self.assertEqual(row["boundary"], "true")
                self.assertIn("selected-C6", row["status"])
                self.assertIn("codec", row["status"])
                if action.startswith("ReplicaEndpointUpdated"):
                    self.assertIn("selected-C13", row["status"])
        text = (root / "docs/cluster/NativeRaProtocol.md").read_text()
        for term in ["101 inputs", "73 types/233 fields", "105 bundle files",
                     "26 native truncation", "one FT per real Node", "multi-FT",
                     "priority=false", "active reconfiguration", "stage 5"]:
            self.assertIn(term, text)


class StageTwoSchemaTests(unittest.TestCase):
    def test_native_load_field_order_omissions_and_map_value_are_preserved(self):
        headers = {
            "StopwatchTime.h": """
class StopwatchTime {
    FABRIC_PRIMITIVE_FIELDS_01(ticks_);
    int64 ticks_;
};
""",
            "ReportLoadInfo.h": """
class ReportLoadInfo : public Serialization::FabricSerializable {
    FABRIC_FIELDS_03(failoverUnitId_, replicaRole_, loadMetrics_);
    bool isStateful_;
    Reliability::ReplicaRole::Enum replicaRole_;
    std::vector<Reliability::LoadBalancingComponent::LoadMetric> loadMetrics_;
    Reliability::FailoverUnitId failoverUnitId_;
};
""",
            "LoadOrMoveCostDescription.h": """
class LoadOrMoveCostDescription : public Serialization::FabricSerializable {
    FABRIC_FIELDS_06(failoverUnitId_, serviceName_, isStateful_,
                    primaryEntries_, secondaryEntries_, secondaryEntriesMap_);
    bool isReset_;
    std::map<Federation::NodeId, std::vector<LoadMetricStats>> secondaryEntriesMap_;
    std::vector<LoadMetricStats> secondaryEntries_;
    std::vector<LoadMetricStats> primaryEntries_;
    bool isStateful_;
    std::wstring serviceName_;
    Common::Guid failoverUnitId_;
};
""",
        }
        models = [row for row in schema.MODELS if row[0] in {
            "StopwatchTime", "ReportLoadInfo", "LoadOrMoveCostDescription",
        }]
        with (
            patch.object(schema, "MODELS", models),
            patch.object(schema, "SCALARS", schema.SCALARS | {
                "Reliability::FailoverUnitId": "FailoverUnitId",
                "Reliability::LoadBalancingComponent::LoadMetric": "LoadMetric",
                "LoadMetricStats": "LoadMetricStats",
            }),
            patch.object(Path, "read_text", autospec=True,
                         side_effect=lambda path: headers[path.name]),
        ):
            extracted = schema.extract(Path("unused-source-model"))
        by_name = {row[0]: row for row in extracted}
        self.assertEqual(by_name["StopwatchTime"][3], "FABRIC_PRIMITIVE_FIELDS_01")
        self.assertEqual([f[0] for f in by_name["ReportLoadInfo"][4]],
                         ["failoverUnitId_", "replicaRole_", "loadMetrics_"])
        self.assertEqual(by_name["LoadOrMoveCostDescription"][4][-1][2],
                         "std::map<NodeId, std::vector<LoadMetricStats>>")
        for row in extracted:
            self.assertNotIn("isReset_", [field[0] for field in row[4]])
        with self.assertRaisesRegex(ValueError, "unmapped native field type"):
            schema.adapted("std::map<Federation::NodeId, std::vector<Unsupported>>")


class StageTwoReceiptTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("build") / f"stage2-codec-validator-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.bundle = self.root / "bundle"
        (self.bundle / "native").mkdir(parents=True)
        inputs = {"model-only": "not-a-native-fixture"}
        self.source = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
        files = {"schema.h", "native-oracle", "ra-conformance"} | {
            f"native/{case}.bin" for case in consumer.CASES
        }
        for name in files:
            (self.bundle / name).write_bytes(b"receipt-validator-model-only")
        self.manifest = {
            "format": 1, "native_revision": consumer.REVISION,
            "head": "a" * 40, "source_digest": self.source, "source_inputs": inputs,
            "native_sources": {"model": "not-execution"},
            "schema_types": consumer.SCHEMA_TYPES, "schema_fields": consumer.SCHEMA_FIELDS,
            "target": "Linux-x86_64-short-wchar",
            "native_constants": {"PToP": 0x8013, "PToPDirect": 0},
            "generation_factories": consumer.GENERATION_FACTORIES,
            "peer_factories": consumer.PEER_FACTORIES,
            "replica_factories": consumer.REPLICA_FACTORIES,
            "configuration_factories": consumer.CONFIGURATION_FACTORIES,
            "evidence": "source-baseline-native-stream-with-schema-adapters",
            "cases": sorted(consumer.CASES),
            "files": {name: hashlib.sha256((self.bundle / name).read_bytes()).hexdigest()
                      for name in files},
        }
        self.save()

    def save(self):
        (self.bundle / "manifest.json").write_text(json.dumps(self.manifest))

    def test_complete_inventory_is_accepted_but_old_schema_counts_are_not(self):
        self.assertEqual(consumer.validate(self.bundle, "a" * 40, self.source), self.manifest)
        for key, old in [
            ("schema_types", 59), ("schema_fields", 199),
            ("schema_types", 73), ("schema_fields", 233),
        ]:
            current = self.manifest[key]
            self.manifest[key] = old
            self.save()
            with self.assertRaisesRegex(ValueError, "source/schema"):
                consumer.validate(self.bundle, "a" * 40, self.source)
            self.manifest[key] = current

    def test_every_stage_two_case_is_required_and_integrity_checked_before_execution(self):
        cases = {f"{profile}-{root}" for root in consumer.STAGE_TWO_ROOTS
                 for profile in ["empty", "rich"]}
        cases |= {f"action-{action}" for action in consumer.STAGE_TWO_ACTIONS}
        cases |= {"fmm-node-activate-request", "fmm-node-deactivate-request",
                  "legacy-report-load", "modern-report-load"}
        with patch.object(consumer.subprocess, "check_output") as execute:
            for case in sorted(cases):
                with self.subTest(case=case):
                    self.manifest["cases"].remove(case)
                    self.save()
                    with self.assertRaises(ValueError):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(consumer.CASES)
                    self.save()
                    path = self.bundle / f"native/{case}.bin"
                    original = path.read_bytes()
                    path.write_bytes(b"tampered-stage-two-model")
                    with self.assertRaisesRegex(ValueError, "integrity"):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            execute.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_every_generation_case_is_required_before_any_execution(self):
        with patch.object(consumer.subprocess, "check_output") as execute:
            for case in sorted(consumer.GENERATION_CASES):
                with self.subTest(case=case):
                    self.manifest["cases"].remove(case)
                    self.save()
                    with self.assertRaises(ValueError):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(consumer.CASES)
                    self.save()
                    path = self.bundle / f"native/{case}.bin"
                    original = path.read_bytes()
                    path.write_bytes(b"tampered-generation-model")
                    with self.assertRaisesRegex(ValueError, "integrity"):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            execute.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_generation_factory_metadata_cannot_be_substituted(self):
        for action in consumer.GENERATION_ACTIONS:
            for key, wrong in [("actor", 10), ("high_priority", False)]:
                with self.subTest(action=action, field=key):
                    self.manifest["generation_factories"] = {
                        name: dict(fields) for name, fields in consumer.GENERATION_FACTORIES.items()
                    }
                    self.manifest["generation_factories"][action][key] = wrong
                    self.save()
                    with self.assertRaisesRegex(ValueError, "source/schema"):
                        consumer.validate(self.bundle, "a" * 40, self.source)

    def test_replica_cases_and_factory_metadata_are_required_before_execution(self):
        with patch.object(consumer.subprocess, "check_output") as execute:
            for case in consumer.REPLICA_CASES:
                self.manifest["cases"].remove(case)
                self.save()
                with self.assertRaises(ValueError):
                    consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                self.manifest["cases"] = sorted(consumer.CASES)
            for action in consumer.REPLICA_FACTORIES:
                for key, wrong in [("actor", 10), ("high_priority", False)]:
                    self.manifest["replica_factories"] = {
                        name: dict(fields) for name, fields in consumer.REPLICA_FACTORIES.items()
                    }
                    self.manifest["replica_factories"][action][key] = wrong
                    self.save()
                    with self.assertRaisesRegex(ValueError, "source/schema"):
                        consumer.validate(self.bundle, "a" * 40, self.source)
            execute.assert_not_called()
        self.assertFalse((self.root / "output").exists())

    def test_every_peer_case_and_factory_is_required_before_any_execution(self):
        with patch.object(consumer.subprocess, "check_output") as execute:
            for case in sorted(consumer.PEER_CASES):
                with self.subTest(case=case):
                    self.manifest["cases"].remove(case)
                    self.save()
                    with self.assertRaises(ValueError):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    self.manifest["cases"] = sorted(consumer.CASES)
                    self.save()
                    path = self.bundle / f"native/{case}.bin"
                    original = path.read_bytes()
                    path.write_bytes(b"tampered-peer-model")
                    with self.assertRaisesRegex(ValueError, "integrity"):
                        consumer.consume(self.bundle, self.root / "output", "a" * 40, self.source)
                    path.write_bytes(original)
            for action in consumer.PEER_ACTIONS:
                for key, wrong in [("actor", 11), ("high_priority", False)]:
                    with self.subTest(action=action, field=key):
                        self.manifest["peer_factories"] = {
                            name: dict(fields) for name, fields in consumer.PEER_FACTORIES.items()
                        }
                        self.manifest["peer_factories"][action][key] = wrong
                        self.save()
                        with self.assertRaisesRegex(ValueError, "source/schema"):
                            consumer.validate(self.bundle, "a" * 40, self.source)
            execute.assert_not_called()
        self.assertFalse((self.root / "output").exists())


if __name__ == "__main__":
    unittest.main()
