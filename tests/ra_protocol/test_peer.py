"""Peer schema/metadata guards; native stream execution is a separate receipt."""
import csv
import unittest
from pathlib import Path
from unittest.mock import patch

from . import consumer, schema


class PeerSchemaTests(unittest.TestCase):
    def test_native_inheritance_keeps_a_nested_deactivation_object_in_next_scope(self):
        headers = {
            "ReplicaDeactivationInfo.h": """
class ReplicaDeactivationInfo : public Serialization::FabricSerializable {
    FABRIC_FIELDS_02(deactivationEpoch_, catchupLSN_);
    int64 catchupLSN_;
    Reliability::Epoch deactivationEpoch_;
};
""",
            "RAReplicaMessageBody.h": """
class RAReplicaMessageBody : public ReplicaMessageBody {
    FABRIC_FIELDS_01(deactivationInfo_);
    Reliability::ReplicaDeactivationInfo deactivationInfo_;
};
""",
        }
        models = [row for row in schema.MODELS if row[0] in {
            "ReplicaDeactivationInfo", "RAReplicaMessageBody",
        }]
        with (
            patch.object(schema, "MODELS", models),
            patch.object(schema, "SCALARS", schema.SCALARS | {
                "Reliability::Epoch": "Epoch",
            }),
            patch.object(Path, "read_text", autospec=True,
                         side_effect=lambda path: headers[path.name]),
        ):
            extracted = schema.extract(Path("unused-source-model"))
        self.assertEqual(extracted[1][2], "ReplicaMessageBody")
        self.assertEqual(extracted[1][4], [
            ("deactivationInfo_", "Reliability::ReplicaDeactivationInfo", "ReplicaDeactivationInfo"),
        ])
        self.assertEqual([field[0] for field in extracted[0][4]],
                         ["deactivationEpoch_", "catchupLSN_"])
        self.assertEqual(extracted[0][4][1][2], "LONG64")
        with patch.object(schema, "MODELS", [models[1]]), patch.object(
            Path, "read_text", return_value=headers["RAReplicaMessageBody.h"].replace(
                "public ReplicaMessageBody", "public WrongBase",
            ),
        ):
            with self.assertRaisesRegex(ValueError, "inheritance"):
                schema.extract(Path("unused-source-model"))

    def test_peer_factory_actor_priority_literal_and_uniqueness_are_source_bound(self):
        lines = [
            f'RSMessage::{action} = CreateRAToRAMessage(L"{action}", true);'
            for action in sorted(consumer.PEER_ACTIONS)
        ]
        text = "\n".join(lines)
        with patch.object(Path, "read_text", return_value=text):
            self.assertEqual(schema.peer_factories(Path("unused-source-model")),
                             consumer.PEER_FACTORIES)
        for bad in [
            text.replace('L"CreateReplica"', 'L"AddReplica"'),
            text.replace("true", "false", 1),
            text.replace("CreateRAToRAMessage", "CreateRAToFMMessage", 1),
            text + "\n" + lines[0],
        ]:
            with self.subTest(text=bad), patch.object(Path, "read_text", return_value=bad):
                with self.assertRaisesRegex(ValueError, "native peer factory"):
                    schema.peer_factories(Path("unused-source-model"))

    def test_catalog_selects_two_peer_actions_and_bounded_fm_add_without_reconfiguration(self):
        root = Path(__file__).resolve().parents[2]
        with (root / "docs/cluster/NativeRaRegistry.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        peer = [row for row in rows if row["direction"] in {
            "primary-RA-to-replica-RA", "replica-RA-to-primary-RA",
            "coordinating-RA-to-replica-RA", "replica-RA-to-coordinating-RA",
            "primary-candidate-RA-to-replica-RA", "replica-RA-to-primary-candidate-RA",
        }]
        self.assertEqual(len(peer), 8)
        self.assertEqual({row["action"] for row in peer if "selected" in row["status"]},
                         consumer.PEER_ACTIONS)
        self.assertEqual(sum("selected" in row["status"] for row in rows if row not in peer), 33)
        records = {row["action"]: row for row in rows}
        self.assertEqual(records["CreateReplica"]["ra_metadata"], "FC")
        self.assertEqual(records["CreateReplicaReply"]["ra_metadata"], "F")
        for action in ["AddReplica", "AddReplicaReply"]:
            self.assertIn("selected-C9", records[action]["status"])
        for action in ["GetLSN", "GetLSNReply",
                       "Deactivate", "DeactivateReply", "Activate", "ActivateReply"]:
            self.assertNotIn("selected", records[action]["status"])
        text = (root / "docs/cluster/NativeRaProtocol.md").read_text()
        for term in ["124 inputs", "77 types/240 fields", "128 bundle files",
                     "34 native truncation", "InBuild=1", "not AddReplicaReply"]:
            self.assertIn(term, text)

    def test_add_replica_factory_actor_priority_and_uniqueness_are_source_bound(self):
        lines = [
            'RSMessage::AddReplica = CreateFMToRAMessage(L"AddReplica", true);',
            'RSMessage::AddReplicaReply = CreateRAToFMMessage(L"AddReplicaReply", true);',
        ]
        text = "\n".join(lines)
        with patch.object(Path, "read_text", return_value=text):
            self.assertEqual(schema.replica_factories(Path("unused-source-model")),
                             consumer.REPLICA_FACTORIES)
        for bad in [text.replace('L"AddReplica"', 'L"AddPrimary"'),
                    text.replace("true", "false", 1),
                    text.replace("CreateFMToRAMessage", "CreateRAToRAMessage"),
                    text + "\n" + lines[0]]:
            with self.subTest(text=bad), patch.object(Path, "read_text", return_value=bad):
                with self.assertRaisesRegex(ValueError, "native replica factory"):
                    schema.replica_factories(Path("unused-source-model"))


if __name__ == "__main__":
    unittest.main()
