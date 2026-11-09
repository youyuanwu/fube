"""Generation schema/catalog guards; model data is not native execution."""
import csv
import unittest
from pathlib import Path
from unittest.mock import patch

from . import consumer, schema


class GenerationTests(unittest.TestCase):
    def test_native_macro_order_preserves_primitive_error_and_epoch_object(self):
        headers = {
            "GenerationProposalReplyMessageBody.h": """
class GenerationProposalReplyMessageBody : public Serialization::FabricSerializable {
    FABRIC_FIELDS_03(proposedGeneration_, currentGeneration_, error_);
    Common::ErrorCode error_;
    GenerationNumber currentGeneration_;
    GenerationNumber proposedGeneration_;
};
""",
            "GenerationUpdateMessageBody.h": """
class GenerationUpdateMessageBody : public Serialization::FabricSerializable {
    FABRIC_FIELDS_01(fmServiceEpoch_);
    Reliability::Epoch fmServiceEpoch_;
};
""",
        }
        models = [row for row in schema.MODELS if row[0] in {
            "GenerationProposalReplyMessageBody", "GenerationUpdateMessageBody",
        }]
        with (
            patch.object(schema, "MODELS", models),
            patch.object(schema, "SCALARS", schema.SCALARS | {
                "GenerationNumber": "GenerationNumber", "Common::ErrorCode": "ErrorCode",
                "Reliability::Epoch": "Epoch",
            }),
            patch.object(Path, "read_text", autospec=True,
                         side_effect=lambda path: headers[path.name]),
        ):
            extracted = schema.extract(Path("unused-source-model"))
        self.assertEqual([field[0] for field in extracted[0][4]],
                         ["proposedGeneration_", "currentGeneration_", "error_"])
        self.assertEqual([field[2] for field in extracted[0][4]],
                         ["GenerationNumber", "GenerationNumber", "ErrorCode"])
        self.assertEqual(extracted[1][4], [("fmServiceEpoch_", "Reliability::Epoch", "Epoch")])
        error = next(row for row in schema.MODELS if row[0] == "ErrorCode")
        self.assertIsNone(error[2], "ErrorCode is a native primitive, not an object")

    def test_native_factory_drift_is_an_explicit_failure(self):
        lines = []
        for action in consumer.GENERATION_ACTIONS:
            factory = ("CreateFMToRAMessage"
                       if action in {"GenerationProposal", "GenerationUpdate"}
                       else "CreateRAToFMMessage")
            lines.append(f'RSMessage::{action} = {factory}(L"{action}", true);')
        text = "\n".join(lines)
        with patch.object(Path, "read_text", return_value=text):
            self.assertEqual(schema.generation_factories(Path("unused-source-model")),
                             consumer.GENERATION_FACTORIES)
        for bad in [
            text.replace('L"GenerationProposal"', 'L"WrongProposal"'),
            text.replace("true", "false", 1),
            text.replace("CreateRAToFMMessage", "CreateFMToRAMessage", 1),
            text + "\n" + lines[0],
        ]:
            with self.subTest(text=bad), patch.object(Path, "read_text", return_value=bad):
                with self.assertRaisesRegex(ValueError, "native generation factory"):
                    schema.generation_factories(Path("unused-source-model"))

    def test_catalog_selects_only_handshake_not_upload_or_full_rebuild(self):
        root = Path(__file__).resolve().parents[2]
        with (root / "docs/cluster/NativeRaRegistry.csv").open(newline="") as stream:
            rows = {row["action"]: row for row in csv.DictReader(stream)}
        for action in consumer.GENERATION_ACTIONS:
            self.assertIn("selected-C7", rows[action]["status"])
            self.assertEqual(rows[action]["boundary"], "true")
        for action in ["LFUMUpload", "LFUMUploadReply", "GFUMTransfer"]:
            self.assertNotIn("selected", rows[action]["status"])
        text = (root / "docs/cluster/NativeRaProtocol.md").read_text()
        for term in [
            "116 inputs", "75 types/237 fields", "120 bundle files",
            "31 native truncation", "correlated", "FMM bodyless branch",
            "not LFUM upload", "send generation",
        ]:
            self.assertIn(term, text)


if __name__ == "__main__":
    unittest.main()
