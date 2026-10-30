"""Offline catalog integrity; optional pinned source reconciliation is explicit."""
import csv
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CATALOG = ROOT / "docs/cluster/NativeRaProtocol.md"
REGISTRY = ROOT / "docs/cluster/NativeRaRegistry.csv"
REVISION = "3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d"


def rows():
    with REGISTRY.open(newline="") as stream:
        return list(csv.DictReader(stream))


def audit_source(native):
    """Fail closed on another source revision, missing actions or metadata."""
    head = subprocess.check_output(
        ["git", "-C", str(native), "rev-parse", "HEAD"], text=True
    ).strip()
    if head != REVISION:
        raise ValueError(f"native revision {head} != {REVISION}")
    base = native / "src/prod/src/Reliability/Failover"
    source = (base / "common/RSMessage.cpp").read_text()
    # Compare every initialized symbol, wire action and exact source line.
    found = {}
    for number, line in enumerate(source.splitlines(), 1):
        match = re.search(
            r'Global<RSMessage>\s+RSMessage::(\w+)\s*=.*?L"([^"]+)"', line
        )
        if match:
            found[match[1]] = (match[2], number)
    declared = set(re.findall(
        r"static\s+Common::Global<RSMessage>\s+(\w+)\s*;",
        (base / "common/RSMessage.h").read_text(),
    ))
    expected = {r["symbol"]: (r["action"], int(r["registry_line"])) for r in rows()}
    if found != expected:
        raise ValueError(f"registry drift: found={len(found)} expected={len(expected)}")
    if declared != set(found) | {"DropReplica", "DropReplicaReply"}:
        raise ValueError("header declarations drifted")
    metadata = set(re.findall(
        r"RSMessage::Get(\w+)\(\)",
        (base / "ra/Communication.MessageMetadataTable.cpp").read_text(),
    ))
    classified = {r["symbol"] for r in rows()}
    if len(metadata) != 33 or not metadata <= classified:
        raise ValueError(f"metadata drift: {len(metadata)}; unclassified {metadata - classified}")
    return len(found), len(declared), len(metadata)


class CatalogTests(unittest.TestCase):
    def test_registry_is_complete_and_classified(self):
        records = rows()
        self.assertEqual(len(records), 119)
        self.assertEqual(len({r["symbol"] for r in records}), 119)
        self.assertEqual(len({r["action"] for r in records}), 119)
        self.assertEqual(sum(r["boundary"] == "true" for r in records), 66)
        for record in records:
            for key, value in record.items():
                self.assertTrue(value.strip(), (record["symbol"], key))
            self.assertIn(record["boundary"], {"true", "false"})
            self.assertIn(record["factory_actor"], {"FM", "RA", "RS", "ServiceResolver"})
            self.assertNotIn(record["classification"].lower(), {"unknown", "unclassified"})

    def test_every_action_has_durable_contract_or_exclusion(self):
        text = CATALOG.read_text()
        self.assertIn(REVISION, text)
        for record in (r for r in rows() if r["boundary"] == "true"):
            self.assertIn(f'`{record["action"]}`', text, record["symbol"])
        for action in ["DropReplica", "DropReplicaReply", "CreateReplica",
                       "GetLSN", "Deactivate", "Activate", "NodeUp", "NodeUpAck"]:
            self.assertIn(f"`{action}`", text)
        for term in ["ConfigurationMessageBody base", "droppedReplicas_",
                     "isLastReplicaUpMessage_", "isFromFMM_", "LONG64",
                     "Deprecated", "staged"]:
            self.assertIn(term, text)

    def test_pinned_links_and_reference_labels(self):
        text = CATALOG.read_text()
        definitions = dict(re.findall(r"^\[([^\]]+)\]:\s+(\S+)", text, re.M))
        uses = set(re.findall(r"\[[^\]\n]+\]\[([^\]]+)\]", text))
        self.assertFalse(uses - definitions.keys())
        native_links = [url for url in definitions.values()
                        if "github.com/microsoft/service-fabric/" in url]
        self.assertGreater(len(native_links), 150)
        for url in native_links:
            self.assertIn(f"/blob/{REVISION}/", url)

    def test_no_enabled_capability_claim_from_inventory(self):
        records = {r["symbol"]: r for r in rows()}
        for action in ["AddInstance", "AddInstanceReply", "RemoveInstance",
                       "RemoveInstanceReply", "ReplicaUp", "ReplicaUpReply",
                       "ServiceTypeNotification", "ServiceTypeNotificationReply"]:
            self.assertIn("selected", records[action]["status"])
        for action in ["RemoveReplica", "DeleteReplica", "ReplicaDropped", "ReplicaDown"]:
            self.assertNotIn("selected", records[action]["status"])
        self.assertIn("not a", CATALOG.read_text())

    def test_removal_mapping_retains_native_and_runtime_boundaries(self):
        removal = (ROOT / "docs/cluster/RaStatelessRemoval.md").read_text()
        for term in [REVISION, "Obliterate", "ReplicaUp", "ForceDelete", "StaleRequest",
                     "not execution", "trusted", "format 2", "volatile",
                     "queued/in-flight Open", "40 files"]:
            if term == "40 files":
                self.assertIn(term, (ROOT / "tests/ra_protocol/README.md").read_text())
            else:
                self.assertIn(term, removal)

    def test_replacement_status_keeps_resend_adapter_and_eventual_wire_boundaries(self):
        replacement = (ROOT / "docs/cluster/RaStatelessReplacement.md").read_text()
        for term in [REVISION, "FM resend", "not a second replacement state machine",
                     "processing time", "TESTASSERT", "absent application-upgrade",
                     "not new enabled IPC wire codecs", "unchanged",
                     "36 bidirectional", "43 types/129 fields", "65,536",
                     "commit-before-effects", "capability-disabled does not mean omitted"]:
            self.assertIn(term, replacement)
        records = {r["symbol"]: r for r in rows()}
        self.assertIn("drop-resend-replacement", records["AddInstance"]["status"])
        self.assertEqual(sum("selected" in r["status"] for r in records.values()), 8)


if __name__ == "__main__":
    unittest.main()
