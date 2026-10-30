"""Offline fault-harness models only; none of these are native qualification."""
import contextlib
import copy
import io
import itertools
import json
import struct
import sys
import unittest
import urllib.error
import uuid
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

from tests.onebox.shared import data
from tests.offline.onebox.native_data.test_fube_data_test import observation


PARTITION = "00000000-0000-0000-0000-000000000123"
MAIN_PARTITION = "00000000-0000-0000-0000-000000000789"
OPERATION = "00000000-0000-0000-0000-000000000456"
SERVICE = "fabric:/Owned/DataLossChanged-model"


def primary(value, index):
    for position, replica in enumerate(value["replicas"]):
        chosen = position == index
        role = "Primary" if chosen else "ActiveSecondary"
        replica.update(role=role, service_http_status=200 if chosen else 503)
        rpc = replica["rpc"]
        rpc.update(role=role, current_role=role,
                   read_status="Granted" if chosen else "NotPrimary",
                   write_status="Granted" if chosen else "NotPrimary")
        if rpc["engine_kind"] == "rust":
            rpc["replicator_role"] = role
    chosen = value["replicas"][index]
    value["primary"] = {"resolver": "fube-client", "endpoint": chosen["address"],
                        "native_pid": chosen["rpc"]["pid"], "rpc": copy.deepcopy(chosen["rpc"])}
    return value


def wire_model(mixed=False, rust_primary=False):
    value = observation(mixed)
    value["partition"] = PARTITION
    endpoints = []
    for index, replica in enumerate(value["replicas"]):
        engine = f"00000000-0000-0000-0000-{index + 10:012d}"
        endpoint = f"127.0.0.1:{3100 + index}/{PARTITION}-{replica['replica']};{engine};transport"
        endpoints.append(endpoint)
        rpc = replica["rpc"]
        rpc.update(partition=PARTITION, engine_incarnation=engine,
                   replication_bind_address="127.0.0.1:0")
        if rpc["engine_kind"] == "rust":
            rpc["replication_address"] = endpoint
        rpc["state"].update(last=20, history_count=0, data_loss_calls=0,
                            data_loss_changed_calls=0, last_data_loss_boundary=None)
        rpc["engine"]["progress"] = dict(assigned=20, received=20, applied=20,
                                         committed=20, client_completed=20, completed=20)
    position = 1 if mixed and rust_primary else 0
    primary(value, position)
    for index, replica in enumerate(value["replicas"]):
        rpc = replica["rpc"]
        rpc["observed_primary"] = endpoints[position] if index != position else None
        rpc["engine"]["peers"] = [
            {"address": endpoint, "configured": True, "building": False, "cleanup_required": False}
            for other, endpoint in enumerate(endpoints) if other != index
        ] if index == position else []
    primary(value, position)
    return value, endpoints


def receipt(value, source, kind):
    own = source["rpc"]["replication_address"]
    selected = (source["rpc"]["engine"]["peers"][0]["address"] if source["role"] == "Primary"
                else source["rpc"]["observed_primary"])
    def wrong(endpoint):
        address, identity = endpoint.split("/", 1)
        actor, _, suffix = identity.split(";")
        return f"{address}/{actor};00000000-0000-0000-0000-000000000999;{suffix}"
    epoch = {"data_loss_number": 1, "configuration_number": 2 << 32}
    result = {"qualification_only": True, "admission_only": False, "kind": kind,
            "from": wrong(own) if kind == "wrong_sender" else own,
            "target": wrong(selected) if kind == "wrong_target" else selected,
            "native_selected_peer": selected, "sequence": 21, "native_epoch": dict(epoch),
            "sent_epoch": {"data_loss_number": 1, "configuration_number": 1 << 32} if kind == "stale_epoch" else dict(epoch)}
    # Explicit offline completed-frame fixture, not a native capture.
    message_id = str(uuid.uuid4())
    body = b"offline-wire-fault-model:" + uuid.UUID(message_id).bytes_le
    frame = struct.pack("<I", len(body) + 12) + b"\0" * 8 + body
    own_socket, selected_socket = own.split("/", 1)[0], selected.split("/", 1)[0]
    result["delivery"] = {
        "classification": "socket_write_all_and_flush", "session": 1,
        "local": "127.0.0.1:40001" if source["role"] == "Primary" else own_socket,
        "remote": selected_socket if source["role"] == "Primary" else "127.0.0.1:40002",
        "remote_listener": selected_socket, "message_id": {"guid": message_id, "index": 1},
        "frame_hex": frame.hex(), "frame_bytes": len(frame),
    }
    return result


def wire_observation(reply):
    """Offline independent-ingress model paired with the explicit frame fixture."""
    delivery = reply["delivery"]
    return {
        "classification": "destination_kernel_ingress", "interface": "lo", "kernel_drops": 0,
        "allowed": [[reply["from"].split("/", 1)[0], reply["native_selected_peer"].split("/", 1)[0]]],
        "seen_packets": 1, "wire_bytes": delivery["frame_bytes"] + 40,
        "payload_bytes": delivery["frame_bytes"],
        "packets": [{"local": delivery["local"], "remote": delivery["remote"],
                     "sequence": 100, "payload_hex": delivery["frame_hex"]}],
    }


def loss_model(changed, native=False):
    seed = data.Ledger()
    for index in range(20):
        record = seed.mutation(f"seed-{index}")
        seed.reply(record, 200, {"request_id": record["id"], "sequence": index + 1, "outcome": "committed"})
    before = observation(native)
    before.update(service=SERVICE, partition=PARTITION)
    for replica in before["replicas"]:
        rpc = replica["rpc"]
        rpc.update(partition=PARTITION, native_current_progress=20)
        rpc["state"].update(
            last=20, history_count=20, data_loss_calls=0, data_loss_changed_calls=0,
            last_data_loss_boundary=None, results={key: record["sequence"] for key, record in seed.records.items()},
            values={record["key"]: record["value"] for record in seed.records.values()})
        rpc["engine"]["progress"].update(assigned=20, applied=20)
    primary(before, 1 if native else 0)
    after = copy.deepcopy(before)
    for index in ((0, 1) if native else (0, 2)):
        replacement = after["replicas"][index]
        replacement["replica"] = replacement["rpc"]["replica"] = 99 + index
        replacement["rpc"].update(incarnation="replacement", engine_incarnation="replacement")
        replacement["address"] = replacement["rpc"]["service_address"] = (
            f"http://127.0.0.1:{2000 + index}/data/replacement/service")
    after["native_partition"]["epoch"]["data_loss"] += 1
    retained = {key: record for key, record in seed.records.items() if not changed or record["sequence"] <= 5}
    last = 5 if changed else 20
    for replica in after["replicas"]:
        rpc = replica["rpc"]
        rpc["native_epoch"]["data_loss_number"] += 1
        rpc["native_current_progress"] = last
        rpc["engine"]["progress"].update(assigned=last, applied=last)
        rpc["state"].update(last=last, history_count=len(retained),
                            results={key: record["sequence"] for key, record in retained.items()},
                            values={record["key"]: record["value"] for record in retained.values()})
    survivor_index = 2 if native else 1
    survivor = after["replicas"][survivor_index]
    survivor["rpc"]["state"].update(data_loss_calls=1, data_loss_changed_calls=1 if changed else 0,
                                     last_data_loss_boundary=5 if changed else None)
    primary(after, survivor_index)
    return before, after, seed


def completed(operation=None):
    operation = operation or {"service": SERVICE, "partition": PARTITION}
    return {"State": "Completed", "InvokeDataLossResult": {"ErrorCode": 0, "SelectedPartition": {
        "ServiceName": operation["service"], "PartitionId": operation["partition"]}}}


def operation_record():
    return {"service": SERVICE, "partition": PARTITION, "operation_id": OPERATION, "start_http_status": 202}


class NativeFaultHttpModels(unittest.TestCase):
    def test_exact_native_paths_encode_service_id_and_never_request_full_loss_or_force(self):
        service = "fabric:/Owned/name with+symbols"
        path = data.fas_path(service, PARTITION, OPERATION, "StartDataLoss")
        self.assertEqual(urlsplit(path).path,
                         f"/Faults/Services/Owned~name%20with%2Bsymbols/$/GetPartitions/{PARTITION}/$/StartDataLoss")
        self.assertEqual(parse_qs(urlsplit(path).query),
                         {"api-version": ["6.0"], "OperationId": [OPERATION], "DataLossMode": ["PartialDataLoss"]})
        progress = data.fas_path(service, PARTITION, OPERATION, "GetDataLossProgress")
        self.assertEqual(parse_qs(urlsplit(progress).query), {"api-version": ["6.0"], "OperationId": [OPERATION]})
        cancel = data.fas_path(service, PARTITION, OPERATION, "Cancel")
        self.assertEqual(urlsplit(cancel).path, "/Faults/$/Cancel")
        self.assertEqual(parse_qs(urlsplit(cancel).query)["Force"], ["false"])
        for partition in ("partition", "00000000-0000-0000-0000-000000000000", PARTITION + "?other=1"):
            with self.assertRaises((ValueError, RuntimeError)):
                data.fas_path(service, partition, OPERATION, "StartDataLoss")

    def test_management_executes_only_on_the_original_allocation_with_bounded_http(self):
        allocation = MagicMock()
        allocation.execute.return_value = '{"http_status":202,"body":null}'
        path = data.fas_path(SERVICE, PARTITION, OPERATION, "StartDataLoss")
        self.assertEqual(data.native_management(allocation, "POST", path)["http_status"], 202)
        allocation.execute.assert_called_once_with("python3", "-c", data.NATIVE_MANAGEMENT, "POST", path, timeout=15)
        allocation.sf.assert_not_called()
        for invalid in ("//other-host/path", "http://other-host/path", "/path#fragment"):
            with self.assertRaises(RuntimeError):
                data.native_management(allocation, "POST", invalid)

    def test_remote_http_snippet_uses_empty_post_and_actual_native_error_json(self):
        path = data.fas_path(SERVICE, PARTITION, OPERATION, "StartDataLoss")
        for method, status, body in (("POST", 202, b""), ("GET", 200, json.dumps(completed()).encode())):
            response = MagicMock(code=status)
            response.__enter__.return_value = response
            response.read.return_value = body
            opener = MagicMock()
            opener.open.return_value = response
            with patch("urllib.request.build_opener", return_value=opener), \
                 patch.object(sys, "argv", ["native-management", method, path]), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                exec(data.NATIVE_MANAGEMENT, {})
            request = opener.open.call_args.args[0]
            self.assertEqual(request.full_url, "http://127.0.0.1:19080" + path)
            self.assertEqual(request.get_method(), method)
            self.assertEqual(request.data, b"" if method == "POST" else None)
            self.assertEqual(opener.open.call_args.kwargs, {"timeout": 10})
            response.read.assert_called_once_with(data.MAX_RESPONSE + 1)
            self.assertEqual(json.loads(output.getvalue())["http_status"], status)
        error = urllib.error.HTTPError("http://127.0.0.1:19080/", 403, "denied", {},
                                       io.BytesIO(b'{"Error":{"Code":"E_ACCESSDENIED"}}'))
        opener.open.side_effect = error
        with patch("urllib.request.build_opener", return_value=opener), \
             patch.object(sys, "argv", ["native-management", "POST", path]), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            exec(data.NATIVE_MANAGEMENT, {})
        reply = json.loads(output.getvalue())
        with self.assertRaisesRegex(RuntimeError, "required native.*403.*E_ACCESSDENIED"):
            data.native_success(reply, 202, "StartDataLoss")
        opener.open.side_effect = urllib.error.HTTPError(
            "http://127.0.0.1:19080/", 404, "unsupported route", {}, io.BytesIO(b"<html>not an API</html>"))
        with patch("urllib.request.build_opener", return_value=opener), \
             patch.object(sys, "argv", ["native-management", "POST", path]), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            exec(data.NATIVE_MANAGEMENT, {})
        with self.assertRaisesRegex(RuntimeError, "required native.*404.*not an API"):
            data.native_success(json.loads(output.getvalue()), 202, "StartDataLoss")

    def test_source_backed_fas_json_requires_actual_completed_result_and_owned_partition(self):
        operation = operation_record()
        self.assertEqual(data.fas_state(completed(), operation), "Completed")
        mutations = [
            lambda value: value.update(State=3),
            lambda value: value.update(State="Unknown"),
            lambda value: value.pop("InvokeDataLossResult"),
            lambda value: value["InvokeDataLossResult"].update(ErrorCode="0"),
            lambda value: value["InvokeDataLossResult"].update(ErrorCode=1),
            lambda value: value["InvokeDataLossResult"]["SelectedPartition"].update(ServiceName="fabric:/Other"),
            lambda value: value["InvokeDataLossResult"]["SelectedPartition"].update(PartitionId=OPERATION),
        ]
        for mutate in mutations:
            value = completed()
            mutate(value)
            with self.subTest(value=value), self.assertRaises((RuntimeError, ValueError)):
                data.fas_state(value, operation)
        for state in ("Faulted", "Cancelled", "ForceCancelled"):
            with self.assertRaises(RuntimeError):
                data.fas_state({"State": state}, operation)

    def test_progress_poll_is_bounded_and_native_faults_are_not_passes(self):
        operation = operation_record()
        replies = [{"http_status": 200, "body": {"State": "Running"}},
                   {"http_status": 200, "body": {"State": "RollingBack"}},
                   {"http_status": 200, "body": completed()}]
        with patch.object(data, "native_management", side_effect=replies) as management, \
             patch.object(data.time, "sleep"):
            self.assertEqual(data.poll_fas(MagicMock(), operation), completed())
        self.assertTrue(operation["native_cleanup_terminal"])
        self.assertTrue(all(call.args[1] == "GET" and "GetDataLossProgress" in call.args[2]
                            for call in management.call_args_list))
        clock = iter((0, .1, 1, 2, 2.1, 3, 4))
        operation = operation_record()
        with patch.object(data, "native_management", return_value=replies[0]) as management, \
             patch.object(data.time, "monotonic", side_effect=lambda: next(clock)), \
             patch.object(data.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "bounded polling"):
                data.poll_fas(MagicMock(), operation, timeout=2)
        self.assertLessEqual(management.call_count, 2)
        self.assertFalse(operation.get("native_cleanup_terminal", False))
        for reply in ({"http_status": 404, "body": {"Error": {"Code": "UnknownAPI"}}},
                      {"http_status": 200, "body": {"State": "Faulted", "InvokeDataLossResult": {"ErrorCode": -1}}}):
            with patch.object(data, "native_management", return_value=reply):
                with self.assertRaises(RuntimeError):
                    data.poll_fas(MagicMock(), operation_record())

    def test_cancellation_polls_normal_native_rollback_and_only_its_recorded_operation_id(self):
        operation = operation_record()
        replies = [
            {"http_status": 200, "body": {"State": "Running"}},
            {"http_status": 200, "body": None},
            {"http_status": 200, "body": {"State": "RollingBack"}},
            {"http_status": 200, "body": {"State": "Cancelled"}},
        ]
        with patch.object(data, "native_management", side_effect=replies) as management, \
             patch.object(data.time, "sleep"):
            data.cancel_fas(MagicMock(), operation)
        self.assertTrue(operation["native_cleanup_terminal"])
        self.assertEqual(operation["state"], "Cancelled")
        calls = management.call_args_list
        self.assertEqual([call.args[1] for call in calls], ["GET", "POST", "GET", "GET"])
        self.assertIn("/Faults/$/Cancel?", calls[1].args[2])
        self.assertTrue(all(parse_qs(urlsplit(call.args[2]).query)["OperationId"] == [OPERATION] for call in calls))
        self.assertIn("Force=false", calls[1].args[2])

    def test_cancel_response_alone_or_force_cancel_never_establishes_transport_cleanup(self):
        operation = operation_record()
        replies = [{"http_status": 200, "body": {"State": "Running"}},
                   {"http_status": 200, "body": None},
                   {"http_status": 200, "body": {"State": "ForceCancelled"}}]
        with patch.object(data, "native_management", side_effect=replies):
            with self.assertRaisesRegex(RuntimeError, "transport cleanup"):
                data.cancel_fas(MagicMock(), operation)
        self.assertFalse(operation.get("native_cleanup_terminal", False))

    def test_ambiguous_start_missing_query_still_attempts_cancel_and_completion_race_is_verified(self):
        operation = operation_record()
        replies = [
            {"http_status": 503, "body": {"Error": {"Code": "Unavailable"}}},
            {"http_status": 409, "body": {"Error": {"Code": "AlreadyCompleted"}}},
            {"http_status": 200, "body": completed()},
        ]
        with patch.object(data, "native_management", side_effect=replies) as management:
            data.cancel_fas(MagicMock(), operation)
        self.assertEqual(management.call_args_list[1].args[1], "POST")
        self.assertTrue(operation["native_cleanup_terminal"])
        self.assertIn("cleanup_query_error", operation)

    def test_unaccepted_command_absence_requires_native_exact_error_not_an_unknown_404(self):
        operation = operation_record()
        operation.pop("start_http_status")
        absent = {"http_status": 404, "body": {"Error": {"Code": "FABRIC_E_TEST_COMMAND_OPERATION_ID_NOT_FOUND"}}}
        with patch.object(data, "native_management", return_value=absent) as management:
            data.cancel_fas(MagicMock(), operation)
        self.assertEqual(operation["state"], "Absent")
        self.assertEqual(management.call_count, 1)
        with patch.object(data, "native_management", return_value={"http_status": 404, "body": {}}):
            with self.assertRaises(RuntimeError):
                data.cancel_fas(MagicMock(), operation_record())


class WireFaultModels(unittest.TestCase):
    def test_rust_destination_wire_identity_does_not_invent_public_transport_suffix(self):
        value, endpoints = wire_model()
        source = value["replicas"][1]
        source["rpc"]["observed_primary"] = endpoints[0].removesuffix(";transport")
        proof = receipt(value, source, "stale_copy_ack")
        result = data.wire_receipt(value, source, "stale_copy_ack", proof, wire_observation(proof))
        self.assertEqual(result["destination_kind"], "rust")
        self.assertIsNone(data.wire_endpoint(proof["native_selected_peer"])["suffix"])
        self.assertEqual(data.wire_endpoint(endpoints[0])["suffix"], ";transport")

    def test_native_empty_transport_suffix_reaches_actual_wire_receipt_validation(self):
        value, endpoints = wire_model(True, True)
        source = value["replicas"][1]
        source["rpc"]["replication_address"] = endpoints[1].removesuffix("transport")
        for kind in ("stale_epoch", "wrong_sender", "wrong_target"):
            proof = receipt(value, source, kind)
            result = data.wire_receipt(value, source, kind, proof, wire_observation(proof))
            self.assertEqual(result["source_kind"], "rust")
        self.assertEqual(data.wire_endpoint(source["rpc"]["replication_address"])["suffix"], ";")
        for suffix in (";bad/path", ";bad id", ";a;b"):
            with self.assertRaises(RuntimeError):
                data.wire_endpoint(endpoints[1].removesuffix(";transport") + suffix)

    def test_both_rust_to_native_negative_directions_correlate_actual_configured_peers(self):
        for rust_primary, kinds in ((True, ("stale_epoch", "wrong_sender", "wrong_target")),
                                    (False, ("wrong_sender", "stale_copy_ack", "wrong_target"))):
            value, _ = wire_model(True, rust_primary)
            source = value["replicas"][1]
            for kind in kinds:
                with self.subTest(rust_primary=rust_primary, kind=kind):
                    proof = receipt(value, source, kind)
                    result = data.wire_receipt(value, source, kind, proof, wire_observation(proof))
                    self.assertEqual(result["source_kind"], "rust")
                    self.assertEqual(result["destination_kind"], "native-classic-v1")
                    # The native fixture UUID/port-zero bind is not EndOpen.
                    self.assertIn("RA-owned GUID/port not invented", result["address_evidence"])

    def test_admission_receipt_rejects_unconfigured_peer_identity_and_fabricated_epoch_or_sequence(self):
        value, endpoints = wire_model(True, True)
        source = value["replicas"][1]
        actions = [
            lambda reply: reply.update(admission_only=True),
            lambda reply: reply.update(sequence=22),
            lambda reply: reply.update(sent_epoch=reply["native_epoch"]),
            lambda reply: reply.update(native_selected_peer=endpoints[1]),
            lambda reply: reply.update(target=endpoints[2]),
            lambda reply: reply.update(**{"from": endpoints[0]}),
        ]
        for action in actions:
            reply = receipt(value, source, "stale_epoch")
            action(reply)
            with self.assertRaises(RuntimeError):
                data.wire_receipt(value, source, "stale_epoch", reply)
        value["replicas"][0]["rpc"]["replication_bind_address"] = "127.0.0.2:0"
        with self.assertRaisesRegex(RuntimeError, "listener host"):
            data.wire_receipt(value, source, "stale_epoch", receipt(value, source, "stale_epoch"))

    def test_rejection_evidence_requires_unchanged_native_epoch_identity_ledger_state_and_progress(self):
        before, _ = wire_model()
        data.stable_data(before, copy.deepcopy(before))
        actions = [
            lambda value: value["native_partition"]["epoch"].update(data_loss=2),
            lambda value: value["replicas"][0]["rpc"]["native_epoch"].update(configuration_number=9 << 32),
            lambda value: value["replicas"][0]["rpc"].update(engine_incarnation="different"),
            lambda value: value["replicas"][0].update(role="ActiveSecondary"),
            lambda value: value["replicas"][0]["rpc"]["state"]["results"].update(invalid_probe=21),
            lambda value: value["replicas"][0]["rpc"]["state"]["values"].update(invalid_probe="must-not-apply"),
            lambda value: value["replicas"][0]["rpc"]["state"].update(last=21),
            lambda value: value["replicas"][0]["rpc"]["engine"]["progress"].update(assigned=21),
        ]
        for action in actions:
            after = copy.deepcopy(before)
            action(after)
            with self.assertRaises(RuntimeError):
                data.stable_data(before, after)

    def test_scenario_sends_exactly_one_wire_parameter_then_queries_and_continues_writes_and_copy(self):
        before, _ = wire_model()
        qualification = data.Qualification(MagicMock(), MagicMock(), {"application": "fabric:/Owned"}, False)
        evidence = {}
        latest = {}
        capture = MagicMock()
        capture.__enter__.return_value.finish.side_effect = lambda: wire_observation(latest["reply"])
        def send(endpoint, operation, parameters, **kwargs):
            self.assertEqual(operation, "test")
            self.assertEqual(set(parameters), {"wire_probe"})
            source = next(replica for replica in before["replicas"] if replica["address"] == endpoint)
            latest["reply"] = receipt(before, source, parameters["wire_probe"])
            return 200, latest["reply"]
        with patch.object(qualification, "converge", return_value=before), \
             patch.object(qualification, "wait", return_value=before), \
             patch.object(data, "native_snapshot", return_value=before) as native, \
             patch.object(data, "request", side_effect=send) as request, \
             patch.object(qualification, "writes", return_value={"sequences": [21]}) as writes, \
             patch.object(qualification, "copy") as positive_copy, patch.object(data.time, "sleep"), \
             patch.object(data, "WireCapture", return_value=capture):
            qualification.stale_wire_receipts(evidence)
        self.assertEqual(request.call_count, 6)
        self.assertEqual(native.call_count, 18)
        self.assertEqual(writes.call_count, 7)  # real post-move primary-session establishment plus six positives
        self.assertEqual(positive_copy.call_args.kwargs, {"target_kind": "rust"})
        self.assertNotIn("status", evidence)  # A model is not a native PASS report.
        with patch.object(qualification, "converge", return_value=before), \
             patch.object(qualification, "wait", return_value=before), \
             patch.object(qualification, "writes", return_value={"sequences": [21]}), \
             patch.object(data, "request", return_value=(400, {"error": "wire API unsupported"})), \
             patch.object(data, "WireCapture", return_value=capture):
            with self.assertRaisesRegex(RuntimeError, "required actual wire probe API"):
                qualification.stale_wire_receipts({})


class DataLossInvariantModels(unittest.TestCase):
    def test_source_partial_loss_math_removes_both_of_two_but_three_with_rust_primary_leaves_native(self):
        for nodes, primary_node, survivor_kinds in (
                (("N0010", "N0030"), "N0010", set()),
                (data.MIXED_NODES, "N0020", {"native-classic-v1"})):
            secondaries = set(nodes) - {primary_node}
            choices = list(itertools.combinations(secondaries, len(nodes) // 2))
            self.assertTrue(choices)
            for selected in choices:
                survivors = set(nodes) - {primary_node, *selected}
                self.assertEqual(len(survivors), 0 if len(nodes) == 2 else 1)
                self.assertEqual({data.NODE_ENGINES[node] for node in survivors}, survivor_kinds)

    def test_changed_and_unchanged_native_callbacks_preserve_exact_authoritative_contents_and_progress(self):
        for native in (False, True):
            for changed in (False, True):
                with self.subTest(native=native, changed=changed):
                    before, after, seed = loss_model(changed, native)
                    result = data.data_loss_invariants(before, after, seed, changed)
                    self.assertEqual(len(result.records), 5 if changed else 20)
                    self.assertEqual(len(seed.records), 20)
                    self.assertTrue(all(record["committed"] for record in seed.records.values()))

    def test_changed_requires_surviving_actual_callback_increment_and_not_just_fas_completion(self):
        actions = [
            lambda value: value["native_partition"]["epoch"].update(data_loss=1),
            lambda value: value["replicas"][1]["rpc"]["state"].update(data_loss_calls=0, data_loss_changed_calls=0),
            lambda value: value["replicas"][1]["rpc"]["state"].update(data_loss_changed_calls=0),
            lambda value: value["replicas"][1]["rpc"]["state"].update(last_data_loss_boundary=4),
            lambda value: value["replicas"][0]["rpc"]["state"].update(last=20),
            lambda value: value["replicas"][0]["rpc"]["state"]["results"].update(**{"seed-19": 20}),
            lambda value: value["replicas"][0]["rpc"]["state"]["values"].update(**{"key-seed-19": "value-seed-19"}),
            lambda value: value["replicas"][1]["rpc"]["engine"]["progress"].update(assigned=20),
        ]
        for action in actions:
            before, after, seed = loss_model(True)
            action(after)
            # Refresh the primary's independently resolved RPC where relevant.
            after["primary"]["rpc"] = copy.deepcopy(after["replicas"][1]["rpc"])
            with self.assertRaises(RuntimeError):
                data.data_loss_invariants(before, after, seed, True)

    def test_unchanged_rejects_changed_callback_missing_acknowledged_values_and_counter_reset(self):
        before, after, seed = loss_model(False)
        after["replicas"][1]["rpc"]["state"]["data_loss_changed_calls"] = 1
        with self.assertRaisesRegex(RuntimeError, "unarmed"):
            data.data_loss_invariants(before, after, seed, False)
        before, after, seed = loss_model(False)
        after["replicas"][0]["rpc"]["state"]["results"].pop("seed-19")
        with self.assertRaises(RuntimeError):
            data.data_loss_invariants(before, after, seed, False)
        before, after, seed = loss_model(False)
        before["replicas"][1]["rpc"]["state"]["data_loss_calls"] = 2
        with self.assertRaisesRegex(RuntimeError, "counters reset"):
            data.data_loss_invariants(before, after, seed, False)

    def test_native_progress_cannot_lie_and_replacement_alone_is_not_survivor_callback_evidence(self):
        before, after, seed = loss_model(True, True)
        after["replicas"][0]["rpc"]["native_current_progress"] = 20
        with self.assertRaisesRegex(RuntimeError, "native replicator progress"):
            data.data_loss_invariants(before, after, seed, True)
        before, after, seed = loss_model(True)
        after["replicas"][1]["rpc"].update(incarnation="another replacement", engine_incarnation="another replacement")
        with self.assertRaisesRegex(RuntimeError, "original survivor"):
            data.data_loss_invariants(before, after, seed, True)

    def test_actual_callback_counter_and_boundary_schema_is_required_even_on_copied_replacements(self):
        for field, invalid in (("data_loss_calls", True), ("data_loss_calls", -1),
                               ("data_loss_changed_calls", "0"), ("last_data_loss_boundary", "5")):
            before, after, seed = loss_model(True)
            after["replicas"][0]["rpc"]["state"][field] = invalid
            with self.subTest(field=field, invalid=invalid), self.assertRaises(RuntimeError):
                data.data_loss_invariants(before, after, seed, True)
        before, after, seed = loss_model(True)
        after["replicas"][0]["rpc"]["state"].pop("last_data_loss_boundary")
        with self.assertRaisesRegex(RuntimeError, "boundary telemetry"):
            data.data_loss_invariants(before, after, seed, True)

    def test_mixed_auxiliary_requires_three_ready_before_fault_but_client_can_use_real_minimum(self):
        value = observation(True)
        data.assert_data(value, value["service"], True, 3, 2)
        client = data.Client(MagicMock(), value["service"], True, data.Ledger(), 3, 2)
        with patch.object(data, "native_snapshot", return_value=value):
            self.assertIs(client.refresh(), value)
            value["replicas"] = [value["replicas"][0], value["replicas"][2]]
            self.assertIs(client.refresh(),value)
            # Fault setup and qualification convergence remain full-three checks.
            with self.assertRaisesRegex(RuntimeError, "cardinality"):
                data.assert_data(value,value["service"],True,3,2)
            value["native_partition"].update(target=2, minimum=2)
            with self.assertRaisesRegex(RuntimeError, "requested size"):
                client.refresh()
        self.assertEqual(client.endpoint, value["primary"]["endpoint"])

    def test_mixed_claim_requires_the_original_native_survivor_callback_and_new_native_primary(self):
        actions = [
            lambda before, after: primary(before, 0),
            lambda before, after: primary(after, 1),
            lambda before, after: after["replicas"][2]["rpc"].update(engine_kind="rust"),
            lambda before, after: before["native_partition"].update(target=2),
        ]
        for action in actions:
            before, after, seed = loss_model(True, True)
            action(before, after)
            with self.subTest(action=action), self.assertRaisesRegex(RuntimeError, "mixed partial loss"):
                data.data_loss_invariants(before, after, seed, True)


class AuxiliaryOwnershipModels(unittest.TestCase):
    def qualification(self, mixed=False, profile="rust"):
        expected = data.TypeIdentity("OwnedType", "1.1", "c" * 64, "d" * 64, (1, 2, 3))
        original = data.ApplicationIdentity("fabric:/Owned", expected, "e" * 64, "OwnedType_App1")
        allocation = MagicMock()
        allocation.sf.return_value = "[]"
        qualification = data.Qualification(allocation, MagicMock(),
            {"application": "fabric:/Owned", "profile": profile}, mixed, original)
        return qualification, original

    @staticmethod
    def auxiliary(original, partition=PARTITION):
        return {"service": SERVICE, "partition": partition, "target": 3, "minimum": 2, "mixed": False,
                "application_native_id": original.native_application_id, "original_routes": [], "cleanup": {}}

    def test_same_capability_on_recreated_application_does_not_authorize_auxiliary_mutation(self):
        qualification, original = self.qualification()
        replacement = data.replace(original, native_application_id="OwnedType_App2")
        with patch.object(data, "native_application_identity", return_value=replacement), \
             patch.object(data, "create_data_service") as create:
            with self.assertRaisesRegex(RuntimeError, "instance/capability drift"):
                qualification.create_auxiliary(False, {})
        create.assert_not_called()
        qualification.allocation.sf.assert_not_called()

    def test_unique_auxiliary_record_exists_before_create_even_when_reply_is_lost(self):
        qualification, original = self.qualification()
        evidence = {}
        def lose_response(*args):
            self.assertEqual(len(qualification.auxiliaries), 1)
            record = qualification.auxiliaries[0]
            self.assertIs(evidence["auxiliary"], record)
            self.assertEqual(record["application_native_id"], original.native_application_id)
            self.assertIsNone(record["partition"])
            self.assertEqual(record["create_state"], "requested")
            self.assertTrue(record["service"].startswith("fabric:/Owned/DataLossChanged"))
            self.assertEqual(args[2], record["service"])
            raise TimeoutError("modeled lost native create reply")
        with patch.object(data, "native_application_identity", return_value=original), \
             patch.object(data, "create_data_service", side_effect=lose_response):
            with self.assertRaisesRegex(TimeoutError, "lost native create"):
                qualification.create_auxiliary(True, evidence)
        record = qualification.auxiliaries[0]
        with patch.object(data, "native_application_identity", return_value=original):
            with self.assertRaisesRegex(RuntimeError, "no captured partition"):
                qualification.cleanup_auxiliary(record)
        self.assertFalse(any(call.args[:2] == ("service", "delete")
                             for call in qualification.allocation.sf.call_args_list))

    def test_mixed_uses_three_mixed_nodes_full_uses_three_rust_nodes_without_relabeling(self):
        for profile, mixed, nodes, target, aux_mixed in (
                ("mixed", True, data.MIXED_NODES, 3, True),
                ("full", True, ("N0020", "N0040", "N0050"), 3, False),
                ("native-data", True, ("N0020", "N0040", "N0050"), 3, False),
                ("rust", False, tuple(data.NODE_ENGINES), 3, False)):
            qualification, original = self.qualification(mixed, profile)
            evidence = {}
            def observe(**kwargs):
                record = qualification.auxiliaries[0]
                value = observation(False)
                value.update(service=record["service"], partition=PARTITION)
                return value
            with patch.object(data, "native_application_identity", return_value=original), \
                 patch.object(data, "create_data_service", return_value={}) as create, \
                 patch.object(qualification, "wait", side_effect=observe) as wait, \
                 patch.object(qualification, "auxiliary_partition", return_value=PARTITION):
                record = qualification.create_auxiliary(False, evidence)
            self.assertEqual(create.call_args.args[3:], (nodes, target, 2))
            self.assertEqual(wait.call_args.kwargs["mixed"], aux_mixed)
            self.assertEqual(record["mixed"], aux_mixed)
            self.assertEqual(record["eligible_nodes"], nodes)

    def test_reused_service_name_with_new_partition_cannot_be_deleted(self):
        qualification, original = self.qualification()
        auxiliary = self.auxiliary(original)
        description = {"http_status": 200, "body": {"Name": SERVICE, "ServiceKind": "Stateful"}}
        partitions = {"http_status": 200, "body": {"Items": [
            {"ServiceKind": "Stateful", "PartitionInformation": {"Id": OPERATION}}]}}
        with patch.object(data, "native_application_identity", return_value=original), \
             patch.object(data, "native_management", side_effect=[description, partitions]) as native:
            with self.assertRaisesRegex(RuntimeError, "name reused"):
                qualification.cleanup_auxiliary(auxiliary)
        self.assertEqual(native.call_args_list[0].args[2],
                         "/Applications/Owned/$/GetServices/Owned~DataLossChanged-model?api-version=6.0")
        qualification.allocation.sf.assert_not_called()

    def test_cleanup_cancels_only_recorded_fas_before_guarded_partition_deletion_and_route_retirement(self):
        qualification, original = self.qualification()
        auxiliary = self.auxiliary(original)
        auxiliary["original_routes"] = ["http://127.0.0.1:2000/data/original/service"]
        auxiliary["fas"] = operation_record()
        order = []
        def cancel(allocation, operation):
            self.assertIs(operation, auxiliary["fas"])
            operation["native_cleanup_terminal"] = True
            order.append("cancel")
        def check(*args, **kwargs):
            order.append("partition")
            return PARTITION
        qualification.allocation.sf.side_effect = lambda *args: order.append("delete")
        def wait(check, description, timeout):
            order.append("absence")
            return True
        with patch.object(data, "cancel_fas", side_effect=cancel), \
             patch.object(data, "native_application_identity", return_value=original), \
             patch.object(qualification, "auxiliary_partition", side_effect=check), \
             patch.object(data.runtime, "wait_condition", side_effect=wait), \
             patch.object(data, "request", return_value=(404, {})):
            qualification.cleanup_auxiliary(auxiliary)
        self.assertEqual(order, ["cancel", "partition", "delete", "absence"])
        self.assertTrue(auxiliary["cleanup"]["native_absence_confirmed"])
        self.assertTrue(auxiliary["cleanup"]["original_routes_retired"])
        qualification.allocation.sf.assert_called_once_with(
            "service", "delete", "--service-id", "Owned~DataLossChanged-model")

    def test_unknown_404_never_establishes_native_auxiliary_absence(self):
        qualification, original = self.qualification()
        auxiliary = self.auxiliary(original)
        for response, accepted in (
                ({"http_status": 404, "body": {"Error": {"Code": "UnknownAPI"}}}, False),
                ({"http_status": 404, "body": {"Error": {"Code": "FABRIC_E_SERVICE_DOES_NOT_EXIST"}}}, True),
                ({"http_status": 404, "body": {"Error": {"Code": "FABRIC_E_APPLICATION_NOT_FOUND"}}}, True),
                ({"http_status": 204, "body": None}, True)):
            with patch.object(data, "native_management", return_value=response):
                if accepted:
                    self.assertIsNone(qualification.auxiliary_partition(auxiliary, allow_absent=True))
                else:
                    with self.assertRaisesRegex(RuntimeError, "required native"):
                        qualification.auxiliary_partition(auxiliary, allow_absent=True)

    def test_retired_route_cannot_still_serve_data(self):
        auxiliary = {"original_routes": ["http://127.0.0.1:2000/data/old/service"],
                     "cleanup": {"native_absence_confirmed": True}}
        with patch.object(data, "request", return_value=(200, {"state": "still usable"})):
            with self.assertRaisesRegex(RuntimeError, "usable original route"):
                data.Qualification.retired_auxiliary_routes(auxiliary)
        self.assertFalse(auxiliary["cleanup"].get("original_routes_retired", False))


class DataLossCommandModels(unittest.TestCase):
    qualification = AuxiliaryOwnershipModels.qualification
    auxiliary = staticmethod(AuxiliaryOwnershipModels.auxiliary)

    def test_native_fasless_driver_removes_only_original_auxiliary_primary_and_one_secondary(self):
        before, _, _ = loss_model(False, True)
        qualification, original = self.qualification(True, "mixed")
        auxiliary = self.auxiliary(original)
        auxiliary.update(mixed=True, eligible_nodes=data.MIXED_NODES)
        removed = []
        def remove(value, replica):
            self.assertIs(value, before)
            removed.append((replica["node"], replica["replica"], replica["role"]))
        evidence = {}
        with patch.object(qualification, "guard_application"), \
             patch.object(qualification, "auxiliary_partition", return_value=PARTITION), \
             patch.object(qualification, "remove", side_effect=remove), \
             patch.object(data.runtime, "wait_native_replica_absent", return_value={"absent": True}) as absent, \
             patch.object(data, "native_management") as fas:
            qualification.native_partial_loss_removals(auxiliary, before, evidence)
        self.assertEqual(len(removed), 2)
        self.assertEqual(sum(role == "Primary" for _, _, role in removed), 1)
        self.assertEqual(sum(role == "ActiveSecondary" for _, _, role in removed), 1)
        self.assertIn("N0020", {node for node, _, _ in removed})
        self.assertEqual(absent.call_count, 2)
        fas.assert_not_called()
        self.assertEqual(evidence["native_removal_driver"]["accepted_requests"], 2)
        with patch.object(qualification, "remove") as remove:
            with self.assertRaises(RuntimeError):
                qualification.native_partial_loss_removals(
                    {**auxiliary, "partition": MAIN_PARTITION}, before, {})
            remove.assert_not_called()

    def move_fixture(self):
        after, _, seed = loss_model(False, True)
        before = copy.deepcopy(after)
        before["native_partition"]["epoch"]["configuration"] -= 1 << 32
        for replica in before["replicas"]:
            rpc = replica["rpc"]
            rpc["native_epoch"]["configuration_number"] -= 1 << 32
        primary(before, 0)
        qualification, original = self.qualification(True, "mixed")
        auxiliary = self.auxiliary(original)
        auxiliary.update(mixed=True, eligible_nodes=data.MIXED_NODES)
        reply = {"request": "MovePrimary", "partition": PARTITION, "target": "N0020",
                 "constraints": "enforced", "native_request_completed": True}
        return qualification, auxiliary, before, after, seed, reply

    def test_auxiliary_move_requires_original_application_partition_and_observed_active_secondary(self):
        for invalid in ("application", "partition", "main_service", "already_primary", "ineligible"):
            qualification, auxiliary, before, _, seed, _ = self.move_fixture()
            partition = PARTITION
            if invalid == "application":
                auxiliary["application_native_id"] = "OwnedType_App2"
            elif invalid == "partition":
                partition = MAIN_PARTITION
            elif invalid == "main_service":
                auxiliary["service"] = qualification.service
            elif invalid == "already_primary":
                primary(before, 1)
            else:
                auxiliary["eligible_nodes"] = ("N0010", "N0030")
            with self.subTest(invalid=invalid), \
                 patch.object(qualification, "guard_application"), \
                 patch.object(qualification, "auxiliary_partition", return_value=partition), \
                 patch.object(data.runtime, "run") as move:
                with self.assertRaises(RuntimeError):
                    qualification.move_auxiliary_primary(auxiliary, before, "N0020", seed, {})
            move.assert_not_called()

    def test_auxiliary_move_rejects_another_guid_target_or_unenforced_native_receipt(self):
        for field, invalid in (("partition", MAIN_PARTITION), ("target", "N0010"),
                               ("constraints", "ignored"), ("native_request_completed", False)):
            qualification, auxiliary, before, _, seed, reply = self.move_fixture()
            reply[field] = invalid
            with self.subTest(field=field), \
                 patch.object(qualification, "guard_application"), \
                 patch.object(qualification, "auxiliary_partition", return_value=PARTITION), \
                 patch.object(data.runtime, "run", return_value=json.dumps(reply)), \
                 patch.object(qualification, "wait") as wait:
                with self.assertRaisesRegex(RuntimeError, "constraints enforced"):
                    qualification.move_auxiliary_primary(auxiliary, before, "N0020", seed, {})
            wait.assert_not_called()

    def test_auxiliary_move_requires_full_membership_epoch_agreement_all_applied_and_same_lifetimes(self):
        for invalid in ("membership", "epoch", "applied", "native_progress", "lifetime", "unadvanced"):
            qualification, auxiliary, before, after, seed, reply = self.move_fixture()
            if invalid == "membership":
                after["replicas"].pop()
            elif invalid == "epoch":
                after["replicas"][0]["rpc"]["native_epoch"]["configuration_number"] += 1 << 32
            elif invalid == "applied":
                after["replicas"][2]["rpc"]["engine"]["progress"]["applied"] = 19
            elif invalid == "native_progress":
                after["replicas"][2]["rpc"]["native_current_progress"] = 19
            elif invalid == "lifetime":
                after["replicas"][2]["rpc"]["incarnation"] = "replacement"
            else:
                after["native_partition"]["epoch"] = dict(before["native_partition"]["epoch"])
                for replica in after["replicas"]:
                    rpc = replica["rpc"]
                    native_epoch = before["native_partition"]["epoch"]
                    rpc["native_epoch"] = {
                        "data_loss_number": native_epoch["data_loss"],
                        "configuration_number": native_epoch["configuration"]}
                primary(after, 1)
            with self.subTest(invalid=invalid), \
                 patch.object(qualification, "guard_application"), \
                 patch.object(qualification, "auxiliary_partition", return_value=PARTITION), \
                 patch.object(data.runtime, "run", return_value=json.dumps(reply)), \
                 patch.object(qualification, "wait", return_value=after):
                with self.assertRaises(RuntimeError):
                    qualification.move_auxiliary_primary(auxiliary, before, "N0020", seed, {})

    def test_changed_and_unchanged_dispatch_native_fas_with_local_ledger_and_truthful_next_assignment(self):
        for changed, mixed, initial_rust_primary in itertools.product((False, True), (False, True), (False, True)):
            before, after, seed = loss_model(changed, mixed)
            qualification, original = self.qualification(mixed, "mixed" if mixed else "rust")
            qualification.allocation.sf.return_value = json.dumps([
                {"name": "fabric:/System/FaultAnalysisService"}])
            auxiliary = self.auxiliary(original)
            auxiliary.update(mixed=mixed, eligible_nodes=data.MIXED_NODES if mixed else tuple(data.NODE_ENGINES))
            initial = copy.deepcopy(before)
            away = copy.deepcopy(before)
            if mixed:
                for value, configuration, selected in (
                        (initial, 0 if initial_rust_primary else 1 << 32, 1 if initial_rust_primary else 0),
                        (away, 1 << 32, 0)):
                    value["native_partition"]["epoch"]["configuration"] = configuration
                    for replica in value["replicas"]:
                        rpc = replica["rpc"]
                        rpc["native_epoch"]["configuration_number"] = configuration
                    primary(value, selected)
            # Primary application state is authoritative application progress;
            # its queue's applied frontier can still be the reconciliation
            # boundary until the next role/epoch operation.
            for value in (before, initial, away):
                for replica in value["replicas"]:
                    if replica["role"] == "Primary" and replica["rpc"]["engine_kind"] == "rust":
                        replica["rpc"]["engine"]["progress"]["applied"] = 0
            final = copy.deepcopy(after)
            retained = data.data_loss_invariants(before, after, seed, changed)
            start = 6 if changed else 21
            for index in range(4):
                record = retained.mutation(f"post-native-data-loss-{index}")
                retained.reply(record, 200, {"request_id": record["id"], "sequence": start + index, "outcome": "committed"})
            for replica in final["replicas"]:
                replica["rpc"]["state"].update(
                    last=start + 3, history_count=len(retained.records),
                    results={key: record["sequence"] for key, record in retained.records.items()},
                    values={record["key"]: record["value"] for record in retained.records.values()})
            primary(final, 2 if mixed else 1)
            evidence = {}
            main, _ = wire_model()
            main["partition"] = MAIN_PARTITION
            for replica in main["replicas"]:
                replica["rpc"]["partition"] = MAIN_PARTITION
            primary(main, 0)
            main_ledger = qualification.ledger
            def create_aux(changed, evidence):
                qualification.auxiliaries.append(auxiliary)
                evidence.update(auxiliary=auxiliary, initial_observation=initial)
                return auxiliary
            def client_factory(bundle, service, mixed, ledger, target, minimum):
                self.assertEqual((service, mixed, target, minimum), (SERVICE, auxiliary["mixed"], 3, 2))
                client = MagicMock()
                def write(request_id):
                    record = ledger.mutation(request_id)
                    sequence = max((item["sequence"] or 0 for item in ledger.records.values()), default=0) + 1
                    ledger.reply(record, 200, {"request_id": request_id, "sequence": sequence, "outcome": "committed"})
                    return record
                client.write.side_effect = write
                return client
            def arm(endpoint, operation, parameters):
                self.assertEqual(operation, "test")
                self.assertEqual(parameters, {"data_loss_boundary": 5})
                self.assertEqual(evidence["before"], before)
                if mixed:
                    self.assertEqual(evidence["pre_fas_moves"][-1]["after"], before)
                return 200, {"qualification_only": True, "armed_only": True, "data_loss_boundary": 5}
            def move(*args, **kwargs):
                self.assertEqual(args[:4], (str(qualification.bundle.observer), "127.0.0.1:19000", SERVICE,
                                            "--move-primary"))
                self.assertEqual(kwargs, {"timeout": 75})
                return json.dumps({"request": "MovePrimary", "partition": PARTITION, "target": args[4],
                                   "constraints": "enforced", "native_request_completed": True})
            def management(allocation, method, path):
                self.assertIs(allocation, qualification.allocation)
                self.assertEqual(method, "POST")
                self.assertIs(auxiliary["fas"], evidence["native_fas"])
                self.assertEqual(auxiliary["fas"]["application_native_id"], original.native_application_id)
                self.assertEqual(parse_qs(urlsplit(path).query)["DataLossMode"], ["PartialDataLoss"])
                self.assertIn(f"/Faults/Services/Owned~DataLossChanged-model/$/GetPartitions/{PARTITION}/", path)
                self.assertEqual(evidence["before"], before)
                return {"http_status": 202, "body": None}
            def cleanup_aux(record):
                record["cleanup"].update(native_absence_confirmed=True, original_routes_retired=True)
            waits = [initial] + ([away, before] if initial_rust_primary else [before]) if mixed else [before]
            waits += ([before] if changed else []) + [after, final]
            def observe(**kwargs):
                self.assertEqual((kwargs["service"], kwargs["target"], kwargs["minimum"], kwargs["mixed"]),
                                 (SERVICE, 3, 2, mixed))
                value = waits.pop(0)
                data.assert_data(value, SERVICE, mixed, 3, 2)
                if kwargs.get("accept"):
                    self.assertTrue(kwargs["accept"](value) or kwargs["accept"](value))
                return value
            with patch.object(qualification, "converge", return_value=main), \
                 patch.object(qualification, "stateless_siblings", return_value=[]), \
                 patch.object(qualification, "unchanged_stateless", return_value=[]), \
                 patch.object(qualification, "create_auxiliary", side_effect=create_aux), \
                 patch.object(qualification, "wait", side_effect=observe), \
                 patch.object(qualification, "guard_application"), \
                 patch.object(qualification, "auxiliary_partition", return_value=PARTITION), \
                 patch.object(qualification, "cleanup_auxiliary", side_effect=cleanup_aux) as cleanup, \
                 patch.object(data, "Client", side_effect=client_factory), \
                 patch.object(data, "request", side_effect=arm) as request, \
                 patch.object(data.runtime, "run", side_effect=move) as native_move, \
                 patch.object(data, "native_management", side_effect=management), \
                 patch.object(data, "poll_fas", return_value=completed()):
                qualification.data_loss_case(evidence, changed)
            self.assertEqual(waits, [])
            self.assertIs(qualification.ledger, main_ledger)
            self.assertEqual(qualification.service, "fabric:/Owned/Data")
            self.assertEqual(request.call_count, 3 if changed else 0)
            self.assertEqual(evidence["subsequent_writes"][0]["sequence"], start)
            self.assertEqual(evidence["final_workload"]["last"], start + 3)
            self.assertEqual(len(evidence["seed_workload"]["results"]), 20)
            self.assertEqual(native_move.call_count, (2 if initial_rust_primary else 1) if mixed else 0)
            self.assertEqual(evidence["expected_survivor_engine_kind"], "native-classic-v1" if mixed else "rust")
            self.assertEqual({callback["engine_kind"] for callback in evidence["actual_survivor_callbacks"]},
                             {"native-classic-v1"} if mixed else {"rust"})
            if mixed:
                self.assertEqual(evidence["pre_fas_moves"][-1]["move_request"]["target"], "N0020")
                self.assertEqual([target["node"] for target in evidence["pre_fas_moves"][-1]["eligible_targets"]
                                  if target["engine_kind"] == "rust"], ["N0020"])
                self.assertEqual(evidence["after_native_data_loss"]["primary"]["rpc"]["engine_kind"], "native-classic-v1")
            self.assertNotIn("status", evidence)
            cleanup.assert_called_once_with(auxiliary)

    def test_seed_or_arming_failure_still_cleans_the_auxiliary_before_leaving_the_case(self):
        for stage in ("seed", "move", "arming"):
            mixed = stage == "move"
            qualification, original = self.qualification(mixed, "mixed" if mixed else "rust")
            before, _, _ = loss_model(True, mixed)
            if mixed:
                primary(before, 0)
            auxiliary = self.auxiliary(original)
            auxiliary.update(mixed=mixed, eligible_nodes=data.MIXED_NODES)
            def create_aux(changed, evidence):
                qualification.auxiliaries.append(auxiliary)
                evidence.update(auxiliary=auxiliary, initial_observation=before)
                return auxiliary
            client = MagicMock()
            if stage == "seed":
                client.write.side_effect = RuntimeError("seed failed")
            def cleanup_aux(record):
                record["cleanup"].update(native_absence_confirmed=True, original_routes_retired=True)
            # The arming case needs a real populated local seed ledger; fail
            # specifically on the required callback-only hook reply.
            def make_client(bundle, service, mixed, ledger, target, minimum):
                if stage == "seed":
                    return client
                def write(request_id):
                    record = ledger.mutation(request_id)
                    sequence = len(ledger.records)
                    ledger.reply(record, 200, {"request_id": request_id, "sequence": sequence, "outcome": "committed"})
                    return record
                client.write.side_effect = write
                return client
            with patch.object(qualification, "converge", return_value=before), \
                 patch.object(qualification, "stateless_siblings", return_value=[]), \
                 patch.object(qualification, "create_auxiliary", side_effect=create_aux), \
                 patch.object(qualification, "wait", return_value=before), \
                 patch.object(data, "Client", side_effect=make_client), \
                 patch.object(data, "request", return_value=(400, {"error": "hook unavailable"})), \
                 patch.object(qualification, "guard_application"), \
                 patch.object(qualification, "auxiliary_partition", return_value=PARTITION), \
                 patch.object(data.runtime, "run", side_effect=RuntimeError("move failed")), \
                 patch.object(qualification, "cleanup_auxiliary", side_effect=cleanup_aux) as cleanup, \
                 patch.object(data, "native_management") as native:
                with self.assertRaisesRegex(RuntimeError, {
                        "seed": "seed failed", "move": "move failed", "arming": "arming API"}[stage]):
                    qualification.data_loss_case({}, True)
            cleanup.assert_called_once_with(auxiliary)
            native.assert_not_called()

    def test_required_inventory_dispatches_real_functions_instead_of_overwriting_missing(self):
        qualification, _ = self.qualification()
        # This dispatch-only model does not execute scenario(); supply its
        # successful prerequisite explicitly rather than bypassing the gate.
        qualification.result["scenarios"] = {
            "deployment_identity": {"status": "PASS"}, "writes_100": {"status": "PASS"}}
        names = []
        with patch.object(qualification, "scenario", side_effect=lambda name, action: names.append((name, action))):
            qualification.run()
        actions = dict(names)
        self.assertEqual(actions["stale_wire_receipts"], qualification.stale_wire_receipts)
        self.assertEqual(actions["authoritative_data_loss"], qualification.authoritative_data_loss)
        for profile in ("rust", "mixed", "full"):
            self.assertIn("stale_wire_receipts", data.required_scenarios(profile))
            self.assertIn("authoritative_data_loss", data.required_scenarios(profile))
        self.assertLess([name for name, _ in names].index("authoritative_data_loss"),
                        [name for name, _ in names].index("ledger_capacity_backpressure"))

    def test_required_native_api_failure_is_fail_not_missing_or_pass(self):
        qualification, _ = self.qualification()
        qualification.result["scenarios"] = {"authoritative_data_loss": {"status": "MISSING", "reason": "not executed"}}
        def action(evidence):
            data.native_success({"http_status": 403, "body": {"Error": {"Code": "E_ACCESSDENIED"}}},
                                202, "StartDataLoss")
        qualification.scenario("authoritative_data_loss", action)
        report = qualification.result["scenarios"]["authoritative_data_loss"]
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("required native", report["error"])
        self.assertNotIn("reason", report)


if __name__ == "__main__":
    unittest.main()
