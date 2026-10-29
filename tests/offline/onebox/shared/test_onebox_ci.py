"""Offline receipt/route models, not native or CI qualification."""
import contextlib
from argparse import Namespace
import copy
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import unittest
from unittest.mock import patch

from tests.onebox.shared import ci, fixture
from tests.offline.onebox.native_data import test_fube_data_test as fixtures


@unittest.skipUnless(ci.sys.platform == "linux", "Linux strict-bundle and onebox receipt models")
class ProofFiles(fixtures.ProjectFiles):
    def setUp(self):
        super().setUp()
        self.current = {
            "source": "a" * 40, "tree": "b" * 40, "checkout": "a" * 40, "head": "c" * 40,
            "merge": "a" * 40, "run_id": "100", "attempt": "1", "clean": True,
        }
        self.env = patch.dict(os.environ, {
            "GITHUB_SHA": "a" * 40, "FUBE_CI_HEAD": "c" * 40, "FUBE_CI_MERGE": "a" * 40,
            "GITHUB_RUN_ID": "100", "GITHUB_RUN_ATTEMPT": "1", "FUBE_CI_RUN_ID": "100",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        self.context = patch.object(ci, "context", side_effect=lambda: copy.deepcopy(self.current))
        self.context.start()
        self.addCleanup(self.context.stop)
        self.directory = self.bundle()
        self.tar = self.root / "bundle.tar.gz"
        with tarfile.open(self.tar, "w:gz") as archive:
            archive.add(self.directory, arcname=".")
        self.archive = self.root / "native.tar.zst"
        self.archive.write_bytes(b"modeled upstream nextest archive")
        self.producer = self.root / "producer.json"
        self.assertEqual(self.cli("produce", "--bundle-tar", self.tar, "--bundle-dir", self.directory,
                                  "--archive", self.archive, "--out", self.producer), 0)
        self.built = ci.read(self.producer)

    def cli(self, *args):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            result = ci.main([str(arg) for arg in args])
        self.last_stdout = output.getvalue()
        return result

    def emit(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        return path

    def wrapped(self, kind, value):
        return {"version": 1, "kind": kind, "status": "PASS", "errors": [], **value}

    def validated(self, route):
        path = self.root / "workers" / route / "validated-input.json"
        self.assertEqual(self.cli("validate", "--receipt", self.producer, "--bundle-tar", self.tar,
                                  "--bundle-dir", self.directory, "--archive", self.archive,
                                  "--route", route, "--out", path), 0, self.last_stdout)
        return path

    def provenance(self):
        return {
            "executable_source_revision": self.current["source"],
            "bundle_manifest_sha256": self.built["inputs"]["manifest"]["sha256"],
            "ci_head": self.current["head"], "ci_merge": self.current["merge"], "ci_run_id": "100",
            "qualification_script_sha256": self.built["inputs"]["qualification_script"]["sha256"],
        }

    @staticmethod
    def upload():
        return {"path": "fube-data-" + "d" * 32, "absence_verified": True,
                "native_response": {"status": 404, "body": None}}

    @staticmethod
    def copy_proof(ack=False, source_kind="rust"):
        result = {"concurrent_writes": {"successful": 100},
                  "live_copy": {"commits_inside_observed_build_horizon_window": 100}}
        if ack:
            result["destination_copy_ack"] = {
                "blocked_after_receive": True, "released": True, "negative_observations": 2,
                "held_operation": {"sequence": 12, "boundary": 10},
                "source_eos": ({
                    "kind": "destination_EOS_receipt_at_source",
                    "peer": {"replica": 42, "copy_horizon": 10, "copy_received": -1,
                             "copy_applied": 11, "copy_finish": 20, "applied": 0, "building": True},
                } if source_kind == "rust" else {
                    "kind": "native_source_enumeration_EOS_and_destination_CopyEnd_receipt",
                    "observation": {"requested_horizon": 10, "enumeration_end": True, "items": 12,
                                    "begin_count": 13, "end_count": 13, "callback_count": 13},
                }),
                "completed_native_replica": {"replica": 42, "status": "Ready", "rpc": {"copy_ack_hold": {
                    "released": True, "acknowledged": True, "held": False,
                    "expired": False, "cancelled": False, "sequence": 12,
                    "boundary": 10, "copy_end_received": True}}},
            }
        return result

    def essential_data(self, profile, reject_quorum=False):
        nodes = ["N0020", "N0040", "N0010"] if profile == "rust" else ["N0020", "N0030", "N0010"]
        kinds = ["rust"] * 3 if profile == "rust" else [ci.data.NODE_ENGINES[node] for node in nodes]
        rows = [{
            "node": node, "replica": 1000 + index, "incarnation": 2000 + index if kind == "rust" else "engine-" + node,
            "engine_incarnation": "engine-" + node, "host": "host-" + node,
            "pid": 3000 + index, "engine_kind": kind,
        } for index, (node, kind) in enumerate(zip(nodes, kinds))]
        service, partition = "fabric:/Fixture/Data", "data-partition"
        initial = {"initial-1-" + str(index): index + 1 for index in range(100)}

        def writes(results):
            return {"requested": len(results), "successful": len(results),
                    "ids": list(results), "sequences": list(results.values())}

        def snapshot(results, primary=0, configuration=1, members=None, name=service, part=partition):
            replicas = []
            members = members or rows
            for index, member in enumerate(members):
                rpc = {**member, "profile": "fube-data-v1", "partition": part}
                endpoint = "http://" + rpc["node"] + "/" + part + "/" + str(rpc["replica"]) + "/service"
                role = ("Primary" if index == primary else
                        "Secondary" if rpc["engine_kind"] == "rust" else "ActiveSecondary")
                rpc.update(role=role, current_role=role, service_address=endpoint,
                           state={"last": max(results.values()), "copy_in_progress": False, "results": results,
                                  "values": {"key-" + key: "value-" + key for key in results},
                                  "history_count": len(results), "retained_bytes": len(results) * 100},
                           engine={"closed": False, "fault": None, "copy_done": True,
                                   "progress": dict.fromkeys(ci.PROGRESS_SPEC, max(results.values()))})
                if role == "ActiveSecondary":
                    rpc["engine"]["progress"].update(assigned=0, committed=0, completed=0, client_completed=0)
                elif role == "Primary" and rpc["engine_kind"] == "rust":
                    rpc["engine"]["progress"]["applied"] = 0
                replicas.append({
                    "node": rpc["node"], "replica": rpc["replica"], "role": "Primary" if index == primary else "ActiveSecondary",
                    "status": "Ready", "address": endpoint, "service_http_status": 200 if index == primary else 503,
                    "deployed": {"host_process_id": rpc["pid"]}, "rpc": rpc,
                })
            placed = replicas[primary]
            return {
                "observer": "fube-client", "service": name, "partition": part,
                "native_partition": {"target": len(members), "minimum": 1 if len(members) == 1 else 2,
                                     "status": "Ready", "epoch": {"data_loss": 1, "configuration": configuration}},
                "replicas": replicas, "primary": {
                    "resolver": "fube-client", "endpoint": placed["address"],
                    "native_pid": placed["rpc"]["pid"], "rpc": copy.deepcopy(placed["rpc"])},
            }

        def workload(results, successful=None):
            return {"successful": len(results) if successful is None else successful,
                    "accepted": len(results), "last": max(results.values())}

        def process(rpc, action="inspect"):
            return {
                "pid": rpc["pid"], "host": rpc["host"], "start_ticks": rpc["pid"] * 10,
                "code_sequence": rpc["pid"] * 100, "action": action,
                "executable_sha256": self.built["inputs"]["app" if rpc["engine_kind"] == "rust" else "native_oracle"]["sha256"],
                "classification": "native-free" if rpc["engine_kind"] == "rust" else "native-classic-dynamic",
                "native_libraries": dict.fromkeys(("fabricruntime", "fabriccommon", "fabricclient"),
                                                rpc["engine_kind"] != "rust"),
                "activation_environment": {
                    "Fabric_NodeName": rpc["node"], "Fabric_ApplicationHostId": rpc["host"],
                    "Fabric_CodePackageInstanceSeqNum": str(rpc["pid"] * 100)},
            }

        def expected(rpc, captured=False):
            return {
                "native_pid": rpc["pid"], "host": rpc["host"], "node": rpc["node"], "kind": rpc["engine_kind"],
                "engine_sha256": process(rpc)["executable_sha256"],
                "native_entry_point_instance": str(rpc["pid"] * 100 + 1), "native_code_version": "1.1",
                "native_query": {
                    "Name": "Code", "ServiceManifestName": "RustRuntimeService", "Version": "1.1",
                    "MainEntryPoint": {"ProcessId": str(rpc["pid"]), "InstanceId": str(rpc["pid"] * 100 + 1)}},
                **({"captured": process(rpc)} if captured else {}),
            }

        def duplicate(request_id, sequence):
            return {"request_id": request_id, "sequence": sequence, "duplicate": True, "outcome": "committed"}

        def move(before, after):
            return {"before": before, "after": after, "old_primary_http_status": 503,
                    "move_request": {"request": "MovePrimary", "constraints": "enforced",
                                     "native_request_completed": True, "partition": partition,
                                     "target": after["primary"]["rpc"]["node"]}}

        baseline = snapshot(initial)
        values = {"key-" + key: "value-" + key for key in initial}
        for replica in baseline["replicas"]:
            replica["rpc"]["state"]["values"] = values
        baseline["primary"]["rpc"]["state"]["values"] = values
        original_id = next(iter(initial))
        post_move = {"planned-move-2-" + str(index): 101 + index for index in range(5)}
        moved_results = {**initial, **post_move}
        moved = snapshot(moved_results, primary=1, configuration=2)
        lost_id, lost_sequence = "lost-reply-6", 106
        lost_results = {**moved_results, lost_id: lost_sequence}
        lost_before = snapshot(lost_results, primary=1, configuration=2)
        lost_after = snapshot(lost_results, configuration=3)
        retry = duplicate(lost_id, lost_sequence)
        record = {"id": lost_id, "key": "key-" + lost_id, "value": "value-" + lost_id,
                  "sequence": lost_sequence, "committed": True, "attempts": [
                      {"outcome": "ambiguous_transport", "reply_intentionally_unread": True},
                      {"http_status": 200, "reply": retry}, {"http_status": 200, "reply": retry}]}
        no_quorum = {"id": "no-quorum-7", "sequence": 107, "committed": False, "attempts": [
            {"outcome": "acceptance_unknown_until_HTTP_reply", "native_quorum_unavailable": True},
            {"http_status": 202, "reply": {"request_id": "no-quorum-7", "outcome": "ambiguous",
                                         "sequence": 107, "duplicate": False}}]}
        if reject_quorum:
            no_quorum["sequence"] = None
            no_quorum["attempts"][1] = {
                "http_status": 503, "reply": {"request_id": "no-quorum-7", "outcome": "rejected", "duplicate": False}}
        recovered_results = lost_results if reject_quorum else {**lost_results, "no-quorum-7": 107}
        recovered = snapshot(recovered_results, configuration=3)
        old = recovered["primary"]["rpc"]
        diagnostics = copy.deepcopy(old)
        diagnostics["engine"]["progress"]["committed"] = 106
        post_failure = {"abrupt-primary-loss-8-" + str(index): max(recovered_results.values()) + 1 + index
                        for index in range(5)}
        failure_results = {**recovered_results, **post_failure}
        replaced_rows = copy.deepcopy(rows)
        replaced_rows[0].update(host="replacement-host", pid=4000, engine_incarnation="replacement-engine",
                                incarnation=4001 if kinds[0] == "rust" else "replacement-engine")
        failed_over = snapshot(failure_results, primary=1, configuration=4, members=replaced_rows)
        new = failed_over["primary"]["rpc"]
        auxiliary_row = {**replaced_rows[1], "replica": 9001,
                         "incarnation": 9002 if kinds[1] == "rust" else "auxiliary-engine",
                         "engine_incarnation": "auxiliary-engine"}
        auxiliary = snapshot({"auxiliary": 1}, members=[auxiliary_row],
                             name="fabric:/Fixture/CloseIsolation", part="auxiliary-partition")
        capacity_results = dict(failure_results)
        capacity_results.update({"c-" + str(index): index + 1 for index in range(len(failure_results), 4096)})
        full = snapshot(capacity_results, configuration=4, members=replaced_rows)
        rejection = {"id": "capacity-rejected", "sequence": None, "committed": False, "attempts": [{
            "http_status": 503, "reply": {"request_id": "capacity-rejected", "outcome": "rejected",
                                        "duplicate": False, "error": "native resource limit: provider history/identity ledger"}}]}
        scenarios = {
            "writes_100": {"writes": writes(initial), "observation": baseline,
                           "workload": {**workload(initial), "results": initial, "values": values}},
            "identified_retry": {
                "identical_retry": duplicate(original_id, 1),
                "conflicting_retry": {"http_status": 503, "reply": {
                    "outcome": "rejected", "request_id": original_id, "duplicate": False,
                    "error": "invalid input: conflicting request identity"}},
                "workload": workload(initial), "observation": baseline},
            "planned_move": {
                **move(baseline, snapshot(initial, primary=1, configuration=2)),
                "post_move_writes": writes(post_move), "observation": moved, "workload": workload(moved_results)},
            "lost_reply_dedup": {**move(lost_before, lost_after), "before_move": lost_before,
                                "observation": lost_after, "workload": workload(lost_results), "retry_outcome": "committed"},
            "unavailable_quorum": {
                "before": lost_after, "observation": recovered, "workload": workload(recovered_results, 106),
                "paused_processes": [process(r["rpc"], "pause") for r in lost_after["replicas"][1:]],
                "resume": [process(r["rpc"], "resume") for r in lost_after["replicas"][1:]],
                "native_while_unavailable": {"Items": [{
                    "TargetReplicaSetSize": 3, "MinReplicaSetSize": 2, "PartitionInformation": {"Id": partition}}]},
                "write": no_quorum, "accepted_write_outcome_after_recovery": no_quorum, "diagnostics": diagnostics},
            "abrupt_primary_loss": {
                "observation": failed_over, "workload": workload(failure_results, len(failure_results) - (not reject_quorum)),
                "native_identity": expected(old, captured=True),
                "kill": {**process(old, "kill"), "old_process_exit_confirmed": True},
                "process_wide_not_per_unit": True, "new_primary_native_identity": expected(new),
                "new_primary_process": process(new),
                "replacement_hosts": [{"native_identity": expected(failed_over["replicas"][0]["rpc"]),
                                       "process": process(failed_over["replicas"][0]["rpc"])}],
                "post_failure_writes": writes(post_failure), "lost_reply_retries_after_recovery_copy": [retry]},
            "per_unit_close": {
                "observation": failed_over, "workload": workload(failure_results, len(failure_results) - (not reject_quorum)),
                "unrelated_before": failed_over, "unrelated_after": failed_over, "auxiliary_before": auxiliary,
                "old_auxiliary_http_status": 404, "native_departure": {
                    "observer": "fube-client", "node": new["node"], "service": auxiliary["service"],
                    "fixture_contacted": False, "deployed": []}},
            "ledger_capacity_backpressure": {
                "observation": full, "workload": workload(capacity_results, 4096 - (not reject_quorum)),
                "new_successful_writes": 4096 - len(failure_results), "capacity_rejection": rejection,
                "old_identity_retry_at_capacity": duplicate(original_id, 1)},
        }
        if profile == "rust":
            def siblings(members):
                return [{"native_pid": row["pid"], "native_entry_point_instance": row["pid"] * 100 + 1,
                         "rpc": {"node": row["node"], "partition": "stateless-" + row["node"],
                                 "instance": index + 1, "incarnation": index + 1,
                                 "pid": row["pid"], "host": row["host"]}} for index, row in enumerate(members)]
            scenarios["abrupt_primary_loss"].update(
                stateless_siblings_before=siblings(rows), stateless_siblings_after=siblings(replaced_rows))
            scenarios["per_unit_close"].update(
                stateless_siblings_before=siblings(replaced_rows), stateless_siblings_after=siblings(replaced_rows))
        processes = [{"native_identity": expected(r["rpc"]), "process": process(r["rpc"])}
                     for r in baseline["replicas"]]
        return scenarios, {lost_id: record}, processes

    def data_report(self, profile, name, owner, container, start, reject_quorum=False):
        required = ci.data.required_scenarios(profile)
        scenarios = {case: {"status": "PASS", "started_at": start + index / 100,
                            "finished_at": start + (index + 1) / 100}
                     for index, case in enumerate(required)}
        essential, ledger, processes = self.essential_data(profile, reject_quorum)
        for case, evidence in essential.items():
            scenarios[case].update(evidence)
        scenarios["deployment_identity"]["processes"] = processes
        scenarios["apply_ack_delay"].update(blocked_after_receive=True, elapsed_seconds=2.0)
        for case in ("replacement_copy", "copy_cancellation_recovery"):
            scenarios[case].update(self.copy_proof())
        groups = [{"primary_kind": "rust", "receipts": []}]
        if profile == "native-data":
            groups.append({"primary_kind": "native-classic-v1", "receipts": []})
        for index, (role, kind) in enumerate((
                ("Primary", "stale_epoch"), ("Primary", "wrong_sender"), ("Primary", "wrong_target"),
                ("ActiveSecondary", "wrong_sender"), ("ActiveSecondary", "stale_copy_ack"),
                ("ActiveSecondary", "wrong_target"))):
            group = groups[0 if profile == "rust" or index < 3 else 1]
            group["receipts"].append({
                "rust_role": role, "kind": kind,
                "peer_correlation": {
                    "source_kind": "rust", "destination_kind": "rust" if profile == "rust" else "native-classic-v1",
                    "wire_delivery": {
                        "classification": "destination_kernel_ingress_not_native_parse_or_rejection",
                        "frame_sha256": "e" * 64, "frame_bytes": 100,
                    },
                },
            })
        scenarios["stale_wire_receipts"]["coverage"] = groups
        scenarios["authoritative_data_loss"].update({
            case: {
                "native_fas": {"native_cleanup_terminal": True},
                "before": {"native_partition": {"epoch": {"data_loss": 1, "configuration": 2}}},
                "after_native_data_loss": {"native_partition": {"epoch": {"data_loss": 2, "configuration": 2}}},
                "actual_survivor_callbacks": [{
                    "engine_kind": "rust", "state": {
                        "data_loss_calls": 1, "data_loss_changed_calls": 1 if changed else 0,
                        "last_data_loss_boundary": 5 if changed else None}}],
            } for case, changed in (("unchanged", False), ("changed", True))
        })
        if profile == "native-data":
            for case in ci.data.MIXED_SCENARIOS[:2]:
                moves = (["native-classic-v1", "rust"] * 2 if case.startswith("rust_primary")
                         else ["rust", "native-classic-v1"] * 2)
                scenarios[case].update(
                    apply_ack_delay={"blocked_after_receive": True},
                    replacement_copy=self.copy_proof(ack=True, source_kind=(
                        "rust" if case.startswith("rust_primary") else "native-classic-v1")),
                    post_copy_no_write_moves=[{
                        "requested_engine": engine, "no_intervening_client_writes": True,
                        "old_primary_http_status": 503,
                        **({"actual_fresh_native_ack_frontiers": [{
                            "replica": replica, "received": 10, "applied": 10,
                            "building": False, "cleanup_required": False} for replica in (1, 2)],
                            "last_native_observation": {"primary": {"rpc": {
                                "engine_kind": "rust", "state": {"last": 10},
                                "engine": {"progress": {"assigned": 10, "committed": 10}},
                            }}}}
                           if engine == "rust" else {}),
                    } for engine in moves])
            scenarios["native_interrupted_copy_epoch"].update(self.copy_proof())
            scenarios["native_interrupted_copy_epoch"].update(
                before={"native_partition": {"epoch": {"data_loss": 1, "configuration": 2}}},
                observation={"native_partition": {"epoch": {"data_loss": 1, "configuration": 3}}},
                interrupt_move={"native_move": "accepted"})
        return {
            "profile": profile, "sf_version": "11.8.121.4", "status": "PASS", "errors": [], "missing_or_failed": [],
            "required_scenarios": list(required), "scenarios": scenarios, "identified_workload": ledger,
            "onebox": name, "owner": owner, "container_id": container,
            "provenance": self.provenance(),
            "bundle": {
                "source_revision": self.current["source"], "version": ci.data.BUNDLE_VERSION,
                "profile": ci.data.BUNDLE_PROFILE, "artifacts": {
                    name: {"sha256": self.built["inputs"][key]["sha256"]} for name, key in (
                        ("bin/rust-runtime-app", "app"), ("bin/runtime_observer", "observer"),
                        ("bin/native-replicator-oracle", "native_oracle"),
                        ("legacy/build-info.json", "nested_manifest"))},
            },
            "cleanup": {
                "application": "deleted; absence confirmed", "type": "unprovisioned; absence confirmed",
                "upload": "original reservation removed", "image_store_upload": self.upload(),
                "onebox": "caller-owned allocation retained",
            },
            "auxiliary_data_services": [{
                "mixed": False, "eligible_nodes": ["N0020", "N0040", "N0050"] if profile == "native-data"
                else list(ci.data.NODE_ENGINES), "changed": changed, "target": 3, "minimum": 2,
                "cleanup": {"native_absence_confirmed": True, "original_routes_retired": True},
            } for changed in (False, True)],
        }

    def native_reports(self, allocation, start):
        digest = self.built["inputs"]["native_archive"]["sha256"]
        common = {"runtime_version": "11.8.121.4", "profile": "clear", "archive_sha256": digest}
        cases = [{"case": name, "passed": True} for name in ci.CLEAR_CASES]
        cases[1]["result"], cases[2]["result"] = True, False
        cases[3].update(operation_count=32, initially_pending_count=32)
        cases[-1].update(native_ping_passed=True, closed_without_operation=True, terminal_error_observed=True)
        nodes = dict.fromkeys((
            "typed_stable_fields_match", "binary_ports_match_onebox_manifest", "exact_name_hit_miss_case_prefix",
            "status_default_all_up_down", "empty_name_token_omitted",
            "valid_continuation_after_final_id_is_empty", "connection_survives_native_error"), True)
        nodes["native_node_count"] = 5
        nodes["native_observation_start_unix_ms"] = int(start * 1000) + 1
        nodes["native_observation_end_unix_ms"] = int(start * 1000) + 10
        clear = {**common, "executed_cases": cases, "node_list": nodes}
        # Clear really has no archive field; the successful command binds it.
        del clear["archive_sha256"]
        health = {
            **common, "owner": "ci-100-1", "cases": [{
                "case": name, "native_start_ms": int(start * 1000) + 11,
                "native_end_ms": int(start * 1000) + 12} for name in ci.HEALTH_CASES],
            "concurrent_operations": 32, "unknown_node_code": -2147017705,
            "unknown_node_management_code": "FABRIC_E_HEALTH_ENTITY_NOT_FOUND",
            "cancelled_after_first_poll": True, "completed_at_unix_ms": int(start * 1000) + 20,
            "healthy_reuse_after_error": {"aggregated_health_state": 1},
            "final_healthy_reuse": {"aggregated_health_state": 1},
        }
        names = {**common, **dict.fromkeys((
            "initial_http_absent", "native_create_succeeded", "http_present_after_create",
            "reuse_after_duplicate", "native_delete_succeeded", "http_absent_after_delete",
            "reuse_after_missing", "shared_native_client", "owned_cleanup_confirmed"), True),
            "duplicate_create_code": -2147017792, "missing_delete_code": -2147017791}
        lists = {**common, **dict.fromkeys((
            "initial_absence_confirmed", "application_native_continuation_followed",
            "service_native_continuation_followed", "filters_and_parameter_exclusion",
            "mixed_shared_native_connection", "reuse_after_native_error", "owned_cleanup_confirmed"), True),
            "application_count": 2, "service_count": 4,
            **dict.fromkeys(("topology", "deployment_inspection", "independent_applications",
                             "independent_services", "native_applications", "native_services"), [{"model": 1}])}
        del lists["profile"]
        notification = {
            **common, "allocation": allocation, "passed": list(ci.NOTIFICATION_CASES),
            "owned_cleanup_confirmed": True, "resolved": [{"model": 1}], "named": {"model": 1},
            "ack_retry": {"successful_acks_dropped": 1, "native_page_retries": 1,
                          "subsequent_target_acks_forwarded": 1, "duplicate_versions": 0},
            "endpoint_change": {"from": 2, "to": 1}, "role_change": {"model": 1},
        }
        return dict(zip(ci.NATIVE_FILES, (clear, health, names, lists, notification)))

    def real_policy_report(self):
        case = fixtures.LegacyDeploymentTests(
            "test_real_full_dispatch_and_configuration_upgrade_preserve_original_identity_and_clean_both_versions")
        case.setUp()
        try:
            outer, _, dispatched = case.run_legacy()
            self.assertEqual(dispatched, ["stateful_singleton", "stateful_three", "controls_and_authority",
                                          "restart", "wait_stateful"])
        finally:
            case.doCleanups()
        return {
            **outer, "version": 1, "kind": "legacy", "profile": "legacy", "status": "PASS", "context": self.current,
            "producer_sha256": ci.identity(self.producer)["sha256"], "provenance": self.provenance(),
            "allocation": ci.binding("owned", "a" * 32, "original-cid"),
        }

    def command_outcomes(self, route):
        values = {key: "success" for key in ci.OUTCOMES}
        if route == "client-runtime":
            values["native_data"] = "skipped"
            values["native_clients"] = {
                "outcome": "success", "archive_sha256": self.built["inputs"]["native_archive"]["sha256"],
                "filter": "group(onebox)", "no_tests": "fail",
            }
        else:
            values.update(native_clients="skipped", rust_data="skipped", legacy="skipped")
        return values

    def worker(self, route):
        validated = self.validated(route)
        directory = validated.parent
        if route == "client-runtime":
            name, owner, container = "owned", "a" * 32, "original-cid"
            start = ci.read(validated)["validated_at"]
            self.emit(directory / "rust-report.json", self.data_report("rust", name, owner, container, start))
            preserved = self.real_policy_report()
            # The real modeled policy ran later than validation, but fixture
            # suite endpoints are intentionally ordered within this same worker.
            clean_start = max(ci.time.time(), start + 1)
            self.emit(directory / "legacy-report.json", preserved)
            for file, value in self.native_reports(name, start).items():
                self.emit(directory / "client" / file, value)
        else:
            name, owner, container = "native-owned", "b" * 32, "native-original-cid"
            start = ci.read(validated)["validated_at"]
            self.emit(directory / "native-data-report.json", self.data_report("native-data", name, owner, container, start))
            clean_start = start + 1
        self.emit(directory / "cleanup.json", self.wrapped("cleanup", {
            "context": self.current, "route": route, "producer_sha256": ci.identity(self.producer)["sha256"],
            "allocation": ci.binding(name, owner), "started_at": clean_start, "finished_at": clean_start + 1,
            "destroy": {"name": name, "phase": "destroyed", "data_deleted": True, "absence_verified": True,
                        "independent_absence": {"container": [container],
                                               "volume": ["sf-onebox-" + name + "_data"],
                                               "network": ["network-" + name]}},
        }))
        self.emit(directory / "fixture.json", self.wrapped("onebox-fixture", {
            "context": self.current, "route": route, "producer_sha256": ci.identity(self.producer)["sha256"],
            "allocation": ci.binding(name, owner, container), "ownership": "created",
            "outcomes": dict.fromkeys(ci.FIXTURE_PHASES, "success"),
            "started_at": start - 1, "ready_at": start, "finished_at": clean_start + 1,
        }))
        self.emit(directory / "diagnostics.json", {
            "name": name, "project": "sf-onebox-" + name, "errors": [],
        })
        commands = self.command_outcomes(route)
        self.emit(directory / "suite-outcomes.json", {
            "version": 1, "partition": route, "status": "PASS", "errors": [],
            "manifest_sha256": self.built["inputs"]["suite_manifest"]["sha256"],
            "suites": {name: commands[name] for name in ci.suites.MANIFEST.selected(route)},
        })
        return directory

    def sealing(self, route, directory, output=None, outcomes=None):
        clean = (ci.read(directory / "cleanup.json") if (directory / "cleanup.json").exists()
                 else {"finished_at": ci.time.time()})
        with patch.object(ci.time, "time", return_value=clean["finished_at"] + 1):
            return self.cli("seal", "--route", route, "--producer", self.producer,
                            "--validated-input", directory / "validated-input.json", "--evidence-dir", directory,
                            "--out", output or directory / "worker-proof.json",
                            "--outcomes", json.dumps(outcomes or self.command_outcomes(route)))

    def aggregating(self, output=None, needs=None):
        return self.cli("aggregate", "--producer", self.producer, "--worker-root", self.root / "workers",
                        "--needs-json", json.dumps(needs or {"producer": "success", "workers": "success"}),
                        "--out", output or self.root / "aggregate.json")


class InputTests(ProofFiles):
    def test_changed_manifest_rejected_before_allocation(self):
        changed = self.root / "suites.toml"
        changed.write_bytes(ci.suites.PATH.read_bytes() + b"\n# changed after producer\n")
        with patch.object(ci.suites, "PATH", changed), \
                patch.object(ci.runtime, "create_owned") as create:
            self.assertEqual(self.cli(
                "validate", "--receipt", self.producer, "--bundle-tar", self.tar,
                "--bundle-dir", self.directory, "--archive", self.archive,
                "--route", "client-runtime", "--out", self.root / "changed-input.json"), 1)
        create.assert_not_called()

    def test_produce_validate_both_routes_external_identity_no_allocation(self):
        with patch.object(ci.runtime, "create_owned", side_effect=AssertionError("no allocation")):
            for route in ci.ROUTES:
                path = self.validated(route)
                value = ci.read(path)
                self.assertEqual(value["inputs"], self.built["inputs"])
                self.assertEqual(value["context"], self.current)
        self.assertEqual(set(self.built["inputs"]), {
            "bundle_tar", "manifest", "nested_manifest", "app", "observer",
            "native_oracle", "native_archive", "qualification_script", "runtime_script", "suite_manifest"})
        self.assertEqual(ci.data.load_bundle(self.directory).source_revision, self.current["source"])

    def test_tamper_tar_archive_and_source_rejected_before_allocation(self):
        for target in ("tar", "archive", "manifest", "source", "tree", "head", "merge", "attempt", "run_id"):
            with self.subTest(target=target), patch.object(ci.runtime, "Allocation") as allocation, \
                    patch.object(ci.runtime, "create_owned") as create:
                originals = {}
                if target in ("tar", "archive", "manifest"):
                    path = {"tar": self.tar, "archive": self.archive,
                            "manifest": self.directory / "build-info.json"}[target]
                    originals[path] = path.read_bytes()
                    path.write_bytes(originals[path] + b"tamper")
                else:
                    old = self.current[target]
                    self.current[target] = "2" if target in ("attempt", "run_id") else "d" * 40
                try:
                    out = self.root / ("invalid-" + target + ".json")
                    self.assertEqual(self.cli("validate", "--receipt", self.producer, "--bundle-tar", self.tar,
                                              "--bundle-dir", self.directory, "--archive", self.archive,
                                              "--route", "client-runtime", "--out", out), 1)
                    self.assertEqual(ci.read(out)["status"], "FAIL")
                    allocation.assert_not_called()
                    create.assert_not_called()
                finally:
                    for path, content in originals.items():
                        path.write_bytes(content)
                    if not originals:
                        self.current[target] = old

    def test_producer_rejects_mismatched_tar_content_links_and_extra_inventory(self):
        cases = ("extra", "link", "different")
        for case in cases:
            path = self.root / (case + ".tar")
            with tarfile.open(path, "w") as archive:
                archive.add(self.directory, arcname=".")
                member = tarfile.TarInfo("unexpected" if case == "extra" else "bin/rust-runtime-app")
                if case == "link":
                    member.type = tarfile.SYMTYPE
                    member.linkname = "build-info.json"
                else:
                    member.size = 1
                archive.addfile(member, None if case == "link" else io.BytesIO(b"x"))
            self.assertEqual(self.cli("produce", "--bundle-tar", path, "--bundle-dir", self.directory,
                                      "--archive", self.archive, "--out", self.root / (case + ".json")), 1)

    def test_output_collision_and_bundle_receipt_rejected(self):
        self.assertEqual(self.cli("produce", "--bundle-tar", self.tar, "--bundle-dir", self.directory,
                                  "--archive", self.archive, "--out", self.producer), 1)
        out = self.directory / "receipt.json"
        self.assertEqual(self.cli("produce", "--bundle-tar", self.tar, "--bundle-dir", self.directory,
                                  "--archive", self.archive, "--out", out), 1)
        self.assertFalse(out.exists())

    def test_actual_git_context_clean_checkout_pr_and_attempt_validation(self):
        self.context.stop()
        def git(*args):
            return {("status", "--porcelain", "--untracked-files=normal"): "",
                    ("rev-parse", "HEAD"): "a" * 40,
                    ("rev-parse", "HEAD^{tree}"): "b" * 40}[args]
        with patch.object(ci, "git", side_effect=git):
            self.assertEqual(ci.context(), self.current)
            for variable, value in (("GITHUB_SHA", "d" * 40), ("FUBE_CI_MERGE", "d" * 40),
                                    ("GITHUB_RUN_ATTEMPT", "0"), ("GITHUB_RUN_ID", "true")):
                with patch.dict(os.environ, {variable: value}), self.assertRaises(ci.ProofError):
                    ci.context()
        with patch.object(ci, "git", return_value="dirty"), self.assertRaises(ci.ProofError):
            ci.context()
        self.context.start()


class FixtureTests(ProofFiles):
    def setUp(self):
        super().setUp()
        self.args = Namespace(
            name="fixture-owned", partition="native-data", owner=None,
            prebuilt=self.directory, bundle_tar=self.tar, producer=self.producer, archive=None,
            report_dir=self.root / "fixture-proof", config_dir=None, fixture_owner=None,
            github_output=self.root / "github-output",
        )
        self.events = []

        def create(name, *, on_allocated):
            self.events.append("allocate")
            on_allocated("a" * 32)
            self.events.append("healthy")
            return "a" * 32

        def ready(name, owner):
            self.events.append("ready")
            return Namespace(container_id="fixture-container")

        def diagnostics(args, owner):
            self.events.append("diagnostics")
            with ci.reserve(args.report_dir / "diagnostics.json") as output:
                ci.write(output, {"name": args.name, "project": "sf-onebox-" + args.name, "errors": []})

        def destroy(name, owner):
            self.events.append("destroy")
            return {
                "name": name, "phase": "destroyed", "data_deleted": True, "absence_verified": True,
                "independent_absence": {"container": ["fixture-container"],
                                       "volume": ["sf-onebox-" + name + "_data"], "network": ["fixture-network"]},
            }

        for attribute, target, name, effect in (
                ("create", ci.runtime, "create_owned", create),
                ("ready", ci.runtime, "Allocation", ready),
                ("diagnostics", fixture, "diagnostics", diagnostics),
                ("destroy", ci.runtime, "destroy_owned", destroy)):
            mocker = patch.object(target, name, side_effect=effect)
            setattr(self, attribute, mocker.start())
            self.addCleanup(mocker.stop)

    def exercise(self, execute=None):
        def passed(selected, args, manifest_hash):
            self.events.append("suites")
            self.assertEqual(args.owner, "a" * 32)
            return 0
        with contextlib.redirect_stdout(io.StringIO()) as output, contextlib.redirect_stderr(output):
            status = fixture.run(ci.suites.MANIFEST.selected(self.args.partition), self.args, execute or passed)
        self.console = output.getvalue()
        return status, ci.read(self.args.report_dir / "fixture.json")

    def test_owned_partition_creates_waits_and_cleans_once(self):
        status, result = self.exercise()
        self.assertEqual(status, 0)
        self.assertEqual(self.events, ["allocate", "healthy", "ready", "suites", "diagnostics", "destroy"])
        self.assertEqual(result["ownership"], "created")
        self.assertEqual(result["outcomes"], dict.fromkeys(ci.FIXTURE_PHASES, "success"))
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)
        self.assertEqual(result["allocation"], ci.binding("fixture-owned", "a" * 32, "fixture-container"))
        self.assertIn("owner=" + "a" * 32, self.args.github_output.read_text())
        self.assertIn("cleaned=true", self.args.github_output.read_text())
        self.assertNotIn('"' + "a" * 32 + '"', (self.args.report_dir / "fixture.json").read_text())

    def test_invalid_inputs_never_allocate_or_attach(self):
        self.tar.write_bytes(b"tampered")
        status, result = self.exercise()
        self.assertEqual(status, 1)
        self.assertEqual(result["outcomes"]["validate"], "failure")
        for operation in (self.create, self.ready, self.diagnostics, self.destroy):
            operation.assert_not_called()
        self.assertFalse(self.args.github_output.exists())

    def test_partial_startup_publishes_original_owner_before_cleanup(self):
        def partial(name, *, on_allocated):
            on_allocated("a" * 32)
            self.assertIn("owner=" + "a" * 32, self.args.github_output.read_text())
            raise TimeoutError("private native startup error")
        self.create.side_effect = partial
        execute = unittest.mock.Mock()
        status, result = self.exercise(execute)
        self.assertEqual(status, 1)
        execute.assert_not_called()
        self.ready.assert_not_called()
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)
        self.assertEqual(result["outcomes"]["destroy"], "success")
        self.assertIn("cleaned=true", self.args.github_output.read_text())
        self.assertNotIn("private native", self.console)

    def test_caller_owned_allocation_is_retained_after_suite_failure(self):
        self.args.owner = "a" * 32
        ready = self.ready.side_effect
        for case in ("success", "suite-failure", "readiness-failure"):
            self.args.report_dir = self.root / case
            self.ready.side_effect = RuntimeError("owner mismatch") if case == "readiness-failure" else ready
            status, result = self.exercise(unittest.mock.Mock(return_value=1 if case == "suite-failure" else 0))
            self.assertEqual(status, 0 if case == "success" else 1)
            self.create.assert_not_called()
            self.destroy.assert_not_called()
            self.assertEqual(result["ownership"], "reused")
            self.assertEqual(result["outcomes"]["create"], "reused")
            self.assertEqual(result["outcomes"]["destroy"], "retained")
            self.assertFalse(self.args.github_output.exists())

    def test_cleanup_failure_keeps_emergency_capability_and_failure(self):
        self.destroy.side_effect = RuntimeError("replacement owner; private detail")
        status, result = self.exercise()
        self.assertEqual(status, 1)
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)
        self.assertEqual(result["outcomes"]["destroy"], "failure")
        self.assertIn("owner=" + "a" * 32, self.args.github_output.read_text())
        self.assertNotIn("cleaned=", self.args.github_output.read_text())
        self.assertNotIn("private detail", (self.args.report_dir / "cleanup.json").read_text())

    def test_diagnostics_failure_cannot_skip_cleanup(self):
        self.diagnostics.side_effect = OSError("collector unavailable")
        status, result = self.exercise()
        self.assertEqual(status, 1)
        self.assertEqual(result["outcomes"]["diagnostics"], "failure")
        self.assertEqual(result["outcomes"]["destroy"], "success")
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)

    def test_suite_interruption_still_cleans_original_allocation(self):
        status, result = self.exercise(unittest.mock.Mock(side_effect=KeyboardInterrupt))
        self.assertEqual(status, 1)
        self.assertEqual(result["outcomes"]["suites"], "failure")
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)

    def test_mutated_suite_arguments_cannot_retarget_cleanup(self):
        def changed(selected, args, manifest_hash):
            args.name, args.owner = "replacement", "b" * 32
            return 0
        status, result = self.exercise(changed)
        self.assertEqual(status, 0)
        self.destroy.assert_called_once_with("fixture-owned", "a" * 32)
        self.assertEqual(result["allocation"]["name"], "fixture-owned")


class CleanupLegacyTests(ProofFiles):
    def test_cleanup_missing_or_tampered_producer_always_uses_original_owner(self):
        for problem in ("missing", "tampered", "context"):
            path = self.producer if problem != "missing" else self.root / "missing.json"
            original = self.producer.read_bytes()
            if problem == "tampered":
                self.producer.write_text('{"version":true}')
            if problem == "context":
                self.current["attempt"] = "2"
            with patch.object(ci.runtime, "destroy_owned", return_value={
                "name": "partial", "phase": "destroyed", "data_deleted": True, "absence_verified": True,
                "independent_absence": {"container": [], "volume": [], "network": []},
            }) as destroy, patch.object(ci.runtime, "Allocation") as allocation:
                out = self.root / ("cleanup-" + problem + ".json")
                self.assertEqual(self.cli("cleanup", "--name", "partial", "--owner", "a" * 32,
                                          "--route", "native-data", "--producer", path, "--report", out), 1)
                destroy.assert_called_once_with("partial", "a" * 32)
                allocation.assert_not_called()
                report = ci.read(out)
                self.assertEqual(report["status"], "FAIL")
                self.assertTrue(report["destroy"]["absence_verified"])
                self.assertNotIn("a" * 32, self.last_stdout)
            self.producer.write_bytes(original)
            self.current["attempt"] = "1"

    def test_cleanup_owner_drift_not_adopted_and_output_collision_cannot_block_destroy(self):
        out = self.emit(self.root / "existing.json", {"immutable": True})
        with patch.object(ci.runtime, "destroy_owned", side_effect=RuntimeError("owner changed")) as destroy:
            self.assertEqual(self.cli("cleanup", "--name", "original", "--owner", "a" * 32,
                                      "--route", "client-runtime", "--producer", self.producer, "--report", out), 1)
            destroy.assert_called_once_with("original", "a" * 32)
            self.assertEqual(ci.read(out), {"immutable": True})
        missing = self.root / "failed-destroy.json"
        with patch.object(ci.runtime, "destroy_owned", side_effect=RuntimeError("owner changed; secret")):
            self.assertEqual(self.cli("cleanup", "--name", "original", "--owner", "a" * 32,
                                      "--route", "client-runtime", "--producer", self.producer,
                                      "--report", missing), 1)
        self.assertIsNone(ci.read(missing)["destroy"])
        self.assertNotIn("secret", missing.read_text())

    @unittest.skipUnless(ci.sys.platform == "linux", "real descriptor-relative policy fixture")
    def test_legacy_actual_hardened_wrapper_real_policy_and_bare_pass_rejection(self):
        preserved = self.real_policy_report()
        self.assertEqual(ci.validate_legacy(preserved, self.built)["assertions"], ["legacy/legacy_full"])
        with patch.object(ci.runtime, "Allocation") as allocation, \
                patch.object(ci.data, "legacy_full") as dispatch, \
                patch.object(ci.runtime, "create_owned") as create:
            allocation.return_value.container_id = "original-cid"
            def execute(owned, bundle, result):
                result.update(copy.deepcopy(preserved))
            dispatch.side_effect = execute
            report = self.root / "legacy.json"
            self.assertEqual(self.cli("legacy", "--name", "owned", "--owner", "a" * 32,
                                      "--prebuilt", self.directory, "--producer", self.producer,
                                      "--report", report), 0)
            self.assertTrue(ci.same(ci.read(report)["scenarios"], preserved["scenarios"]))
            dispatch.assert_called_once()
            create.assert_not_called()
        bare = copy.deepcopy(preserved)
        bare["scenarios"]["legacy_full"]["qualification"] = {
            "status": ci.LEGACY_STATUS, "errors": [], "cleanup": {"safe_owner_cleanup": True}}
        with self.assertRaises((ci.ProofError, KeyError)):
            ci.validate_legacy(bare, self.built)
        for field in ("safe_owner_cleanup", "independent_absence", "image_store_uploads"):
            broken = copy.deepcopy(preserved)
            del broken["scenarios"]["legacy_full"]["qualification"]["cleanup"][field]
            with self.subTest(field=field), self.assertRaises((ci.ProofError, KeyError)):
                ci.validate_legacy(broken, self.built)

    def test_legacy_validation_failure_never_attaches_or_compiles(self):
        self.archive.write_bytes(b"unrelated archive")
        self.current["source"] = "f" * 40
        with patch.object(ci.runtime, "Allocation") as allocation, patch.object(ci.data, "legacy_full") as dispatch:
            self.assertEqual(self.cli("legacy", "--name", "owned", "--owner", "a" * 32,
                                      "--prebuilt", self.directory, "--producer", self.producer,
                                      "--report", self.root / "legacy-failed.json"), 1)
            allocation.assert_not_called()
            dispatch.assert_not_called()

    def test_legacy_real_wrapper_rejects_fake_policy_completion_and_keeps_failed_nested_report(self):
        with patch.object(ci.runtime, "Allocation") as allocation, \
                patch.object(ci.runtime, "qualify", return_value={
                    "status": ci.LEGACY_STATUS, "errors": [], "cleanup": {"safe_owner_cleanup": True}}) as qualify:
            allocation.return_value.name = "owned"
            allocation.return_value.owner = "a" * 32
            allocation.return_value.container_id = "original-cid"
            path = self.root / "fake-legacy.json"
            self.assertEqual(self.cli("legacy", "--name", "owned", "--owner", "a" * 32,
                                      "--prebuilt", self.directory, "--producer", self.producer, "--report", path), 1)
            report = ci.read(path)
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["scenarios"]["legacy_full"]["status"], "FAIL")
            self.assertEqual(report["scenarios"]["legacy_full"]["qualification"]["status"], ci.LEGACY_STATUS)
            self.assertIsInstance(qualify.call_args.kwargs["deployment_policy"], ci.data.NativeDataDeploymentPolicy)
            self.assertEqual(qualify.call_args.kwargs["stateful"], "three")
            self.assertTrue(qualify.call_args.kwargs["full"])


@unittest.skipUnless(ci.sys.platform == "linux", "uses existing real native policy offline fixture")
class WorkerTests(ProofFiles):
    def test_seal_merges_lifecycle_with_actual_manifest_execution(self):
        for route in ci.ROUTES:
            directory = self.worker(route)
            finished = ci.read(directory / "fixture.json")["finished_at"]
            with patch.object(ci.time, "time", return_value=finished + 1):
                self.assertEqual(self.cli(
                    "seal", "--route", route, "--producer", self.producer,
                    "--validated-input", directory / "validated-input.json", "--evidence-dir", directory,
                    "--out", directory / "worker-proof.json"), 0, self.last_stdout)

    def test_fixture_readiness_and_original_ownership_cannot_be_forged(self):
        directory = self.worker("native-data")
        path = directory / "fixture.json"
        original = ci.read(path)
        mutations = (
            lambda value: value.update(ownership="reused"),
            lambda value: value["allocation"].update(owner_sha256="b" * 64),
            lambda value: value["outcomes"].update(ready="failure"),
            lambda value: value.update(ready_at=value["finished_at"] + 1),
        )
        for index, mutation in enumerate(mutations):
            value = copy.deepcopy(original)
            mutation(value)
            self.emit(path, value)
            self.assertEqual(self.sealing("native-data", directory,
                                          output=self.root / f"bad-fixture-{index}.json"), 1)

    def test_missing_failed_or_mismatched_execution_cannot_pass(self):
        directory = self.worker("native-data")
        path = directory / "suite-outcomes.json"
        original = ci.read(path)
        mutations = (
            lambda value: value.update(manifest_sha256="f" * 64),
            lambda value: value.update(status="FAIL"),
            lambda value: value["suites"].clear(),
            lambda value: value["suites"].update(native_data="skipped"),
        )
        for index, mutation in enumerate(mutations):
            value = copy.deepcopy(original)
            mutation(value)
            self.emit(path, value)
            self.assertEqual(self.sealing("native-data", directory,
                                          output=self.root / f"bad-execution-{index}.json"), 1)
        path.unlink()
        self.assertEqual(self.sealing("native-data", directory,
                                      output=self.root / "missing-execution.json"), 1)

    def reject_data_mutations(self, changes, routes=ci.ROUTES):
        for route in ci.ROUTES:
            self.assertEqual(self.sealing(route, self.worker(route)), 0, self.last_stdout)
        for route, filename in (("client-runtime", "rust-report.json"), ("native-data", "native-data-report.json")):
            if route not in routes:
                continue
            directory = self.root / "workers" / route
            path, proof_path = directory / filename, directory / "worker-proof.json"
            # These are locally generated raw fixtures, not gate input parsing.
            # Real entry points below must stream them through bounded DATA_SPEC.
            original, proof = json.loads(path.read_text()), ci.read(proof_path)
            for index, (name, change) in enumerate(changes):
                with self.subTest(route=route, witness=name):
                    broken = copy.deepcopy(original)
                    change(broken)
                    self.emit(path, broken)
                    output = self.root / (route + "-seal-" + str(index) + ".json")
                    self.assertEqual(self.sealing(route, directory, output), 1, self.last_stdout)
                    forged = copy.deepcopy(proof)
                    forged["reports"][filename] = ci.identity(path, ci.REPORT)
                    self.emit(proof_path, forged)
                    output = self.root / (route + "-aggregate-" + str(index) + ".json")
                    self.assertEqual(self.aggregating(output), 1, self.last_stdout)
                    result = ci.read(output)
                    self.assertEqual(result["status"], "FAIL")
                    self.assertNotIn("sealed worker report hash/proof mismatch", str(result["errors"]))
            self.emit(path, original)
            self.emit(proof_path, proof)
        self.assertEqual(self.aggregating(self.root / "restored-aggregate.json"), 0, self.last_stdout)

    def test_essential_witness_absence_rejected_by_seal_and_rehashed_aggregate(self):
        fields = {
            "writes_100": ("writes", "workload", "observation"),
            "identified_retry": ("identical_retry", "conflicting_retry"),
            "planned_move": ("before", "after", "move_request", "post_move_writes"),
            "unavailable_quorum": ("paused_processes", "resume", "native_while_unavailable",
                                   "write", "diagnostics", "accepted_write_outcome_after_recovery"),
            "lost_reply_dedup": ("before_move", "retry_outcome"),
            "abrupt_primary_loss": ("native_identity", "kill", "new_primary_native_identity",
                                    "new_primary_process", "replacement_hosts", "post_failure_writes",
                                    "lost_reply_retries_after_recovery_copy"),
            "per_unit_close": ("auxiliary_before", "unrelated_before", "unrelated_after", "native_departure"),
            "ledger_capacity_backpressure": ("capacity_rejection", "old_identity_retry_at_capacity",
                                             "new_successful_writes"),
        }
        changes = [(case + "/" + field, lambda v, case=case, field=field: v["scenarios"][case].pop(field))
                   for case, names in fields.items() for field in names]
        for case in fields:
            changes.append((case + "/bare-PASS", lambda v, case=case: v["scenarios"].update({
                case: {key: v["scenarios"][case][key] for key in ("status", "started_at", "finished_at")}})))
        changes.append(("root/identified_workload", lambda v: v.pop("identified_workload")))
        self.reject_data_mutations(changes)

    def test_contradictory_typed_witnesses_rejected_by_seal_and_rehashed_aggregate(self):
        self.reject_data_mutations([
            ("zero-writes", lambda v: v["scenarios"]["writes_100"]["writes"].update(
                requested=0, successful=0, ids=[], sequences=[])),
            ("false-write-count", lambda v: v["scenarios"]["writes_100"]["writes"].update(successful=False)),
            ("bool-sequence", lambda v: v["scenarios"]["writes_100"]["writes"]["sequences"].__setitem__(0, True)),
            ("duplicate-sequence", lambda v: v["scenarios"]["writes_100"]["writes"]["sequences"].__setitem__(1, 1)),
            ("reordered-sequences", lambda v: v["scenarios"]["writes_100"]["writes"]["sequences"].reverse()),
            ("zero-workload", lambda v: v["scenarios"]["writes_100"]["workload"].update(
                successful=0, accepted=0, last=0, results={}, values={})),
            ("provider-map-conflict", lambda v: v["scenarios"]["writes_100"]["observation"]["replicas"][1]
                ["rpc"]["state"]["results"].update({"initial-1-0": 999})),
            ("provider-value-conflict", lambda v: v["scenarios"]["writes_100"]["observation"]["replicas"][1]
                ["rpc"]["state"]["values"].update({"key-initial-1-0": "wrong"})),
            ("secondary-no-apply", lambda v: v["scenarios"]["writes_100"]["observation"]["replicas"][1]
                ["rpc"]["engine"]["progress"].update(applied=0)),
            ("bool-provider-last", lambda v: v["scenarios"]["writes_100"]["observation"]["replicas"][1]
                ["rpc"]["state"].update(last=True)),
            ("bool-committed", lambda v: v["scenarios"]["writes_100"]["observation"]["primary"]
                ["rpc"]["engine"]["progress"].update(committed=True)),
            ("retry-not-duplicate", lambda v: v["scenarios"]["identified_retry"]["identical_retry"].update(duplicate=False)),
            ("retry-different-LSN", lambda v: v["scenarios"]["identified_retry"]["identical_retry"].update(sequence=2)),
            ("retry-different-ID", lambda v: v["scenarios"]["identified_retry"]["identical_retry"].update(request_id="absent")),
            ("conflict-assigned", lambda v: v["scenarios"]["identified_retry"]["conflicting_retry"]["reply"].update(sequence=1)),
            ("move-not-native", lambda v: v["scenarios"]["planned_move"]["move_request"].update(native_request_completed=False)),
            ("move-wrong-target", lambda v: v["scenarios"]["planned_move"]["move_request"].update(target="N0050")),
            ("move-epoch-unchanged", lambda v: v["scenarios"]["planned_move"]["after"]["native_partition"]["epoch"]
                .update(configuration=1)),
            ("move-old-route-200", lambda v: v["scenarios"]["planned_move"].update(old_primary_http_status=200)),
            ("move-replaced-old-process", lambda v: v["scenarios"]["planned_move"]["after"]["replicas"][0]
                ["rpc"].update(host="unrelated-host")),
            ("quorum-native-target-bool", lambda v: v["scenarios"]["unavailable_quorum"]["native_while_unavailable"]
                ["Items"][0].update(TargetReplicaSetSize=True)),
            ("quorum-manufactured-commit", lambda v: v["scenarios"]["unavailable_quorum"]["write"].update(committed=True)),
            ("quorum-advanced-frontier", lambda v: v["scenarios"]["unavailable_quorum"]["diagnostics"]
                ["engine"]["progress"].update(committed=107)),
            ("quorum-wrong-paused-process", lambda v: v["scenarios"]["unavailable_quorum"]["paused_processes"][0]
                .update(pid=1)),
            ("quorum-resume-false", lambda v: v["scenarios"]["unavailable_quorum"]["resume"][0].update(action=False)),
            ("lost-reply-was-read", lambda v: v["identified_workload"]["lost-reply-6"]["attempts"][0]
                .update(reply_intentionally_unread=False)),
            ("lost-reply-not-duplicate", lambda v: v["identified_workload"]["lost-reply-6"]["attempts"][1]
                ["reply"].update(duplicate=False)),
            ("lost-reply-false-commit-record", lambda v: v["identified_workload"]["lost-reply-6"].update(committed=False)),
            ("lost-reply-different-LSN", lambda v: v["identified_workload"]["lost-reply-6"].update(sequence=105)),
            ("lost-reply-not-replicated", lambda v: v["scenarios"]["lost_reply_dedup"]["before_move"]["replicas"][1]
                ["rpc"]["state"]["results"].pop("lost-reply-6")),
            ("kill-not-exited", lambda v: v["scenarios"]["abrupt_primary_loss"]["kill"].update(old_process_exit_confirmed=False)),
            ("kill-wrong-host", lambda v: v["scenarios"]["abrupt_primary_loss"]["native_identity"].update(host="other")),
            ("replacement-wrong-native-PID", lambda v: v["scenarios"]["abrupt_primary_loss"]
                ["new_primary_native_identity"]["native_query"]["MainEntryPoint"].update(ProcessId="1")),
            ("failure-epoch-unchanged", lambda v: v["scenarios"]["abrupt_primary_loss"]["observation"]
                ["native_partition"]["epoch"].update(configuration=3)),
            ("close-no-shared-process", lambda v: v["scenarios"]["per_unit_close"]["auxiliary_before"]["primary"]
                ["rpc"].update(pid=1)),
            ("close-main-replaced", lambda v: v["scenarios"]["per_unit_close"]["unrelated_after"]["primary"]
                ["rpc"].update(engine_incarnation="replacement")),
            ("close-native-departure-fake", lambda v: v["scenarios"]["per_unit_close"]["native_departure"]
                .update(fixture_contacted=True)),
            ("close-native-unit-still-present", lambda v: v["scenarios"]["per_unit_close"]["native_departure"]
                .update(deployed=[{"replica_id": 9001}])),
            ("root-process-identity-wrong", lambda v: v["scenarios"]["deployment_identity"]["processes"][0]
                ["native_identity"].update(native_pid=True)),
            ("capacity-null", lambda v: v["scenarios"]["ledger_capacity_backpressure"].update(capacity_rejection=None)),
            ("capacity-rejected-sequence", lambda v: v["scenarios"]["ledger_capacity_backpressure"]
                ["capacity_rejection"].update(sequence=4097)),
            ("capacity-reply-assigned", lambda v: v["scenarios"]["ledger_capacity_backpressure"]
                ["capacity_rejection"]["attempts"][0]["reply"].update(sequence=4097)),
            ("capacity-zero-writes", lambda v: v["scenarios"]["ledger_capacity_backpressure"].update(new_successful_writes=0)),
            ("capacity-bool-writes", lambda v: v["scenarios"]["ledger_capacity_backpressure"].update(new_successful_writes=True)),
            ("capacity-history-short", lambda v: v["scenarios"]["ledger_capacity_backpressure"]["observation"]
                ["replicas"][1]["rpc"]["state"].update(history_count=4095)),
            ("capacity-bool-bytes", lambda v: v["scenarios"]["ledger_capacity_backpressure"]["observation"]
                ["replicas"][1]["rpc"]["state"].update(retained_bytes=True)),
            ("capacity-over-budget", lambda v: v["scenarios"]["ledger_capacity_backpressure"]["observation"]
                ["replicas"][1]["rpc"]["state"].update(retained_bytes=16 * ci.SMALL + 1)),
            ("capacity-missing-old-ID", lambda v: v["scenarios"]["ledger_capacity_backpressure"]["observation"]
                ["primary"]["rpc"]["state"]["results"].pop("initial-1-0")),
            ("capacity-old-retry-false", lambda v: v["scenarios"]["ledger_capacity_backpressure"]
                ["old_identity_retry_at_capacity"].update(duplicate=False)),
            ("capacity-old-retry-bool-LSN", lambda v: v["scenarios"]["ledger_capacity_backpressure"]
                ["old_identity_retry_at_capacity"].update(sequence=True)),
        ])

    def test_rust_shared_process_siblings_are_required_but_not_invented_for_native_profile(self):
        self.reject_data_mutations([
            ("failure-sibling-absence", lambda v: v["scenarios"]["abrupt_primary_loss"].pop("stateless_siblings_after")),
            ("close-sibling-absence", lambda v: v["scenarios"]["per_unit_close"].pop("stateless_siblings_before")),
            ("close-sibling-replacement", lambda v: v["scenarios"]["per_unit_close"]["stateless_siblings_after"][0]
                ["rpc"].update(host="replaced")),
            ("failure-colocated-sibling-survived", lambda v: v["scenarios"]["abrupt_primary_loss"]
                ["stateless_siblings_after"][0].update(
                    native_entry_point_instance=v["scenarios"]["abrupt_primary_loss"]
                    ["stateless_siblings_before"][0]["native_entry_point_instance"])),
        ], routes=("client-runtime",))

    def test_cross_field_capacity_recovery_and_close_rollbacks_reject_resealed_proofs(self):
        def reject_committed_id(value):
            capacity = value["scenarios"]["ledger_capacity_backpressure"]
            original = capacity["old_identity_retry_at_capacity"]["request_id"]
            capacity["capacity_rejection"]["id"] = original
            for attempt in capacity["capacity_rejection"]["attempts"]:
                attempt["reply"]["request_id"] = original

        def lost_from_recovery(value, wrong=False):
            snapshot = value["scenarios"]["abrupt_primary_loss"]["observation"]
            request_id, record = next(iter(value["identified_workload"].items()))
            for rpc in [*(replica["rpc"] for replica in snapshot["replicas"]), snapshot["primary"]["rpc"]]:
                if wrong:
                    rpc["state"]["results"][request_id] = record["sequence"] - 1
                else:
                    rpc["state"]["results"].pop(request_id, None)

        def close_rollback(value):
            snapshot = value["scenarios"]["per_unit_close"]["unrelated_before"]
            last = snapshot["primary"]["rpc"]["state"]["last"] + 1
            for replica in snapshot["replicas"]:
                rpc = replica["rpc"]
                rpc["state"].update(last=last, history_count=last,
                                    retained_bytes=rpc["state"]["retained_bytes"] + 32)
                rpc["state"]["results"]["close-survival-canary"] = last
                rpc["engine"]["progress"]["received"] = last
                if replica["role"] == "Primary":
                    rpc["engine"]["progress"].update(assigned=last, committed=last,
                                                     completed=last, client_completed=last)
                    snapshot["primary"]["rpc"] = copy.deepcopy(rpc)
                else:
                    rpc["engine"]["progress"]["applied"] = last

        self.reject_data_mutations([
            ("capacity-rejects-committed-ID", reject_committed_id),
            ("recovered-providers-omit-lost-ID", lost_from_recovery),
            ("recovered-providers-change-lost-LSN", lambda value: lost_from_recovery(value, True)),
            ("unrelated-main-rollback-across-close", close_rollback),
        ])

    def test_selected_committed_contents_and_new_writes_bind_every_provider_through_both_gates(self):
        def mutate(value, scenario, snapshots, field, key, replacement=None, remove=True, primary_only=False):
            for name in snapshots:
                snapshot = value["scenarios"][scenario][name]
                rpcs = [snapshot["primary"]["rpc"]]
                if not primary_only:
                    rpcs += [replica["rpc"] for replica in snapshot["replicas"]]
                for rpc in rpcs:
                    if remove:
                        rpc["state"][field].pop(key, None)
                    else:
                        rpc["state"][field][key] = replacement

        changes = []
        for scenario, snapshots in (
                ("identified_retry", ("observation",)),
                ("planned_move", ("after", "observation")),
                ("lost_reply_dedup", ("after", "observation")),
                ("unavailable_quorum", ("before", "observation")),
                ("abrupt_primary_loss", ("observation",)),
                ("per_unit_close", ("unrelated_after", "observation")),
                ("ledger_capacity_backpressure", ("observation",))):
            for field, key in (("results", "initial-1-0"), ("values", "key-initial-1-0")):
                for remove in (True, False):
                    changes.append((f"{scenario}/{field}/{remove}", lambda value, scenario=scenario,
                                    snapshots=snapshots, field=field, key=key, remove=remove:
                                    mutate(value, scenario, snapshots, field, key,
                                           999 if field == "results" else "changed", remove)))
        changes.append(("baseline-primary-view-content", lambda value:
                        mutate(value, "writes_100", ("observation",), "results", "initial-1-0",
                               primary_only=True)))
        for scenario, writes, prefix in (
                ("planned_move", "post_move_writes", "planned-move-2-"),
                ("abrupt_primary_loss", "post_failure_writes", "abrupt-primary-loss-8-")):
            changes.append((scenario + "/unobserved-new-IDs", lambda value, scenario=scenario, writes=writes:
                            value["scenarios"][scenario][writes].update(
                                ids=["never-observed-" + str(index) for index in range(5)])))
            for field, key in (("results", prefix + "0"), ("values", "key-" + prefix + "0")):
                for remove in (True, False):
                    changes.append((f"{scenario}/new/{field}/{remove}", lambda value, scenario=scenario,
                                    field=field, key=key, remove=remove:
                                    mutate(value, scenario, ("observation",), field, key,
                                           999 if field == "results" else "changed", remove)))
            changes.append((scenario + "/primary-view-only", lambda value, scenario=scenario, prefix=prefix:
                            mutate(value, scenario, ("observation",), "results", prefix + "0",
                                   primary_only=True)))
        self.reject_data_mutations(changes)

    def test_quorum_success_all_completion_lag_and_later_primary_poll_are_valid(self):
        for route, filename in (("client-runtime", "rust-report.json"), ("native-data", "native-data-report.json")):
            directory = self.worker(route)
            path = directory / filename
            value = json.loads(path.read_text())
            for name in ("writes_100", "abrupt_primary_loss"):
                snapshot = value["scenarios"][name]["observation"]
                placed = next(replica["rpc"] for replica in snapshot["replicas"] if replica["role"] == "Primary")
                if placed["engine_kind"] != "rust":
                    continue
                placed["engine"]["progress"]["completed"] -= 1
                snapshot["primary"]["rpc"]["engine"]["progress"]["completed"] -= 1
            # A subsequent HTTP poll can then observe an additional ACK.
            primary = value["scenarios"]["abrupt_primary_loss"]["observation"]["primary"]["rpc"]
            if primary["engine_kind"] == "rust":
                primary["engine"]["progress"]["completed"] += 1
            self.emit(path, value)
            self.assertEqual(self.sealing(route, directory), 0, self.last_stdout)
        self.assertEqual(self.aggregating(), 0, self.last_stdout)

    def test_primary_poll_progress_regression_and_noninteger_counters_reject_both_gates(self):
        self.reject_data_mutations([
            ("primary-completion-regression", lambda value: value["scenarios"]["abrupt_primary_loss"]
                ["observation"]["primary"]["rpc"]["engine"]["progress"].update(completed=0)),
            ("primary-completion-bool", lambda value: value["scenarios"]["abrupt_primary_loss"]
                ["observation"]["primary"]["rpc"]["engine"]["progress"].update(completed=True)),
        ])

    def test_unavailable_quorum_pre_assignment_rejection_is_valid_without_manufactured_acceptance(self):
        for route, filename, profile in (("client-runtime", "rust-report.json", "rust"),
                                         ("native-data", "native-data-report.json", "native-data")):
            directory = self.worker(route)
            path = directory / filename
            value = json.loads(path.read_text())
            essential, ledger, _ = self.essential_data(profile, reject_quorum=True)
            for case, evidence in essential.items():
                value["scenarios"][case].update(evidence)
            value["identified_workload"] = ledger
            self.emit(path, value)
            self.assertEqual(self.sealing(route, directory), 0, self.last_stdout)
        self.assertEqual(self.aggregating(), 0, self.last_stdout)

    def test_exact_two_routes_namespaced_32_plus_5_and_hashes_after_teardown(self):
        for route in ci.ROUTES:
            directory = self.worker(route)
            self.assertEqual(self.sealing(route, directory), 0, self.last_stdout)
        with patch.object(ci.runtime, "destroy_owned", side_effect=AssertionError("not aggregate daemon")), \
                patch.object(ci.runtime, "Allocation", side_effect=AssertionError("not aggregate daemon")):
            self.assertEqual(self.aggregating(), 0, self.last_stdout)
        result = ci.read(self.root / "aggregate.json")
        self.assertEqual(len(result["assertions"]), 32)
        self.assertEqual(len(set(result["assertions"])), 32)
        self.assertEqual(len(result["native_tests"]), 5)
        self.assertIn("rust/writes_100", result["assertions"])
        self.assertIn("native-data/writes_100", result["assertions"])
        self.assertNotIn("full", result["coverage"])

    def test_failed_skipped_or_omitted_independent_commands_never_seal_green(self):
        directory = self.worker("client-runtime")
        for command in ci.OUTCOMES:
            values = self.command_outcomes("client-runtime")
            values[command] = "failure" if command != "native_data" else "success"
            self.assertEqual(self.sealing("client-runtime", directory,
                                          self.root / ("failed-" + command + ".json"), values), 1, command)
        values = self.command_outcomes("client-runtime")
        values["legacy"] = "skipped"
        self.assertEqual(self.sealing("client-runtime", directory, self.root / "skipped.json", values), 1)
        values = self.command_outcomes("client-runtime")
        values["native_clients"]["no_tests"] = "pass"
        self.assertEqual(self.sealing("client-runtime", directory, self.root / "empty-native.json", values), 1)
        del values["legacy"]
        self.assertEqual(self.sealing("client-runtime", directory, self.root / "omitted-command.json", values), 1)

    def test_native_data_negative_gates_aux_policy_cleanup_and_exact_17_cases(self):
        directory = self.worker("native-data")
        path = directory / "native-data-report.json"
        original = json.loads(path.read_text())
        mutations = [
            lambda v: v.update(profile="mixed"),
            lambda v: v["scenarios"].pop("native_interrupted_copy_epoch"),
            lambda v: v["scenarios"].update(legacy_full={"status": "PASS"}),
            lambda v: v["auxiliary_data_services"][0].update(eligible_nodes=["N0010", "N0020", "N0030"]),
            lambda v: v["auxiliary_data_services"][1]["cleanup"].update(original_routes_retired=False),
            lambda v: v["cleanup"]["image_store_upload"].update(absence_verified=1),
            lambda v: v["scenarios"]["apply_ack_delay"].update(blocked_after_receive=False),
            lambda v: v["scenarios"]["rust_primary_native_secondary"]["replacement_copy"]
                ["destination_copy_ack"].update(negative_observations=True),
            lambda v: v["scenarios"]["native_primary_rust_secondary"]["replacement_copy"]
                ["destination_copy_ack"].update(released=False),
            lambda v: v["scenarios"]["native_primary_rust_secondary"]
                ["post_copy_no_write_moves"][0].pop("actual_fresh_native_ack_frontiers"),
            lambda v: v["scenarios"]["stale_wire_receipts"]["coverage"][0]["receipts"][0]
                ["peer_correlation"]["wire_delivery"].update(classification="sender_admission"),
            lambda v: v["scenarios"]["native_interrupted_copy_epoch"]["live_copy"]
                .update(commits_inside_observed_build_horizon_window=99),
            lambda v: v["provenance"].update(executable_source_revision="f" * 40),
            lambda v: v["cleanup"].update(onebox={"phase": "destroyed"}),
        ]
        for index, mutate in enumerate(mutations):
            value = copy.deepcopy(original)
            mutate(value)
            self.emit(path, value)
            self.assertEqual(self.sealing("native-data", directory, self.root / ("bad-data-" + str(index) + ".json")),
                             1, index)
        self.emit(path, original)
        self.assertEqual(self.sealing("native-data", directory), 0)

    def test_heterogeneous_native_proofs_no_five_fake_pass_statuses(self):
        directory = self.worker("client-runtime")
        for index, file in enumerate(ci.NATIVE_FILES):
            path = directory / "client" / file
            original = ci.read(path)
            self.emit(path, {"status": "PASS"})
            self.assertEqual(self.sealing("client-runtime", directory,
                                          self.root / ("fake-native-" + str(index) + ".json")), 1)
            self.emit(path, original)
        self.assertEqual(self.sealing("client-runtime", directory), 0)

    def test_source_eos_variants_require_typed_payloads_bound_to_actual_held_copy(self):
        directory = self.worker("native-data")
        path = directory / "native-data-report.json"
        original = json.loads(path.read_text())
        for case in ci.data.MIXED_SCENARIOS[:2]:
            changes = [
                lambda gate: gate.update(source_eos={"kind": False}),
                lambda gate: gate.update(source_eos={"kind": "unknown"}),
                lambda gate: gate["source_eos"].pop(
                    "peer" if case.startswith("rust_primary") else "observation"),
                lambda gate: gate["held_operation"].update(boundary=True),
                lambda gate: gate["completed_native_replica"]["rpc"]["copy_ack_hold"].update(sequence=13),
            ]
            if case.startswith("rust_primary"):
                changes.extend((
                    lambda gate: gate["source_eos"]["peer"].update(copy_received=True),
                    lambda gate: gate["source_eos"]["peer"].update(replica=43),
                    lambda gate: gate["source_eos"]["peer"].update(copy_applied=12),
                    lambda gate: gate["source_eos"]["peer"].update(building=False),
                ))
            else:
                changes.extend((
                    lambda gate: gate["source_eos"]["observation"].update(requested_horizon=11),
                    lambda gate: gate["source_eos"]["observation"].update(enumeration_end=1),
                    lambda gate: gate["source_eos"]["observation"].update(items=False),
                    lambda gate: gate["source_eos"]["observation"].update(end_count=14),
                    lambda gate: gate["source_eos"]["observation"].update(begin_count=12),
                    lambda gate: gate["source_eos"]["observation"].update(end_count=1),
                    lambda gate: gate["source_eos"]["observation"].update(callback_count=1),
                ))
            for index, change in enumerate(changes):
                broken = copy.deepcopy(original)
                change(broken["scenarios"][case]["replacement_copy"]["destination_copy_ack"])
                self.emit(path, broken)
                self.assertEqual(self.sealing("native-data", directory,
                    self.root / (case + "-eos-" + str(index) + ".json")), 1, (case, index))
        self.emit(path, original)
        self.assertEqual(self.sealing("native-data", directory), 0)

    def test_aggregate_rechecks_source_eos_even_when_tampered_report_hash_is_resealed(self):
        for route in ci.ROUTES:
            self.assertEqual(self.sealing(route, self.worker(route)), 0)
        directory = self.root / "workers/native-data"
        path = directory / "native-data-report.json"
        report = json.loads(path.read_text())
        proof_path = directory / "worker-proof.json"
        proof = ci.read(proof_path)
        for index, variant in enumerate(("wrong_tag", "inconsistent_counts")):
            broken = copy.deepcopy(report)
            if variant == "wrong_tag":
                broken["scenarios"]["rust_primary_native_secondary"]["replacement_copy"][
                    "destination_copy_ack"]["source_eos"] = {"kind": False}
            else:
                broken["scenarios"]["native_primary_rust_secondary"]["replacement_copy"][
                    "destination_copy_ack"]["source_eos"]["observation"].update(
                        begin_count=1, end_count=1, callback_count=1)
            self.emit(path, broken)
            forged = copy.deepcopy(proof)
            forged["reports"]["native-data-report.json"] = ci.identity(path, ci.REPORT)
            self.emit(proof_path, forged)
            output = self.root / ("aggregate-eos-" + str(index) + ".json")
            self.assertEqual(self.aggregating(output), 1)
            self.assertEqual(ci.read(output)["status"], "FAIL")

    def test_each_archived_native_digest_and_essential_boolean_is_enforced(self):
        directory = self.worker("client-runtime")
        for index, file in enumerate(ci.NATIVE_FILES[1:]):
            path = directory / "client" / file
            original = ci.read(path)
            broken = copy.deepcopy(original)
            broken["archive_sha256"] = "f" * 64
            self.emit(path, broken)
            self.assertEqual(self.sealing("client-runtime", directory,
                                          self.root / ("native-digest-" + str(index) + ".json")), 1)
            self.emit(path, original)
        path = directory / "client" / ci.NATIVE_FILES[0]
        original = ci.read(path)
        for index, value in enumerate((False, 1, "true")):
            broken = copy.deepcopy(original)
            broken["executed_cases"][0]["passed"] = value
            self.emit(path, broken)
            self.assertEqual(self.sealing("client-runtime", directory,
                                          self.root / ("native-case-" + str(index) + ".json")), 1)
        self.emit(path, original)

    def test_missing_post_cleanup_original_owner_and_resource_id_drift(self):
        directory = self.worker("native-data")
        path = directory / "cleanup.json"
        original = ci.read(path)
        path.unlink()
        self.assertEqual(self.sealing("native-data", directory, self.root / "missing-cleanup.json"), 1)
        changes = [
            lambda v: v["allocation"].update(owner_sha256="f" * 64),
            lambda v: v["destroy"]["independent_absence"].update(container=["replacement"]),
            lambda v: v["destroy"]["independent_absence"].update(volume=[]),
            lambda v: v["destroy"].update(absence_verified=False),
            lambda v: v["destroy"].update(data_deleted=1),
            lambda v: v.update(status="FAIL", errors=["actual cleanup failed"]),
        ]
        for index, change in enumerate(changes):
            value = copy.deepcopy(original)
            change(value)
            self.emit(path, value)
            self.assertEqual(self.sealing("native-data", directory,
                                          self.root / ("cleanup-drift-" + str(index) + ".json")), 1)

    def test_aggregate_rejects_job_results_missing_extra_stale_tamper_and_wrong_proofs(self):
        for route in ci.ROUTES:
            self.assertEqual(self.sealing(route, self.worker(route)), 0)
        for index, result in enumerate(("failure", "skipped", "cancelled", "success-extra")):
            for job in ("producer", "workers"):
                needs = {"producer": "success", "workers": "success", job: result}
                self.assertEqual(self.aggregating(self.root / (job + str(index) + ".json"), needs), 1)
        path = self.root / "workers/native-data/worker-proof.json"
        original = ci.read(path)
        for index, mutation in enumerate((
                lambda v: v["context"].update(attempt="0"),
                lambda v: v.update(route="client-runtime"),
                lambda v: v.update(version=True),
                lambda v: v.update(status="FAIL", errors=["suite failed"]),
                lambda v: v["reports"]["cleanup.json"].update(sha256="f" * 64),
                lambda v: v.update(assertions=["native-data/writes_100"]),
                lambda v: v["timing"].update(worker_elapsed_seconds=True),
                lambda v: v.update(extra="unexpected proof"),
        )):
            broken = copy.deepcopy(original)
            mutation(broken)
            self.emit(path, broken)
            self.assertEqual(self.aggregating(self.root / ("bad-proof-" + str(index) + ".json")), 1, index)
        self.emit(path, original)
        report_path = path.parent / "native-data-report.json"
        content = report_path.read_bytes()
        report_path.write_bytes(content + b" ")
        self.assertEqual(self.aggregating(self.root / "tampered-report.json"), 1)
        report_path.write_bytes(content)
        extra = self.emit(path.parent / "unexpected.json", {})
        self.assertEqual(self.aggregating(self.root / "extra-report.json"), 1)
        extra.unlink()
        path.unlink()
        self.assertEqual(self.aggregating(self.root / "missing-worker.json"), 1)

    def test_aggregate_rejects_shared_original_names_owners_containers_and_networks(self):
        client = self.worker("client-runtime")
        self.assertEqual(self.sealing("client-runtime", client), 0)
        native = self.worker("native-data")
        report_path, cleanup_path = native / "native-data-report.json", native / "cleanup.json"
        report, cleanup = json.loads(report_path.read_text()), ci.read(cleanup_path)
        lifecycle = ci.read(native / "fixture.json")
        for key in ("name", "owner", "container", "network"):
            altered, clean = copy.deepcopy(report), copy.deepcopy(cleanup)
            if key == "name":
                altered["onebox"] = clean["allocation"]["name"] = clean["destroy"]["name"] = "owned"
                clean["destroy"]["independent_absence"]["volume"] = ["sf-onebox-owned_data"]
            elif key == "owner":
                altered["owner"] = "a" * 32
                clean["allocation"]["owner_sha256"] = ci.owner_hash("a" * 32)
            elif key == "container":
                altered["container_id"] = "original-cid"
                clean["destroy"]["independent_absence"]["container"] = ["original-cid"]
            else:
                clean["destroy"]["independent_absence"]["network"] = ["network-owned"]
            self.emit(report_path, altered)
            self.emit(cleanup_path, clean)
            changed = copy.deepcopy(lifecycle)
            changed["allocation"] = {**clean["allocation"],
                                     "container_id": clean["destroy"]["independent_absence"]["container"][0]}
            self.emit(native / "fixture.json", changed)
            self.emit(native / "diagnostics.json", {
                "name": clean["allocation"]["name"],
                "project": "sf-onebox-" + clean["allocation"]["name"], "errors": [],
            })
            self.assertEqual(self.sealing("native-data", native), 0, key)
            self.assertEqual(self.aggregating(self.root / ("shared-" + key + ".json")), 1, key)
            (native / "worker-proof.json").unlink()

class BoundedParserTests(fixtures.ProjectFiles):
    def test_selected_content_digest_is_canonical_local_strict_and_bounded(self):
        path = self.root / "contents.json"
        selected = {"initial-1": 1, "planned-move-2": 2}
        spec = {"results": ci.DigestSpec(ci.PrefixSpec(("initial-", "planned-move-"), True))}
        for records in (selected, dict(reversed(list(selected.items())))):
            path.write_text(json.dumps({"results": {**records, "discarded": "x" * 1000}}))
            self.assertEqual(ci.read(path, ci.REPORT, spec), {"results": ci.content_digest(selected)})
        # A supplied hash is not an attestation; the parser computes over the
        # selected entries itself and cannot fabricate the missing identities.
        path.write_text(json.dumps({"results": ci.content_digest(selected)}))
        self.assertNotEqual(ci.read(path, ci.REPORT, spec)["results"], ci.content_digest(selected))
        for body in ('{"results":[]}', '{"results":{"discarded":{"x":1,"x":2}}}',
                     '{"results":{"initial-1":1,"initial-1":2}}'):
            path.write_text(body)
            with self.assertRaises(ci.ProofError):
                ci.read(path, ci.REPORT, spec)
        path.write_text(json.dumps({"results": {"initial-1": "x" * ci.SMALL}}))
        with self.assertRaises(ci.ProofError):
            ci.read(path, ci.REPORT, spec)

    def test_lost_reply_prefix_projection_retains_real_attempts_not_the_full_identity_ledger(self):
        path = self.root / "workload.json"
        lost = {"id": "lost-reply-6", "sequence": 106, "committed": True, "attempts": [
            {"outcome": "ambiguous_transport", "reply_intentionally_unread": True}]}
        discarded = {str(index): {"payload": "x" * 1000} for index in range(4096)}
        path.write_text(json.dumps({"identified_workload": {**discarded, "lost-reply-6": lost}}))
        projected = ci.read(path, ci.REPORT, {"identified_workload": ci.DATA_SPEC["identified_workload"]})
        self.assertEqual(projected, {"identified_workload": {"lost-reply-6": lost}})
        self.assertLess(len(ci.encode(projected)), 1024)
        path.write_text('{"identified_workload":{"discarded":{"id":1,"id":2},"lost-reply-6":{}}}')
        with self.assertRaises(ci.ProofError):
            ci.read(path, ci.REPORT, ci.DATA_SPEC)
        path.write_text('{"identified_workload":[]}')
        with self.assertRaises(ci.ProofError):
            ci.read(path, ci.REPORT, ci.DATA_SPEC)

    def test_scalars_crossing_stream_chunks_do_not_truncate_numeric_prefixes(self):
        path = self.root / "chunks.json"
        for token in ("1.25", "1.25e-3", "-12.0E+4", "true", "null", '"escaped\\\\\\"text"'):
            for split in range(1, len(token)):
                prefix = '{"value":'
                path.write_text(" " * (65536 - len(prefix) - split) + prefix + token + "}")
                with self.subTest(token=token, split=split):
                    self.assertEqual(ci.read(path), json.loads('{"value":' + token + "}"))

    def test_projection_discards_large_arrays_but_detects_nested_duplicates(self):
        path = self.root / "large.json"
        path.write_text('{"keep":{"passed":true},"discard":['
                        + ",".join('{"large":"' + "x" * 1000 + '"}' for _ in range(3000)) + "]}")
        self.assertEqual(ci.read(path, ci.REPORT, {"keep": True}), {"keep": {"passed": True}})
        path.write_text('{"keep":true,"discard":{"x":1,"x":2}}')
        with self.assertRaises(ci.ProofError):
            ci.read(path, ci.REPORT, {"keep": True})

    def test_bounds_symlink_nonregular_duplicates_nonfinite_and_bad_structure(self):
        with self.assertRaises(ci.ProofError):
            ci.Projection(io.StringIO('{"grow":"' + "x" * 100 + '"}'), limit=32).value(True)
        path = self.root / "proof.json"
        for value in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', '{"x":1}garbage',
                      '{"x":01}', '{"x":', '{"x":true,}', '[]'):
            path.write_text(value)
            with self.subTest(value=value), self.assertRaises((ci.ProofError, ValueError)):
                ci.read(path)
        path.write_text('{"x":"' + "a" * ci.SMALL + '"}')
        with self.assertRaises(ci.ProofError):
            ci.read(path)
        with self.assertRaises(ci.ProofError):
            ci.read(self.root)
        path.write_text("{}")
        link = self.root / "link.json"
        try:
            link.symlink_to(path)
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaises(ci.ProofError):
            ci.read(link)
        oversize = self.root / "oversize.json"
        with oversize.open("wb") as file:
            file.truncate(ci.REPORT + 1)
        with self.assertRaises(ci.ProofError):
            ci.read(oversize, ci.REPORT)


if __name__ == "__main__":
    unittest.main()
