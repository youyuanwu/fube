"""Offline receipt/route models, not native or CI qualification."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import unittest
from unittest.mock import patch

import onebox_ci as ci
import test_fube_data_test as fixtures


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
    def copy_proof(ack=False):
        result = {"concurrent_writes": {"successful": 100},
                  "live_copy": {"commits_inside_observed_build_horizon_window": 100}}
        if ack:
            result["destination_copy_ack"] = {
                "blocked_after_receive": True, "released": True, "negative_observations": 2,
                "source_eos": {"kind": "actual source EOS"},
                "completed_native_replica": {"rpc": {"copy_ack_hold": {
                    "released": True, "acknowledged": True, "held": False,
                    "expired": False, "cancelled": False}}},
            }
        return result

    def data_report(self, profile, name, owner, container, start):
        required = ci.data.required_scenarios(profile)
        scenarios = {case: {"status": "PASS", "started_at": start + index / 100,
                            "finished_at": start + (index + 1) / 100}
                     for index, case in enumerate(required)}
        kinds = ["rust"] * 3 if profile == "rust" else ["native-classic-v1", "rust", "native-classic-v1"]
        scenarios["deployment_identity"]["processes"] = [{
            "native_identity": {
                "kind": kind, "engine_sha256": self.built["inputs"]["app" if kind == "rust" else "native_oracle"]["sha256"]},
            "process": {
                "executable_sha256": self.built["inputs"]["app" if kind == "rust" else "native_oracle"]["sha256"],
                "classification": "native-free" if kind == "rust" else "native-classic-dynamic",
                "native_libraries": dict.fromkeys(("fabricruntime", "fabriccommon", "fabricclient"), kind != "rust"),
            },
        } for kind in kinds]
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
                    replacement_copy=self.copy_proof(ack=True),
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
            "required_scenarios": list(required), "scenarios": scenarios,
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
    def test_produce_validate_both_routes_external_identity_no_allocation(self):
        with patch.object(ci.runtime, "create_owned", side_effect=AssertionError("no allocation")):
            for route in ci.ROUTES:
                path = self.validated(route)
                value = ci.read(path)
                self.assertEqual(value["inputs"], self.built["inputs"])
                self.assertEqual(value["context"], self.current)
        self.assertEqual(set(self.built["inputs"]), {
            "bundle_tar", "manifest", "nested_manifest", "app", "observer",
            "native_oracle", "native_archive", "qualification_script", "runtime_script"})
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
        original = ci.read(path, ci.REPORT)
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
        report, cleanup = ci.read(report_path), ci.read(cleanup_path)
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
            self.assertEqual(self.sealing("native-data", native), 0, key)
            self.assertEqual(self.aggregating(self.root / ("shared-" + key + ".json")), 1, key)
            (native / "worker-proof.json").unlink()

class BoundedParserTests(fixtures.ProjectFiles):
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
