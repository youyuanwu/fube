"""Offline ownership/qualification regressions; no native interoperability claim."""
import contextlib
import copy
import io
import json
import sys
import unittest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import rust_runtime_test as runtime


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class BinaryBundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / "app"
        self.observer = self.root / "observer"
        for binary in (self.app, self.observer):
            binary.write_bytes(binary.name.encode())
            binary.chmod(0o755)
        self.binaries = runtime.Binaries(self.app, self.observer, "rust-runtime-app\n  mssf-runtime")

    def bundle(self, name="bundle"):
        directory = self.root / name
        with patch.object(runtime, "build_binaries", return_value=self.binaries):
            runtime.build_bundle(directory)
        return directory

    def test_build_only_produces_verified_executables_without_allocation(self):
        directory = self.root / "bundle"
        with patch("sys.argv", ["rust_runtime_test.py", "--build-bundle", str(directory)]), \
             patch.object(runtime, "build_binaries", return_value=self.binaries), \
             patch.object(runtime, "create_owned") as create, \
             contextlib.redirect_stdout(io.StringIO()):
            runtime.main()
            create.assert_not_called()
        with patch.object(runtime, "run", side_effect=AssertionError("bundle loading must not invoke Cargo")):
            binaries = runtime.load_bundle(directory)
        self.assertEqual(binaries.app.read_bytes(), self.app.read_bytes())
        self.assertEqual(binaries.observer.read_bytes(), self.observer.read_bytes())
        self.assertEqual(binaries.dependency_graph, self.binaries.dependency_graph)
        self.assertEqual(binaries.app.stat().st_mode & 0o777, 0o755)
        self.assertEqual(binaries.observer.stat().st_mode & 0o777, 0o755)

    def test_invalid_prebuilt_fails_before_allocation_without_build_fallback(self):
        for failure in ("missing-bundle", "missing-observer", "tampered-app", "missing-graph", "native-graph"):
            with self.subTest(failure=failure):
                directory = self.bundle(failure)
                if failure == "missing-bundle":
                    directory = directory / "absent"
                elif failure == "missing-observer":
                    (directory / "runtime_observer").unlink()
                elif failure == "tampered-app":
                    (directory / "rust-runtime-app").write_bytes(b"replaced after build")
                else:
                    path = directory / "build-info.json"
                    metadata = json.loads(path.read_text())
                    metadata["dependency_graph"] = "" if failure == "missing-graph" else "mssf-core"
                    path.write_text(json.dumps(metadata))
                with patch("sys.argv", ["rust_runtime_test.py", "owned", "--prebuilt", str(directory)]), \
                     patch.object(runtime, "build_binaries") as build, \
                     patch.object(runtime, "create_owned") as create, \
                     patch.object(runtime, "Allocation") as allocation, \
                     contextlib.redirect_stdout(io.StringIO()) as output:
                    with self.assertRaises(SystemExit):
                        runtime.main()
                    build.assert_not_called()
                    create.assert_not_called()
                    allocation.assert_not_called()
                    self.assertEqual(json.loads(output.getvalue())["status"], "FAIL")

    def test_prebuilt_qualification_uses_bundle_app_and_observer_without_cargo(self):
        binaries = runtime.load_bundle(self.bundle())
        allocation = MagicMock()
        allocation.name = "owned"
        allocation.owner = "capability"
        allocation.container_id = "cid"
        allocation.sf.return_value = '{"items":[]}'
        def run(*args, **_kwargs):
            self.assertNotEqual(args[0], "cargo")
            return ""
        with patch.object(runtime, "run", side_effect=run), \
             patch.object(runtime, "build_binaries", side_effect=AssertionError("must not build")), \
             patch.object(runtime, "package", return_value=(self.root, "minted-type", "1.1")) as package, \
             patch.object(runtime, "restart", return_value={"native": "model observation"}) as restart, \
             patch.object(runtime, "controls_and_authority") as controls:
            result = runtime.qualify(allocation, True, binaries)
        self.assertEqual(result["status"], "PASS_FULL_STATELESS_PROFILE")
        self.assertEqual(package.call_args.args[1], binaries.app)
        self.assertEqual(restart.call_args.kwargs["observer"], binaries.observer)
        self.assertEqual(controls.call_args.kwargs["observer"], binaries.observer)
        self.assertEqual(result["dependency_graph"], binaries.dependency_graph)
        self.assertEqual(result["binary_sha256"]["rust-runtime-app"], runtime.digest(binaries.app))
        observed = {"resolver": "mssf-client", "rpc": {"service": "fabric:/Owned/Singleton", "pid": 7},
                    "native_pid": 7}
        with patch.object(runtime, "run", return_value=json.dumps(observed)) as command:
            runtime.observe("fabric:/Owned/Singleton", observer=binaries.observer)
        self.assertEqual(command.call_args.args[0], str(binaries.observer))

    def test_default_build_is_locked_and_checks_native_dependencies(self):
        with patch.object(runtime, "run", return_value=self.binaries.dependency_graph) as command:
            binaries = runtime.build_binaries()
        self.assertEqual(binaries.dependency_graph, self.binaries.dependency_graph)
        commands = [call.args for call in command.call_args_list]
        self.assertTrue(any(args[:2] == ("cargo", "build") and "rust-runtime-app" in args for args in commands))
        self.assertTrue(any("--example" in args and "runtime_observer" in args for args in commands))
        self.assertTrue(all("--locked" in args for args in commands))
        with patch.object(runtime, "run", return_value="rust-runtime-app\n  mssf-core"):
            with self.assertRaisesRegex(RuntimeError, "native SF adapters"):
                runtime.build_binaries()


class ReportingEvidenceTests(unittest.TestCase):
    def test_load_requires_fm_distinctive_value_and_timestamp(self):
        query = {"PrimaryLoadMetricReports": [
            {"Name": "RuntimeWork", "Value": 731, "LastReportedUtc": "2026-10-27T01:00:00Z"},
            {"Name": "_MoveCost_", "Value": 3, "LastReportedUtc": "2026-10-27T01:00:00Z"}]}
        self.assertTrue(runtime.load_value(query, "RuntimeWork", 731))
        self.assertTrue(runtime.load_value(query, "_MoveCost_", 3))
        self.assertFalse(runtime.load_value(query, "RuntimeWork", 947))
        query["PrimaryLoadMetricReports"][0]["LastReportedUtc"] = ""
        self.assertFalse(runtime.load_value(query, "RuntimeWork", 731))

    def test_health_source_property_and_duplicate_identity(self):
        query = {"HealthEvents": [
            {"SourceId": "Other", "Property": "Ready"},
            {"SourceId": "RustRuntime", "Property": "Ready", "IsExpired": True}]}
        self.assertTrue(runtime.health_event(query, "Ready")["IsExpired"])
        self.assertIsNone(runtime.health_event(query, "Removed"))
        query["HealthEvents"].append(query["HealthEvents"][1])
        with self.assertRaises(RuntimeError):
            runtime.health_event(query, "Ready")

    def test_partition_fault_replacement_is_not_process_replacement(self):
        before = {"rpc": {"partition": "p", "instance": 5, "incarnation": 6, "pid": 7, "host": "h"}}
        runtime.require_same_instance(copy.deepcopy(before), before)
        for field in before["rpc"]:
            changed = copy.deepcopy(before)
            changed["rpc"][field] = "changed"
            with self.assertRaises(RuntimeError):
                runtime.require_same_instance(changed, before)

    def test_report_command_uses_prebuilt_independent_observer(self):
        observed = {"resolver": "mssf-client", "native_pid": 7,
                    "rpc": {"pid": 7, "service": "fabric:/Owned/Concurrent", "report": {"ok": True}}}
        observer = Path("/bundle") / "runtime_observer"
        with patch.object(runtime, "run", return_value=json.dumps(observed)) as command:
            runtime.observe("fabric:/Owned/Concurrent", "int:0", observer=observer,
                            command="op=fault")
        command.assert_called_once_with(str(observer), "127.0.0.1:19000",
                                       "fabric:/Owned/Concurrent", "int:0", "op=fault", timeout=45)


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AllocationTests(unittest.TestCase):
    def setUp(self):
        self.state = {"name": "owned", "owner": "capability", "project": "sf-onebox-owned",
                      "runtime_version": "11.8.121.4"}
        self.container = {
            "Id": "immutable-container", "State": {"Running": True},
            "Config": {"Labels": {runtime.OWNER_LABEL: "capability",
                                  "com.docker.compose.project": "sf-onebox-owned",
                                  "com.docker.compose.service": "onebox"}},
            "Mounts": [{"Name": "sf-onebox-owned_data", "Destination": "/var/lib/sf-node"}],
        }

    def test_exact_owner_volume_version_and_running_identity(self):
        self.assertEqual(runtime.validate_allocation(self.state, self.container, "owned", "capability"),
                         "immutable-container")
        for value in ("other", "", "capability-wrong"):
            with self.assertRaises(RuntimeError):
                runtime.validate_allocation(self.state, self.container, "owned", value)
        for field in ("owner", "project", "runtime_version", "name"):
            changed = dict(self.state)
            changed[field] = "wrong"
            with self.assertRaises(RuntimeError):
                runtime.validate_allocation(changed, self.container, "owned", "capability")
        changed = copy.deepcopy(self.container)
        changed["Mounts"][0]["Name"] = "unrelated"
        with self.assertRaises(RuntimeError):
            runtime.validate_allocation(self.state, changed, "owned", "capability")
        changed = copy.deepcopy(self.container)
        changed["State"]["Running"] = False
        with self.assertRaises(RuntimeError):
            runtime.validate_allocation(self.state, changed, "owned", "capability")

    def test_collision_or_partial_creation_never_gets_name_only_cleanup(self):
        with patch("sys.argv", ["rust_runtime_test.py", "owned"]), \
             patch.object(runtime, "create_owned", side_effect=RuntimeError("controller rejected existing/partial allocation")) as create, \
             patch.object(runtime,"destroy_owned") as destroy, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                runtime.main()
            create.assert_called_once_with("owned")
            destroy.assert_not_called()
        # The controller itself owns partial-create recovery. A failed create
        # cannot grant this script a cleanup capability for a pre-existing box.

    def test_successful_create_retains_cleanup_after_constructor_failure(self):
        with patch("sys.argv",["rust_runtime_test.py","owned"]), \
             patch.object(runtime,"create_owned",return_value="acquired-owner"), \
             patch.object(runtime,"Allocation",side_effect=RuntimeError("post-create status failed")), \
             patch.object(runtime,"destroy_owned",return_value={"phase":"destroyed"}) as destroy, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):
                runtime.main()
            destroy.assert_called_once_with("owned","acquired-owner")
            result=json.loads(output.getvalue())
            self.assertEqual(result["status"],"FAIL")
            self.assertIn("post-create",result["errors"][0])
            self.assertEqual(result["cleanup"]["onebox"]["phase"],"destroyed")

    def test_cleanup_uses_controller_lock_and_owner_not_readiness(self):
        cluster=MagicMock()
        cluster.state={"owner":"acquired-owner","phase":"down"}
        with patch.object(runtime.onebox_ctl.ctl,"locked",return_value=contextlib.nullcontext(Path("/fake"))), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster):
            runtime.destroy_owned("owned","acquired-owner")
            cluster.destroy.assert_called_once_with("owned")
            cluster.destroy.reset_mock()
            cluster.state["owner"]="different-owner"
            with self.assertRaises(RuntimeError):
                runtime.destroy_owned("owned","acquired-owner")
            cluster.destroy.assert_not_called()

    def test_creation_captures_original_owner_before_controller_unlock(self):
        cluster=MagicMock()
        held={"value":False}
        @contextlib.contextmanager
        def lock(_root,_name):
            held["value"]=True
            yield Path("/fake")
            held["value"]=False
            # Earliest permitted replacement is after creation's transaction.
            cluster.state={"owner":"b"*32}
        def create(*_args):
            self.assertTrue(held["value"])
            cluster.state={"owner":"a"*32}
        cluster.create.side_effect=create
        with patch.object(runtime.onebox_ctl.ctl,"check_local_docker"), \
             patch.object(runtime.onebox_ctl.ctl,"locked",side_effect=lock), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster):
            owner=runtime.create_owned("owned")
        self.assertEqual(owner,"a"*32)
        self.assertEqual(cluster.state["owner"],"b"*32)

    def test_replaced_allocation_never_grants_new_owner_cleanup_authority(self):
        cluster=MagicMock()
        cluster.state={"owner":"b"*32}
        with patch("sys.argv",["rust_runtime_test.py","owned"]), \
             patch.object(runtime,"create_owned",return_value="a"*32), \
             patch.object(runtime,"Allocation",side_effect=RuntimeError("replacement owner mismatch")) as allocation, \
             patch.object(runtime,"qualify") as qualify, \
             patch.object(runtime.onebox_ctl.ctl,"locked",return_value=contextlib.nullcontext(Path("/fake"))), \
             patch.object(runtime.onebox_ctl,"Onebox",return_value=cluster), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):
                runtime.main()
            allocation.assert_called_once_with("owned","a"*32)
            qualify.assert_not_called()
            cluster.destroy.assert_not_called()
            result=json.loads(output.getvalue())
            self.assertEqual(result["status"],"FAIL")
            self.assertIn("owner changed",result["errors"][1])


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class AmbiguousMutationTests(unittest.TestCase):
    def qualify(self,lost,cleanup_failure=False):
        allocation=MagicMock(name="allocation")
        allocation.name="owned";allocation.owner="capability";allocation.container_id="cid"
        allocation.verify.return_value="cid"
        present={"type":False,"app":False}
        calls=[]
        type_name="FubeRustRuntimeabcdefgh1234Type"
        application="fabric:/FubeRustRuntimeabcdefgh1234"
        def sf(*args,**_kwargs):
            calls.append(args)
            if args==("application","list"):
                return json.dumps({"items":[{"name":application,"typeName":type_name,"typeVersion":"1.1"}] if present["app"] else []})
            if args==("application","type-list"):
                return json.dumps({"items":[{"name":type_name,"version":"1.1"}] if present["type"] else []})
            if args[:2]==("application","provision"):
                present["type"]=True
                if lost=="provision":raise subprocess.TimeoutExpired("accepted provision",1)
            if args[:2]==("application","create"):
                present["app"]=True
                if lost=="create":raise subprocess.TimeoutExpired("accepted create",1)
            if args[:2]==("application","delete"):
                if cleanup_failure:raise RuntimeError("cleanup delete failed")
                present["app"]=False
            if args[:2]==("application","unprovision"):present["type"]=False
            return "{}"
        allocation.sf.side_effect=sf
        with patch.object(runtime,"run",return_value="native-free"), \
             patch.object(runtime,"digest",return_value="a"*64), \
             patch.object(runtime.uuid,"uuid4",return_value=MagicMock(hex="abcdefgh1234")), \
             patch.object(runtime,"package",return_value=(Path("/generated"),type_name,"1.1")):
            result=runtime.qualify(allocation)
        return result,present,calls

    def test_accepted_provision_timeout_is_reconciled_and_removed(self):
        result,present,calls=self.qualify("provision")
        self.assertEqual(result["status"],"FAIL")
        self.assertFalse(present["type"])
        self.assertTrue(any(v[:2]==("application","unprovision") for v in calls))
        self.assertIn("accepted provision",result["errors"][0])

    def test_accepted_create_timeout_deletes_only_minted_app_then_type(self):
        result,present,calls=self.qualify("create")
        self.assertEqual(result["status"],"FAIL")
        self.assertEqual(present,{"type":False,"app":False})
        delete=next(i for i,v in enumerate(calls) if v[:2]==("application","delete"))
        unprovision=next(i for i,v in enumerate(calls) if v[:2]==("application","unprovision"))
        self.assertLess(delete,unprovision)
        self.assertIn("accepted create",result["errors"][0])

    def test_cleanup_error_preserves_original_failure_and_recovery_identity(self):
        result,present,_=self.qualify("create",cleanup_failure=True)
        self.assertEqual(result["status"],"FAIL")
        self.assertTrue(present["app"])
        self.assertIn("accepted create",result["errors"][0])
        self.assertIn("cleanup delete failed",result["errors"][1])
        self.assertEqual(result["cleanup"]["retained_for_recovery"]["application"],result["application"])

    def test_existing_owner_allocation_is_never_destroyed(self):
        with patch("sys.argv",["rust_runtime_test.py","owned","--existing-owner","capability"]), \
             patch.object(runtime,"Allocation"),patch.object(runtime,"qualify",return_value={"status":"FAIL","errors":["failure"],"cleanup":{}}), \
             patch.object(runtime,"destroy_owned") as destroy,contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit):runtime.main()
            destroy.assert_not_called()
            self.assertEqual(json.loads(output.getvalue())["cleanup"]["onebox"],"caller-owned allocation retained")


@unittest.skipUnless(sys.platform=="linux","Linux-only application-runtime harness")
class ProcessTests(unittest.TestCase):
    def fixture(self):
        return {"native_pid": 321, "native_entry_point": "/owned/app/rust-runtime-app",
                "rpc": {"host": "native-host", "service": "fabric:/Owned/Singleton", "code_sequence": 99}}

    def probe(self, expected, *, host="native-host", maps="/usr/lib/libc.so.6", stale=False):
        class FakePath:
            def __init__(self, value):
                self.value = value

            def __truediv__(self, value):
                return FakePath(self.value + "/" + str(value))

            def read_bytes(self):
                return (f"Fabric_ApplicationHostId={host}\0Fabric_ApplicationName=fabric:/Owned\0"
                        "Fabric_CodePackageInstanceSeqNum=99\0").encode()

            def read_text(self):
                if self.value.endswith("/stat"):
                    return "321 (rust-runtime-app) S 119 " + "0 " * 40
                return f"1-2 r-xp 0 0 0 {maps}"

        with patch("sys.argv", ["probe", json.dumps(expected), "kill"]), \
             patch("pathlib.Path", FakePath), \
             patch("os.pidfd_open", side_effect=ProcessLookupError if stale else None, return_value=44), \
             patch("os.readlink", return_value="/owned/app/rust-runtime-app"), \
             patch("os.close"), patch("signal.pidfd_send_signal") as signal, \
             contextlib.redirect_stdout(io.StringIO()):
            try:
                exec(runtime.PROCESS, {})
            except (RuntimeError, ProcessLookupError):
                self.assertFalse(signal.called)
                return False
            self.assertEqual(signal.call_args.args[:2], (44, 9))
            return True

    def test_pidfd_signal_is_bound_to_verified_native_identity(self):
        self.assertTrue(self.probe(self.fixture()))
        self.assertFalse(self.probe(self.fixture(), host="reused-pid-different-host"))
        self.assertFalse(self.probe(self.fixture(), stale=True))
        self.assertFalse(self.probe(self.fixture(), maps="/opt/libFabricRuntime.so"))
        self.assertFalse(self.probe(self.fixture(), maps="/opt/libFabricCommon.so"))
        changed = self.fixture()
        changed["native_entry_point"] = "/unrelated/rust-runtime-app"
        self.assertFalse(self.probe(changed))


if __name__ == "__main__":
    unittest.main()
