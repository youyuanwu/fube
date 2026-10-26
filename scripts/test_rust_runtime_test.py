"""Offline ownership/qualification regressions; no native interoperability claim."""
import contextlib
import copy
import io
import json
import unittest
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import rust_runtime_test as runtime


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
             patch.object(runtime, "run", side_effect=RuntimeError("controller rejected existing/partial allocation")) as call, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                runtime.main()
            self.assertEqual(call.call_count, 1)
            self.assertEqual(call.call_args.args[2], "create")
        # The controller itself owns partial-create recovery. A failed create
        # cannot grant this script a cleanup capability for a pre-existing box.

    def test_successful_create_retains_cleanup_after_constructor_failure(self):
        with patch("sys.argv",["rust_runtime_test.py","owned"]), \
             patch.object(runtime,"run",return_value="{}"), \
             patch.object(Path,"read_text",return_value='{"owner":"acquired-owner"}'), \
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
