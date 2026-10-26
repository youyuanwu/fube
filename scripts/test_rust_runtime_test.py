"""Offline ownership/qualification regressions; no native interoperability claim."""
import contextlib
import copy
import io
import json
import unittest
from unittest.mock import patch

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
             patch.object(runtime, "run", side_effect=RuntimeError("controller rejected existing/partial allocation")) as call:
            with self.assertRaises(RuntimeError):
                runtime.main()
            self.assertEqual(call.call_count, 1)
            self.assertEqual(call.call_args.args[2], "create")
        # The controller itself owns partial-create recovery. A failed create
        # cannot grant this script a cleanup capability for a pre-existing box.


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
