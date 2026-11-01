import json
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tests.onebox import native_channel


class NativeChannelFixtureTests(unittest.TestCase):
    def test_partial_startup_uses_original_capability_for_cleanup(self):
        def create(name, on_allocated):
            on_allocated("original-owner")
            raise RuntimeError("readiness failed")

        with patch.object(native_channel.runtime, "create_owned", side_effect=create), \
                patch.object(native_channel.runtime, "destroy_owned",
                             return_value={"absence_verified": True}) as destroy:
            with self.assertRaisesRegex(RuntimeError, "readiness failed"):
                native_channel.qualify("owned-channel")
            destroy.assert_called_once_with("owned-channel", "original-owner")

    def test_failure_before_allocation_never_destroys_other_resources(self):
        with patch.object(native_channel.runtime, "create_owned",
                          side_effect=RuntimeError("collision refused")), \
                patch.object(native_channel.runtime, "destroy_owned") as destroy:
            with self.assertRaisesRegex(RuntimeError, "collision refused"):
                native_channel.qualify("collision")
            destroy.assert_not_called()

    def test_child_failure_still_verifies_owned_resource_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "build/oneboxes/owned-channel"
            state.mkdir(parents=True)
            (state / "state.json").write_text(json.dumps({"runtime_version": "11.8.121.4"}))
            for source in native_channel.SOURCES:
                path = root / source
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture source")

            def create(name, on_allocated):
                on_allocated("original-owner")

            with patch.object(native_channel, "ROOT", root), \
                    patch.object(native_channel.runtime, "create_owned", side_effect=create), \
                    patch.object(native_channel.runtime, "Allocation") as allocation, \
                    patch.object(native_channel.runtime, "run", return_value="head"), \
                    patch.object(native_channel, "run_test", side_effect=RuntimeError("native test failed")), \
                    patch.object(native_channel.runtime, "destroy_owned",
                                 return_value={"absence_verified": True}) as destroy:
                allocation.return_value.container_id = "container"
                with self.assertRaisesRegex(RuntimeError, "native test failed"):
                    native_channel.qualify("owned-channel")
                destroy.assert_called_once_with("owned-channel", "original-owner")
                self.assertFalse(list((root / "build").glob("channel-native-*")))

    def test_missing_exchange_inventory_cannot_be_a_success(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.json"
            config.write_text("{}")
            with patch.object(native_channel.subprocess, "Popen") as child:
                child.return_value.returncode = 0
                child.return_value.communicate.return_value = (
                    "test result: ok. 0 passed; 0 failed; 1 ignored;\n", None)
                with self.assertRaisesRegex(RuntimeError, "exchange inventory"):
                    native_channel.run_test(config)

    def test_interruption_reaps_child_before_original_allocation_destroy(self):
        events = []
        interrupt = KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "build/oneboxes/owned-channel"
            state.mkdir(parents=True)
            (state / "state.json").write_text(json.dumps({"runtime_version": "11.8.121.4"}))
            for source in native_channel.SOURCES:
                path = root / source
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture source")

            def create(name, on_allocated):
                on_allocated("original-owner")

            def communicate(timeout):
                if not events:
                    events.append("interrupted")
                    raise interrupt
                events.append("reaped")
                return "", None

            def destroy(name, owner):
                events.append("destroyed")
                return {"absence_verified": True}

            with patch.object(native_channel, "ROOT", root), \
                    patch.object(native_channel.runtime, "create_owned", side_effect=create), \
                    patch.object(native_channel.runtime, "Allocation") as allocation, \
                    patch.object(native_channel.runtime, "run", return_value="head"), \
                    patch.object(native_channel.subprocess, "Popen") as child, \
                    patch.object(native_channel.os, "killpg") as kill, \
                    patch.object(native_channel.runtime, "destroy_owned", side_effect=destroy):
                allocation.return_value.container_id = "container"
                child.return_value.pid = 123456
                child.return_value.communicate.side_effect = communicate
                with self.assertRaises(KeyboardInterrupt) as caught:
                    native_channel.qualify("owned-channel")
                self.assertIs(caught.exception, interrupt)
                kill.assert_called_once_with(123456, signal.SIGTERM)
                self.assertEqual(events, ["interrupted", "reaped", "destroyed"])
                self.assertFalse(list((root / "build").glob("channel-native-*")))

    def test_timeout_escalates_and_reaps_exact_owned_process_group(self):
        with patch.object(native_channel.subprocess, "Popen") as child, \
                patch.object(native_channel.os, "killpg") as kill:
            child.return_value.pid = 123456
            child.return_value.communicate.side_effect = [
                subprocess.TimeoutExpired("cargo", 300),
                subprocess.TimeoutExpired("cargo", 35),
                ("", None),
            ]
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                native_channel.run_test(Path("/unused/config"))
            self.assertEqual([call.args for call in kill.call_args_list],
                             [(123456, signal.SIGTERM), (123456, signal.SIGKILL)])
            self.assertEqual([call.kwargs for call in child.return_value.communicate.call_args_list],
                             [{"timeout": 300}, {"timeout": 35}, {"timeout": 10}])

    def test_already_exited_child_does_not_mask_original_interruption(self):
        interrupt = KeyboardInterrupt()
        with patch.object(native_channel.subprocess, "Popen") as child, \
                patch.object(native_channel.os, "killpg", side_effect=ProcessLookupError):
            child.return_value.communicate.side_effect = [interrupt, ("", None)]
            with self.assertRaises(KeyboardInterrupt) as caught:
                native_channel.run_test(Path("/unused/config"))
            self.assertIs(caught.exception, interrupt)
            self.assertEqual(child.return_value.communicate.call_count, 2)
