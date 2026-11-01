import json
from pathlib import Path
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
