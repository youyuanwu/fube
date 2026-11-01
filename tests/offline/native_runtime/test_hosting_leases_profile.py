"""Profile routing/provenance regression, not native wire execution."""
import unittest

from tests.native_runtime import runner


class HostingLeaseProfileTests(unittest.TestCase):
    def test_new_profile_does_not_replace_original_native_selections(self):
        self.assertEqual(runner.SCENARIOS[:5],
                         ("bootstrap", "close", "abort",
                          "manifest-close", "manifest-abort"))
        self.assertEqual(runner.SCENARIOS[5], "hosting-leases")
        self.assertEqual(runner.SCENARIOS[6:], ("hosting-registration",))
        self.assertEqual(set(runner.BINARY_OUTPUTS),
                         {"fube-test-host", "stateless-test-app",
                          "manifest_acceptance"})


if __name__ == "__main__":
    unittest.main()
