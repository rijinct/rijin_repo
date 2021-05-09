import unittest
import monitoring_utils


class TestMonitoringUtils(unittest.TestCase):

    def test_clamp(self):
        lower_bound, value, upper_bound = 2, 15, 10
        result = monitoring_utils.MonitoringUtils.clamp(lower_bound, value,
                                                        upper_bound)
        self.assertEqual(result, 10)
