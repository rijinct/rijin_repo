'''
Created on 16-May-2020

@author: deerakum
'''
import sys
import unittest

from mock import patch

from healthmonitoring.collectors.jobs.qs.cache import _execute


class TestQsCache(unittest.TestCase):

    @patch('healthmonitoring.collectors.jobs.qs.cache.ConnectionFactory')
    def test_dimension_cache(self, MockConnectionFactory):
        sys.argv[1] = "DIMENSION"
        MockConnectionFactory.get_connection().fetch_records.return_value = [
            ("tip_denorm_1", 18), ("subscriber_1", 20)
        ]
        output = _execute()
        self.assertListEqual(output, [{
            'TableName': 'tip_denorm_1',
            'ExpectedCount': 18,
            'ActualCount': 18
        }, {
            'TableName': 'subscriber_1',
            'ExpectedCount': 20,
            'ActualCount': 20
        }])

    @patch('healthmonitoring.collectors.jobs.qs.cache.ConnectionFactory')
    def test_agg_cache(self, MockConnectionFactory):
        sys.argv[1] = "DAY"
        MockConnectionFactory.get_connection().fetch_records.return_value = [
            ("ps_cei2_bcsi_1_day", 36), ("ps_cei2_bcsi_1_day", 36)
        ]
        output = _execute()
        self.assertListEqual(output, [{
            'TableName': 'ps_cei2_bcsi_1_day',
            'ExpectedCount': 36,
            'ActualCount': 36
        }])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
