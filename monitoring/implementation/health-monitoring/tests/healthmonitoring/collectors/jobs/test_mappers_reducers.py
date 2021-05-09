'''
Created on 17-Aug-2020

@author: deerakum
'''
import datetime
import unittest

from mock import patch

from healthmonitoring.collectors.jobs.mapper_reducers import _execute


class TestDetails(unittest.TestCase):

    TEST_DATA = [
        (datetime.datetime(2020, 8, 9, 0, 0, 0),
         'Perf_DATA_CP_SEGG_1_DAY_AggregateJob', 1584815400000, 'Default', 61,
         34, '0 days 7 hours 52 minutes 34 seconds 150 msec', 10.08, 1971489)
    ]

    @patch(
        'healthmonitoring.framework.util.loader.SpecificationLoader._is_config_map_time_stamp_changed',  # noqa: E501
        return_value=False)
    @patch('healthmonitoring.collectors.utils.queries.QueryExecutor.execute',
           return_value=TEST_DATA)
    def test_mappers_reducers(self, mock_execute,
                              mock_is_config_map_time_stamp_changed):
        output = _execute()
        expected = [{
            'Job_Name': 'Perf_DATA_CP_SEGG_1_DAY_AggregateJob',
            'Partition': 1584815400000,
            'TimeZone': 'Default',
            'Mappers': 61,
            'Reducers': 34,
            'CPU Time':
            '0 days 7 hours 52 minutes 34 seconds 150 msec',  # noqa: E501
            'Real Time': 10.08,
            'Records': 1971489
        }]
        self.assertEqual(output, expected)


if __name__ == '__main__':
    unittest.main()
