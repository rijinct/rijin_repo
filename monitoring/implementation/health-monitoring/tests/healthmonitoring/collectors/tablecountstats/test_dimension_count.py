'''
Created on 11-May-2020

@author: deerakum
'''
import unittest

from healthmonitoring.collectors.tablecountstats.dimension_count import _execute  # noqa: 501
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from mock import patch
from healthmonitoring.framework.util.loader import SpecificationLoader


class TestDimensionCount(unittest.TestCase):

    @staticmethod
    def get_output(command, **args):
        return {
            Command.HIVE_SHOW_TABLES: ['es_tip_1'],
            Command.HIVE_GET_RECORD_COUNT_FROM_TABLE: '2'
        }[command]

    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(DateTimeUtil,
                  'get_current_hour_date',
                  return_value='2020-05-07 19:15:32')
    @patch.object(SpecificationLoader, '_is_config_map_time_stamp_changed',
                  return_value=False)
    @patch.object(SpecificationLoader, '_repopulate_hosts')
    def test_dimension_count(self, mock_get_current_hour_date,
                             mock_is_config_map_time_stamp_changed,
                             mock_repopulate_hosts):
        output = _execute()
        self.assertListEqual(output, [{
            'Date': '2020-05-07 19:15:32',
            'Dimension Table': 'es_tip_1',
            'Count': '2'
        }])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
