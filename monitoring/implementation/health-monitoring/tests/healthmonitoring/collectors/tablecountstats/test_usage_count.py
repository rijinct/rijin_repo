'''
Created on 25-May-2020

@author: praveenD
'''
import unittest

from mock import patch

from healthmonitoring.collectors.tablecountstats.usage_count import _execute, Collector  # noqa: E501
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.framework.util.loader import SpecificationLoader


DATE_TIME_TO_MOCK = '2020-05-24 14:59:35'


class TestUsageCount(unittest.TestCase):

    @staticmethod
    def get_output(command, **args):
        output = {Command.HIVE_USAGE_TABLE_CONTENT: '0,11\n1,4\n2,10'}
        return output[command]

    @staticmethod
    def collect():
        return ([('table1', '0,11\n1,4\n2,10'), ('table2', '0,6\n1,3\n2,14')],
                [('table3', '0,9\n1,150\n2,80'),
                 ('table4', '0,120\n1,100\n2,200')])

    @patch.object(DateTimeUtil,
                  'get_current_hour_date',
                  return_value=DATE_TIME_TO_MOCK)
    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(Collector, 'collect', new=collect)
    @patch.object(SpecificationLoader, '_is_config_map_time_stamp_changed',
                  return_value=False)
    @patch.object(SpecificationLoader, '_repopulate_hosts')
    def test_get_table_content(self, mock_get_current_hour_date,
                               mock_is_config_map_time_stamp_changed,
                               mock_repopulate_hosts):
        output = _execute()
        output_to_test = output[:3] + output[-1:]
        self.assertListEqual(output_to_test,
                             [{
                                 'Date': DATE_TIME_TO_MOCK,
                                 'Hour': 0,
                                 'table1': 11,
                                 'table2': 6,
                                 'table3': 9,
                                 'table4': 120,
                                 'sum_of_mobile_usage_row_values': 17,
                                 'sum_of_fixed_line_usage_row_values': 129
                             }, {
                                 'Date': DATE_TIME_TO_MOCK,
                                 'Hour': 1,
                                 'table1': 4,
                                 'table2': 3,
                                 'table3': 150,
                                 'table4': 100,
                                 'sum_of_mobile_usage_row_values': 7,
                                 'sum_of_fixed_line_usage_row_values': 250
                             }, {
                                 'Date': DATE_TIME_TO_MOCK,
                                 'Hour': 2,
                                 'table1': 10,
                                 'table2': 14,
                                 'table3': 80,
                                 'table4': 200,
                                 'sum_of_mobile_usage_row_values': 24,
                                 'sum_of_fixed_line_usage_row_values': 280
                             }, {
                                 'Date': DATE_TIME_TO_MOCK,
                                 'Hour': 24,
                                 'table1': 25,
                                 'table2': 23,
                                 'table3': 239,
                                 'table4': 420,
                                 'sum_of_mobile_usage_row_values': 48,
                                 'sum_of_fixed_line_usage_row_values': 659
                             }])


if __name__ == '__main__':
    unittest.main()
