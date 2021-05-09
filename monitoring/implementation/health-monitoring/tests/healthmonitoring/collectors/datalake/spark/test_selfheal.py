'''
Created on 12-May-2020

@author: deerakum
'''
import unittest

from mock import patch

from healthmonitoring.collectors.datalake.spark.selfheal import _execute
from healthmonitoring.collectors.utils.command import CommandExecutor
from healthmonitoring.collectors.utils.file_util import FileUtils
from healthmonitoring.framework.util.loader import SpecificationLoader


class TestSparkSelfHeal(unittest.TestCase):

    @staticmethod
    def get_output(command, **args):
        if args.get('host') == "spark2":
            return "0"
        else:
            return "1"

    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(FileUtils, 'get_latest_file', return_value="filename.csv")
    @patch.object(SpecificationLoader, '_is_config_map_time_stamp_changed',
                  return_value=False)
    @patch.object(SpecificationLoader, '_repopulate_hosts')
    def test_spark_self_heal_when_all_are_down(
            self, mock_get_latest_file, mock_is_config_map_time_stamp_changed,
            mock_repopulate_hosts):
        output = _execute()
        self.assertListEqual(output, [{
            'host': 'spark1',
            'spark-type': 'spark-flexi',
            'value': "1"
        }, {
            'host': 'spark2',
            'spark-type': 'spark-flexi',
            'value': "0"
        }, {
            'host': 'spark1',
            'spark-type': 'spark-portal',
            'value': "1"
        }, {
            'host': 'spark2',
            'spark-type': 'spark-portal',
            'value': "0"
        }, {
            'host': 'spark1',
            'spark-type': 'spark-app',
            'value': "1"
        }, {
            'host': 'spark2',
            'spark-type': 'spark-app',
            'value': "0"
        }])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
