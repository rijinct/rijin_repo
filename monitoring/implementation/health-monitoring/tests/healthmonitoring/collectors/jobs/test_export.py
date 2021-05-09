'''
Created on 24-May-2020

@author: deerakum
'''
import sys
import unittest

from mock import patch, MagicMock

from healthmonitoring.collectors.jobs.export import _execute
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.file_util import FileUtils


class TestExport(unittest.TestCase):

    @staticmethod
    def get_output(command, **args):
        return {Command.GET_SIZE_OF_DIR: '40'}[command]

    @patch('healthmonitoring.collectors.utils.queries.ConnectionFactory')
    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(DateTimeUtil, 'get_current_hour_date',
                  MagicMock(return_value='2020-05-25 19:15:32'))
    @patch.object(FileUtils, 'get_files_count_in_dir',
                  MagicMock(return_value=6))
    def test_sdk_export(self, MockConnectionFactory):
        sys.argv[1] = "sdk"
        MockConnectionFactory.get_connection().fetch_records.return_value = [
            ("Exp_SMS_HVC_EXPORT_1_15MIN_ExportJob",
             "/mnt/staging/export/RTD/CEMOD/SMS/SMS_GROUP_EXPORT"),
            ("Exp_LTE_SQM_EXPORT_1_15MIN_ExportJob",
             "/mnt/sftp/sqmuser/sqmdata")
        ]
        output = _execute()
        self.assertListEqual(output, [{
            'Date': '2020-05-25 19:15:32',
            'Export Job': '/mnt/staging/export/RTD/CEMOD/SMS/SMS_GROUP_EXPORT',
            'No. of Files': 6,
            'Size': '40'
        }, {
            'Date': '2020-05-25 19:15:32',
            'Export Job': '/mnt/sftp/sqmuser/sqmdata',
            'No. of Files': 6,
            'Size': '40'
        }])

    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(DateTimeUtil, 'get_current_hour_date',
                  MagicMock(return_value='2020-05-25 19:15:32'))
    @patch.object(
        FileUtils,
        'get_files_in_dir',
        MagicMock(return_value=[
            "11720_CEI_Top_Bottom_X_CEI_for_Segments_25-05-2020_13-19-18-302.zip"  # noqa: E501
        ]))
    @patch.object(FileUtils, 'get_file_size_in_mb',
                  MagicMock(return_value=2.45))
    def test_cct_export(self):
        sys.argv[1] = "cct"
        output = _execute()
        self.assertEqual(output, [{
            'Date': '2020-05-25 19:15:32',
            'Export Job': 'CEI_Top_Bottom_X_CEI_for_Segments',
            'No. of Files': 0,
            'Size': 2.45
        }])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
