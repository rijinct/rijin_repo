import unittest
from mock import patch

from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.etl.backlogs import _execute


class TestBacklog(unittest.TestCase):

    @staticmethod
    def get_output(command, **args):
        return {
            Command.GET_COUNT_FROM_HDFS: '10',
            Command.PROCESS_CHECK: "7895"
            }[command]

    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(DateTimeUtil, 'get_current_hour_date',
                  return_value='2020-05-07 19:15:32')
    def test_execute(self, mock_get_current_hour_date):
        expected = {
            'Time': '2020-05-07 19:15:32',
            'DataSource': 'LTE',
            'datFileCountMnt(/mnt/staging/import)': '10',
            'errorDatFileCountMnt(/mnt/staging/import)': '10',
            'processingDatFileCountMnt(/mnt/staging/import)': '10',
            'datFileCountNgdbUsage(/ngdb/us/import)': '10',
            'datFileCountNgdbWork(/ngdb/us/work)': '10'
        }
        output = _execute()
        self.assertTrue(expected in output)

    @patch('healthmonitoring.collectors.etl.backlogs.CommandExecutor')
    def test_main_for_empty_response(self, MockCommandExecutor):
        MockCommandExecutor.is_process_to_be_executed.return_value = False
        self.assertEqual(_execute(), [])


if __name__ == "__main__":
    unittest.main()
