'''
Created on 20-May-2020

@author: praveenD
'''
import datetime
import sys
import unittest

from mock import patch

from healthmonitoring.collectors.aggregations.details import _execute, Processor  # noqa: 501
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil


class TestDetails(unittest.TestCase):
    @patch('healthmonitoring.collectors.utils.queries.ConnectionFactory')
    def test_get_hour_aggregations(self, MockConnectionFactory):
        sys.argv = ['', 'HOUR']
        MockConnectionFactory.get_connection().fetch_records.return_value = [
            ('7Hour===8Hour', 'Perf_CEI2_INDEX_1_1_HOUR_AggregateJob',
             datetime.datetime(2020, 5, 20, 8, 24,
                               27), datetime.datetime(2020, 5, 20, 8, 32,
                                                      45), 8.0, 'S')
        ]
        output = _execute()
        self.assertListEqual(output, [{
            "Time_Frame": '7Hour===8Hour',
            "Job_Name": 'Perf_CEI2_INDEX_1_1_HOUR_AggregateJob',
            "Start_Time": '2020-05-20 08:24:27',
            "End_Time": '2020-05-20 08:32:45',
            "Execution_Duration": 8.0,
            "Status": 'S'
        }])

    @patch.object(DateTimeUtil,
                  'now',
                  return_value=DateTimeUtil.parse_date(
                      "2020-08-06 18:00:00",
                      DateTimeUtil.POSTGRES_DATE_FORMAT))
    def test_processor(self, mock_now):
        raw_data = [
            'DAY',
            [(5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:22:47', 'None', None, 'R'),
             (5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:22:47', '2020-08-06 16:25:47', 3.0, 'R'),
             (5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:30:47', 'None', None, 'R')]
        ]

        expected = [
            'DAY',
            [(5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:22:47', 'None', 97.22, 'R'),
             (5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:22:47', '2020-08-06 16:25:47', 3.0, 'R'),
             (5, 'Exp_CIMA_MOBILE_APP_GROUP_1_DAY_ExportJob',
              '2020-08-06 16:30:47', 'None', 89.22, 'R')]
        ]
        self.assertEqual(Processor(raw_data).process(), expected)

    @patch('healthmonitoring.collectors.utils.queries.ConnectionFactory')
    def test_get_job_count_summary(self, MockConnectionFactory):
        sys.argv = ['', 'HOURSUMMARY']
        MockConnectionFactory.get_connection().fetch_records.return_value = [
            ('1', datetime.datetime(2020, 5, 21, 10, 24,
                                    27), 116, 46, 70, 0, 0)
        ]
        output = _execute()
        self.assertListEqual(output, [{
            'HOUR': '1',
            'HOUR_agg_end_time': '2020-05-21 10:24:27',
            "Total_Jobs_Executed": 116,
            "Zero_Duration": 46,
            "Success": 70,
            "Error": 0,
            "Running": 0
        }])


if __name__ == "__main__":
    unittest.main()
