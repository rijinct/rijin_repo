'''
Created on 04-Jun-2020

@author: praveenD
'''
import unittest
from datetime import datetime
from mock import patch

from healthmonitoring.collectors.summaryreport.summary_report import _FileOperations, _Boundary, _JobCount, _JobStatus  # noqa: 501
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.file_util import FileUtils


class TestSummaryReport(unittest.TestCase):
    TODAY_DATE_TIME = '2020-06-02 12:00:00'

    @staticmethod
    def read_file(file):
        return """time_frame,jobid,maxvalue
 23Hour==0Hour , Perf_CEI2_TT_1_HOUR_AggregateJob               , 3000-01-01 00:00:00
 23Hour==0Hour , Perf_RADIO_CELL_1_HOUR_AggregateJob            , 2020-06-02 23:00:00
time_frame,jobid,maxvalue
 12Hour==13Hour , Perf_CEI2_TT_1_HOUR_AggregateJob               , 3000-01-01 00:00:00
 12Hour==13Hour , Perf_ROAMER_VLR_1_HOUR_AggregateJob            , 2020-06-02 12:00:00
 12Hour==13Hour , Perf_CEI2_MGW_1_HOUR_AggregateJob              , 2020-06-02 12:00:00
 12Hour==13Hour , Exp_ISA_PS_BB_APP_SEGG_1_HOUR_ExportJob        , 2020-05-26 01:45:00
 12Hour==13Hour , Exp_ISA_PS_BB_STR_SEGG_1_HOUR_ExportJob        , 2020-05-11 19:45:00
 12Hour==13Hour , Usage_TT_1_LoadJob            , 2020-06-02 23:00:00
"""  # noqa :501

    @staticmethod
    def readlines(file):
        return [
            'hour,hour_agg_end_time,total_jobs_executed,zero_duration,success,error,running,Last Job Name',  # noqa :501
            '0,2020-06-02 01:01:43,114,23,112,0,0,Perf_BB_APP_SUBS_1_HOUR_AggregateJob',  # noqa :501
            '1,2020-06-02 06:38:37,116,19,114,1,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
            '5,2020-06-02 05:35:39,116,21,114,0,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
            '6,2020-06-02 06:33:50,116,18,114,0,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
            '7,2020-06-02 12:36:03,116,18,114,1,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob'  # noqa :501
        ]

    @staticmethod
    def get_data_for_test(file):
        return """1,Exp_CEI2_CCI_EXPORT_1_DAY_ExportJob,2020-04-17 07:39:33,2020-04-17 07:45:08,5,S
1,Exp_HIVETOHBASELOADER_1_1_DAY_ExportJob,2020-04-17 05:27:36,2020-04-17 06:05:33,37,S
day,job_name,start_time,end_time,executionduration,status
2,Perf_CDM2_SEGG_1_1_DAY_AggregateJob,2020-06-03 03:30:25,2020-06-03 04:13:53,,S
2,Perf_CEI2_O_INDEX_CITY_1_DAY_AggregateJob,2020-06-03 03:30:25,2020-06-03 03:45:48,15,S
2,Perf_CEI2_O_INDEX_2_1_DAY_AggregateJob,2020-06-03 01:52:07,2020-06-03 03:30:25,98,S
2,Perf_BB_WS_CITY_FAIL_1_DAY_AggregateJob,2020-06-03 02:10:08,2020-06-03 02:40:04,59,S
2,Perf_CEI2_FL_CEI_O_INDEX_2_1_DAY_AggregateJob,2020-06-03 02:29:42,2020-06-03 02:34:29,104,S"""  # noqa :501

    @patch.object(DateTimeUtil,
                  'now',
                  return_value=datetime.strptime(TODAY_DATE_TIME,
                                                 '%Y-%m-%d %H:%M:%S'))
    @patch.object(FileUtils, 'read_file', new=read_file)
    @patch.object(_FileOperations,
                  'check_file_exists_and_not_empty',
                  return_value=True)
    def test_hour_boundary(self, arg1, arg2):
        output = _Boundary.get_hour_boundary()
        self.assertListEqual(
            output,
            [
                'time_frame,jobid,maxvalue',
                '12Hour==13Hour,Perf_CEI2_TT_1_HOUR_AggregateJob,3000-01-01 00:00:00',  # noqa : 501
                '12Hour==13Hour,Perf_ROAMER_VLR_1_HOUR_AggregateJob,2020-06-02 12:00:00',  # noqa : 501
                '12Hour==13Hour,Perf_CEI2_MGW_1_HOUR_AggregateJob,2020-06-02 12:00:00',  # noqa : 501
                '12Hour==13Hour,Exp_ISA_PS_BB_APP_SEGG_1_HOUR_ExportJob,2020-05-26 01:45:00Red',  # noqa : 501
                '12Hour==13Hour,Exp_ISA_PS_BB_STR_SEGG_1_HOUR_ExportJob,2020-05-11 19:45:00Red',  # noqa : 501
                '12Hour==13Hour,Usage_TT_1_LoadJob,2020-06-02 23:00:00Red'  # noqa : 501
            ])

    @patch.object(FileUtils, 'readlines', new=readlines)
    @patch.object(_FileOperations,
                  'check_file_exists_and_not_empty',
                  return_value=True)
    def test_hour_job_count(self, arg):
        output = _JobCount.get_hour_job_count()
        self.assertListEqual(
            output,
            [
                'hour,hour_agg_end_time,total_jobs_executed,zero_duration,success,error,running,Last Job Name',  # noqa :501
                '0,2020-06-02 01:01:43,114,23,112,0,0,Perf_BB_APP_SUBS_1_HOUR_AggregateJob',  # noqa :501
                '1,2020-06-02 06:38:37,116,19,114,1Red,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
                '5,2020-06-02 05:35:39,116,21,114,0,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
                '6,2020-06-02 06:33:50,116,18,114,0,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob',  # noqa :501
                '7,2020-06-02 12:36:03,116,18,114,1Red,0,Perf_CDM2_SEGG_1_HOUR_AggregateJob'  # noqa :501
            ])

    @patch.object(FileUtils, 'read_file', new=get_data_for_test)
    @patch.object(_FileOperations,
                  'check_file_exists_and_not_empty',
                  return_value=True)
    def test_day_job_status(self, arg):
        output = _JobStatus.get_day_job_status()
        self.assertListEqual(
            output,
            [
                'day,job_name,start_time,end_time,executionduration,status',
                '2,Perf_CDM2_SEGG_1_1_DAY_AggregateJob,2020-06-03 03:30:25,2020-06-03 04:13:53,Red,S',  # noqa :501
                '2,Perf_CEI2_O_INDEX_CITY_1_DAY_AggregateJob,2020-06-03 03:30:25,2020-06-03 03:45:48,15,S',  # noqa :501
                '2,Perf_CEI2_O_INDEX_2_1_DAY_AggregateJob,2020-06-03 01:52:07,2020-06-03 03:30:25,98Red,S',  # noqa :501
                '2,Perf_BB_WS_CITY_FAIL_1_DAY_AggregateJob,2020-06-03 02:10:08,2020-06-03 02:40:04,59,S',  # noqa :501
                '2,Perf_CEI2_FL_CEI_O_INDEX_2_1_DAY_AggregateJob,2020-06-03 02:29:42,2020-06-03 02:34:29,104Red,S'  # noqa :501
            ])


if __name__ == '__main__':
    unittest.main()
