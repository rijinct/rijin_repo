import unittest
import sys
from table_counts_and_var_factor import TableCount
from dbConnectionManager import DbConnection
from dateTimeUtil import DateTimeUtil


class TestTableCountsAndVarFactor(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTableCountsAndVarFactor, self).__init__(*args, **kwargs)
        DbConnection.getHiveConnectionAndExecute = self.execute_sql
        DateTimeUtil.requiredTimeInfo = self.get_required_time_info

    def execute_sql(self, sql):
        if sql == 'show tables':
            return 'ps_app_all_subs_1_day\nps_cdm2_segg_1_1_day'
        elif 'show columns from ps_app_all_subs_1_day' in sql:
            return 'imsi_id\nmsisdn'
        elif 'show columns from ps_cdm2_segg_1_1_day' in sql:
            return 'group_id'
        elif 'select count(*) from' in sql:
            return '100'
        else:
            return '50,20'

    def get_required_time_info(self, level):
        return '2020-04-14 13-20-36', 1586802600000, 1586888999000

    def test_get_table_counts(self):
        sys.argv = ['table_counts_and_var_factor', 'day']
        table_count_obj = TableCount()
        output = table_count_obj.get_and_write_table_counts()
        self.assertEqual(output, [{
            'Date': '2020-04-14 13:20:36',
            'Table': 'ps_app_all_subs_1_day',
            'TotalCount': '50',
            'DistinctCountOfSelectedColumn': '20',
            'VarFactor': '2.5',
            'Column': 'imsi'
        }, {
            'Date': '2020-04-14 13:20:36',
            'Table': 'ps_cdm2_segg_1_1_day',
            'TotalCount': '100',
            'DistinctCountOfSelectedColumn': '-1',
            'VarFactor': '-1',
            'Column': 'NA'
        }])
