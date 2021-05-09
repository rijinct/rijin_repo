import unittest
import os
import etl_topologies_status
from dbConnectionManager import DbConnection


class TestTableCount(unittest.TestCase):
    @staticmethod
    def get_time_and_delay(path):
        if '/mnt/staging/import' in path:
            return '2020-02-14 13:40:09', '40482'
        else:
            return '2020-02-05 14:47:07', '53375'

    def execute_sql(self, sql, dbType, qsType=None):
        return '''SMS,SMS_1/SMS_1,SMS_1/SMS_1,Usage_SMS_1_LoadJob,5MIN,202\
0-01-20 01:55:00'''

    def declare_methods(self):
        DbConnection.getConnectionAndExecuteSql = self.execute_sql
        etl_topologies_status.get_last_modification_time_and_delay = \
            TestTableCount.get_time_and_delay
        os.environ["LOCAL_TZ"] = 'Asia/Kolkata'

    def test_get_topologies_info(self):
        self.declare_methods()
        topologies = etl_topologies_status.get_topologies_info()
        self.assertEqual(topologies, [[
            'SMS', 'SMS_1/SMS_1', 'SMS_1/SMS_1', 'Usage_SMS_1_LoadJob', '5MIN',
            '2020-01-20 01:55:00'
        ]])

    def test_get_toplogies_details(self):
        self.declare_methods()
        topologies = etl_topologies_status.get_topologies_info()
        details = etl_topologies_status.get_toplogies_details(topologies)
        usage_delay = etl_topologies_status.get_usage_delay(
            '2020-01-20 01:55:00')
        self.assertEqual(details, [{
            'Topology': 'SMS',
            'Partition': '5MIN',
            'InputTime': '2020-02-14 13:40:09',
            'InputDelay': '40482',
            'OutputTime': '2020-02-05 14:47:07',
            'OutputDelay': '53375',
            'UsageJob': 'Usage_SMS_1_LoadJob',
            'UsageBoundary': '2020-01-20 01:55:00',
            'UsageDelay': usage_delay
        }])
