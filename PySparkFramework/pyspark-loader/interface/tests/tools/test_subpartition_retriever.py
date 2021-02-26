#  """
#  @author: pranchak
#  """

import unittest
import sys
import sqlparse
sys.path.extend(['tools/', 'framework'])
import framework.util # noqa E402
from tools.subpartition_retriever import SubpartitionRetriever # noqa E402


class TestSubpartitionRetriever(unittest.TestCase):
    def get_hive_query(self, sql_file_name):
        with open(sql_file_name, 'r') as file:
            hive_query = file.read()
            hive_query = hive_query.replace('\n', ' ')
            return sqlparse.parse(hive_query)[0]

    def test_get_sub_partition_multiple_partition(self):
        pr = SubpartitionRetriever()
        sqlpath = framework.util.get_resource_path(
            'PC_Query_PS_SMS_DP_ROLLUP_CITY_1_DAY.sql', True)
        query = self.get_hive_query(sqlpath)
        self.assertEqual(['dt=#LOWER_BOUND #TIMEZONE_PARTITION', 'region'],
                         pr.get_subpartition(query))


if __name__ == '__main__':
    unittest.main()
