'''
Created on 22-Jul-2020

@author: a4yadav
'''
import os
import sys
import unittest

import tools.hash_parser
import tools.tools_util as util

sys.path.append('framework/')
from framework.util import get_resource_path  # noqa E402


class TestHashParser(unittest.TestCase):
    URL = 'jdbc:postgresql://postgressdk-fip.rijin.com:5432/sai?ssl'
    YAML = 'Perf_SMS_SEGG_1_HOUR_AggregateJob.yaml'
    REPLACED_YAML = 'replaced_Perf_SMS_SEGG_1_HOUR_AggregateJob.yaml'
    APP_DEF_FILE = r'application_definitions.sh'
    ORG_QUERY = 'select * from table_name where data>#HOME_COUNTRY_CODE ' \
                'and data2=#VOLTE_MOS_TX_AVG_INT# or ' \
                'data3=#Coefficient_1_2_3# and data3<#maxCountCity and ' \
                'data4>=#US_BASE_WEIGHT_SMS_USAGE# and ' \
                'data5<=#AVG_USAGE_VOICE_CS_USAGE#'  # noqaL 501
    REP_QUERY = 'select * from table_name where data>9 and ' \
                'data2=0.2732538677688573 * ( 80.0 - VOLTE_MOS_TX_AVG ) ' \
                'or data3=7.551166324860851 and data3<-1 and data4>=(' \
                'nvl(RECEIVE_FAILURE,0)+nvl(RECEIVE_SUCCESS,0) and ' \
                'data5<=49'  # noqaL 501
    LB_UB_QUERY = 'select * from table where date>#LOWERBOUND_DATE and ' \
                  'date<=#UPPER_BOUND'  # noqaL 501
    LB_UB_OUTPUT = 'select * from table where date>2020-08-02 ' \
                   '23:00:00.000000 and date<=1596393000000'  # noqaL 501

    class MockCursor:
        def __init__(self, query):
            self._query = query
            self._postgres_entity_query = "select param_value from " \
                                          "saidata.es_home_country_lookup_1 where param_name='HOME_COUNTRY_CODE'"  # noqaL 501

        def fetchall(self):
            if self._query == tools.hash_parser.spec_formula_query:
                return [
                    ('VOLTE_SEGG_REG_ATTEMPTS_-637382389',
                     'REG_ATTEMPTS_INT',
                     'SUM(VOLTE_REG_ATTEMPTS)', 'SUM'),
                    ('VOLTE_SEGG_REG_ATTEMPTS_-549190109',
                     'REG_ATTEMPTS_INT',
                     '$formula$', 'SUM'),
                    ('SMS_SEGG_NUMBER_OF_FAILURES_1312351928',
                     'NUMBER_OF_FAILURES', 'SUM(NUMBER_OF_SMS_FAILURES)',
                     ''),
                    ('SMS_SEGG_NUMBER_OF_FAILURES_1270910228',
                     'NUMBER_OF_FAILURES_CONT',
                     'SUM(NUMBER_OF_SMS_FAILURES)',
                     '')
                    ]
            elif self._query == tools.hash_parser.kpi_formula_query:
                return [(
                    'VOLTE_MOS_TX_AVG_INT',
                    "#COEFFICIENT_60.0_80.0_20# * ( 80.0 - "
                    "VOLTE_MOS_TX_AVG )")
                    ]
            elif self._query == tools.hash_parser.usage_formula_query:
                return [('SMS_USAGE',
                         '(nvl(RECEIVE_FAILURE,0)+nvl(RECEIVE_SUCCESS,0)',
                         ''),
                        ('VOICE_CS_USAGE',
                         'nvl(SEND_FAILURE,0)+nvl(SEND_SUCCESS,0))', '49')]
            elif self._query == tools.hash_parser.entity_parameters_query:
                return [
                    ('#HOME_COUNTRY_CODE', self._postgres_entity_query, '',
                     'POSTGRES'),
                    (
                        '#maxCountCity',
                        "select max(city_id) from ES_CITY_1 where "
                        "city_id not in (-1,-99,-10)",
                        # noqaL 501
                        '-1',
                        'HIVE')
                    ]
            elif self._query == self._postgres_entity_query:
                return ['9']

    class MockConnection:
        def __init__(self):
            pass

        @staticmethod
        def close():
            pass

    @staticmethod
    def get_cursor(query):
        return TestHashParser.MockCursor(
                query), TestHashParser.MockConnection

    @staticmethod
    def _write_yaml(file_name, data):
        pass

    def set_up(self):
        tools.hash_parser.get_cursor = TestHashParser.get_cursor
        tools.hash_parser._write_yaml = TestHashParser._write_yaml

    def test_hash_parse(self):
        self.set_up()
        path = r'{}'.format(get_resource_path(TestHashParser.YAML, True))
        yaml_file = util.read_yaml(path)
        expected = {
            'common': {
                'description': 'Usage: sms_segg_1_hour.py',
                'number_of_output_files': 2
                }, 'sql': {
                'query': 'select * from table_name where data>9 and '
                         'data2=0.2732538677688573 * ( 80.0 - '
                         'VOLTE_MOS_TX_AVG ) or data3=7.551166324860851 '
                         'and data3<-1 and data4>=(nvl(RECEIVE_FAILURE,'
                         '0)+nvl(RECEIVE_SUCCESS,0) and data5<=49'
                },
            'target_path':
                'hdfs://projectcluster/ngdb/ps/SMS_1/SMS_SEGG_1_HOUR',
            'partition': {
                'columns': ['dt', 'tz'],
                'columns_with_values': ['dt=1598121000000', 'tz=default']
                }
            }
        self.assertEqual(expected,
                         tools.hash_parser.hash_replace_query(yaml_file,
                                                              1596389400000,
                                                              1596393000000,
                                                              "Perf_SMS_DAY"))

    def test_get_sdk_db_url(self):
        os.environ['IS_K8S'], os.environ[
            'PROJECT_SDK_DB_URL'] = '1', TestHashParser.URL
        self.assertEqual(TestHashParser.URL,
                         tools.hash_parser.get_sdk_db_url())
        os.environ.pop('IS_K8S')
        os.environ.pop('PROJECT_SDK_DB_URL')

    def test_get_sdk_db_url_from_file(self):
        tools.hash_parser.APPLICATION_DEF_PATH = r'{}'.format(
                get_resource_path(TestHashParser.APP_DEF_FILE, True))
        self.assertEqual(TestHashParser.URL,
                         tools.hash_parser.get_sdk_db_url())

    def test_get_sdk_db_param(self):
        self.assertEqual(
                {
                    'PORT': '5432',
                    'HOST': 'postgressdk-fip.rijin.com',
                    'DATABASE': 'sai'
                    },
                tools.hash_parser.get_sdk_db_param(TestHashParser.URL))

    def test_replace_hash(self):
        file_name = TestHashParser.YAML.replace('.yaml', '')
        replaced_query = tools.hash_parser.replace_hash(
                TestHashParser.REP_QUERY, file_name)
        self.assertEqual(TestHashParser.REP_QUERY, replaced_query)

    def test_replace_lb_ub(self):
        replaced_query = tools.hash_parser.replace_lb_ub(
                TestHashParser.LB_UB_QUERY, 1596389400000, 1596393000000)
        self.assertEqual(TestHashParser.LB_UB_OUTPUT, replaced_query)


if __name__ == "__main__":
    unittest.main()
